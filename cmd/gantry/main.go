// Command gantry runs the Gantry P2P agent.
//
// Subcommands:
//
//	gantry version      print build information and exit
//	gantry agent        run the full agent (mirror + transfer + libp2p + ...)
//
// Phase 1: `agent` wires the cache, origin client, mirror endpoint, and
// metrics endpoint. Peer/DHT subsystems land in Phase 2+.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gantry/gantry/internal/cache"
	"github.com/gantry/gantry/internal/cdsub"
	"github.com/gantry/gantry/internal/coldstart"
	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/coord"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/discovery"
	gantrylog "github.com/gantry/gantry/internal/gantrylog"
	"github.com/gantry/gantry/internal/hrw"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/ifaces/fakes"
	"github.com/gantry/gantry/internal/inflight"
	"github.com/gantry/gantry/internal/members"
	"github.com/gantry/gantry/internal/metrics"
	"github.com/gantry/gantry/internal/mirror"
	"github.com/gantry/gantry/internal/origin"
	"github.com/gantry/gantry/internal/transfer"
)

// version is overridden via -ldflags at release time.
var version = "0.0.0-dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(2)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return errors.New("gantry: subcommand required")
	}

	switch args[0] {
	case "version", "-version", "--version":
		fmt.Printf("gantry %s %s/%s (go %s)\n",
			version, runtime.GOOS, runtime.GOARCH, runtime.Version())
		return nil
	case "agent":
		return runAgent(args[1:])
	case "help", "-h", "-help", "--help":
		return runHelp(args[1:])
	default:
		printUsage(os.Stderr)
		return fmt.Errorf("gantry: unknown subcommand %q", args[0])
	}
}

func printUsage(w *os.File) {
	_, _ = fmt.Fprintln(w, `Usage: gantry <subcommand> [flags]

Subcommands:
  agent      run the Gantry P2P agent
  version    print build information
  help       print help for the agent subcommand`)
}

func runHelp(args []string) error {
	if len(args) == 0 || args[0] == "agent" {
		fs, _ := buildAgentFlagSet(config.NewDefault())
		fs.SetOutput(os.Stdout)
		_, _ = fmt.Fprintln(os.Stdout, "Usage: gantry agent [flags]")
		_, _ = fmt.Fprintln(os.Stdout)
		_, _ = fmt.Fprintln(os.Stdout, "Flags:")
		fs.PrintDefaults()
		return nil
	}
	return fmt.Errorf("gantry help: unknown topic %q", args[0])
}

// buildAgentFlagSet constructs the `gantry agent` flag set bound to c. The
// returned *string is the --config flag's value (read before re-parsing
// with the file-derived defaults). Exposed for runHelp.
func buildAgentFlagSet(c *config.Config) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to YAML config file")
	c.BindFlags(fs)
	return fs, configPath
}

func runAgent(args []string) error {
	c, err := loadAgentConfig(args)
	if err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := gantrylog.New(os.Stderr, c.LogLevel, c.LogFormat)
	slog.SetDefault(logger)
	logger.Info("gantry starting",
		slog.String("version", version),
		slog.String("go", runtime.Version()),
		slog.String("os", runtime.GOOS),
		slog.String("arch", runtime.GOARCH),
		slog.Any("config", c.Redacted()),
	)

	// Metrics registry + Phase 1+2 instruments.
	reg := metrics.New()
	reg.RegisterDefaultCollectors()
	inst := newPhase1Metrics(reg)
	p2 := newPhase2Metrics(reg)

	// Cache.
	cstore, err := cache.Open(c.CacheDir, c.CacheBudgetBytes,
		cache.WithLogger(logger),
		cache.WithMetrics(
			func() { inst.cacheHit.Inc() },
			func() { inst.cacheMiss.Inc() },
			func(int64) { /* eviction count owned by Phase 6 */ },
		),
	)
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}
	defer func() { _ = cstore.Close() }()

	// Origin.
	originClient, err := origin.New(c,
		origin.WithLogger(logger),
		origin.WithMetrics(
			func(_ string, _ int64) {}, // start: no instrument yet
			func(kind string, _ int64) { inst.originPullSuccess.WithLabelValues(kind).Inc() },
			func(kind, class string) { inst.originPullFailure.WithLabelValues(kind, class).Inc() },
		),
	)
	if err != nil {
		return fmt.Errorf("origin: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Phase 2 — libp2p Host + DHT.
	disco, err := discovery.New(ctx, discovery.FromConfig(c))
	if err != nil {
		return fmt.Errorf("discovery: %w", err)
	}
	defer func() { _ = disco.Close() }()
	logger.Info("libp2p host ready", slog.String("peer_id", disco.PeerID().String()))

	// Phase 2 — peer dialer + transfer endpoint.
	peerClient := transfer.NewClient()
	transferSrv := transfer.New(cstore,
		transfer.WithLogger(logger),
		transfer.WithMetrics(
			func() { p2.peerServe.Inc() },
			func() { p2.peerMiss.Inc() },
		),
	)
	transferStop, err := transferSrv.ListenAndServe(c.TransferListen)
	if err != nil {
		return fmt.Errorf("transfer listen: %w", err)
	}
	logger.Info("transfer endpoint listening", slog.String("addr", c.TransferListen))

	// Phase 3 — membership view + cold-start orchestrator. Members
	// requires Kubernetes credentials (in-cluster or explicit
	// kubeconfig); when neither is available we fall back to a
	// single-self membership view that disables cold-start so the
	// mirror keeps Phase 1 behaviour for local development.
	memberView, membersStop := buildMembers(ctx, c, disco, logger)
	defer membersStop()

	// Phase 3 — in-flight map + coord client + coord server + metrics.
	inflightMap := inflight.New(inflight.DefaultStalls(), nil)
	p3 := newPhase3Metrics(reg, inflightMap)

	coordClient := coord.NewClient(disco.LibP2P(),
		coord.WithClientLogger(logger),
	)
	// pullerPump bridges inbound please_pull RPCs to the local origin
	// puller (§5.2 step 7). The pump itself MUST NOT block the coord
	// stream handler; the actual origin fetch + cache write + dht
	// Provide all happen in a detached goroutine.
	pullerPump := newPullerPump(inflightMap, originClient, cstore, disco, logger)
	coordServer := coord.NewServer(cstore, memberView, inflightMap,
		coord.WithLogger(logger),
		coord.WithMetrics(coord.MetricsHooks{
			OnPullIntentServed:  func() { p3.coordPullIntentServed.Inc() },
			OnPleasePullServed:  func() { p3.coordPleasePullServed.Inc() },
			OnPleasePullStarted: func() { p3.coordPleasePullStarted.Inc() },
			OnStreamError:       func() { p3.coordStreamError.Inc() },
		}),
		coord.WithPullerPump(pullerPump),
	)
	coordServer.Bind(disco.LibP2P())

	// Phase 3 cold-start orchestrator. Disabled when memberView is the
	// single-self stub: in dev/test mode every cache miss with empty
	// DHT must still reach origin via the Phase 1 path.
	var coldStartResolver mirror.ColdStartResolver
	if hasMultiNodeMembership(memberView) {
		selfZone := lookupSelfZone(memberView)
		realResolver := coldstart.New(coldstart.Options{
			Members:   memberView,
			Discovery: disco,
			Coord:     coordClient,
			Inflight:  inflightMap,
			Logger:    logger,
			HrwK:      c.HRWK,
			HrwScope:  hrw.ParseScope(c.HRWTopologyScope),
			SelfZone:  selfZone,
			Metrics: coldstart.MetricsHooks{
				OnRankMismatch: func(kindLabel string, _ ifaces.NodeID) {
					p3.hrwRankMismatch.WithLabelValues(kindLabel).Inc()
				},
				OnDhtFalseEmpty: func() { p3.dhtFalseEmpty.Inc() },
				OnTopKProbeHit:  func() { p3.topkProbeHit.Inc() },
				OnColdStartDuration: func(kindLabel, outcome string, d time.Duration) {
					p3.coldStartDuration.WithLabelValues(kindLabel, outcome).Observe(d.Seconds())
				},
			},
		})
		coldStartResolver = coldStartAdapter{r: realResolver}
		logger.Info("cold-start orchestrator wired",
			slog.Int("hrw_k", c.HRWK),
			slog.String("hrw_scope", c.HRWTopologyScope),
		)
	} else {
		logger.Info("cold-start orchestrator disabled (single-node membership)")
	}

	// Mirror with Phase 2 peer fallback.
	mirrorSrv := mirror.New(c, cstore, originClient,
		mirror.WithLogger(logger),
		mirror.WithMetrics(
			func() {}, // cache hit already counted by cache hook
			func() {}, // cache miss already counted by cache hook
			func(kind string) { inst.originPullTotal.WithLabelValues(kind).Inc() },
			func(class string) { inst.originFailureTotal.WithLabelValues(class).Inc() },
		),
		mirror.WithDiscovery(disco, peerClient),
		mirror.WithPeerMetrics(
			func(outcome string) { p2.peerFetch.WithLabelValues(outcome).Inc() },
			func(success bool) {
				if success {
					p2.peerDialSuccess.Inc()
				} else {
					p2.peerDialFailure.Inc()
				}
			},
		),
		mirror.WithDhtLookupMetric(func(outcome string, dur time.Duration) {
			p2.dhtLookup.WithLabelValues(outcome).Inc()
			p2.dhtLookupDur.WithLabelValues(outcome).Observe(dur.Seconds())
		}),
		mirror.WithColdStart(coldStartResolver),
	)

	mirrorStop, err := mirrorSrv.ListenAndServe(c.MirrorListen)
	if err != nil {
		return fmt.Errorf("mirror listen: %w", err)
	}
	logger.Info("mirror endpoint listening", slog.String("addr", c.MirrorListen))

	// Phase 2 — cdsub announce loop. NoOpSource is used when no real
	// containerd is available; the loop still exercises List/Subscribe
	// and is harmless. A real containerd-event source is a Phase 2
	// follow-up bound under a Linux build tag.
	cdSub := cdsub.New(cdsub.NoOpSource{}, disco,
		cdsub.WithLogger(logger),
		cdsub.WithMetrics(
			func() { p2.dhtProvide.Inc() },
			func() { p2.dhtProvideErr.Inc() },
			func(int) { p2.dhtReconcile.Inc() },
			func() { p2.cdsubReconnect.Inc() },
		),
	)
	cdsubDone := make(chan error, 1)
	go func() { cdsubDone <- cdSub.Run(ctx) }()

	// Phase 2 — re-announce every digest currently in the local cache so
	// peers can discover content held over from a previous boot. Runs in
	// the background; failures are logged but never fatal.
	go announceCachedDigests(ctx, cstore.Digests(), disco, logger, p2)

	metricsHTTP := &http.Server{
		Addr:              c.MetricsListen,
		Handler:           reg.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsErr := make(chan error, 1)
	go func() {
		err := metricsHTTP.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			metricsErr <- err
		}
		close(metricsErr)
	}()
	logger.Info("metrics endpoint listening", slog.String("addr", c.MetricsListen))

	// Block until signal or metrics-server crash.
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-metricsErr:
		logger.Error("metrics endpoint died", slog.Any("err", err))
	case err := <-cdsubDone:
		logger.Error("cdsub loop exited unexpectedly", slog.Any("err", err))
	}

	// Graceful shutdown with a 10s budget. Stop accepting new requests
	// first (mirror + transfer), then close libp2p.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := mirrorStop(shutdownCtx); err != nil {
		logger.Warn("mirror shutdown error", slog.Any("err", err))
	}
	if err := transferStop(shutdownCtx); err != nil {
		logger.Warn("transfer shutdown error", slog.Any("err", err))
	}
	if err := metricsHTTP.Shutdown(shutdownCtx); err != nil {
		logger.Warn("metrics shutdown error", slog.Any("err", err))
	}
	logger.Info("gantry stopped")
	return nil
}

// loadAgentConfig merges YAML, env, and flags into a *config.Config. Two-
// pass parsing: first pass reads --config; second pass overlays flags onto
// (defaults < YAML < env).
func loadAgentConfig(args []string) (*config.Config, error) {
	c := config.NewDefault()
	fs, configPath := buildAgentFlagSet(c)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if *configPath != "" {
		c2, _, err := config.Load(args, os.Getenv, *configPath)
		if err != nil {
			return nil, err
		}
		return c2, nil
	}
	if err := c.LoadEnv(os.Getenv); err != nil {
		return nil, err
	}
	fs, _ = buildAgentFlagSet(c)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return c, nil
}

// phase1Metrics groups the §7.6 metric subset that Phase 1 emits.
type phase1Metrics struct {
	cacheHit           prometheus.Counter
	cacheMiss          prometheus.Counter
	originPullTotal    *prometheus.CounterVec
	originPullSuccess  *prometheus.CounterVec
	originPullFailure  *prometheus.CounterVec
	originFailureTotal *prometheus.CounterVec
}

func newPhase1Metrics(reg *metrics.Registry) *phase1Metrics {
	return &phase1Metrics{
		cacheHit: reg.NewCounter("cache", prometheus.CounterOpts{
			Name: "p2p_cache_hit_total",
			Help: "Cache hits served from the local content store.",
		}),
		cacheMiss: reg.NewCounter("cache", prometheus.CounterOpts{
			Name: "p2p_cache_miss_total",
			Help: "Cache misses that fell through to origin.",
		}),
		originPullTotal: reg.NewCounterVec("origin", prometheus.CounterOpts{
			Name: "p2p_origin_pull_total",
			Help: "Origin pulls started, labeled by OCI URL kind.",
		}, []string{"kind"}),
		originPullSuccess: reg.NewCounterVec("origin", prometheus.CounterOpts{
			Name: "p2p_origin_pull_success_total",
			Help: "Origin pulls that streamed to completion.",
		}, []string{"kind"}),
		originPullFailure: reg.NewCounterVec("origin", prometheus.CounterOpts{
			Name: "p2p_origin_pull_failure_total",
			Help: "Origin pulls that terminated with an *OriginError.",
		}, []string{"kind", "class"}),
		originFailureTotal: reg.NewCounterVec("mirror", prometheus.CounterOpts{
			Name: "p2p_origin_failure_total",
			Help: "Origin failures observed by the mirror, by §5.8 class.",
		}, []string{"class"}),
	}
}

// phase2Metrics groups Phase 2 metrics for peer fallback, DHT advertise,
// and transfer endpoint (§7.6).
type phase2Metrics struct {
	peerServe       prometheus.Counter
	peerMiss        prometheus.Counter
	peerFetch       *prometheus.CounterVec
	peerDialSuccess prometheus.Counter
	peerDialFailure prometheus.Counter
	dhtProvide      prometheus.Counter
	dhtProvideErr   prometheus.Counter
	dhtReconcile    prometheus.Counter
	dhtLookup       *prometheus.CounterVec
	dhtLookupDur    *prometheus.HistogramVec
	dhtAdvertise    prometheus.Counter
	cdsubReconnect  prometheus.Counter
}

func newPhase2Metrics(reg *metrics.Registry) *phase2Metrics {
	return &phase2Metrics{
		peerServe: reg.NewCounter("transfer", prometheus.CounterOpts{
			Name: "p2p_peer_serve_total",
			Help: "Peer-fetch endpoint requests served from the local cache.",
		}),
		peerMiss: reg.NewCounter("transfer", prometheus.CounterOpts{
			Name: "p2p_peer_miss_total",
			Help: "Peer-fetch endpoint requests that 404'd locally.",
		}),
		peerFetch: reg.NewCounterVec("mirror", prometheus.CounterOpts{
			Name: "p2p_peer_fetch_total",
			Help: "Peer fetches initiated by the mirror miss path.",
		}, []string{"outcome"}),
		peerDialSuccess: reg.NewCounter("mirror", prometheus.CounterOpts{
			Name: "p2p_peer_dial_success_total",
			Help: "Successful peer dials from the mirror miss path.",
		}),
		peerDialFailure: reg.NewCounter("mirror", prometheus.CounterOpts{
			Name: "p2p_peer_dial_failure_total",
			Help: "Failed peer dials from the mirror miss path.",
		}),
		dhtProvide: reg.NewCounter("discovery", prometheus.CounterOpts{
			Name: "p2p_dht_provide_total",
			Help: "DHT Provide calls that succeeded.",
		}),
		dhtProvideErr: reg.NewCounter("discovery", prometheus.CounterOpts{
			Name: "p2p_dht_provide_error_total",
			Help: "DHT Provide calls that errored.",
		}),
		dhtReconcile: reg.NewCounter("discovery", prometheus.CounterOpts{
			Name: "p2p_dht_reconcile_total",
			Help: "cdsub reconciliation cycles completed.",
		}),
		dhtLookup: reg.NewCounterVec("discovery", prometheus.CounterOpts{
			Name: "p2p_dht_lookup_total",
			Help: "DHT FindProviders calls, labeled by outcome.",
		}, []string{"outcome"}),
		dhtLookupDur: reg.NewHistogramVec("discovery", prometheus.HistogramOpts{
			Name:    "p2p_dht_lookup_duration_seconds",
			Help:    "DHT FindProviders call latency in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		}, []string{"outcome"}),
		dhtAdvertise: reg.NewCounter("discovery", prometheus.CounterOpts{
			Name: "p2p_dht_advertise_total",
			Help: "Cached digests re-announced via dht.Provide at startup.",
		}),
		cdsubReconnect: reg.NewCounter("discovery", prometheus.CounterOpts{
			Name: "p2p_cdsub_reconnect_total",
			Help: "cdsub subscriber reconnect attempts.",
		}),
	}
}

// announceCachedDigests issues a dht.Provide for every digest currently
// held in the local cache. The plan calls for this at startup so peers
// who join an existing cluster can discover previously-cached content
// before any new image-event activity. Runs to completion or until ctx
// fires; per-digest failures are logged at DEBUG only.
func announceCachedDigests(ctx context.Context, ds []digest.Digest, dht *discovery.Host, logger *slog.Logger, p2 *phase2Metrics) {
	if len(ds) == 0 {
		return
	}
	logger.Info("re-announcing cached digests", slog.Int("count", len(ds)))
	announced := 0
	for _, d := range ds {
		if ctx.Err() != nil {
			break
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := dht.Provide(pctx, d)
		cancel()
		if err != nil {
			logger.Debug("re-announce failed",
				slog.String("digest", d.String()),
				slog.Any("err", err),
			)
			continue
		}
		announced++
		if p2 != nil {
			p2.dhtAdvertise.Inc()
		}
	}
	logger.Info("re-announce complete",
		slog.Int("announced", announced),
		slog.Int("total", len(ds)),
	)
}

// phase3Metrics groups the §7.6 instruments owned by Phase 3:
// HRW-rank-mismatch detection, DHT-false-empty observability, top-K
// probe hit rate, in-flight pull gauge, cold-start latency, and coord
// stream counters.
type phase3Metrics struct {
	hrwRankMismatch        *prometheus.CounterVec
	dhtFalseEmpty          prometheus.Counter
	topkProbeHit           prometheus.Counter
	coldStartDuration      *prometheus.HistogramVec
	coordPullIntentServed  prometheus.Counter
	coordPleasePullServed  prometheus.Counter
	coordPleasePullStarted prometheus.Counter
	coordStreamError       prometheus.Counter
}

func newPhase3Metrics(reg *metrics.Registry, infl *inflight.Map) *phase3Metrics {
	// in_flight_pulls is a GaugeFunc that polls inflightMap.Len() on
	// every scrape — no separate counter update path needed.
	_ = reg.NewGaugeFunc("coord", prometheus.GaugeOpts{
		Name: "p2p_in_flight_pulls",
		Help: "Current count of in-flight digest pulls on this node.",
	}, func() float64 { return float64(infl.Len()) })

	return &phase3Metrics{
		hrwRankMismatch: reg.NewCounterVec("coord", prometheus.CounterOpts{
			Name: "p2p_hrw_rank_mismatch_total",
			Help: "pull_intent_query responses where the responder's reported HRW rank disagrees with the requester's view (informer divergence, §5.3).",
		}, []string{"digest_kind"}),
		dhtFalseEmpty: reg.NewCounter("coord", prometheus.CounterOpts{
			Name: "p2p_dht_false_empty_total",
			Help: "Cases where DHT FindProviders returned 0 but a peer's pull_intent_query reported has_cached=true (DHT degradation indicator, §5.2).",
		}),
		topkProbeHit: reg.NewCounter("coord", prometheus.CounterOpts{
			Name: "p2p_topk_probe_hit_total",
			Help: "Cold-start cascade resolutions before reaching rule 7 (i.e., the top-K probe avoided an origin pull).",
		}),
		coldStartDuration: reg.NewHistogramVec("coord", prometheus.HistogramOpts{
			Name:    "p2p_cold_start_duration_seconds",
			Help:    "Wall-clock time spent in the cold-start orchestrator per Resolve call.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
		}, []string{"digest_kind", "outcome"}),
		coordPullIntentServed: reg.NewCounter("coord", prometheus.CounterOpts{
			Name: "p2p_coord_pull_intent_served_total",
			Help: "pull_intent_query RPCs answered by this node's coord server.",
		}),
		coordPleasePullServed: reg.NewCounter("coord", prometheus.CounterOpts{
			Name: "p2p_coord_please_pull_served_total",
			Help: "please_pull RPCs answered by this node's coord server.",
		}),
		coordPleasePullStarted: reg.NewCounter("coord", prometheus.CounterOpts{
			Name: "p2p_coord_please_pull_started_total",
			Help: "Digests transitioned to in_flight via please_pull on this node.",
		}),
		coordStreamError: reg.NewCounter("coord", prometheus.CounterOpts{
			Name: "p2p_coord_stream_error_total",
			Help: "Malformed or oversized coord streams rejected by this node.",
		}),
	}
}

// buildMembers tries to construct a k8s-informer-backed Members
// Manager. When required config is missing or the informer fails to
// start, it falls back to a single-self stub (so dev/test runs stay
// functional). Returns the Members impl plus a stop function the caller
// MUST defer.
func buildMembers(ctx context.Context, c *config.Config, disco *discovery.Host, logger *slog.Logger) (ifaces.Members, func()) {
	// Required inputs for the real informer path.
	if c.NodeName == "" || c.MembersLabelSelector == "" {
		logger.Info("members: using single-self stub (NodeName/LabelSelector unset)")
		return singleSelfMembers(c, disco), func() {}
	}
	mgr, err := members.New(members.Options{
		NodeName:      c.NodeName,
		Namespace:     c.MembersNamespace,
		LabelSelector: c.MembersLabelSelector,
		ZoneLabelKey:  c.ZoneLabelKey,
		Kubeconfig:    c.MembersKubeconfig,
	})
	if err != nil {
		logger.Warn("members.New failed; falling back to single-self stub", slog.Any("err", err))
		return singleSelfMembers(c, disco), func() {}
	}
	if err := mgr.Start(ctx); err != nil {
		logger.Warn("members.Start failed; falling back to single-self stub", slog.Any("err", err))
		return singleSelfMembers(c, disco), func() {}
	}
	syncCtx, syncCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := mgr.WaitForSync(syncCtx); err != nil {
		logger.Warn("members initial sync timed out", slog.Any("err", err))
	}
	syncCancel()
	logger.Info("members informer ready",
		slog.String("node_name", c.NodeName),
		slog.Int("peers", len(mgr.Snapshot())),
	)
	return mgr, mgr.Stop
}

// singleSelfMembers returns a single-entry Members view for dev/test
// runs that have no Kubernetes cluster behind them.
func singleSelfMembers(c *config.Config, disco *discovery.Host) ifaces.Members {
	id := c.NodeName
	if id == "" {
		id = disco.PeerID().String()
	}
	return fakes.NewMembers(ifaces.NodeID(id), ifaces.Node{
		ID:   ifaces.NodeID(id),
		Addr: c.TransferListen,
	})
}

// hasMultiNodeMembership reports whether the membership snapshot has
// any node other than self. Used to gate cold-start orchestrator
// wiring: a single-node view degrades cold-start to "always 5xx" which
// is wrong for dev mode.
func hasMultiNodeMembership(m ifaces.Members) bool {
	self := m.Self()
	for _, n := range m.Snapshot() {
		if n.ID != self {
			return true
		}
	}
	return false
}

// lookupSelfZone returns the zone label of this node from the members
// snapshot, or "" if absent. Used to seed coldstart.Options.SelfZone
// under HrwScope = "zone".
func lookupSelfZone(m ifaces.Members) string {
	self := m.Self()
	for _, n := range m.Snapshot() {
		if n.ID == self {
			return n.Zone
		}
	}
	return ""
}

// coldStartAdapter bridges *coldstart.Resolver to mirror.ColdStartResolver
// without forcing the mirror package to import internal/coldstart.
type coldStartAdapter struct{ r *coldstart.Resolver }

func (a coldStartAdapter) Resolve(ctx context.Context, d digest.Digest, kind ifaces.OriginRefKind, registry, repository string, expectedSize int64) (*mirror.ColdStartResolution, error) {
	res, err := a.r.Resolve(ctx, d, kind, registry, repository, expectedSize)
	if err != nil {
		return nil, err
	}
	return &mirror.ColdStartResolution{Providers: res.Providers, Outcome: res.Outcome}, nil
}

// newPullerPump returns the coord.PullerPump that backs inbound
// please_pull RPCs. Per §5.2 step 7, the pump's job is to dedupe via
// the in-flight map, kick off the origin pull on a background
// goroutine, and return promptly so the coord stream handler can
// reply with OUTCOME_STARTED or OUTCOME_ALREADY_PULLING.
//
// On success, the pulled bytes land in the local cache (digest-
// verifying writer) and are then advertised via dht.Provide so peer
// requesters can discover them through the warm path. Failures during
// the background fetch are logged; Phase 4 will plumb them into the
// negative cache (§5.8).
func newPullerPump(infl *inflight.Map, originClient ifaces.OriginPuller, cstore ifaces.Cache, disco *discovery.Host, logger *slog.Logger) coord.PullerPump {
	lg := logger.With(slog.String("subsystem", "puller-pump"))
	return func(_ context.Context, registry, repository string, d digest.Digest, kind ifaces.OriginRefKind) (time.Time, bool, *coord.NegativeEntry) {
		// Dedupe at this node: if a pull is already running, the
		// stream handler must report ALREADY_PULLING with the existing
		// start time so the requester can run the §5.6 stall check.
		h, existing, already := infl.Start(d, kind, 0)
		if already {
			return existing.StartedAt, true, nil
		}
		// Detach the actual fetch from the stream handler. The pump
		// returns immediately; the goroutine owns the inflight handle.
		startedAt := existing.StartedAt
		go runOriginPull(originClient, cstore, disco, lg, h, registry, repository, d, kind)
		return startedAt, false, nil
	}
}

// runOriginPull executes an origin pull → cache write → dht.Provide
// pipeline for d. Caller owns the inflight handle and must arrange for
// Done() to be called exactly once; we do that here on every exit path.
func runOriginPull(originClient ifaces.OriginPuller, cstore ifaces.Cache, disco *discovery.Host, lg *slog.Logger, h *inflight.Handle, registry, repository string, d digest.Digest, kind ifaces.OriginRefKind) {
	defer h.Done()

	// Background context: the requesting peer's stream is already
	// closed by the time we get here. We bound the pull by a generous
	// ceiling so a hung origin can't leak the in-flight slot forever.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ref := ifaces.OriginRef{
		Registry:   registry,
		Repository: repository,
		Digest:     d,
		Kind:       kind,
	}
	rc, _, err := originClient.Pull(ctx, ref)
	if err != nil {
		lg.Warn("origin pull failed",
			slog.String("digest", d.String()),
			slog.String("registry", registry),
			slog.String("repository", repository),
			slog.Any("err", err),
		)
		return
	}
	defer func() { _ = rc.Close() }()

	w, err := cstore.Writer(ctx, d)
	if err != nil {
		lg.Warn("cache writer open failed",
			slog.String("digest", d.String()),
			slog.Any("err", err),
		)
		return
	}
	defer func() { _ = w.Abort(ctx) }()

	if _, err := io.Copy(w, rc); err != nil {
		lg.Warn("origin pull copy failed",
			slog.String("digest", d.String()),
			slog.Any("err", err),
		)
		return
	}
	if err := w.Commit(ctx); err != nil {
		lg.Warn("cache commit failed (digest mismatch or io error)",
			slog.String("digest", d.String()),
			slog.Any("err", err),
		)
		return
	}

	// Advertise the new digest so peer requesters can discover us
	// through the warm path.
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer provCancel()
	if err := disco.Provide(provCtx, d); err != nil {
		lg.Debug("dht.Provide failed",
			slog.String("digest", d.String()),
			slog.Any("err", err),
		)
		return
	}
	lg.Info("please_pull served",
		slog.String("digest", d.String()),
		slog.String("registry", registry),
		slog.String("repository", repository),
	)
}
