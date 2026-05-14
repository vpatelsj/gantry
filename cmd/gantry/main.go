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
	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/discovery"
	gantrylog "github.com/gantry/gantry/internal/gantrylog"
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
