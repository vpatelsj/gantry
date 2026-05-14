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
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/gantry/gantry/internal/cache"
	"github.com/gantry/gantry/internal/cdsub"
	"github.com/gantry/gantry/internal/coldstart"
	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/coord"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/discovery"
	"github.com/gantry/gantry/internal/hrw"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/ifaces/fakes"
	"github.com/gantry/gantry/internal/inflight"
	gantrylog "github.com/gantry/gantry/internal/log"
	"github.com/gantry/gantry/internal/manifest"
	"github.com/gantry/gantry/internal/members"
	"github.com/gantry/gantry/internal/metrics"
	"github.com/gantry/gantry/internal/mirror"
	"github.com/gantry/gantry/internal/negcache"
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
	p6 := newPhase6Metrics(reg)

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

	// Cache. §7.4: LRU at the layer level with provider-count
	// deferral and forced-eviction headroom. The DHT-backed callback
	// is wired now that disco is up; statfs probes the cache volume
	// for the forced-eviction floor.
	cstore, err := cache.Open(c.CacheDir, c.CacheBudgetBytes,
		cache.WithLogger(logger),
		cache.WithMetrics(
			func() { inst.cacheHit.Inc() },
			func() { inst.cacheMiss.Inc() },
			func(int64) { /* per-eviction byte counter intentionally unbound */ },
		),
		cache.WithEviction(cache.EvictionPolicy{
			ProviderCount: func(ctx context.Context, d digest.Digest) (int, error) {
				ps, perr := disco.FindProviders(ctx, d)
				if perr != nil {
					return 0, perr
				}
				return len(ps), nil
			},
			Threshold:        c.EvictionProviderCountThreshold,
			HeadroomPct:      c.CacheForcedEvictionHeadroomPct,
			DiskFree:         cache.DefaultDiskFree(c.CacheDir),
			OnForcedEviction: func() { p6.cacheForcedEvictionTotal.Inc() },
		}),
	)
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}
	defer func() { _ = cstore.Close() }()

	// Phase 2 — peer dialer + transfer endpoint. The cdsub source is
	// constructed early so the transfer endpoint can read blobs out
	// of containerd's content store on cache miss; without this hop
	// peers receive DHT announcements for digests cdsub knows about
	// but the transfer endpoint then 404s on them, defeating the
	// whole point of cdsub-driven advertisement.
	peerClient := transfer.NewClient()
	cdsubSrc := newCdsubSource(c, logger)
	secondaryBlobs := cdsubBlobSource(cdsubSrc)
	transferOpts := []transfer.Option{
		transfer.WithLogger(logger),
		transfer.WithMetrics(
			func() { p2.peerServe.Inc() },
			func() { p2.peerMiss.Inc() },
		),
	}
	if secondaryBlobs != nil {
		transferOpts = append(transferOpts, transfer.WithSecondaryBlobSource(secondaryBlobs))
		logger.Info("transfer: containerd content store wired as secondary blob source")
	}
	transferSrv := transfer.New(cstore, transferOpts...)
	transferStop, err := transferSrv.ListenAndServe(c.TransferListen)
	if err != nil {
		return fmt.Errorf("transfer listen: %w", err)
	}
	logger.Info("transfer endpoint listening", slog.String("addr", c.TransferListen))

	// Phase 3 — membership view + cold-start orchestrator. Members
	// requires Kubernetes credentials (in-cluster or explicit
	// kubeconfig); when neither is available we fall back to a
	// single-self membership view that disables cold-start so the
	// mirror keeps Phase 1 behaviour for local development. When
	// production K8s env vars are set (GANTRY_NODE_NAME etc.)
	// failure to start the informer is fatal — silently degrading
	// to single-node mode in production would advertise a healthy
	// agent that is in fact running with no peer coordination.
	memberView, membersStop, err := buildMembers(ctx, c, disco, logger)
	if err != nil {
		return fmt.Errorf("members: %w", err)
	}
	defer membersStop()

	// Phase 3 (cont.) — self-announce: write libp2p peer.ID, listen
	// multiaddrs, and the transfer endpoint into our own Pod's
	// annotations so peer agents can discover this node without
	// operator-supplied bootstrap_peers. Fire-and-forget so a missing
	// `pods/patch` RBAC permission only degrades discovery, not the
	// agent. Only attempted when the real informer is in use.
	if mgr, ok := memberView.(*members.Manager); ok && c.PodName != "" {
		go announceSelfAndBootstrap(ctx, mgr, disco, c, logger)
	}

	// Phase 5 — wire the routing-table target now that memberView is
	// online. §7.7 defines target = min(informer_node_count,
	// kademlia_max_routing_table_size); the constant cap of 256 is
	// derived from kad-dht's bucket-size 20 × log2(10000) ≈ 266 and
	// rounded down. Read live on every score call.
	const kademliaMaxRoutingTable = 256
	if monitor := disco.Monitor(); monitor != nil {
		monitor.SetRoutingTableTarget(func() int {
			sz := len(memberView.Snapshot())
			if sz > kademliaMaxRoutingTable {
				return kademliaMaxRoutingTable
			}
			return sz
		})
	}

	// Phase 3 — in-flight map + coord client + coord server + metrics.
	inflightMap := inflight.New(inflight.DefaultStalls(), nil)
	p3 := newPhase3Metrics(reg, inflightMap)

	// Phase 4 — §5.8 origin-failure negative cache (puller-local) +
	// stall-takeover metric. Constructed before the pump so the pump
	// can consult it; the coord server is given a thin adapter so
	// pull_intent_query responses surface recently_failed.
	p4 := newPhase4Metrics(reg)
	negCache := negcache.New(negcache.Options{
		Initial:    c.OriginFailureCooldownInitial,
		Max:        c.OriginFailureCooldownMax,
		Multiplier: c.OriginFailureCooldownMultiplier,
		OnEnter:    func(class ifaces.FailureClass) { p4.observeEnter(class) },
		OnHit:      func(class ifaces.FailureClass) { p4.observeHit(class) },
		OnSize:     func(n int) { p4.setSize(n) },
	})

	// Phase 5 — DHT health gauge, NF5 origin-fallback counter, and
	// top-K expansion counter. Health source is the discovery host
	// (Phase 2 monitor); when running without monitoring (test mode)
	// it returns 1.0.
	p5 := newPhase5Metrics(reg, disco.Health)

	coordClient := coord.NewClient(disco.LibP2P(),
		coord.WithClientLogger(logger),
		// Resolve NodeID → peer.ID via the live membership snapshot:
		// each peer publishes its libp2p peer.ID into a pod
		// annotation (§7.3) which Members reads in Snapshot. This
		// lets the cluster use stable K8s node names as NodeIDs
		// while still dialing libp2p RPCs to the right peer.
		coord.WithPeerIDResolver(membershipPeerIDResolver(memberView, logger)),
	)
	// pullerPump bridges inbound please_pull RPCs to the local origin
	// puller (§5.2 step 7). The pump itself MUST NOT block the coord
	// stream handler; the actual origin fetch + cache write + dht
	// Provide all happen in a detached goroutine. pullerPumpWG tracks
	// outstanding goroutines so graceful shutdown can wait for
	// dht.Provide calls to flush before disco.Close fires (§Phase 6).
	var pullerPumpWG sync.WaitGroup
	pullerPump := newPullerPump(inflightMap, originClient, cstore, disco, negCache, logger, &pullerPumpWG, func() {
		p2.dhtProvideErr.WithLabelValues("origin_pull_announce").Inc()
	})
	coordOpts := []coord.Option{
		coord.WithLogger(logger),
		coord.WithMetrics(coord.MetricsHooks{
			OnPullIntentServed:  func() { p3.coordPullIntentServed.Inc() },
			OnPleasePullServed:  func() { p3.coordPleasePullServed.Inc() },
			OnPleasePullStarted: func() { p3.coordPleasePullStarted.Inc() },
			OnStreamError:       func() { p3.coordStreamError.Inc() },
		}),
		coord.WithNegativeCache(negCacheAdapter{c: negCache}),
		coord.WithPullerPump(pullerPump),
	}
	// Effective local availability: pull_intent_query OR's the Gantry
	// cache with the optional secondary blob source (containerd's
	// content store on Linux). Without this, every blob containerd
	// pulled outside Gantry — which cdsub announces on the DHT —
	// would report has_cached=false on the wire and trigger redundant
	// please_pull / origin fetches even though the transfer endpoint
	// would already serve them.
	if secondaryBlobs != nil {
		coordOpts = append(coordOpts, coord.WithSecondaryBlobSource(secondaryBlobs))
	}
	coordServer := coord.NewServer(cstore, memberView, inflightMap, coordOpts...)
	coordServer.Bind(disco.LibP2P())

	// Phase 3 cold-start orchestrator. Enabled whenever the real
	// Kubernetes membership informer is in use; disabled only for
	// the dev-mode single-self fake (where there are no peers to
	// coordinate with by definition). The previous "Snapshot has
	// non-self entry" gate broke first-cluster boot — see
	// hasMultiNodeMembership for the full rationale.
	var coldStartResolver mirror.ColdStartResolver
	var layerPrefetcher mirror.LayerPrefetcher
	if hasMultiNodeMembership(memberView) {
		selfZone := lookupSelfZone(memberView)
		realResolver := coldstart.New(coldstart.Options{
			Members:               memberView,
			Discovery:             disco,
			Coord:                 coordClient,
			Inflight:              inflightMap,
			Logger:                logger,
			HrwK:                  c.HRWK,
			HrwScope:              hrw.ParseScope(c.HRWTopologyScope),
			SelfZone:              selfZone,
			TransientCooldownCap:  c.OriginFailureHonorWindowCap,
			TopKExpansionFactor:   c.TopKExpansionFactorDegraded,
			TrustedFailureClasses: parseTrustedFailureClasses(c.OriginFailureClassesTrustedClusterWide, logger),
			Metrics: coldstart.MetricsHooks{
				OnRankMismatch: func(kindLabel string, _ ifaces.NodeID) {
					p3.hrwRankMismatch.WithLabelValues(kindLabel).Inc()
				},
				OnDhtFalseEmpty: func() { p3.dhtFalseEmpty.Inc() },
				OnTopKProbeHit:  func() { p3.topkProbeHit.Inc() },
				OnColdStartDuration: func(kindLabel, outcome string, d time.Duration) {
					p3.coldStartDuration.WithLabelValues(kindLabel, outcome).Observe(d.Seconds())
				},
				OnDesignatedPullerTakeover: func(kindLabel string) {
					p4.designatedPullerTakeoverTotal.WithLabelValues(kindLabel).Inc()
				},
				OnTopKExpansion: func(reason string) {
					p5.topkExpansionTotal.WithLabelValues(reason).Inc()
				},
				OnPrefetchBatch: func(pullers, digests int) {
					p3.prefetchBatchesTotal.Inc()
					p3.prefetchDigestsTotal.Add(float64(digests))
					p3.prefetchPullersPerBatch.Observe(float64(pullers))
				},
			},
		})
		coldStartResolver = coldStartAdapter{r: realResolver}
		layerPrefetcher = newLayerPrefetcher(realResolver, cstore, logger)
		logger.Info("cold-start orchestrator wired",
			slog.Int("hrw_k", c.HRWK),
			slog.String("hrw_scope", c.HRWTopologyScope),
		)
	} else {
		logger.Info("cold-start orchestrator disabled (single-self membership; no Kubernetes informer)")
	}

	// Phase 5 — NF5 direct-origin fallback controller (§5.7). Wired
	// only when the cold-start resolver is also wired; without
	// orchestration there is no `ErrColdStartExhausted` path to gate.
	var nf5Ctrl *mirror.NF5Controller
	if coldStartResolver != nil {
		monitor := disco.Monitor()
		nf5Ctrl = mirror.NewNF5(mirror.NF5Options{
			Logger:           logger,
			JitterBase:       c.NF5JitterBase,
			PerNodeRateLimit: c.NF5PerNodeRateLimit,
			ClusterSize:      func() int { return len(memberView.Snapshot()) },
			InBootstrap: func() bool {
				if monitor == nil {
					return false
				}
				return monitor.InBootstrapWindow(c.BootstrapWindow, c.BootstrapRoutingTablePct)
			},
			HealthyEnough: func() bool {
				// Decline NF5 when DHT is Unhealthy (<0.3). The empty
				// DHT answer can't be trusted, and we'd rather 5xx and
				// let kubelet back off than thunder the origin.
				return disco.Health() >= 0.3
			},
			Inflight: inflightMap,
			Recheck: func(ctx context.Context, d digest.Digest) bool {
				// Final post-jitter probe: did anyone publish a
				// provider record while we slept? If so, NF5 declines
				// and the client retries through the warm path on its
				// next attempt.
				rcCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
				defer cancel()
				prov, err := disco.FindProviders(rcCtx, d)
				return err == nil && len(prov) > 0
			},
			OnFallback: func() { p5.originFallbackTotal.Inc() },
			OnDecline: func(reason string) {
				p5.originFallbackDeclineTotal.WithLabelValues(reason).Inc()
			},
		})
		logger.Info("NF5 origin-fallback wired",
			slog.Duration("jitter_base", c.NF5JitterBase),
			slog.Int("per_node_rate_limit", c.NF5PerNodeRateLimit),
			slog.Duration("bootstrap_window", c.BootstrapWindow),
		)
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
		mirror.WithPeerFetchLatencyMetric(func(outcome string, d time.Duration) {
			p2.peerFetchDur.WithLabelValues(outcome).Observe(d.Seconds())
		}),
		mirror.WithDhtLookupMetric(func(outcome string, dur time.Duration) {
			p2.dhtLookup.WithLabelValues(outcome).Inc()
			p2.dhtLookupDur.WithLabelValues(outcome).Observe(dur.Seconds())
		}),
		mirror.WithProvideErrorMetric(func(op string) {
			p2.dhtProvideErr.WithLabelValues(op).Inc()
		}),
		mirror.WithColdStart(coldStartResolver),
		mirror.WithLayerPrefetcher(layerPrefetcher),
		mirror.WithNF5(nf5Ctrl),
	)

	mirrorStop, err := mirrorSrv.ListenAndServe(c.MirrorListen)
	if err != nil {
		return fmt.Errorf("mirror listen: %w", err)
	}
	logger.Info("mirror endpoint listening", slog.String("addr", c.MirrorListen))

	// Phase 2 — cdsub announce loop. cdsubSrc was constructed above
	// (see the transfer-endpoint block) so the transfer endpoint can
	// chain into the same containerd content store on cache miss.
	cdSub := cdsub.New(cdsubSrc, disco,
		cdsub.WithLogger(logger),
		cdsub.WithMetrics(
			func() { p2.dhtProvide.Inc() },
			func() { p2.dhtProvideErr.WithLabelValues("cdsub_announce").Inc() },
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

	// Phase 6 — readiness state. /readyz waits for three signals:
	// (1) members informer initial sync, (2) DHT routing table
	// non-empty, (3) cache scan complete. (3) is implicit because
	// cache.Open() runs synchronously above, but we set a flag here
	// so the relationship is explicit in the probe logic.
	var (
		membersReady atomic.Bool
		cacheReady   atomic.Bool
	)
	cacheReady.Store(true)
	go func() {
		if err := memberView.WaitForSync(ctx); err == nil {
			membersReady.Store(true)
		}
	}()

	readyCheck := func() (string, bool) {
		if !cacheReady.Load() {
			return "cache scan not complete", false
		}
		if !membersReady.Load() {
			return "members informer not synced", false
		}
		if disco.RoutingTableSize() < 1 {
			return "dht routing table empty", false
		}
		return "", true
	}

	// Phase 6 — operations HTTP listener. Per §Phase 6 plan,
	// /healthz includes liveness + readiness; /livez is the pure
	// liveness probe; /readyz is the pure readiness probe. Kubernetes
	// conventions vary, so we expose all three.
	mux := http.NewServeMux()
	mux.Handle("/metrics", reg.Handler())
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if reason, ok := readyCheck(); !ok {
			http.Error(w, reason, http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if reason, ok := readyCheck(); !ok {
			http.Error(w, reason, http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	metricsHTTP := &http.Server{
		Addr:              c.MetricsListen,
		Handler:           mux,
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
	logger.Info("ops endpoint listening", slog.String("addr", c.MetricsListen),
		slog.String("paths", "/metrics, /livez, /healthz, /readyz"))

	// Block until signal or metrics-server crash.
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-metricsErr:
		logger.Error("metrics endpoint died", slog.Any("err", err))
	case err := <-cdsubDone:
		logger.Error("cdsub loop exited unexpectedly", slog.Any("err", err))
	}

	// Graceful shutdown (§Phase 6). Order:
	//   1. Mirror.Drain() — every new /v2/ request immediately gets
	//      503 so containerd's hosts.toml falls through to origin.
	//      Does NOT close the listener yet — existing kubelet
	//      connections need a chance to complete.
	//   2. Transfer.Shutdown — drains in-flight peer transfers so a
	//      requesting peer doesn't see its pull cut mid-stream.
	//   3. Mirror.Shutdown — closes the listener, drains in-flight
	//      handlers up to the shutdown deadline.
	//   4. Wait for cdsub.Run + outstanding pull-pump Provide calls
	//      to flush before libp2p is closed.
	//   5. Ops endpoint (Shutdown) — last so /readyz can keep
	//      reporting NotReady while we drain.
	//   6. discovery.Close (deferred above) — closes the libp2p
	//      host; membersStop (deferred above) stops the informer.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	mirrorSrv.Drain()
	if err := transferStop(shutdownCtx); err != nil {
		logger.Warn("transfer shutdown error", slog.Any("err", err))
	}
	if err := mirrorStop(shutdownCtx); err != nil {
		logger.Warn("mirror shutdown error", slog.Any("err", err))
	}
	// cdsub already cancelled by the outer ctx; wait briefly for its
	// pending Provide calls to flush.
	select {
	case <-cdsubDone:
	case <-shutdownCtx.Done():
		logger.Warn("cdsub did not drain within shutdown budget")
	}
	// Release the underlying containerd gRPC client if the source
	// owns one. NoOpSource doesn't implement io.Closer so this is a
	// best-effort type assertion.
	if closer, ok := cdsubSrc.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			logger.Warn("cdsub source close error", slog.Any("err", err))
		}
	}
	// §Phase 6: "flushes DHT Provide for any newly committed entries."
	// runOriginPull goroutines own one inflight handle and one
	// dht.Provide call each; pullerPumpWG counts them so we can let
	// pending Provides flush before disco.Close fires below.
	pumpDone := make(chan struct{})
	go func() { pullerPumpWG.Wait(); close(pumpDone) }()
	select {
	case <-pumpDone:
	case <-shutdownCtx.Done():
		logger.Warn("puller-pump did not drain within shutdown budget")
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
	peerFetchDur    *prometheus.HistogramVec
	peerDialSuccess prometheus.Counter
	peerDialFailure prometheus.Counter
	dhtProvide      prometheus.Counter
	dhtProvideErr   *prometheus.CounterVec
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
		peerFetchDur: reg.NewHistogramVec("mirror", prometheus.HistogramOpts{
			Name:    "p2p_peer_fetch_duration_seconds",
			Help:    "End-to-end peer-fetch latency from FetchFromPeer dial to terminal outcome (hit = cache commit, error/stall/notfound = first failing branch). Together with p2p_peer_fetch_total{outcome} this isolates dial vs. body vs. commit-time-digest-verification slowness.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
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
		dhtProvideErr: reg.NewCounterVec("discovery", prometheus.CounterOpts{
			Name: "p2p_dht_provide_error_total",
			Help: "DHT Provide calls that errored, labelled by call site (cdsub_announce, peer_fetch_readvertise, cache_reannounce, origin_pull_announce). Without the label a hung kad-dht is indistinguishable from a misbehaving cdsub source.",
		}, []string{"op"}),
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
			if p2 != nil {
				p2.dhtProvideErr.WithLabelValues("cache_reannounce").Inc()
			}
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
	hrwRankMismatch         *prometheus.CounterVec
	dhtFalseEmpty           prometheus.Counter
	topkProbeHit            prometheus.Counter
	coldStartDuration       *prometheus.HistogramVec
	coordPullIntentServed   prometheus.Counter
	coordPleasePullServed   prometheus.Counter
	coordPleasePullStarted  prometheus.Counter
	coordStreamError        prometheus.Counter
	prefetchBatchesTotal    prometheus.Counter
	prefetchDigestsTotal    prometheus.Counter
	prefetchPullersPerBatch prometheus.Histogram
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
		prefetchBatchesTotal: reg.NewCounter("coord", prometheus.CounterOpts{
			Name: "p2p_prefetch_batches_total",
			Help: "Speculative manifest-pre-fan PleasePull batches dispatched (one per distinct HRW rank-0 puller per manifest serve, §5.2).",
		}),
		prefetchDigestsTotal: reg.NewCounter("coord", prometheus.CounterOpts{
			Name: "p2p_prefetch_digests_total",
			Help: "Layer/config digests carried in speculative manifest-pre-fan batches (cumulative sum across batches, §5.2).",
		}),
		prefetchPullersPerBatch: reg.NewHistogram("coord", prometheus.HistogramOpts{
			Name:    "p2p_prefetch_pullers_per_manifest",
			Help:    "Distribution of distinct HRW rank-0 pullers contacted per manifest pre-fan call.",
			Buckets: prometheus.LinearBuckets(1, 1, 10),
		}),
	}
}

// isProductionMode reports whether the caller has set any of the
// Kubernetes-Downward-API signals that imply the agent is running
// inside a real cluster (DaemonSet wiring sets all three via
// metadata.name, spec.nodeName, and a fixed Namespace env var). When
// true, a single-self membership fallback is unsafe because the
// operator believes peer coordination is active.
func isProductionMode(c *config.Config) bool {
	return c.NodeName != "" || c.PodName != "" || c.MembersNamespace != ""
}

// buildMembers tries to construct a k8s-informer-backed Members
// Manager. Behaviour depends on whether production-mode env vars
// signal that K8s membership is expected:
//
//   - Dev mode (NodeName, PodName, and MembersNamespace all empty):
//     fall back silently to a single-self stub. Cold-start is
//     disabled downstream via hasMultiNodeMembership; the agent
//     serves the Phase 1 direct-mirror path. This is the path local
//     `go run` invocations take.
//
//   - Production mode (any of NodeName / PodName / MembersNamespace
//     non-empty): an informer construction or Start failure is
//     fatal. Returning a single-self stub here would advertise a
//     healthy agent that is silently running with no peer
//     coordination at all — worse than crash-looping, because the
//     operator sees no signal.
//
//   - WaitForSync timeout remains a warning regardless of mode: the
//     informer may sync moments later, the bootstrap loop retries
//     dialing periodically, and the readiness probe blocks until
//     the routing table is non-empty.
func buildMembers(ctx context.Context, c *config.Config, disco *discovery.Host, logger *slog.Logger) (ifaces.Members, func(), error) {
	prodMode := isProductionMode(c)
	// Required inputs for the real informer path.
	if c.NodeName == "" || c.MembersLabelSelector == "" {
		if prodMode {
			return nil, nil, fmt.Errorf("production mode (NodeName/PodName/Namespace set) but NodeName or LabelSelector missing: refusing to silently fall back to single-self stub")
		}
		logger.Info("members: using single-self stub (NodeName/LabelSelector unset)")
		return singleSelfMembers(c, disco), func() {}, nil
	}
	mgr, err := members.New(members.Options{
		NodeName:      c.NodeName,
		Namespace:     c.MembersNamespace,
		LabelSelector: c.MembersLabelSelector,
		ZoneLabelKey:  c.ZoneLabelKey,
		Kubeconfig:    c.MembersKubeconfig,
		TransferPort:  transferPortFromListen(c.TransferListen),
	})
	if err != nil {
		if prodMode {
			return nil, nil, fmt.Errorf("members.New: %w", err)
		}
		logger.Warn("members.New failed; falling back to single-self stub (dev mode)", slog.Any("err", err))
		return singleSelfMembers(c, disco), func() {}, nil
	}
	if err := mgr.Start(ctx); err != nil {
		if prodMode {
			mgr.Stop()
			return nil, nil, fmt.Errorf("members.Start: %w", err)
		}
		logger.Warn("members.Start failed; falling back to single-self stub (dev mode)", slog.Any("err", err))
		mgr.Stop()
		return singleSelfMembers(c, disco), func() {}, nil
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
	return mgr, mgr.Stop, nil
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

// hasMultiNodeMembership reports whether cold-start coordination
// should be enabled. Previously this checked Snapshot() for any non-
// self entry, which deadlocked first-cluster boot: on a fresh
// cluster no peer is Ready yet, Snapshot() returns just self, cold-
// start was disabled for the whole process lifetime, and the agent
// silently degraded to direct-origin pulls forever — the exact
// scenario cold-start is most needed for.
//
// Cold-start is now enabled whenever the membership view is backed
// by the real Kubernetes informer (*members.Manager). The single-
// self fake is the only mode that disables it: that mode is for
// dev/test runs with no cluster at all, where there are no peers
// to coordinate with by definition.
//
// The orchestrator itself handles an empty peer view internally
// (NF5 / ErrColdStartExhausted fall-through), so it does not need
// a populated snapshot at construction time.
func hasMultiNodeMembership(m ifaces.Members) bool {
	_, isManager := m.(*members.Manager)
	return isManager
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

// parseTrustedFailureClasses converts the string-form config slice
// (`origin_failure_classes_trusted_cluster_wide`) to the typed
// ifaces.FailureClass values consumed by the cold-start rule-1
// short-circuit. Unknown class names are logged and dropped; an
// empty / all-unknown result lets coldstart.New fall back to its
// default {auth, not_found, rate_limited}.
func parseTrustedFailureClasses(raw []string, logger *slog.Logger) []ifaces.FailureClass {
	if len(raw) == 0 {
		return nil
	}
	known := map[string]ifaces.FailureClass{
		string(ifaces.FailureAuth):        ifaces.FailureAuth,
		string(ifaces.FailureNotFound):    ifaces.FailureNotFound,
		string(ifaces.FailureRateLimited): ifaces.FailureRateLimited,
		string(ifaces.FailureTransient):   ifaces.FailureTransient,
	}
	out := make([]ifaces.FailureClass, 0, len(raw))
	for _, s := range raw {
		if fc, ok := known[s]; ok {
			out = append(out, fc)
			continue
		}
		if logger != nil {
			logger.Warn("config: unknown origin_failure_classes_trusted_cluster_wide entry; dropped",
				slog.String("value", s),
			)
		}
	}
	return out
}

// transferPortFromListen parses the port number out of a `host:port`
// listen spec such as "0.0.0.0:5001" or ":5001". Returns 0 when the
// spec is empty or malformed; members.Snapshot then falls back to a
// bare pod-IP address.
func transferPortFromListen(listen string) int {
	if listen == "" {
		return 0
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return 0
	}
	return n
}

// membershipPeerIDResolver returns a coord.WithPeerIDResolver callback
// that consults the live members snapshot. NodeID → Node.PeerID is the
// fast path; on miss the resolver returns (_, false) so coord.Client
// falls through to its static teach-cache and finally to
// peer.Decode(NodeID). The membership view is read on every call (cheap
// in-memory copy) so newly-joined peers are picked up without restart.
func membershipPeerIDResolver(mv ifaces.Members, logger *slog.Logger) func(ifaces.NodeID) (peer.ID, bool) {
	return func(id ifaces.NodeID) (peer.ID, bool) {
		for _, n := range mv.Snapshot() {
			if n.ID != id || n.PeerID == "" {
				continue
			}
			pid, err := peer.Decode(n.PeerID)
			if err != nil {
				if logger != nil {
					logger.Debug("membership peer-id decode failed",
						slog.String("node_id", string(id)),
						slog.String("peer_id", n.PeerID),
						slog.Any("err", err),
					)
				}
				return "", false
			}
			return pid, true
		}
		return "", false
	}
}

// announceSelfAndBootstrap publishes this agent's libp2p identity into
// its own Pod's annotations, then dials every peer announcement in the
// membership snapshot to seed the kad-dht routing table. The
// announcement is retried with exponential backoff on transient API
// errors; bootstrap dials run on a periodic loop (not once) so a
// rolling deploy where peers patch their annotations at staggered
// times still produces a populated routing table.
//
// The bootstrap snapshot intentionally includes NotReady pods
// (SnapshotForBootstrap) because readiness depends on RoutingTableSize
// being > 0 — a deadlock if every peer is waiting for every other
// peer to be Ready first.
func announceSelfAndBootstrap(ctx context.Context, mgr *members.Manager, disco *discovery.Host, c *config.Config, logger *slog.Logger) {
	// Build the announcement. Wildcard listen addresses (0.0.0.0,
	// ::) are not dialable from other pods; substitute the agent's
	// Pod IP so the published p2p-addrs are usable.
	listenAddrs := disco.Addrs()
	peerID := disco.PeerID()
	multiaddrs := make([]string, 0, len(listenAddrs))
	for _, la := range listenAddrs {
		ma := rewriteWildcardMultiaddr(la.String(), c.PodIP)
		if ma == "" {
			// Skip wildcards we can't rewrite — better no entry
			// than an undialable one.
			continue
		}
		// Format /ip4/.../tcp/.../p2p/<peerID> so peers can dial
		// directly without a separate ID resolution step.
		multiaddrs = append(multiaddrs, ma+"/p2p/"+peerID.String())
	}
	ann := members.SelfAnnouncement{
		PeerID:       peerID.String(),
		P2PAddrs:     multiaddrs,
		TransferAddr: advertisedTransferAddr(c.TransferListen, c.PodIP),
	}

	// Retry the patch with capped exponential backoff. The pod
	// informer is independent of this call so a missing `pods/patch`
	// permission only degrades discovery — the agent still serves.
	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second
	for attempt := 0; attempt < 5; attempt++ {
		err := mgr.AnnounceSelf(ctx, c.PodName, ann)
		if err == nil {
			logger.Info("members: self-announce ok",
				slog.String("pod", c.PodName),
				slog.String("peer_id", peerID.String()),
				slog.Int("p2p_addrs", len(multiaddrs)),
			)
			break
		}
		logger.Warn("members: self-announce failed; will retry",
			slog.Int("attempt", attempt+1),
			slog.Any("err", err),
		)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	// Periodic bootstrap loop. A single ConnectPeers call at startup
	// can miss peers whose AnnounceSelf hasn't completed yet (cold
	// cluster boot, rolling deploys). We poll the bootstrap snapshot
	// every 5s for the first minute, then back off to every 30s
	// while RoutingTableSize is still below a healthy threshold, and
	// stop entirely once the table is populated. kad-dht handles
	// ongoing refresh from there.
	const (
		aggressiveInterval = 5 * time.Second
		relaxedInterval    = 30 * time.Second
		aggressiveBudget   = 60 * time.Second
		healthyRTSize      = 5
	)
	bootstrapStart := time.Now()
	for {
		peerAddrs := bootstrapPeerAddrs(mgr)
		if len(peerAddrs) > 0 {
			connected := disco.ConnectPeers(ctx, peerAddrs)
			logger.Debug("members: bootstrap dial pass",
				slog.Int("connected", connected),
				slog.Int("candidates", len(peerAddrs)),
				slog.Int("routing_table", disco.RoutingTableSize()),
			)
		}
		if disco.RoutingTableSize() >= healthyRTSize {
			logger.Info("members: bootstrap converged; ceasing periodic dials",
				slog.Int("routing_table", disco.RoutingTableSize()),
				slog.Duration("elapsed", time.Since(bootstrapStart)),
			)
			return
		}
		interval := aggressiveInterval
		if time.Since(bootstrapStart) > aggressiveBudget {
			interval = relaxedInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// bootstrapPeerAddrs collects every published p2p multiaddr across all
// peers in the bootstrap-view snapshot, excluding self.
func bootstrapPeerAddrs(mgr *members.Manager) []string {
	peers := mgr.SnapshotForBootstrap()
	out := make([]string, 0, len(peers))
	for _, n := range peers {
		if n.ID == mgr.Self() || len(n.P2PAddrs) == 0 {
			continue
		}
		out = append(out, n.P2PAddrs...)
	}
	return out
}

// rewriteWildcardMultiaddr returns ma with any wildcard IP component
// (/ip4/0.0.0.0 or /ip6/::) replaced by /ip4/<podIP> or /ip6/<podIP>
// depending on the family of podIP. Non-wildcard multiaddrs are
// returned unchanged. Returns "" when the address is a wildcard and
// no usable pod IP is available (signal to the caller to skip
// publishing this entry).
//
// IPv6 dual-stack handling: an IPv6 Pod IP must be advertised under
// the /ip6/ multiaddr family, not /ip4/, otherwise libp2p rejects the
// multiaddr at dial time and the bootstrap pool contains no usable
// entry for v6-only clusters. We parse the Pod IP and pick the
// matching family explicitly.
func rewriteWildcardMultiaddr(ma, podIP string) string {
	isWildcardV4 := strings.HasPrefix(ma, "/ip4/0.0.0.0/")
	isWildcardV6 := strings.HasPrefix(ma, "/ip6/::/")
	if !isWildcardV4 && !isWildcardV6 {
		return ma
	}
	if podIP == "" {
		return ""
	}
	ip := net.ParseIP(podIP)
	if ip == nil {
		return ""
	}
	var (
		family string
		rest   string
	)
	switch {
	case ip.To4() != nil:
		family = "/ip4/" + ip.To4().String()
	default:
		family = "/ip6/" + ip.String()
	}
	if isWildcardV4 {
		rest = ma[len("/ip4/0.0.0.0"):]
	} else {
		rest = ma[len("/ip6/::"):]
	}
	return family + rest
}

// advertisedTransferAddr returns the transfer endpoint to publish on
// the pod's gantry.io/transfer-addr annotation. Wildcard binds map to
// "" so members.Snapshot() composes podIP:transferPort instead (the
// Snapshot fallback path); concrete binds (e.g. a NodePort override)
// are published verbatim.
func advertisedTransferAddr(transferListen, podIP string) string {
	host, port, err := net.SplitHostPort(transferListen)
	if err != nil {
		return transferListen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		if podIP == "" {
			// Outside Kubernetes: leave empty so Snapshot's
			// podIP:transferPort fallback fires (which itself is
			// a no-op without a Pod IP — but that's the right
			// failure mode: no advertised address at all rather
			// than an unreachable 0.0.0.0).
			return ""
		}
		return net.JoinHostPort(podIP, port)
	}
	return transferListen
}

// coldStartAdapter bridges *coldstart.Resolver to mirror.ColdStartResolver
// without forcing the mirror package to import internal/coldstart.
type coldStartAdapter struct{ r *coldstart.Resolver }

func (a coldStartAdapter) Resolve(ctx context.Context, d digest.Digest, kind ifaces.OriginRefKind, registry, repository string, expectedSize int64) (*mirror.ColdStartResolution, error) {
	res, err := a.r.Resolve(ctx, d, kind, registry, repository, expectedSize)
	if err != nil {
		// Translate the cold-start cascade-exhausted sentinel to the
		// mirror-package sentinel that NF5 fallback gates on. Other
		// cold-start errors (failure short-circuit, transient
		// cooldown) are deliberately not translated so the mirror
		// treats them as opaque 5xx — NF5 cannot fire on them.
		if errors.Is(err, coldstart.ErrExhausted) {
			return nil, mirror.ErrColdStartExhausted
		}
		return nil, err
	}
	return &mirror.ColdStartResolution{Providers: res.Providers, Outcome: res.Outcome}, nil
}

// layerPrefetchAdapter implements mirror.LayerPrefetcher: after a
// manifest serve it reads the manifest body back from cache, extracts
// the child layer/config digests, filters out digests already in the
// local cache, and asks the cold-start resolver to issue batched
// please_pull RPCs grouped by HRW rank-0 puller.
//
// The implementation runs in a goroutine spawned by the mirror; it
// MUST NOT panic. All errors are logged at DEBUG.
type layerPrefetchAdapter struct {
	resolver *coldstart.Resolver
	cache    ifaces.Cache
	logger   *slog.Logger
}

// maxManifestBytes caps the size of a manifest body the prefetcher
// is willing to parse. OCI Distribution recommends manifests stay
// well under 4 MiB; a body larger than that almost certainly indicates
// a misconfigured upstream (or attack), and we'd rather skip prefetch
// than allocate a multi-MB buffer per manifest serve.
const maxManifestBytes int64 = 4 * 1024 * 1024

func newLayerPrefetcher(r *coldstart.Resolver, cache ifaces.Cache, logger *slog.Logger) mirror.LayerPrefetcher {
	return &layerPrefetchAdapter{
		resolver: r,
		cache:    cache,
		logger:   logger.With(slog.String("subsystem", "prefetch")),
	}
}

func (p *layerPrefetchAdapter) OnManifestServed(ctx context.Context, registry, repository string, manifestDigest digest.Digest) {
	if p.resolver == nil {
		return
	}
	// Use a fresh deadline so the prefetch survives the request
	// context that just finished; cap at 30s so a stuck prefetch
	// can't pin a goroutine forever.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rc, _, err := p.cache.Open(ctx, manifestDigest)
	if err != nil {
		p.logger.Debug("prefetch: manifest not in cache",
			slog.String("digest", manifestDigest.String()),
			slog.Any("err", err),
		)
		return
	}
	body, err := io.ReadAll(io.LimitReader(rc, maxManifestBytes))
	_ = rc.Close()
	if err != nil {
		p.logger.Debug("prefetch: manifest read failed",
			slog.String("digest", manifestDigest.String()),
			slog.Any("err", err),
		)
		return
	}
	if int64(len(body)) >= maxManifestBytes {
		// Likely truncated; refuse to parse.
		p.logger.Debug("prefetch: manifest exceeds size cap",
			slog.String("digest", manifestDigest.String()),
			slog.Int64("cap", maxManifestBytes),
		)
		return
	}
	children, err := manifest.ChildDigests(body)
	if err != nil {
		p.logger.Debug("prefetch: manifest parse failed",
			slog.String("digest", manifestDigest.String()),
			slog.Any("err", err),
		)
		return
	}
	if len(children) == 0 {
		// Image index or no children — nothing to fan out.
		return
	}

	// Filter out digests already present locally; they don't need
	// prefetching.
	pending := make([]digest.Digest, 0, len(children))
	for _, d := range children {
		has, err := p.cache.Has(ctx, d)
		if err != nil {
			// Treat error as "unknown" — include the digest; the
			// puller's in-flight dedupe handles the case where it's
			// already there.
			pending = append(pending, d)
			continue
		}
		if !has {
			pending = append(pending, d)
		}
	}
	if len(pending) == 0 {
		return
	}
	if err := p.resolver.PrefetchLayers(ctx, pending, registry, repository); err != nil {
		p.logger.Debug("prefetch: PrefetchLayers reported errors",
			slog.String("manifest", manifestDigest.String()),
			slog.Int("layers", len(pending)),
			slog.Any("err", err),
		)
	}
}

// newPullerPump returns the coord.PullerPump that backs inbound
// please_pull RPCs. Per §5.2 step 7, the pump's job is to dedupe via
// the in-flight map, kick off the origin pull on a background
// goroutine, and return promptly so the coord stream handler can
// reply with OUTCOME_STARTED or OUTCOME_ALREADY_PULLING.
//
// On success, the pulled bytes land in the local cache (digest-
// verifying writer) and are then advertised via dht.Provide so peer
// requesters can discover them through the warm path.
//
// On failure, the §5.8 negative cache is consulted/updated:
//   - Before starting an origin pull, the pump checks negCache for an
//     active cooldown; if present, please_pull short-circuits with
//     OUTCOME_RECENTLY_FAILED (cluster-wide propagation).
//   - On terminal origin failure, the goroutine classifies via the
//     *ifaces.OriginError wrapper and records the failure so the next
//     pull_intent_query response surfaces recently_failed.
func newPullerPump(infl *inflight.Map, originClient ifaces.OriginPuller, cstore ifaces.Cache, disco *discovery.Host, neg *negcache.Cache, logger *slog.Logger, wg *sync.WaitGroup, onProvideErr func()) coord.PullerPump {
	lg := logger.With(slog.String("subsystem", "puller-pump"))
	return func(_ context.Context, registry, repository string, d digest.Digest, kind ifaces.OriginRefKind) (time.Time, bool, *coord.NegativeEntry) {
		// §5.8 short-circuit: if we're inside a cooldown window, refuse
		// to start a new origin pull and surface the existing entry so
		// the requester gets recently_failed without round-tripping.
		if neg != nil {
			if e, ok := neg.Lookup(d); ok {
				return time.Time{}, false, &coord.NegativeEntry{
					CooldownUntil: e.CooldownUntil,
					Class:         e.Class,
				}
			}
		}
		// Dedupe at this node: if a pull is already running, the
		// stream handler must report ALREADY_PULLING with the existing
		// start time so the requester can run the §5.6 stall check.
		h, existing, already := infl.Start(d, kind, 0)
		if already {
			return existing.StartedAt, true, nil
		}
		// Detach the actual fetch from the stream handler. The pump
		// returns immediately; the goroutine owns the inflight handle.
		// wg lets graceful shutdown wait for the dht.Provide flush at
		// the end of runOriginPull before closing the libp2p host
		// (§Phase 6 graceful-shutdown contract).
		startedAt := existing.StartedAt
		if wg != nil {
			wg.Add(1)
		}
		go func() {
			if wg != nil {
				defer wg.Done()
			}
			runOriginPull(originClient, cstore, disco, neg, lg, h, registry, repository, d, kind, onProvideErr)
		}()
		return startedAt, false, nil
	}
}

// runOriginPull executes an origin pull → cache write → dht.Provide
// pipeline for d. Caller owns the inflight handle and must arrange for
// Done() to be called exactly once; we do that here on every exit path.
//
// §5.8 wiring:
//   - Terminal origin errors are classified via *ifaces.OriginError and
//     recorded into the negative cache so the next probe surfaces
//     recently_failed.
//   - I/O / cache-side failures (copy + commit) are recorded as
//     FailureTransient: they are not the origin's fault, but treating
//     them as transient blocks the cluster from re-hammering the same
//     puller on a flapping local disk while still self-healing.
//   - On commit success, we clear any prior entry so the ladder resets
//     for the next failure run.
func runOriginPull(originClient ifaces.OriginPuller, cstore ifaces.Cache, disco *discovery.Host, neg *negcache.Cache, lg *slog.Logger, h *inflight.Handle, registry, repository string, d digest.Digest, kind ifaces.OriginRefKind, onProvideErr func()) {
	defer h.Done()

	// Background context: the requesting peer's stream is already
	// closed by the time we get here. We bound the pull by a budget
	// so a hung origin can't leak the in-flight slot forever, but
	// the 5-minute fixed ceiling from earlier was too tight for
	// real-world image sizes (e.g. a 5 GB GPU image at the §7-default
	// 10 MB/s throughput floor needs ~8.5 min on its own). Start with
	// a default budget that covers HEAD/auth and small blobs, then
	// extend post-Pull once we know expectedSize.
	const (
		originPullDefaultBudget = 5 * time.Minute
		originPullMinThroughput = 10 * 1024 * 1024 // 10 MB/s, matches the §7 stall-detection floor
		originPullCeiling       = 30 * time.Minute // absolute ceiling so a stuck pull still releases the slot
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	budget := time.AfterFunc(originPullDefaultBudget, cancel)
	defer budget.Stop()

	ref := ifaces.OriginRef{
		Registry:   registry,
		Repository: repository,
		Digest:     d,
		Kind:       kind,
	}
	rc, expectedSize, err := originClient.Pull(ctx, ref)
	if err != nil {
		recordOriginFailure(neg, d, err, lg, "origin pull failed", registry, repository)
		return
	}
	defer func() { _ = rc.Close() }()

	// Extend the budget based on expectedSize / floor-throughput. The
	// default-budget slack is kept on top so the io.Copy starts with
	// at least originPullDefaultBudget of headroom regardless of size.
	if expectedSize > 0 {
		needed := time.Duration(expectedSize/originPullMinThroughput)*time.Second + originPullDefaultBudget
		if needed > originPullCeiling {
			needed = originPullCeiling
		}
		if needed > originPullDefaultBudget {
			budget.Reset(needed)
		}
	}

	w, err := cstore.Writer(ctx, d)
	if err != nil {
		recordOriginFailure(neg, d, err, lg, "cache writer open failed", registry, repository)
		return
	}
	defer func() { _ = w.Abort(ctx) }()

	if _, err := io.Copy(w, rc); err != nil {
		recordOriginFailure(neg, d, err, lg, "origin pull copy failed", registry, repository)
		return
	}
	if err := w.Commit(ctx); err != nil {
		recordOriginFailure(neg, d, err, lg, "cache commit failed (digest mismatch or io error)", registry, repository)
		return
	}

	// Success: clear any prior negative-cache entry so the next
	// failure starts the ladder from Initial again (§5.8 "Self-healing").
	if neg != nil {
		neg.RecordSuccess(d)
	}

	// Advertise the new digest so peer requesters can discover us
	// through the warm path.
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer provCancel()
	if err := disco.Provide(provCtx, d); err != nil {
		if onProvideErr != nil {
			onProvideErr()
		}
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

// recordOriginFailure classifies err and records the failure into the
// per-puller §5.8 negative cache. Non-§5.8 callers (e.g. cache I/O
// errors not covered by *ifaces.OriginError) are bucketed as
// FailureTransient: see runOriginPull's docs for why we still record
// them. The log is emitted at WARN regardless of class.
func recordOriginFailure(neg *negcache.Cache, d digest.Digest, err error, lg *slog.Logger, msg, registry, repository string) {
	class := ifaces.FailureTransient
	var oe *ifaces.OriginError
	if errors.As(err, &oe) && oe.Class != ifaces.FailureUnspecified {
		class = oe.Class
	}
	lg.Warn(msg,
		slog.String("digest", d.String()),
		slog.String("registry", registry),
		slog.String("repository", repository),
		slog.String("failure_class", string(class)),
		slog.Any("err", err),
	)
	if neg != nil {
		neg.RecordFailure(d, class)
	}
}

// negCacheAdapter bridges *negcache.Cache to coord.NegativeCache.
// Required because internal/negcache must not import internal/coord
// (would cycle on the metric hooks the coord server uses).
type negCacheAdapter struct{ c *negcache.Cache }

func (a negCacheAdapter) Lookup(d digest.Digest) (coord.NegativeEntry, bool) {
	e, ok := a.c.Lookup(d)
	if !ok {
		return coord.NegativeEntry{}, false
	}
	return coord.NegativeEntry{
		CooldownUntil: e.CooldownUntil,
		Class:         e.Class,
	}, true
}

// phase4Metrics groups the §7.6 instruments owned by Phase 4: the §5.8
// negative-cache entry gauge + hit counters, and the §5.6 designated-
// puller takeover counter. The takeover counter is incremented from
// the cold-start orchestrator (requester side); the cache metrics
// come from negcache.Cache callbacks (puller side).
type phase4Metrics struct {
	size                          atomic.Int64
	hits                          *prometheus.CounterVec
	enters                        *prometheus.CounterVec
	designatedPullerTakeoverTotal *prometheus.CounterVec
}

func newPhase4Metrics(reg *metrics.Registry) *phase4Metrics {
	p := &phase4Metrics{}
	_ = reg.NewGaugeFunc("coord", prometheus.GaugeOpts{
		Name: "p2p_negative_cache_entries",
		Help: "Active §5.8 negative-cache entries on this puller (per-digest cooldowns).",
	}, func() float64 { return float64(p.size.Load()) })
	p.hits = reg.NewCounterVec("coord", prometheus.CounterOpts{
		Name: "p2p_negative_cache_hit_total",
		Help: "Lookups against the §5.8 negative cache that returned an active cooldown, by failure class.",
	}, []string{"class"})
	p.enters = reg.NewCounterVec("coord", prometheus.CounterOpts{
		Name: "p2p_negative_cache_enter_total",
		Help: "New or extended §5.8 negative-cache entries by failure class.",
	}, []string{"class"})
	p.designatedPullerTakeoverTotal = reg.NewCounterVec("coord", prometheus.CounterOpts{
		Name: "p2p_designated_puller_takeover_total",
		Help: "Cold-start observations where the rank-0 puller's in-flight pull was older than the §5.2a stall threshold, triggering a §5.6 takeover by the next-ranked node.",
	}, []string{"digest_kind"})
	return p
}

func (p *phase4Metrics) observeEnter(class ifaces.FailureClass) {
	p.enters.WithLabelValues(failureClassLabel(class)).Inc()
}

func (p *phase4Metrics) observeHit(class ifaces.FailureClass) {
	p.hits.WithLabelValues(failureClassLabel(class)).Inc()
}

func (p *phase4Metrics) setSize(n int) { p.size.Store(int64(n)) }

func failureClassLabel(c ifaces.FailureClass) string {
	if c == ifaces.FailureUnspecified {
		return "unspecified"
	}
	return string(c)
}

// phase5Metrics groups the §7.6 instruments owned by Phase 5: the
// DHT health gauge, NF5 direct-origin fallback counter, and top-K
// expansion counter.
type phase5Metrics struct {
	originFallbackTotal        prometheus.Counter
	originFallbackDeclineTotal *prometheus.CounterVec
	topkExpansionTotal         *prometheus.CounterVec
}

func newPhase5Metrics(reg *metrics.Registry, healthScore func() float64) *phase5Metrics {
	p := &phase5Metrics{}
	_ = reg.NewGaugeFunc("discovery", prometheus.GaugeOpts{
		Name: "p2p_dht_health_score",
		Help: "§7.7 geometric-mean DHT health score in [0, 1] (routing-table coverage × p95 lookup latency score × self-test success rate).",
	}, healthScore)
	p.originFallbackTotal = reg.NewCounter("mirror", prometheus.CounterOpts{
		Name: "p2p_origin_fallback_total",
		Help: "§5.7 NF5 direct-origin fallback pulls (last-resort path after cold-start exhaustion).",
	})
	p.originFallbackDeclineTotal = reg.NewCounterVec("mirror", prometheus.CounterOpts{
		Name: "p2p_origin_fallback_decline_total",
		Help: "§5.7 NF5 gating-sequence declines by reason. Without this counter a never-firing NF5 looks identical in metrics to a never-eligible NF5.",
	}, []string{"reason"})
	p.topkExpansionTotal = reg.NewCounterVec("coord", prometheus.CounterOpts{
		Name: "p2p_topk_expansion_total",
		Help: "Cold-start cascade expansions from top-K to top-(K × factor) by reason (degraded DHT, all top-K unreachable).",
	}, []string{"reason"})
	return p
}

// phase6Metrics groups the §7.6 instruments owned by Phase 6. Currently
// just the forced-eviction counter described in §7.4: every increment
// records one §7.4-headroom-driven eviction that bypassed the
// provider-count deferral.
type phase6Metrics struct {
	cacheForcedEvictionTotal prometheus.Counter
}

func newPhase6Metrics(reg *metrics.Registry) *phase6Metrics {
	return &phase6Metrics{
		cacheForcedEvictionTotal: reg.NewCounter("cache", prometheus.CounterOpts{
			Name: "p2p_cache_forced_eviction_total",
			Help: "§7.4 forced cache evictions: increments when free disk fell below cache_budget × cache_forced_eviction_headroom_pct and the agent evicted an LRU candidate regardless of its provider count.",
		}),
	}
}
