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
	"github.com/gantry/gantry/internal/config"
	gantrylog "github.com/gantry/gantry/internal/gantrylog"
	"github.com/gantry/gantry/internal/metrics"
	"github.com/gantry/gantry/internal/mirror"
	"github.com/gantry/gantry/internal/origin"
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

	// Metrics registry + Phase 1 instruments.
	reg := metrics.New()
	reg.RegisterDefaultCollectors()
	inst := newPhase1Metrics(reg)

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

	// Mirror.
	mirrorSrv := mirror.New(c, cstore, originClient,
		mirror.WithLogger(logger),
		mirror.WithMetrics(
			func() {}, // cache hit already counted by cache hook
			func() {}, // cache miss already counted by cache hook
			func(kind string) { inst.originPullTotal.WithLabelValues(kind).Inc() },
			func(class string) { inst.originFailureTotal.WithLabelValues(class).Inc() },
		),
	)

	// Servers: mirror + metrics. Transfer endpoint and libp2p host land in
	// Phase 2; metrics-only-listener is started now so scrapes work even
	// before P2P is wired.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mirrorStop, err := mirrorSrv.ListenAndServe(c.MirrorListen)
	if err != nil {
		return fmt.Errorf("mirror listen: %w", err)
	}
	logger.Info("mirror endpoint listening", slog.String("addr", c.MirrorListen))

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
	}

	// Graceful shutdown with a 10s budget.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := mirrorStop(shutdownCtx); err != nil {
		logger.Warn("mirror shutdown error", slog.Any("err", err))
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
