// Package mirror is the loopback OCI registry mirror containerd talks to
// via hosts.toml (detailed-design.md §7.1).
//
// Phase 1 endpoint contract (cited from archecture.md §API and
// detailed-design.md §5.1a, §7.1):
//
//	GET /v2/                                         200, {"api":"registry/2.0"}
//	GET /healthz                                     200, "ok"
//	GET /v2/<repo>/manifests/<tag>                   503, empty body
//	GET /v2/<repo>/manifests/sha256:<hex>            cache or origin
//	GET /v2/<repo>/blobs/sha256:<hex>                cache or origin
//
// The tag-manifests 503 is the §5.1a "tag fallthrough" — hosts.toml lists
// origin as the next entry, so containerd retries against origin directly.
// Returning 503 (NOT 404) is load-bearing: hosts.toml only falls through
// on 5xx, NOT on 4xx. Returning the wrong code breaks tag-resolution.
//
// ?ns=<registry> routing (§7.1): containerd adds ?ns=<host> to every
// request when hosts.toml specifies `server=<origin>`. If exactly one
// upstream is configured, ?ns= is optional (and ignored if present). When
// more than one upstream is configured, ?ns= MUST match one of them or
// the request returns 404 — there is no safe default.
package mirror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/digestpipe"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/oci"
)

// Server is the mirror HTTP handler.
type Server struct {
	cfg     *config.Config
	cache   ifaces.Cache
	origin  ifaces.OriginPuller
	logger  *slog.Logger
	metrics metricsHooks

	// Phase 2 dependencies — nil-safe. When both dht and peer are set,
	// the cache miss path tries DHT-discovered providers before origin.
	dht  ifaces.DHT
	peer ifaces.PeerDialer

	// Phase 3 cold-start orchestrator (§5.2 7-rule cascade). When set,
	// it is consulted when DHT.FindProviders returns an empty provider
	// set, before the request falls through to origin.
	coldStart ColdStartResolver

	// Phase 5 NF5 direct-origin fallback controller (§5.7). When set,
	// the mirror is permitted to do a controlled direct origin pull
	// after the cold-start cascade reports ErrColdStartExhausted (and
	// the NF5 gating sequence passes). When nil, cold-start exhaustion
	// always returns 5xx.
	nf5 *NF5Controller

	// Speculative layer prefetcher (§5.2 detailed-design L332 / archecture
	// L180). When set, every successful manifest serve fires a
	// fire-and-forget OnManifestServed callback so the prefetcher can
	// parse the body, group child digests by HRW rank-0 puller, and
	// issue batched please_pull RPCs before containerd asks for the
	// layers. Nil-safe.
	prefetcher LayerPrefetcher

	// Phase 2 tunables (zero values fall back to package defaults).
	peerLookupBudget time.Duration
	peerFetchBudget  time.Duration
	maxPeerAttempts  int

	// defaultUpstream is the upstream to use when exactly one is
	// configured and ?ns= is absent.
	defaultUpstream string

	// draining is set to true via Drain() when the agent is shutting
	// down. Once true, every /v2/ request returns 503 immediately so
	// containerd's hosts.toml falls through to origin (§Phase 6
	// graceful-shutdown contract). The check is layered ON TOP of
	// http.Server.Shutdown so that even keep-alive connections that
	// the kernel has already accepted get a 503 instead of normal
	// handling once Drain() has fired.
	draining atomic.Bool

	// startupGated, together with `ready`, implements the §Phase 6
	// startup mirror gate. The mirror's TCP listener accepts traffic
	// from containerd's hostPort plumbing the moment ListenAndServe
	// returns — well before /readyz can pass (members informer sync,
	// DHT routing-table convergence, self-announce patch, cache
	// scan). Without a handler-level gate, image pulls during the
	// startup window would race the agent's own bootstrap: the
	// DHT-empty branch would route to origin instead of to the
	// coordinated cold-start path, and every restarting pod would
	// add its own direct origin pulls to the cluster's total. That
	// silently shreds the F1 invariant for the duration of the
	// startup window.
	//
	// startupGated is set by WithStartupReadinessGate; when set, the
	// /v2/ handler returns 503 (containerd hosts.toml falls through
	// to origin for THAT request, exactly the same as the shutdown
	// drain) until MarkReady() is called. Default-false so existing
	// test fixtures (which build Server without the option) continue
	// to serve immediately.
	startupGated bool

	// ready is a sticky atomic flag: false until MarkReady() is
	// called once, then true forever. Sticky so a /readyz blip
	// (e.g. DHT routing table briefly empty during informer churn)
	// does NOT take the mirror out of service mid-rollout — the
	// startup gate is a one-shot 'wait for first ready' and Drain
	// handles graceful shutdown separately.
	ready atomic.Bool
}

// Drain flips the mirror into shutdown mode: new /v2/ requests return
// 503 immediately. Idempotent. Safe to call from a signal handler.
func (s *Server) Drain() { s.draining.Store(true) }

// MarkReady flips the startup gate from "not yet ready" to "serving"
// for production deployments that opted into WithStartupReadinessGate.
// Sticky: subsequent /readyz flaps do NOT take the mirror back out of
// service — once we have decided to serve we stay serving until Drain.
// Safe to call multiple times; safe to call from any goroutine. No-op
// for Servers that did not opt into the startup gate.
func (s *Server) MarkReady() { s.ready.Store(true) }

type metricsHooks struct {
	onCacheHit                func()
	onCacheMiss               func()
	onOriginSuccess           func(kind string, bytes int64)
	onOriginDownstreamFailure func(kind, class string)
	onPeerFetch               func(outcome string)
	onPeerFetchLatency        func(outcome string, d time.Duration)
	onPeerDialResult          func(success bool)
	onDhtLookup               func(outcome string, dur time.Duration)
	onProvideError            func(op string)
}

// Option configures Server construction.
type Option func(*Server)

// WithLogger plumbs a structured logger into the mirror handler.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		if l != nil {
			s.logger = l.With(slog.String("subsystem", "mirror"))
		}
	}
}

// WithMetrics registers metric callbacks for cache hit and cache
// miss observed by the mirror. The origin pull-family counters are
// intentionally NOT plumbed here — they're split across origin and
// mirror to keep one source of truth per counter:
//
//   - p2p_origin_pull_total{kind} and p2p_origin_failure_total{class}
//     belong to origin.WithMetrics in the origin Client. Origin is
//     the single chokepoint that both the mirror direct-origin path
//     and the coordinated please_pull / runOriginPull goroutine
//     route through, so counting there means dashboards see one
//     source of truth and the operator-facing "is origin sick?"
//     alert (p2p_origin_failure_total) stays consistent across both
//     paths and free of false positives from downstream failures.
//   - p2p_origin_pull_success_total{kind} belongs to the mirror
//     (WithOriginSuccessMetric) because origin can't know whether
//     the caller actually committed bytes — see that option's doc.
//   - p2p_origin_pull_failure_total{kind,class} is fed from BOTH
//     halves: origin's failure hook bumps it on true origin-side
//     failures (with double-bump of p2p_origin_failure_total), and
//     the mirror's WithDownstreamFailureMetric bumps it on
//     downstream failures (with class=transient, NO double-bump of
//     p2p_origin_failure_total).
//
// Counting any of these at the mirror's WithMetrics hook would
// silently undercount the please_pull-coordinated path (the bulk of
// pulls on a hot cluster) and break the started == success + failure
// + in-flight arithmetic identity that all three counters rely on.
func WithMetrics(cacheHit, cacheMiss func()) Option {
	return func(s *Server) {
		s.metrics.onCacheHit = cacheHit
		s.metrics.onCacheMiss = cacheMiss
	}
}

// WithOriginSuccessMetric registers a callback fired by the mirror's
// direct-origin path AFTER it has streamed the response body to
// completion AND committed the bytes to cache (or, when cache is
// unavailable, AFTER the direct-stream digest verifier confirms the
// served bytes match the requested digest). The kind label uses the
// design-doc Prometheus vocabulary (see ifaces.OriginRefKind.MetricLabel).
//
// This hook is the mirror-side half of the origin-success contract:
// origin.Client.Pull no longer reports success itself because it has
// no way to know whether the caller actually drained and verified the
// stream. HEAD requests (which by design never read the body),
// io.Copy interruptions, and cache-commit failures all leave the
// response body Closed without a real success — so reporting success
// on Close() inside origin.Client inflated p2p_origin_pull_success_total
// against operations that never produced a usable byte. The puller
// pump's runOriginPull owns the equivalent hook on the
// please_pull-coordinated path; together they're the two places that
// know what "the origin pull actually succeeded" means.
func WithOriginSuccessMetric(originSuccess func(kind string, bytes int64)) Option {
	return func(s *Server) {
		s.metrics.onOriginSuccess = originSuccess
	}
}

// WithDownstreamFailureMetric registers a callback fired by the
// mirror's direct-origin path when the body has been received from
// origin but a DOWNSTREAM step (io.Copy stall, cw.Commit digest
// mismatch / cache I/O error, directVerifier mismatch) fails before
// the cluster has produced a usable artifact.
//
// Why this is separate from the origin failure-hook
// (origin.WithMetrics' failure closure in cmd/gantry/main.go):
//   - origin.WithMetrics' failure closure is the origin-side
//     terminal counter — it bumps BOTH p2p_origin_pull_failure_total
//     (operator dashboards) AND p2p_origin_failure_total (the
//     "is origin sick?" alert). Origin-side failures are the
//     ones where the origin pull never started, never returned
//     2xx, or returned a non-2xx body. Counting downstream
//     failures (where origin DID return 2xx but the body
//     stalled / corrupted en route to the cache) against the
//     same closure would falsely accuse origin of being sick.
//   - This hook bumps ONLY p2p_origin_pull_failure_total
//     (per-(kind,class) detail) with class="transient", leaving
//     p2p_origin_failure_total reserved for true origin-side
//     failures. Operators see the failure detail without the
//     alert false-positive.
//
// Together with onOriginSuccess and the origin-side failure
// closure, this restores the per-pull arithmetic identity
// for the GET path:
//
//	p2p_origin_pull_total{kind}  ==  p2p_origin_pull_success_total{kind}
//	                              +  p2p_origin_pull_failure_total{kind,class=any}
//	                              +  (in-flight at scrape time)
//
// The twelfth code review flagged the missing terminal counter
// for downstream failures as the second of the two reasons that
// identity drifted positive in production traces. (The first
// was HEAD, fixed in Batch 61 by adding origin.Head.)
func WithDownstreamFailureMetric(downstreamFailure func(kind, class string)) Option {
	return func(s *Server) {
		s.metrics.onOriginDownstreamFailure = downstreamFailure
	}
}

// WithPeerMetrics registers Phase 2 peer-fallback metric callbacks.
// peerFetchOutcome is invoked with one of: "hit", "notfound", "error",
// "stall". peerDialResult is invoked per attempted dial.
func WithPeerMetrics(peerFetchOutcome func(outcome string), peerDialResult func(success bool)) Option {
	return func(s *Server) {
		s.metrics.onPeerFetch = peerFetchOutcome
		s.metrics.onPeerDialResult = peerDialResult
	}
}

// WithPeerFetchLatencyMetric registers a hook that fires once per
// fetchOneProvider call with the terminal outcome label and the
// wall-clock time from the FetchFromPeer dial to either the cache
// commit (hit) or the failing-branch return. Used for the
// p2p_peer_fetch_duration_seconds{outcome} histogram so operators can
// see whether peer fetches are slow because of dial latency, body
// streaming, or commit-time digest verification.
func WithPeerFetchLatencyMetric(onPeerFetchLatency func(outcome string, d time.Duration)) Option {
	return func(s *Server) {
		s.metrics.onPeerFetchLatency = onPeerFetchLatency
	}
}

// WithDhtLookupMetric registers a hook that fires once per FindProviders
// call with the outcome label ("hit", "miss", "timeout", "error") and the
// observed lookup duration. Used to populate p2p_dht_lookup_total and
// p2p_dht_lookup_duration_seconds (§7.6).
func WithDhtLookupMetric(onLookup func(outcome string, dur time.Duration)) Option {
	return func(s *Server) {
		s.metrics.onDhtLookup = onLookup
	}
}

// WithProvideErrorMetric registers a hook that fires when the mirror's
// post-peer-fetch dht.Provide call fails. The hook receives a stable
// label string identifying the call site so a CounterVec keyed by `op`
// can distinguish mirror-internal Provide failures from other sites.
func WithProvideErrorMetric(onProvideErr func(op string)) Option {
	return func(s *Server) {
		s.metrics.onProvideError = onProvideErr
	}
}

// WithDiscovery wires Phase 2 P2P fetch: cache miss → DHT FindProviders →
// PeerDialer.FetchFromPeer (across up to 3 providers) → origin fallback.
// Either argument nil disables P2P fallback entirely (Phase 1 behavior).
func WithDiscovery(d ifaces.DHT, peer ifaces.PeerDialer) Option {
	return func(s *Server) {
		s.dht = d
		s.peer = peer
	}
}

// WithPeerBudgets overrides the default Phase 2 peer-path budgets.
// lookup ≤ 0 means "use default 2s"; fetch ≤ 0 means "use default 10s";
// maxAttempts ≤ 0 means "use default 3".
func WithPeerBudgets(lookup, fetch time.Duration, maxAttempts int) Option {
	return func(s *Server) {
		s.peerLookupBudget = lookup
		s.peerFetchBudget = fetch
		s.maxPeerAttempts = maxAttempts
	}
}

// ColdStartResolver is the subset of *coldstart.Resolver that mirror
// needs. Kept narrow for testability — production wires the concrete
// resolver via WithColdStart.
type ColdStartResolver interface {
	Resolve(ctx context.Context, d digest.Digest, kind ifaces.OriginRefKind, registry, repository string, expectedSize int64) (*ColdStartResolution, error)
}

// ColdStartResolution mirrors *coldstart.Resolution at this boundary
// so the mirror package does not import internal/coldstart (which
// would import internal/mirror by transitivity through wiring).
type ColdStartResolution struct {
	Providers []ifaces.Provider
	Outcome   string
}

// WithColdStart wires Phase 3 cold-start orchestration. When set, the
// orchestrator is consulted on the DHT-empty branch of the cache-miss
// path before falling through to origin.
func WithColdStart(c ColdStartResolver) Option {
	return func(s *Server) { s.coldStart = c }
}

// WithNF5 wires the Phase 5 §5.7 direct-origin fallback controller.
// When non-nil and cold-start exits via ErrColdStartExhausted, the
// mirror runs the NF5 gating sequence (jitter, token bucket, dedup,
// re-check) before falling through to a direct origin pull. When nil,
// cold-start exhaustion always returns 5xx.
func WithNF5(c *NF5Controller) Option {
	return func(s *Server) { s.nf5 = c }
}

// LayerPrefetcher is the speculative wire-level optimisation hook
// (§5.2 detailed-design.md L332 / archecture.md L180). After the
// mirror serves a manifest successfully the mirror invokes
// OnManifestServed in a goroutine so an implementation can fetch
// the just-cached manifest body, parse it, identify child
// layer/config digests, group them by HRW rank-0 puller, and issue
// batched please_pull RPCs to warm the cluster before containerd
// asks for the layers. The mirror never waits for the callback to
// return; failures are the prefetcher's to log.
type LayerPrefetcher interface {
	OnManifestServed(ctx context.Context, registry, repository string, manifestDigest digest.Digest)
}

// WithLayerPrefetcher wires a speculative layer prefetcher. Nil-safe.
func WithLayerPrefetcher(p LayerPrefetcher) Option {
	return func(s *Server) { s.prefetcher = p }
}

// WithStartupReadinessGate opts the mirror into the §Phase 6 startup
// gate: until MarkReady() is called, every /v2/ request returns 503
// with reason "agent starting up". Production callers should pair
// this with a goroutine that polls the same conditions /readyz uses
// and calls MarkReady once they converge — see cmd/gantry/main.go's
// readyCheck-poller for the canonical wiring.
//
// Without this option the Server is "ready immediately" so unit-test
// fixtures (which never call MarkReady) continue to behave as before.
// The shutdown drain (Drain / drainGuard) is independent of this
// gate and always installed.
func WithStartupReadinessGate() Option {
	return func(s *Server) { s.startupGated = true }
}

// New builds a Server bound to the given cache and origin.
func New(cfg *config.Config, cache ifaces.Cache, origin ifaces.OriginPuller, opts ...Option) *Server {
	s := &Server{
		cfg:    cfg,
		cache:  cache,
		origin: origin,
		logger: slog.Default().With(slog.String("subsystem", "mirror")),
	}
	for _, opt := range opts {
		opt(s)
	}
	if len(cfg.UpstreamRegistries) == 1 {
		s.defaultUpstream = cfg.UpstreamRegistries[0].Name
	}
	// Default ready=true so unit-test fixtures, which never call
	// MarkReady, continue to serve immediately. Production callers
	// flip startupGated via WithStartupReadinessGate which forces
	// ready=false at construction and gates the /v2/ handler until
	// MarkReady() fires.
	if !s.startupGated {
		s.ready.Store(true)
	}
	return s
}

// Handler returns an http.Handler suitable for serving on cfg.MirrorListen.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	// Order matters: drainGuard runs FIRST so shutdown wins over a
	// concurrent startup transition (we never want to flip from
	// "starting up" back to serving via a stale MarkReady). The
	// startupGate runs INSIDE drainGuard so a still-not-ready agent
	// also returns 503.
	mux.HandleFunc("/v2/", s.drainGuard(s.startupGate(s.handleV2)))
	mux.HandleFunc("/v2", s.drainGuard(s.startupGate(s.handleV2))) // some clients omit trailing slash
	return mux
}

// drainGuard wraps a /v2/ handler so that once Drain() has been called,
// every new request gets a 503 instead of normal handling. §Phase 6:
// "stops accepting new mirror requests with 503". The 503 (not 404)
// is load-bearing — hosts.toml only falls through on 5xx.
func (s *Server) drainGuard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.draining.Load() {
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			http.Error(w, "agent shutting down", http.StatusServiceUnavailable)
			return
		}
		h(w, r)
	}
}

// startupGate returns 503 until MarkReady is called, but only for
// Servers that opted in via WithStartupReadinessGate. The 503 is
// load-bearing in exactly the same way Drain's 503 is: containerd's
// hosts.toml falls through to origin for the un-served request.
// Without this gate the mirror serves /v2/ traffic the moment
// ListenAndServe returns, racing the agent's own DHT/members/coord
// bootstrap and routing every startup-window pull straight to origin
// outside the coordinated cold-start path.
func (s *Server) startupGate(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.ready.Load() {
			w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
			w.Header().Set("Retry-After", "5")
			http.Error(w, "agent starting up", http.StatusServiceUnavailable)
			return
		}
		h(w, r)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

// handleV2 is the OCI Distribution v2 entry point.
func (s *Server) handleV2(w http.ResponseWriter, r *http.Request) {
	// Common headers.
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")

	path := r.URL.Path
	if path == "/v2/" || path == "/v2" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{}`)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	repo, kind, ref, ok := oci.ParseV2Path(path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	upstream, err := s.resolveUpstream(r)
	if err != nil {
		s.logger.Debug("mirror: unknown ?ns=",
			slog.String("ns", r.URL.Query().Get("ns")),
			slog.String("path", path),
		)
		http.NotFound(w, r)
		return
	}

	if !isDigestRef(ref) {
		// Tag request (§5.1a) — fall through to origin via hosts.toml.
		// 503 (not 404) so containerd retries against the next mirror.
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	d, err := digest.Parse(ref)
	if err != nil {
		http.Error(w, "invalid digest", http.StatusBadRequest)
		return
	}

	s.serveDigest(w, r, upstream, repo, d, kind)
}

func (s *Server) resolveUpstream(r *http.Request) (string, error) {
	ns := r.URL.Query().Get("ns")
	if ns == "" {
		if s.defaultUpstream != "" {
			return s.defaultUpstream, nil
		}
		return "", errors.New("mirror: ?ns= is required when multiple upstreams are configured")
	}
	if ur, ok := s.cfg.ResolveUpstream(ns); ok {
		return ur.Name, nil
	}
	return "", fmt.Errorf("mirror: unknown ns=%q", ns)
}

// serveDigest serves a digest-addressed manifest or blob: cache hit, then
// origin pull with stream-and-cache fallback.
func (s *Server) serveDigest(w http.ResponseWriter, r *http.Request, upstream, repo string, d digest.Digest, kind ifaces.OriginRefKind) {
	ctx := r.Context()
	logger := s.logger.With(
		slog.String("registry", upstream),
		slog.String("repo", repo),
		slog.String("digest", d.String()),
		slog.String("kind", kind.String()),
	)

	// 1. Cache lookup.
	rc, size, err := s.cache.Open(ctx, d)
	if err == nil {
		defer func() { _ = rc.Close() }()
		s.bumpCacheHit()
		writeBlobHeaders(w, d, size, kind)
		s.firePrefetch(kind, upstream, repo, d)
		if r.Method == http.MethodHead {
			return
		}
		if _, err := io.Copy(w, rc); err != nil {
			logger.Debug("mirror: copy from cache failed", slog.Any("err", err))
		}
		return
	}
	var enf *ifaces.ErrNotFound
	if !errors.As(err, &enf) {
		logger.Warn("mirror: cache open error", slog.Any("err", err))
	}

	s.bumpCacheMiss()

	// 2. Peer fallback (Phase 2). If both DHT and PeerDialer are wired,
	// try up to maxPeerAttempts providers from FindProviders. The result
	// is tri-state per design §5.1's "v1 transfer policy":
	//   - served: bytes already written from a peer; we're done.
	//   - exhausted: DHT had providers but all maxAttempts failed (stall
	//     or error). Return 5xx so containerd's hosts.toml mirror chain
	//     promotes the request to origin directly. The agent does *not*
	//     do a direct origin pull here (Phase 5 NF5 owns the controlled
	//     direct-origin path).
	//   - unused: DHT not wired, errored, or returned empty providers.
	//     Fall through to Phase 1's origin path; Phase 3's HRW probe
	//     replaces this leg for the cold-start case.
	if s.dht != nil && s.peer != nil {
		switch s.tryPeerFallback(ctx, w, r, d, kind, upstream, repo, logger) {
		case peerFallbackServed:
			s.firePrefetch(kind, upstream, repo, d)
			return
		case peerFallbackExhausted:
			http.Error(w, "warm path exhausted", http.StatusServiceUnavailable)
			return
		case peerFallbackColdExhausted:
			// §5.7 NF5 last-resort: only attempt a direct origin
			// pull when the controller passes its gating sequence
			// (bootstrap done, DHT healthy enough, no dedup
			// collision, token budget, jitter elapsed without
			// recheck finding a provider).
			if s.nf5 == nil {
				http.Error(w, "warm path exhausted", http.StatusServiceUnavailable)
				return
			}
			proceed, release, err := s.nf5.Allow(ctx, d, kind, 0)
			if release != nil {
				defer release()
			}
			if err != nil || !proceed {
				http.Error(w, "warm path exhausted", http.StatusServiceUnavailable)
				return
			}
			// Fall through to the origin pull below.
		}
	}

	// 3. Origin pull, stream-and-cache.
	//
	// Metric placement (twelfth-review correction):
	//   - p2p_origin_pull_total{kind} bumps inside origin.Pull at
	//     entry (via origin.WithMetrics' onPullStart hook).
	//   - p2p_origin_pull_failure_total{kind,class} +
	//     p2p_origin_failure_total{class} bump inside
	//     origin.recordFailure on origin-side terminal failures
	//     (same WithMetrics closure double-bumps both).
	//   - p2p_origin_pull_success_total{kind} bumps HERE in the
	//     mirror after cw.Commit succeeds (and analogously in
	//     runOriginPull after that path's Commit). Success cannot
	//     live in origin because origin has no way to know whether
	//     the caller actually committed the bytes to cache.
	//
	// HEAD takes the separate s.origin.Head path explicitly so it
	// does NOT bump p2p_origin_pull_total: HEAD is metadata-only,
	// it never warms the cache, so counting it as a pull-attempt
	// inflated the started counter against operations that could
	// fire neither success (no commit) nor downstream-failure
	// (no body copy). See origin.Client.Head's comment for the
	// full rationale.
	pRef := ifaces.OriginRef{Registry: upstream, Repository: repo, Digest: d, Kind: kind}
	if r.Method == http.MethodHead {
		// Metadata-only path. Single round-trip to origin to learn
		// the blob's Content-Length, write headers, return. Does
		// NOT touch the cache writer, does NOT bump any pull-family
		// counter. A subsequent GET for the same digest follows the
		// normal cache-miss origin path below and warms the cache
		// then.
		hsize, herr := s.origin.Head(ctx, pRef)
		if herr != nil {
			writeOriginError(w, herr, logger)
			return
		}
		writeBlobHeaders(w, d, hsize, kind)
		return
	}

	pr, psize, perr := s.origin.Pull(ctx, pRef)
	if perr != nil {
		writeOriginError(w, perr, logger)
		return
	}
	defer func() { _ = pr.Close() }()

	cw, cwerr := s.cache.Writer(ctx, d)
	var dest io.Writer
	var directVerifier *digestpipe.Writer // non-nil only when caching is unavailable
	if cwerr == nil {
		defer func() { _ = cw.Abort(ctx) }() // no-op after Commit
		dest = io.MultiWriter(w, cw)
	} else {
		logger.Warn("mirror: cache writer unavailable; serving without caching", slog.Any("err", cwerr))
		// F7 says the cache layer is what enforces digest verification
		// on origin pulls — and cache.Writer wraps the stream in a
		// digestpipe internally before Commit. When that path is
		// unavailable we still need to verify, otherwise an origin
		// returning corrupted bytes (truncation, content-injection
		// proxy, etc.) leaks straight to the client with no detection.
		// We can't unsend the bytes, but logging a digest mismatch
		// here is the only signal operators get that the origin lied.
		directVerifier = digestpipe.New(w)
		dest = directVerifier
	}

	writeBlobHeaders(w, d, psize, kind)
	written, err := io.Copy(dest, pr)
	if err != nil {
		// Bytes already sent; we can't undo. Cache will be aborted by defer.
		// This is a terminal downstream failure: origin returned 2xx
		// and we drained part of the body, but the stream stalled
		// before EOF. Count it against p2p_origin_pull_failure_total
		// (class=transient) so the per-pull arithmetic
		// (started == success + failure + in_flight) holds. We do
		// NOT also bump p2p_origin_failure_total — origin gave us
		// 2xx, the failure is downstream.
		logger.Debug("mirror: copy stalled", slog.Int64("written", written), slog.Any("err", err))
		s.fireOriginDownstreamFailure(kind, ifaces.FailureTransient)
		return
	}
	if directVerifier != nil {
		if verr := directVerifier.Verify(d); verr != nil {
			logger.Error("mirror: origin direct-stream digest mismatch — corrupted bytes were already served to client",
				slog.String("digest", d.String()),
				slog.Int64("written", written),
				slog.Any("err", verr),
			)
			// Corrupted bytes: do NOT count as origin success.
			// The client already got them, but the cluster did
			// not produce a usable cached/verifiable copy. This
			// is a downstream-detected failure (origin returned
			// 2xx; we caught the mismatch via the in-process
			// digestpipe verifier) so it goes to the downstream
			// counter, not to origin's failure family.
			s.fireOriginDownstreamFailure(kind, ifaces.FailureTransient)
			return
		}
		// Direct-stream verifier passed: bytes were delivered to
		// the client AND digest-matched the requested ref. The
		// cluster did not gain a cache entry (cache was
		// unavailable), but the origin pull itself succeeded
		// end-to-end. Count it.
		s.fireOriginSuccess(kind, written)
		return
	}
	if cwerr == nil {
		if err := cw.Commit(ctx); err != nil {
			// The client already got the bytes; cache just doesn't keep them.
			// cw.Commit is where the cache's internal digestpipe runs;
			// a non-nil error here means EITHER cache I/O failed OR
			// the stream's digest didn't match d. Either way it's a
			// terminal downstream failure of THIS pull (no usable
			// cached copy produced) and must move the arithmetic
			// off in-flight. Origin returned 2xx, so we route this
			// to the downstream counter, NOT to the origin failure
			// family.
			logger.Warn("mirror: cache commit failed", slog.Any("err", err))
			s.fireOriginDownstreamFailure(kind, ifaces.FailureTransient)
			return
		}
		// Re-advertise into the DHT now that we hold a byte-identical
		// copy in our cache. Without this, an NF5-eligible direct
		// origin pull leaves the cluster's only known provider record
		// pointing at the origin instead of at this node — defeating
		// the deduplication promise of §5.2 step 7 specifically for
		// the cold-start-exhausted path that just escalated to origin.
		s.reAdvertiseDigest(d, "mirror_origin_announce", logger)
		s.firePrefetch(kind, upstream, repo, d)
		// Bytes streamed AND committed: this is the canonical
		// mirror-direct origin-pull success. Fire AFTER commit
		// (not after Copy) so a commit failure correctly leaves
		// the operation classified as not-yet-successful even
		// though the client already got the bytes.
		s.fireOriginSuccess(kind, written)
	}
}

// peerFallbackResult is the tri-state outcome of tryPeerFallback.
type peerFallbackResult int

// unhealthyDHTHealthThreshold is the DHT health score below which the
// mirror treats exhausted provider sets as likely-stale and consults
// cold-start (§7.7 rule-7) instead of returning 5xx. Matches the
// coldstart package's own health gate so the two layers agree on what
// "unhealthy" means.
const unhealthyDHTHealthThreshold = 0.7

const (
	// peerFallbackUnused means the DHT layer was bypassed: no DHT call
	// fired (caller-gated), or it errored, or it returned no providers.
	// The caller may fall through to origin (Phase 1 behavior).
	peerFallbackUnused peerFallbackResult = iota
	// peerFallbackServed means a peer's bytes were digest-verified,
	// committed to cache, and streamed to the client. Caller must not
	// write further bytes.
	peerFallbackServed
	// peerFallbackExhausted means the DHT returned providers but all
	// maxAttempts of them failed (stall or error), OR the cold-start
	// cascade short-circuited with an error other than
	// ErrColdStartExhausted (failure short-circuit, transient
	// cooldown, etc.). Per §5.1's v1 transfer policy and §5.8's
	// trusted-cluster-wide failure propagation, the mirror must
	// return 5xx — NF5 must NOT fire here.
	peerFallbackExhausted
	// peerFallbackColdExhausted means the cold-start cascade ran to
	// its final ErrColdStartExhausted exit (no cache, no in-flight,
	// no provider returned by HRW + DHT, both top-K and top-2K
	// already tried). NF5 direct-origin fallback is eligible to fire
	// — and only here.
	peerFallbackColdExhausted
)

// tryPeerFallback attempts to satisfy a cache miss via a DHT-discovered
// peer. Returns one of peerFallbackResult above. No bytes are written to
// w until a peer's body is digest-verified and committed to the local
// cache, so non-served returns guarantee no partial response has been
// emitted.
func (s *Server) tryPeerFallback(ctx context.Context, w http.ResponseWriter, r *http.Request, d digest.Digest, kind ifaces.OriginRefKind, upstream, repo string, logger *slog.Logger) peerFallbackResult {
	lookupBudget := s.peerLookupBudget
	if lookupBudget <= 0 {
		lookupBudget = 2 * time.Second
	}
	fetchBudget := s.peerFetchBudget
	if fetchBudget <= 0 {
		fetchBudget = 10 * time.Second
	}
	maxAttempts := s.maxPeerAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	lookupCtx, cancel := context.WithTimeout(ctx, lookupBudget)
	lookupStart := time.Now()
	providers, err := s.dht.FindProviders(lookupCtx, d)
	lookupDur := time.Since(lookupStart)
	lookupCtxErr := lookupCtx.Err()
	cancel()
	switch {
	case err != nil:
		outcome := "error"
		if errors.Is(lookupCtxErr, context.DeadlineExceeded) {
			outcome = "timeout"
		}
		s.bumpDhtLookup(outcome, lookupDur)
		logger.Debug("mirror: FindProviders error", slog.Any("err", err))
	case len(providers) == 0:
		s.bumpDhtLookup("miss", lookupDur)
	default:
		s.bumpDhtLookup("hit", lookupDur)
	}
	// §7.7 rule-7: when DHT yields no usable provider list — either it
	// errored (timeout / network glitch) or returned an empty set —
	// consult cold-start so the request still flows through NF5
	// rate-limiting rather than stampeding the origin. Cold-start has
	// independent provider sources (HRW + membership informer,
	// in-flight dedup, local cache) that don't depend on DHT, so it can
	// still produce a useful answer even when the DHT layer is down.
	if err != nil || len(providers) == 0 {
		if s.coldStart == nil {
			return peerFallbackUnused
		}
		res, csErr := s.coldStart.Resolve(ctx, d, kind, upstream, repo, 0)
		if csErr != nil {
			logger.Debug("mirror: cold-start exhausted",
				slog.Bool("after_dht_error", err != nil),
				slog.Any("err", csErr),
			)
			// Only ErrColdStartExhausted (rule-7 cascade truly
			// exhausted) makes the request eligible for NF5
			// direct-origin fallback. Failure short-circuit and
			// transient cooldown intentionally short-circuit to
			// 5xx without an origin escape valve.
			if errors.Is(csErr, ErrColdStartExhausted) {
				return peerFallbackColdExhausted
			}
			return peerFallbackExhausted
		}
		providers = res.Providers
	}

	tried := 0
	for _, p := range providers {
		if tried >= maxAttempts {
			break
		}
		tried++
		if s.fetchOneProvider(ctx, w, r, d, kind, upstream, repo, p, fetchBudget, logger) {
			return peerFallbackServed
		}
	}
	// All initial peer attempts failed. If the DHT is unhealthy AND we
	// have a cold-start orchestrator, treat the provider set we just
	// drained as stale and consult cold-start so rule-7 can unblock
	// origin (§7.7). An unhealthy DHT doesn't re-publish provider
	// records reliably, so persisting with "5xx because providers
	// existed" leaves the caller stuck behind dead DHT entries.
	if tried > 0 && s.coldStart != nil && s.dht.Health() < unhealthyDHTHealthThreshold {
		logger.Debug("mirror: peer providers exhausted under unhealthy DHT, consulting cold-start",
			slog.Float64("dht_health", s.dht.Health()),
			slog.Int("attempted", tried),
		)
		res, csErr := s.coldStart.Resolve(ctx, d, kind, upstream, repo, 0)
		if csErr != nil {
			if errors.Is(csErr, ErrColdStartExhausted) {
				return peerFallbackColdExhausted
			}
			return peerFallbackExhausted
		}
		// Cold-start surfaced fresh providers (rule-2 expansion).
		// Give them the remaining maxAttempts budget; collisions with
		// the already-tried set are unlikely because cold-start
		// re-runs FindProviders with an expanded scope.
		for _, p := range res.Providers {
			if tried >= maxAttempts*2 {
				break
			}
			tried++
			if s.fetchOneProvider(ctx, w, r, d, kind, upstream, repo, p, fetchBudget, logger) {
				return peerFallbackServed
			}
		}
	}
	return peerFallbackExhausted
}

// fetchOneProvider streams from one peer into the local cache (which
// verifies the digest on Commit) and, on success, serves from cache. Any
// failure path returns false so the caller can try the next provider; no
// bytes are written to w until the digest verifies.
func (s *Server) fetchOneProvider(ctx context.Context, w http.ResponseWriter, r *http.Request, d digest.Digest, kind ifaces.OriginRefKind, upstream, repo string, p ifaces.Provider, fetchBudget time.Duration, logger *slog.Logger) bool {
	pCtx, cancel := context.WithTimeout(ctx, fetchBudget)
	defer cancel()

	fetchStart := time.Now()
	pRef := ifaces.OriginRef{Registry: upstream, Repository: repo, Digest: d, Kind: kind}
	rc, _, err := s.peer.FetchFromPeer(pCtx, p.Addr, pRef)
	if err != nil {
		var enf *ifaces.ErrNotFound
		if errors.As(err, &enf) {
			s.bumpPeerDial(true)
			s.bumpPeerFetch("notfound")
			s.bumpPeerFetchLatency("notfound", fetchStart)
			logger.Debug("mirror: peer 404",
				slog.String("peer", p.Addr),
				slog.String("node", string(p.NodeID)),
			)
			return false
		}
		s.bumpPeerDial(false)
		s.bumpPeerFetch("error")
		s.bumpPeerFetchLatency("error", fetchStart)
		logger.Debug("mirror: peer fetch error",
			slog.String("peer", p.Addr),
			slog.Any("err", err),
		)
		return false
	}
	defer func() { _ = rc.Close() }()
	s.bumpPeerDial(true)

	cw, cwerr := s.cache.Writer(pCtx, d)
	if cwerr != nil {
		s.bumpPeerFetch("error")
		s.bumpPeerFetchLatency("error", fetchStart)
		logger.Warn("mirror: cache writer unavailable for peer fetch", slog.Any("err", cwerr))
		return false
	}
	defer func() { _ = cw.Abort(pCtx) }()

	if _, err := io.Copy(cw, rc); err != nil {
		s.bumpPeerFetch("stall")
		s.bumpPeerFetchLatency("stall", fetchStart)
		logger.Debug("mirror: peer copy stalled",
			slog.String("peer", p.Addr),
			slog.Any("err", err),
		)
		return false
	}
	if err := cw.Commit(pCtx); err != nil {
		s.bumpPeerFetch("error")
		s.bumpPeerFetchLatency("error", fetchStart)
		logger.Warn("mirror: peer commit failed (likely digest mismatch)",
			slog.String("peer", p.Addr),
			slog.Any("err", err),
		)
		return false
	}

	// Re-advertise this digest into the DHT now that we've cached a
	// byte-identical copy. Without this, peer-fetched blobs were
	// discoverable only via the source peer's announcements, so the
	// provider set never grew — defeating the deduplication promise
	// of the design (detailed-design §5.2 step 7). Fire-and-forget
	// with a 30s budget; bg ctx so client cancellation can't abort
	// the announcement.
	s.reAdvertiseDigest(d, "peer_fetch_readvertise", logger)

	// Re-open from cache and stream verified bytes to the client.
	rcLocal, size, err := s.cache.Open(ctx, d)
	if err != nil {
		s.bumpPeerFetch("error")
		s.bumpPeerFetchLatency("error", fetchStart)
		logger.Warn("mirror: post-commit cache open failed", slog.Any("err", err))
		return false
	}
	defer func() { _ = rcLocal.Close() }()
	s.bumpPeerFetch("hit")
	s.bumpPeerFetchLatency("hit", fetchStart)
	writeBlobHeaders(w, d, size, kind)
	if r.Method == http.MethodHead {
		return true
	}
	if _, err := io.Copy(w, rcLocal); err != nil {
		logger.Debug("mirror: copy from cache (post-peer) failed", slog.Any("err", err))
	}
	return true
}

// firePrefetch invokes the LayerPrefetcher (if any) in a goroutine
// when kind is a manifest. The mirror does NOT wait for the
// callback; the prefetcher's job is to read the manifest body from
// cache and dispatch batched please_pull RPCs entirely in the
// background.
func (s *Server) firePrefetch(kind ifaces.OriginRefKind, registry, repository string, d digest.Digest) {
	if s.prefetcher == nil || kind != ifaces.KindManifest {
		return
	}
	go s.prefetcher.OnManifestServed(context.Background(), registry, repository, d)
}

// reAdvertiseDigest does a fire-and-forget dht.Provide(d) in a
// goroutine with a 30s budget. The op label distinguishes the call
// site for the p2p_dht_provide_error_total{op} counter; common
// values are "peer_fetch_readvertise" (mirror peer-fetch success
// path) and "mirror_origin_announce" (NF5-eligible direct-origin
// pull success path). The background context shields the announce
// from client cancellation.
func (s *Server) reAdvertiseDigest(d digest.Digest, op string, logger *slog.Logger) {
	if s.dht == nil {
		return
	}
	dHash := d
	go func() {
		provCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if perr := s.dht.Provide(provCtx, dHash); perr != nil {
			if s.metrics.onProvideError != nil {
				s.metrics.onProvideError(op)
			}
			logger.Debug("mirror: post-commit dht.Provide failed",
				slog.String("op", op),
				slog.String("digest", dHash.String()),
				slog.Any("err", perr),
			)
		}
	}()
}

// bumpCacheHit / bumpCacheMiss are no-ops if no metric hooks were
// registered. There is intentionally no bumpOriginPull,
// bumpOriginFailure, bumpOriginSuccess, or bumpOriginDownstreamFailure
// helper here. The origin pull-family counters are split deliberately
// across two packages (see the WithMetrics, WithOriginSuccessMetric,
// and WithDownstreamFailureMetric doc comments):
//
//   - p2p_origin_pull_total{kind}            : bumped by origin.Client
//     at Pull entry (via origin.WithMetrics' onPullStart hook).
//     origin.Client.Head deliberately does NOT bump this counter.
//   - p2p_origin_pull_failure_total{kind,class} : bumped from two
//     places — origin.Client.recordFailure (true origin-side
//     failures: non-2xx, network errors) AND the mirror's
//     fireOriginDownstreamFailure (downstream failures after
//     origin returned 2xx: io.Copy stall, cw.Commit, directVerifier).
//   - p2p_origin_failure_total{class}        : bumped ONLY by
//     origin.Client.recordFailure (true origin-side failures).
//     Reserved for the "is the origin sick?" operator alert; the
//     mirror's downstream-failure hook does NOT bump it.
//   - p2p_origin_pull_success_total{kind}    : bumped by the mirror
//     (fireOriginSuccess) after cw.Commit succeeds or the direct-
//     stream digest verifier passes, AND by the puller pump's
//     runOriginPull after that path's cw.Commit. Origin cannot
//     emit success because origin has no way to know whether the
//     caller actually committed bytes.
//
// Together this layout preserves the per-pull arithmetic identity:
//
//	started == success + (origin_failure + downstream_failure) + in_flight
//
// while keeping the operator-facing "origin sick" alert
// (p2p_origin_failure_total) free of false positives from local
// cache I/O errors or origin truncations.
func (s *Server) bumpCacheHit() {
	if s.metrics.onCacheHit != nil {
		s.metrics.onCacheHit()
	}
}
func (s *Server) bumpCacheMiss() {
	if s.metrics.onCacheMiss != nil {
		s.metrics.onCacheMiss()
	}
}

// fireOriginSuccess emits p2p_origin_pull_success_total via the hook
// registered with WithOriginSuccessMetric. Call sites must invoke
// this AFTER the response body has been streamed AND the cluster has
// produced a useful artifact from it (cache commit OK, or
// direct-stream digest verifier passed when cache is unavailable).
// Calling it earlier — e.g. inside a deferred Close() on the origin
// reader — inflates the success counter against HEAD requests, io.Copy
// interruptions, and cache-commit failures (the exact bug the
// eleventh review flagged as "false positives on the success metric").
func (s *Server) fireOriginSuccess(kind ifaces.OriginRefKind, bytes int64) {
	if s.metrics.onOriginSuccess == nil {
		return
	}
	s.metrics.onOriginSuccess(kind.MetricLabel(), bytes)
}

// fireOriginDownstreamFailure emits the per-(kind,class)
// p2p_origin_pull_failure_total via the hook registered with
// WithDownstreamFailureMetric. Call sites must invoke this on
// terminal failures of the downstream pipeline (io.Copy / cw.Commit
// / directVerifier.Verify) AFTER origin returned 2xx. Origin-side
// failures (origin.Pull returned an *ifaces.OriginError) are
// counted separately by origin.WithMetrics' failure closure and
// must NOT also fire this hook — see WithDownstreamFailureMetric's
// doc for the cleanup of the two counters.
func (s *Server) fireOriginDownstreamFailure(kind ifaces.OriginRefKind, class ifaces.FailureClass) {
	if s.metrics.onOriginDownstreamFailure == nil {
		return
	}
	s.metrics.onOriginDownstreamFailure(kind.MetricLabel(), string(class))
}

func (s *Server) bumpPeerFetch(outcome string) {
	if s.metrics.onPeerFetch != nil {
		s.metrics.onPeerFetch(outcome)
	}
}

// bumpPeerFetchLatency emits the peer-fetch duration observation with
// the terminal outcome label. Always paired with bumpPeerFetch; the
// two together describe one fetchOneProvider call.
func (s *Server) bumpPeerFetchLatency(outcome string, start time.Time) {
	if s.metrics.onPeerFetchLatency != nil {
		s.metrics.onPeerFetchLatency(outcome, time.Since(start))
	}
}

func (s *Server) bumpPeerDial(success bool) {
	if s.metrics.onPeerDialResult != nil {
		s.metrics.onPeerDialResult(success)
	}
}

func (s *Server) bumpDhtLookup(outcome string, dur time.Duration) {
	if s.metrics.onDhtLookup != nil {
		s.metrics.onDhtLookup(outcome, dur)
	}
}

// writeBlobHeaders sets the OCI distribution headers the client expects.
func writeBlobHeaders(w http.ResponseWriter, d digest.Digest, size int64, kind ifaces.OriginRefKind) {
	w.Header().Set("Docker-Content-Digest", d.String())
	if kind == ifaces.KindBlob {
		// Reasonable default; client doesn't verify this for content-
		// addressed pulls.
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "application/octet-stream")
		}
	}
	if size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	}
}

// writeOriginError maps an *ifaces.OriginError to an HTTP status code that
// matches what containerd expects from an OCI Distribution endpoint.
//
// Phase 1 mapping (refined by §5.8 in Phase 4):
//
//	FailureAuth         401
//	FailureNotFound     404
//	FailureRateLimited  429
//	FailureTransient    503  (← lets hosts.toml fall through to origin)
func writeOriginError(w http.ResponseWriter, err error, logger *slog.Logger) {
	var oe *ifaces.OriginError
	if !errors.As(err, &oe) {
		logger.Warn("mirror: non-classified origin error", slog.Any("err", err))
		http.Error(w, "origin error", http.StatusBadGateway)
		return
	}
	switch oe.Class {
	case ifaces.FailureAuth:
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	case ifaces.FailureNotFound:
		http.Error(w, "not found", http.StatusNotFound)
	case ifaces.FailureRateLimited:
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	default:
		http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
	}
}

func isDigestRef(ref string) bool { return strings.HasPrefix(ref, "sha256:") }

// ListenAndServe runs the mirror on the configured loopback address. The
// returned function stops the server gracefully.
func (s *Server) ListenAndServe(addr string) (func(context.Context) error, error) {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()
	return func(ctx context.Context) error {
		return srv.Shutdown(ctx)
	}, nil
}
