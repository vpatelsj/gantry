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
	"time"

	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
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

	// Phase 2 tunables (zero values fall back to package defaults).
	peerLookupBudget time.Duration
	peerFetchBudget  time.Duration
	maxPeerAttempts  int

	// defaultUpstream is the upstream to use when exactly one is
	// configured and ?ns= is absent.
	defaultUpstream string
}

type metricsHooks struct {
	onCacheHit       func()
	onCacheMiss      func()
	onOriginPull     func(kind string)
	onOriginFailure  func(class string)
	onPeerFetch      func(outcome string)
	onPeerDialResult func(success bool)
	onDhtLookup      func(outcome string, dur time.Duration)
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

// WithMetrics registers metric callbacks.
func WithMetrics(cacheHit, cacheMiss func(), originPull func(kind string), originFailure func(class string)) Option {
	return func(s *Server) {
		s.metrics.onCacheHit = cacheHit
		s.metrics.onCacheMiss = cacheMiss
		s.metrics.onOriginPull = originPull
		s.metrics.onOriginFailure = originFailure
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

// WithDhtLookupMetric registers a hook that fires once per FindProviders
// call with the outcome label ("hit", "miss", "timeout", "error") and the
// observed lookup duration. Used to populate p2p_dht_lookup_total and
// p2p_dht_lookup_duration_seconds (§7.6).
func WithDhtLookupMetric(onLookup func(outcome string, dur time.Duration)) Option {
	return func(s *Server) {
		s.metrics.onDhtLookup = onLookup
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
	return s
}

// Handler returns an http.Handler suitable for serving on cfg.MirrorListen.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/v2/", s.handleV2)
	mux.HandleFunc("/v2", s.handleV2) // some clients omit trailing slash
	return mux
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

	repo, kind, ref, ok := parseV2Path(path)
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
	// try up to maxPeerAttempts providers from FindProviders before
	// falling through to origin. On any successful peer fetch the bytes
	// are committed to the local cache (digest-verified) and re-served
	// from cache, ensuring the client never sees unverified bytes even
	// if the peer responds with corrupt content.
	if s.dht != nil && s.peer != nil {
		if s.tryPeerFallback(ctx, w, r, d, kind, upstream, repo, logger) {
			return
		}
	}

	s.bumpOriginPull(kind)

	// 3. Origin pull, stream-and-cache.
	pRef := ifaces.OriginRef{Registry: upstream, Repository: repo, Digest: d, Kind: kind}
	pr, psize, perr := s.origin.Pull(ctx, pRef)
	if perr != nil {
		s.bumpOriginFailure(perr)
		writeOriginError(w, perr, logger)
		return
	}
	defer func() { _ = pr.Close() }()

	cw, cwerr := s.cache.Writer(ctx, d)
	var dest io.Writer = w
	if cwerr == nil {
		defer func() { _ = cw.Abort(ctx) }() // no-op after Commit
		dest = io.MultiWriter(w, cw)
	} else {
		logger.Warn("mirror: cache writer unavailable; serving without caching", slog.Any("err", cwerr))
	}

	writeBlobHeaders(w, d, psize, kind)
	if r.Method == http.MethodHead {
		return
	}
	written, err := io.Copy(dest, pr)
	if err != nil {
		// Bytes already sent; we can't undo. Cache will be aborted by defer.
		logger.Debug("mirror: copy stalled", slog.Int64("written", written), slog.Any("err", err))
		return
	}
	if cwerr == nil {
		if err := cw.Commit(ctx); err != nil {
			// The client already got the bytes; cache just doesn't keep them.
			logger.Warn("mirror: cache commit failed", slog.Any("err", err))
		}
	}
}

// tryPeerFallback attempts to satisfy a cache miss via a DHT-discovered
// peer. Returns true if it served a response from a peer; false if the
// caller should fall through to origin. No bytes are written to w until a
// peer's body is digest-verified and committed to the local cache, so a
// false return guarantees no partial response has been emitted.
func (s *Server) tryPeerFallback(ctx context.Context, w http.ResponseWriter, r *http.Request, d digest.Digest, kind ifaces.OriginRefKind, upstream, repo string, logger *slog.Logger) bool {
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
	if err != nil {
		outcome := "error"
		if errors.Is(lookupCtxErr, context.DeadlineExceeded) {
			outcome = "timeout"
		}
		s.bumpDhtLookup(outcome, lookupDur)
		logger.Debug("mirror: FindProviders error", slog.Any("err", err))
		return false
	}
	if len(providers) == 0 {
		s.bumpDhtLookup("miss", lookupDur)
		return false
	}
	s.bumpDhtLookup("hit", lookupDur)

	tried := 0
	for _, p := range providers {
		if tried >= maxAttempts {
			break
		}
		tried++
		if s.fetchOneProvider(ctx, w, r, d, kind, upstream, repo, p, fetchBudget, logger) {
			return true
		}
	}
	return false
}

// fetchOneProvider streams from one peer into the local cache (which
// verifies the digest on Commit) and, on success, serves from cache. Any
// failure path returns false so the caller can try the next provider; no
// bytes are written to w until the digest verifies.
func (s *Server) fetchOneProvider(ctx context.Context, w http.ResponseWriter, r *http.Request, d digest.Digest, kind ifaces.OriginRefKind, upstream, repo string, p ifaces.Provider, fetchBudget time.Duration, logger *slog.Logger) bool {
	pCtx, cancel := context.WithTimeout(ctx, fetchBudget)
	defer cancel()

	pRef := ifaces.OriginRef{Registry: upstream, Repository: repo, Digest: d, Kind: kind}
	rc, _, err := s.peer.FetchFromPeer(pCtx, p.Addr, pRef)
	if err != nil {
		var enf *ifaces.ErrNotFound
		if errors.As(err, &enf) {
			s.bumpPeerDial(true)
			s.bumpPeerFetch("notfound")
			logger.Debug("mirror: peer 404",
				slog.String("peer", p.Addr),
				slog.String("node", string(p.NodeID)),
			)
			return false
		}
		s.bumpPeerDial(false)
		s.bumpPeerFetch("error")
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
		logger.Warn("mirror: cache writer unavailable for peer fetch", slog.Any("err", cwerr))
		return false
	}
	defer func() { _ = cw.Abort(pCtx) }()

	if _, err := io.Copy(cw, rc); err != nil {
		s.bumpPeerFetch("stall")
		logger.Debug("mirror: peer copy stalled",
			slog.String("peer", p.Addr),
			slog.Any("err", err),
		)
		return false
	}
	if err := cw.Commit(pCtx); err != nil {
		s.bumpPeerFetch("error")
		logger.Warn("mirror: peer commit failed (likely digest mismatch)",
			slog.String("peer", p.Addr),
			slog.Any("err", err),
		)
		return false
	}

	// Re-open from cache and stream verified bytes to the client.
	rcLocal, size, err := s.cache.Open(ctx, d)
	if err != nil {
		s.bumpPeerFetch("error")
		logger.Warn("mirror: post-commit cache open failed", slog.Any("err", err))
		return false
	}
	defer func() { _ = rcLocal.Close() }()
	s.bumpPeerFetch("hit")
	writeBlobHeaders(w, d, size, kind)
	if r.Method == http.MethodHead {
		return true
	}
	if _, err := io.Copy(w, rcLocal); err != nil {
		logger.Debug("mirror: copy from cache (post-peer) failed", slog.Any("err", err))
	}
	return true
}

// bumpCacheHit / bumpCacheMiss / bumpOriginPull / bumpOriginFailure are
// no-ops if no metric hooks were registered.
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
func (s *Server) bumpOriginPull(k ifaces.OriginRefKind) {
	if s.metrics.onOriginPull != nil {
		s.metrics.onOriginPull(k.String())
	}
}
func (s *Server) bumpOriginFailure(err error) {
	if s.metrics.onOriginFailure == nil {
		return
	}
	var oe *ifaces.OriginError
	class := string(ifaces.FailureUnspecified)
	if errors.As(err, &oe) {
		class = string(oe.Class)
	}
	s.metrics.onOriginFailure(class)
}

func (s *Server) bumpPeerFetch(outcome string) {
	if s.metrics.onPeerFetch != nil {
		s.metrics.onPeerFetch(outcome)
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

// parseV2Path matches /v2/<repo>/(manifests|blobs)/<reference>. Returns
// the repository, the URL kind (manifest vs blob), the reference (tag or
// digest), and ok=false if the path doesn't match the OCI shape.
func parseV2Path(path string) (repo string, kind ifaces.OriginRefKind, ref string, ok bool) {
	const prefix = "/v2/"
	if !strings.HasPrefix(path, prefix) {
		return "", 0, "", false
	}
	rest := path[len(prefix):]
	// Find the last `/manifests/` or `/blobs/` separator; repo names can
	// contain slashes (e.g. library/nginx).
	idx := strings.LastIndex(rest, "/manifests/")
	if idx >= 0 {
		return rest[:idx], ifaces.KindManifest, rest[idx+len("/manifests/"):], true
	}
	idx = strings.LastIndex(rest, "/blobs/")
	if idx >= 0 {
		return rest[:idx], ifaces.KindBlob, rest[idx+len("/blobs/"):], true
	}
	return "", 0, "", false
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
