// Package transfer is the peer-facing OCI endpoint other Gantry agents pull
// from. It binds `:5001` over HTTP/2 cleartext (h2c) because NetworkPolicy
// restricts the port to inter-node traffic and TLS termination would
// duplicate the libp2p Noise security that Gantry already does for
// coordination RPCs.
//
// Design (detailed-design.md §4.4, archecture.md §API):
//
//   - Same OCI URL shape as the mirror: `/v2/<repo>/blobs/<digest>` and
//     `/v2/<repo>/manifests/<digest>`. Tag-shaped manifest requests at this
//     endpoint return **404 unconditionally** — peers must already know the
//     digest (the §5.1a tag-resolution path runs through the mirror, not
//     here).
//   - Requires `Gantry-Mirrored: 1` request header. Without the header the
//     server returns 400. With the header, the response semantics are:
//     **serve from the local store or return 404**. No DHT lookup, no
//     HRW probe, no `please_pull`, no origin contact. The header is the
//     loop-breaker that prevents two agents from recursing into each
//     other's miss paths.
//   - `Range: bytes=N-M` returns `206 Partial Content` with the correct
//     `Content-Range`. v1 callers always fetch whole blobs, but the
//     contract is preserved for v2 striping.
//   - Metric `p2p_peer_serve_total` is bumped per served body — not
//     `p2p_cache_hit_total`, so cluster scrapes distinguish containerd-
//     facing hits from peer-facing serves.
package transfer

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

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/oci"
)

// MirroredHeader is the OCI-extension header peers MUST include on every
// transfer-endpoint request. It is the loop-breaker described in §4.4.
const MirroredHeader = "Gantry-Mirrored"

// Server serves peer-fetch requests from the local cache.
type Server struct {
	cache     ifaces.Cache
	secondary ifaces.SecondaryBlobSource
	logger    *slog.Logger
	metrics   metricsHooks
}

type metricsHooks struct {
	onPeerServe func()
	onPeerMiss  func()
}

// Option configures a Server.
type Option func(*Server)

// WithLogger plumbs a structured logger.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		if l != nil {
			s.logger = l.With(slog.String("subsystem", "transfer"))
		}
	}
}

// WithMetrics registers metric callbacks.
func WithMetrics(onPeerServe, onPeerMiss func()) Option {
	return func(s *Server) {
		s.metrics = metricsHooks{onPeerServe: onPeerServe, onPeerMiss: onPeerMiss}
	}
}

// WithSecondaryBlobSource registers a fallback blob source consulted on
// cache miss. The canonical implementation reads from containerd's
// content store so that digests announced by the cdsub Source are
// actually serveable to peers (otherwise cdsub announces phantom
// providers and peers 404 on the transfer endpoint). When nil, a cache
// miss returns 404 directly.
func WithSecondaryBlobSource(src ifaces.SecondaryBlobSource) Option {
	return func(s *Server) {
		s.secondary = src
	}
}

// New builds a Server bound to the given cache.
func New(cache ifaces.Cache, opts ...Option) *Server {
	s := &Server{
		cache:  cache,
		logger: slog.Default().With(slog.String("subsystem", "transfer")),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Handler returns the HTTP handler. Callers wrap it with h2c via
// ListenAndServe or use it directly (e.g., httptest in unit tests, where
// HTTP/1.1 is sufficient because no client uses Range over multiplexed
// streams).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/v2/", s.handleV2)
	mux.HandleFunc("/v2", s.handleV2)
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func (s *Server) handleV2(w http.ResponseWriter, r *http.Request) {
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

	if r.Header.Get(MirroredHeader) != "1" {
		http.Error(w, "missing Gantry-Mirrored header", http.StatusBadRequest)
		return
	}

	_, _, ref, ok := oci.ParseV2Path(path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if !strings.HasPrefix(ref, "sha256:") {
		// Tag at peer endpoint → 404 unconditionally (§4.4).
		http.NotFound(w, r)
		return
	}
	d, err := digest.Parse(ref)
	if err != nil {
		http.Error(w, "invalid digest", http.StatusBadRequest)
		return
	}

	s.serveDigest(w, r, d)
}

func (s *Server) serveDigest(w http.ResponseWriter, r *http.Request, d digest.Digest) {
	rc, size, err := s.openBlob(r.Context(), d)
	if err != nil {
		var enf *ifaces.ErrNotFound
		if errors.As(err, &enf) {
			if s.metrics.onPeerMiss != nil {
				s.metrics.onPeerMiss()
			}
			http.NotFound(w, r)
			return
		}
		s.logger.Warn("transfer: cache open error",
			slog.String("digest", d.String()),
			slog.Any("err", err),
		)
		http.Error(w, "cache error", http.StatusInternalServerError)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Docker-Content-Digest", d.String())
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")

	rng := r.Header.Get("Range")
	if rng == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead {
			s.bumpServe()
			return
		}
		if _, err := io.Copy(w, rc); err != nil {
			s.logger.Debug("transfer: copy failed", slog.Any("err", err))
		}
		s.bumpServe()
		return
	}

	start, end, ok := parseSingleRange(rng, size)
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(w, "invalid Range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	length := end - start + 1
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	if r.Method == http.MethodHead {
		s.bumpServe()
		return
	}
	rs, isSeeker := rc.(io.ReadSeeker)
	if !isSeeker {
		// Fall back to discarding the unwanted prefix.
		if _, err := io.CopyN(io.Discard, rc, start); err != nil {
			s.logger.Debug("transfer: discard prefix failed", slog.Any("err", err))
			return
		}
	} else if _, err := rs.Seek(start, io.SeekStart); err != nil {
		s.logger.Debug("transfer: seek failed", slog.Any("err", err))
		return
	}
	if _, err := io.CopyN(w, rc, length); err != nil {
		s.logger.Debug("transfer: range copy failed", slog.Any("err", err))
	}
	s.bumpServe()
}

func (s *Server) bumpServe() {
	if s.metrics.onPeerServe != nil {
		s.metrics.onPeerServe()
	}
}

// openBlob consults the cache first; on ErrNotFound, falls back to the
// optional SecondaryBlobSource (typically the containerd content
// store, wired by main.go when the cdsub source is enabled). Any other
// cache error is returned as-is — we don't paper over real I/O
// failures by silently retrying the secondary.
//
// This is the fix for the cdsub-announces-but-transfer-404s mismatch:
// the Source registers Provider records for digests in containerd's
// content store, but the transfer endpoint only knew about Gantry's
// own cache. Peers would dial in, get a 404, and the announcement
// became misinformation.
func (s *Server) openBlob(ctx context.Context, d digest.Digest) (io.ReadCloser, int64, error) {
	rc, size, err := s.cache.Open(ctx, d)
	if err == nil {
		return rc, size, nil
	}
	var enf *ifaces.ErrNotFound
	if !errors.As(err, &enf) {
		return nil, 0, err
	}
	if s.secondary == nil {
		return nil, 0, err
	}
	rc2, size2, err2 := s.secondary.Open(ctx, d)
	if err2 == nil {
		return rc2, size2, nil
	}
	// Whether the secondary reports ErrNotFound or another error, we
	// hand the original cache ErrNotFound back. Callers above only
	// branch on Not-Found vs error; the secondary is opportunistic.
	var enf2 *ifaces.ErrNotFound
	if errors.As(err2, &enf2) {
		return nil, 0, err
	}
	s.logger.Debug("transfer: secondary blob source error; reporting miss",
		slog.String("digest", d.String()),
		slog.Any("err", err2),
	)
	return nil, 0, err
}

// parseSingleRange parses an RFC 7233 single-range header like
// "bytes=0-499" or "bytes=500-" against a known total size. Multi-range
// requests are rejected (v1 callers don't use them).
func parseSingleRange(h string, size int64) (start, end int64, ok bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(h, prefix) {
		return 0, 0, false
	}
	spec := h[len(prefix):]
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false
	}
	startStr, endStr := spec[:dash], spec[dash+1:]
	if startStr == "" {
		// Suffix: bytes=-N
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 || n > size {
			return 0, 0, false
		}
		return size - n, size - 1, true
	}
	s, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || s < 0 || s >= size {
		return 0, 0, false
	}
	if endStr == "" {
		return s, size - 1, true
	}
	e, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || e < s || e >= size {
		return 0, 0, false
	}
	return s, e, true
}

// ListenAndServe runs the transfer server with h2c support on addr.
// Returns a function that gracefully shuts the server down.
func (s *Server) ListenAndServe(addr string) (func(context.Context) error, error) {
	h2s := &http2.Server{}
	handler := h2c.NewHandler(s.Handler(), h2s)
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := http2.ConfigureServer(srv, h2s); err != nil {
		return nil, fmt.Errorf("transfer: configure h2: %w", err)
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
