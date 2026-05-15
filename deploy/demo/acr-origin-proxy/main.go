// Demo-only counting-proxy SPIKE for the ACR demo plan.
//
// Scope (step 1 of docs/acr-counting-proxy-demo.md §10):
//   - Auth state machine (§3.5): Basic and OCI Bearer-challenge flow,
//     per-scope token cache, AUTH_MODE = basic | bearer | auto.
//   - Single-request proxying with one-shot Bearer-challenge reissue.
//   - Streaming response body via io.CopyBuffer with a 64 KiB buffer
//     (§3.6).
//   - Verbose logging so the Phase 0.5 checklist (§6) can be hand-
//     verified by an operator without metrics yet.
//
// Out of scope until step 2:
//   - Path classification (§3.4).
//   - Prometheus /metrics, /debug/summary, started/completed counter pair.
//   - Unit-test suite (§3.8).
//   - Synthetic-throttle support (§7).
//
// This binary lives in its own Go module under deploy/demo/ and has
// zero imports from github.com/gantry/gantry/... (§1.1 / §3.1 of the
// plan). The demo is fully reverted by deleting deploy/demo/.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultListenAddr      = ":5002"
	defaultAuthMode        = "auto"
	defaultMaxTokenLife    = 30 * time.Minute
	defaultRefreshSkewSecs = 30
	copyBufferBytes        = 64 * 1024 // §3.6
	readHeaderTimeout      = 10 * time.Second
	shutdownGrace          = 5 * time.Second
)

type config struct {
	listen          string
	upstream        *url.URL
	user, pass      string
	authMode        string // "basic" | "bearer" | "auto"
	maxTokenLife    time.Duration
	refreshSkewSecs int
}

type tokenEntry struct {
	token   string
	expires time.Time // zero = "no upstream-declared expiry"
}

type tokenCache struct {
	mu      sync.Mutex
	byScope map[string]tokenEntry
	skew    time.Duration
}

func newTokenCache(skewSecs int) *tokenCache {
	return &tokenCache{
		byScope: make(map[string]tokenEntry),
		skew:    time.Duration(skewSecs) * time.Second,
	}
}

func (c *tokenCache) lookup(scope string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.byScope[scope]
	if !ok {
		return "", false
	}
	// Treat zero-expiry entries as "valid until the proxy restarts" — the
	// caller already caps lifetime at MAX_TOKEN_LIFETIME before storing.
	if !e.expires.IsZero() && time.Until(e.expires) <= c.skew {
		return "", false
	}
	return e.token, true
}

func (c *tokenCache) store(scope, tok string, exp time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byScope[scope] = tokenEntry{token: tok, expires: exp}
}

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	cache := newTokenCache(cfg.refreshSkewSecs)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.Handle("/", proxyHandler(cfg, cache))

	srv := &http.Server{
		Addr:              cfg.listen,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		// WriteTimeout: 0 (unbounded) — large blob streams legitimately take minutes (§3.6).
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		log.Printf("shutdown signal received; closing listener")
		sctx, scancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer scancel()
		_ = srv.Shutdown(sctx)
	}()

	log.Printf("acr-origin-proxy SPIKE listening on %s; upstream=%s; auth_mode=%s",
		cfg.listen, cfg.upstream.String(), cfg.authMode)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
}

func configFromEnv() (*config, error) {
	upstream := os.Getenv("UPSTREAM_REGISTRY")
	if upstream == "" {
		return nil, errors.New("UPSTREAM_REGISTRY env var is required (e.g. https://myacr.azurecr.io)")
	}
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse UPSTREAM_REGISTRY: %w", err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, fmt.Errorf("UPSTREAM_REGISTRY scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("UPSTREAM_REGISTRY has empty host: %q", upstream)
	}

	user := os.Getenv("ACR_USERNAME")
	pass := os.Getenv("ACR_PASSWORD")
	if user == "" || pass == "" {
		return nil, errors.New("ACR_USERNAME and ACR_PASSWORD env vars are required")
	}

	mode := os.Getenv("AUTH_MODE")
	if mode == "" {
		mode = defaultAuthMode
	}
	switch mode {
	case "basic", "bearer", "auto":
	default:
		return nil, fmt.Errorf("AUTH_MODE must be basic|bearer|auto, got %q", mode)
	}

	listen := os.Getenv("LISTEN_ADDR")
	if listen == "" {
		listen = defaultListenAddr
	}

	skewSecs := defaultRefreshSkewSecs
	if v := os.Getenv("REFRESH_SKEW_SECONDS"); v != "" {
		var s int
		if _, err := fmt.Sscanf(v, "%d", &s); err != nil || s <= 0 {
			return nil, fmt.Errorf("REFRESH_SKEW_SECONDS must be a positive integer, got %q", v)
		}
		skewSecs = s
	}

	maxLife := defaultMaxTokenLife
	if v := os.Getenv("MAX_TOKEN_LIFETIME_SECONDS"); v != "" {
		var s int
		if _, err := fmt.Sscanf(v, "%d", &s); err != nil || s <= 0 {
			return nil, fmt.Errorf("MAX_TOKEN_LIFETIME_SECONDS must be a positive integer, got %q", v)
		}
		maxLife = time.Duration(s) * time.Second
	}

	return &config{
		listen:          listen,
		upstream:        u,
		user:            user,
		pass:            pass,
		authMode:        mode,
		maxTokenLife:    maxLife,
		refreshSkewSecs: skewSecs,
	}, nil
}

func proxyHandler(cfg *config, cache *tokenCache) http.Handler {
	client := &http.Client{
		// Default transport is fine for streaming (§3.6).
		// No CheckRedirect override: ACR sometimes 307s blobs to backing
		// blob storage; we want the default redirect-follow behavior so
		// the client experiences a single logical request.
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip any inbound Authorization before we attach the proxy's
		// own credential (§3.5).
		r.Header.Del("Authorization")

		target := *cfg.upstream
		target.Path = singleJoiningSlash(cfg.upstream.Path, r.URL.Path)
		target.RawQuery = r.URL.RawQuery

		start := time.Now()
		resp, refreshed, err := doWithAuth(r.Context(), client, cfg, cache, r.Method, &target, r.Header, r.Body)
		if err != nil {
			log.Printf("ERROR method=%s path=%s upstream=%s err=%v",
				r.Method, r.URL.Path, target.String(), err)
			http.Error(w, "bad gateway: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		copyResponseHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		buf := make([]byte, copyBufferBytes)
		n, copyErr := io.CopyBuffer(w, resp.Body, buf)

		log.Printf("method=%s path=%s status=%d bytes=%d auth_refreshed=%v latency=%s copy_err=%v",
			r.Method, r.URL.Path, resp.StatusCode, n, refreshed,
			time.Since(start).Round(time.Millisecond), copyErr)
	})
}

// doWithAuth issues a single request to ACR. If the response is 401 with
// a parseable Bearer challenge AND AUTH_MODE permits Bearer, it performs
// the OCI token exchange (caching the token by scope), then reissues the
// request once with the Bearer token. The reissue collapses into a single
// logical request from the caller's view (plan §3.2 step 6).
//
// Returns the response, whether a token refresh fired (for logging), and
// the error if any. The caller must Close the response body.
func doWithAuth(
	ctx context.Context,
	client *http.Client,
	cfg *config,
	cache *tokenCache,
	method string,
	target *url.URL,
	headers http.Header,
	body io.Reader,
) (resp *http.Response, refreshed bool, err error) {
	scope := guessScope(target.Path)

	req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, false, err
	}
	copyForwardedHeaders(req.Header, headers)

	switch cfg.authMode {
	case "basic":
		req.Header.Set("Authorization", "Basic "+basicAuth(cfg.user, cfg.pass))
	case "bearer":
		if tok, ok := cache.lookup(scope); scope != "" && ok {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		// Else: send without Authorization and expect ACR to challenge.
	case "auto":
		if tok, ok := cache.lookup(scope); scope != "" && ok {
			req.Header.Set("Authorization", "Bearer "+tok)
		} else {
			req.Header.Set("Authorization", "Basic "+basicAuth(cfg.user, cfg.pass))
		}
	}

	resp, err = client.Do(req)
	if err != nil {
		return nil, false, err
	}

	if resp.StatusCode != http.StatusUnauthorized {
		return resp, false, nil
	}
	if cfg.authMode == "basic" {
		// Forward the 401 verbatim; don't shadow the upstream auth failure.
		return resp, false, nil
	}

	challenge := parseBearerChallenge(resp.Header.Get("WWW-Authenticate"))
	if challenge == nil {
		// 401 without a parseable Bearer challenge — forward as-is.
		return resp, false, nil
	}
	// Drain + close the 401 body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	tok, exp, err := exchangeToken(ctx, client, cfg, challenge)
	if err != nil {
		log.Printf("token exchange failed: scope=%q realm=%q err=%v",
			challenge.scope, challenge.realm, err)
		return nil, false, fmt.Errorf("token exchange (realm=%s): %w", challenge.realm, err)
	}

	cacheScope := challenge.scope
	if cacheScope == "" {
		cacheScope = scope
	}
	if cacheScope != "" {
		// Cap the lifetime at MAX_TOKEN_LIFETIME (plan §3.5 step d).
		lifetimeCap := time.Now().Add(cfg.maxTokenLife)
		if exp.IsZero() || exp.After(lifetimeCap) {
			exp = lifetimeCap
		}
		cache.store(cacheScope, tok, exp)
	}
	log.Printf("token-refresh: scope=%q realm=%q expires_in=%s",
		cacheScope, challenge.realm, time.Until(exp).Round(time.Second))

	// Reissue once with the new Bearer token. The body was either nil
	// (GET/HEAD — the only methods the demo currently exercises) or
	// already consumed by the first attempt; PUT/POST support lands in
	// build-plan step 7.
	req2, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, true, err
	}
	copyForwardedHeaders(req2.Header, headers)
	req2.Header.Set("Authorization", "Bearer "+tok)
	resp, err = client.Do(req2)
	return resp, true, err
}

type bearerChallenge struct {
	realm   string
	service string
	scope   string
}

// Spec: RFC 7235 §4.1 "WWW-Authenticate". OCI distribution v2 adds the
// realm/service/scope parameter set for Bearer.
var (
	bearerSchemeRe = regexp.MustCompile(`(?i)^\s*Bearer\s+`)
	bearerParamRe  = regexp.MustCompile(`(\w+)\s*=\s*"([^"]*)"`)
)

func parseBearerChallenge(h string) *bearerChallenge {
	if h == "" || !bearerSchemeRe.MatchString(h) {
		return nil
	}
	rest := bearerSchemeRe.ReplaceAllString(h, "")
	c := &bearerChallenge{}
	for _, m := range bearerParamRe.FindAllStringSubmatch(rest, -1) {
		switch strings.ToLower(m[1]) {
		case "realm":
			c.realm = m[2]
		case "service":
			c.service = m[2]
		case "scope":
			c.scope = m[2]
		}
	}
	if c.realm == "" {
		return nil
	}
	return c
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	Token       string `json:"token"`
	ExpiresIn   int    `json:"expires_in"`
}

func exchangeToken(
	ctx context.Context,
	client *http.Client,
	cfg *config,
	c *bearerChallenge,
) (string, time.Time, error) {
	u, err := url.Parse(c.realm)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("parse realm %q: %w", c.realm, err)
	}
	q := u.Query()
	if c.service != "" {
		q.Set("service", c.service)
	}
	if c.scope != "" {
		q.Set("scope", c.scope)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Authorization", "Basic "+basicAuth(cfg.user, cfg.pass))
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Read the (small) body for debugging; realm errors are usually
		// a few hundred bytes of JSON.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return "", time.Time{}, fmt.Errorf("realm returned %d: %s",
			resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", time.Time{}, fmt.Errorf("decode token response: %w", err)
	}
	tok := tr.AccessToken
	if tok == "" {
		tok = tr.Token
	}
	if tok == "" {
		return "", time.Time{}, errors.New("token response missing access_token and token fields")
	}
	exp := time.Time{}
	if tr.ExpiresIn > 0 {
		exp = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	}
	return tok, exp, nil
}

func basicAuth(user, pass string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
}

// guessScope extracts an OCI-style scope ("repository:<repo>:pull") from
// an inbound /v2 path so the token cache can be keyed before any
// upstream challenge arrives. Returns "" for paths that aren't /v2-shaped
// or where we can't determine a repository name; in that case the caller
// falls back to the realm-supplied scope.
func guessScope(p string) string {
	if !strings.HasPrefix(p, "/v2/") {
		return ""
	}
	rest := strings.TrimPrefix(p, "/v2/")
	// Repository is everything before the rightmost "/manifests/" or
	// "/blobs/" boundary. This matches the plan's path classifier rule
	// (§3.4) and OCI distribution spec §3.
	idx := lastIndexOfEither(rest, "/manifests/", "/blobs/")
	if idx <= 0 {
		return ""
	}
	repo := rest[:idx]
	return "repository:" + repo + ":pull"
}

func lastIndexOfEither(s, a, b string) int {
	ia := strings.LastIndex(s, a)
	ib := strings.LastIndex(s, b)
	if ia > ib {
		return ia
	}
	return ib
}

// copyForwardedHeaders copies inbound headers to the upstream request,
// dropping hop-by-hop and proxy-specific headers (including any inbound
// Authorization — already stripped at the handler level, but belt-and-
// braces).
func copyForwardedHeaders(dst, src http.Header) {
	for k, vs := range src {
		ck := http.CanonicalHeaderKey(k)
		if hopByHopHeader[ck] {
			continue
		}
		for _, v := range vs {
			dst.Add(ck, v)
		}
	}
}

// copyResponseHeaders copies upstream response headers to the client,
// minus hop-by-hop.
func copyResponseHeaders(dst, src http.Header) {
	for k, vs := range src {
		ck := http.CanonicalHeaderKey(k)
		if hopByHopHeader[ck] {
			continue
		}
		for _, v := range vs {
			dst.Add(ck, v)
		}
	}
}

// hopByHopHeader lists headers a proxy must not forward (RFC 7230 §6.1)
// plus a couple of proxy-policy entries (Host, Authorization) that this
// proxy rewrites rather than forwards.
var hopByHopHeader = map[string]bool{
	"Connection":          true,
	"Proxy-Connection":    true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
	"Host":                true,
	"Authorization":       true, // we attach our own
}

// singleJoiningSlash mirrors net/http/httputil.ReverseProxy's helper:
// joins two paths collapsing adjacent slashes.
func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}
