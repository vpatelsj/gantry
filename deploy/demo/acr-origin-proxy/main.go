// Demo-only counting reverse proxy for the ACR demo plan.
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	defaultListenAddr        = ":5002"
	defaultMetricsListenAddr = ":9090"
	defaultAuthMode          = "auto"
	defaultMaxTokenLife      = 30 * time.Minute
	defaultRefreshSkewSecs   = 30
	copyBufferBytes          = 64 * 1024
	readHeaderTimeout        = 10 * time.Second
	shutdownGrace            = 5 * time.Second
	maxCachedTokens          = 1024
)

type pathClass string

const (
	pathClassBlob             pathClass = "blob"
	pathClassManifestByDigest pathClass = "manifest_by_digest"
	pathClassManifestByTag    pathClass = "manifest_by_tag"
	pathClassPing             pathClass = "ping"
	pathClassOther            pathClass = "other"
)

var allPathClasses = []pathClass{
	pathClassBlob,
	pathClassManifestByDigest,
	pathClassManifestByTag,
	pathClassPing,
	pathClassOther,
}

type config struct {
	listen                string
	metricsListen         string
	upstream              *url.URL
	user                  string
	pass                  string
	authMode              string
	maxTokenLife          time.Duration
	refreshSkewSecs       int
	throttleBlobInflight  int
	throttleRetryAfterSec int
}

type tokenEntry struct {
	token   string
	expires time.Time
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
	if scope == "" {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.byScope[scope]
	if !ok {
		return "", false
	}
	if !e.expires.IsZero() && time.Until(e.expires) <= c.skew {
		return "", false
	}
	return e.token, true
}

func (c *tokenCache) store(scope, tok string, exp time.Time) {
	if scope == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.byScope) >= maxCachedTokens {
		for k := range c.byScope {
			delete(c.byScope, k)
			break
		}
	}
	c.byScope[scope] = tokenEntry{token: tok, expires: exp}
}

type pathTotals struct {
	Requests uint64 `json:"requests"`
	Bytes    uint64 `json:"bytes"`
}

type summary struct {
	Since      string `json:"since"`
	UptimeSecs int64  `json:"uptime_seconds"`
	Totals     totals `json:"totals"`
}

type totals struct {
	RequestsCompleted uint64                   `json:"requests_completed"`
	BytesToClient     uint64                   `json:"bytes_to_client"`
	ByPathClass       map[pathClass]pathTotals `json:"by_path_class"`
}

type observer struct {
	started             *prometheus.CounterVec
	completed           *prometheus.CounterVec
	bytesUpstream       *prometheus.CounterVec
	bytesToClient       *prometheus.CounterVec
	latency             *prometheus.HistogramVec
	inflight            *prometheus.GaugeVec
	authRefresh         *prometheus.CounterVec
	syntheticThrottle   *prometheus.CounterVec
	startedAt           time.Time
	mu                  sync.Mutex
	summary             totals
	inflightByPathClass map[pathClass]int
}

func newObserver(reg *prometheus.Registry, now time.Time) *observer {
	o := &observer{
		started: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "origin_requests_started_total",
			Help: "Logical client requests started by the demo ACR origin proxy.",
		}, []string{"method", "path_class"}),
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "origin_requests_completed_total",
			Help: "Logical client requests completed by the demo ACR origin proxy.",
		}, []string{"method", "path_class", "status"}),
		bytesUpstream: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "origin_bytes_upstream_total",
			Help: "Response body bytes read from upstream by the demo ACR origin proxy.",
		}, []string{"path_class", "status"}),
		bytesToClient: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "origin_bytes_to_client_total",
			Help: "Response body bytes written to clients by the demo ACR origin proxy.",
		}, []string{"path_class", "status"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "origin_latency_seconds",
			Help:    "Logical request latency through the demo ACR origin proxy.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}, []string{"path_class", "status"}),
		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "origin_inflight_requests",
			Help: "Logical requests currently in flight through the demo ACR origin proxy.",
		}, []string{"path_class"}),
		authRefresh: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "origin_auth_token_refresh_total",
			Help: "Bearer token refresh attempts by result.",
		}, []string{"result"}),
		syntheticThrottle: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "origin_synthetic_throttle_total",
			Help: "Synthetic proxy throttles by reason.",
		}, []string{"reason"}),
		startedAt:           now,
		summary:             totals{ByPathClass: make(map[pathClass]pathTotals)},
		inflightByPathClass: make(map[pathClass]int),
	}

	reg.MustRegister(o.started, o.completed, o.bytesUpstream, o.bytesToClient, o.latency, o.inflight, o.authRefresh, o.syntheticThrottle)
	for _, class := range allPathClasses {
		o.summary.ByPathClass[class] = pathTotals{}
		o.inflight.WithLabelValues(string(class)).Set(0)
	}
	o.authRefresh.WithLabelValues("success").Add(0)
	o.authRefresh.WithLabelValues("error").Add(0)
	o.syntheticThrottle.WithLabelValues("blob_inflight").Add(0)
	return o
}

func (o *observer) begin(method string, class pathClass) {
	o.started.WithLabelValues(method, string(class)).Inc()
	o.inflight.WithLabelValues(string(class)).Inc()
	o.mu.Lock()
	o.inflightByPathClass[class]++
	o.mu.Unlock()
}

func (o *observer) finish(method string, class pathClass, status string, upstreamBytes, clientBytes int64, elapsed time.Duration) {
	if upstreamBytes < 0 {
		upstreamBytes = 0
	}
	if clientBytes < 0 {
		clientBytes = 0
	}
	o.completed.WithLabelValues(method, string(class), status).Inc()
	o.bytesUpstream.WithLabelValues(string(class), status).Add(float64(upstreamBytes))
	o.bytesToClient.WithLabelValues(string(class), status).Add(float64(clientBytes))
	o.latency.WithLabelValues(string(class), status).Observe(elapsed.Seconds())
	o.inflight.WithLabelValues(string(class)).Dec()

	o.mu.Lock()
	if o.inflightByPathClass[class] > 0 {
		o.inflightByPathClass[class]--
	}
	o.summary.RequestsCompleted++
	o.summary.BytesToClient += uint64(clientBytes)
	pt := o.summary.ByPathClass[class]
	pt.Requests++
	pt.Bytes += uint64(clientBytes)
	o.summary.ByPathClass[class] = pt
	o.mu.Unlock()
}

func (o *observer) currentInflight(class pathClass) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.inflightByPathClass[class]
}

func (o *observer) recordAuthRefresh(result string) {
	o.authRefresh.WithLabelValues(result).Inc()
}

func (o *observer) recordSyntheticThrottle(reason string) {
	o.syntheticThrottle.WithLabelValues(reason).Inc()
}

func (o *observer) snapshot(now time.Time) summary {
	o.mu.Lock()
	defer o.mu.Unlock()
	byClass := make(map[pathClass]pathTotals, len(allPathClasses))
	for _, class := range allPathClasses {
		byClass[class] = o.summary.ByPathClass[class]
	}
	return summary{
		Since:      o.startedAt.UTC().Format(time.RFC3339),
		UptimeSecs: int64(now.Sub(o.startedAt).Seconds()),
		Totals: totals{
			RequestsCompleted: o.summary.RequestsCompleted,
			BytesToClient:     o.summary.BytesToClient,
			ByPathClass:       byClass,
		},
	}
}

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	registry := prometheus.NewRegistry()
	obs := newObserver(registry, time.Now())
	cache := newTokenCache(cfg.refreshSkewSecs)

	proxyMux := http.NewServeMux()
	proxyMux.HandleFunc("/healthz", healthzHandler)
	proxyMux.Handle("/", proxyHandler(cfg, cache, obs, http.DefaultClient))

	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("/healthz", healthzHandler)
	metricsMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	metricsMux.Handle("/debug/summary", summaryHandler(obs))

	proxySrv := &http.Server{
		Addr:              cfg.listen,
		Handler:           proxyMux,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	metricsSrv := &http.Server{
		Addr:              cfg.metricsListen,
		Handler:           metricsMux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go shutdownOnSignal(ctx, proxySrv, "proxy")
	go shutdownOnSignal(ctx, metricsSrv, "metrics")

	go func() {
		log.Printf("metrics listening on %s", cfg.metricsListen)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics listen: %v", err)
			cancel()
		}
	}()

	log.Printf("acr-origin-proxy listening on %s; upstream=%s; auth_mode=%s", cfg.listen, cfg.upstream.String(), cfg.authMode)
	if err := proxySrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("proxy listen: %v", err)
	}
}

func shutdownOnSignal(ctx context.Context, srv *http.Server, name string) {
	<-ctx.Done()
	log.Printf("shutdown signal received; closing %s listener", name)
	sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = srv.Shutdown(sctx)
}

func healthzHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
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

	mode := valueOrDefault(os.Getenv("AUTH_MODE"), defaultAuthMode)
	switch mode {
	case "basic", "bearer", "auto":
	default:
		return nil, fmt.Errorf("AUTH_MODE must be basic|bearer|auto, got %q", mode)
	}

	skewSecs, err := positiveIntEnv("REFRESH_SKEW_SECONDS", defaultRefreshSkewSecs)
	if err != nil {
		return nil, err
	}
	maxLifeSecs, err := positiveIntEnv("MAX_TOKEN_LIFETIME_SECONDS", int(defaultMaxTokenLife.Seconds()))
	if err != nil {
		return nil, err
	}
	throttleBlobInflight, err := nonNegativeIntEnv("THROTTLE_BLOB_INFLIGHT", 0)
	if err != nil {
		return nil, err
	}
	throttleRetryAfter, err := positiveIntEnv("THROTTLE_RETRY_AFTER_SECONDS", 5)
	if err != nil {
		return nil, err
	}

	return &config{
		listen:                valueOrDefault(os.Getenv("LISTEN_ADDR"), defaultListenAddr),
		metricsListen:         valueOrDefault(os.Getenv("METRICS_LISTEN_ADDR"), defaultMetricsListenAddr),
		upstream:              u,
		user:                  user,
		pass:                  pass,
		authMode:              mode,
		maxTokenLife:          time.Duration(maxLifeSecs) * time.Second,
		refreshSkewSecs:       skewSecs,
		throttleBlobInflight:  throttleBlobInflight,
		throttleRetryAfterSec: throttleRetryAfter,
	}, nil
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func positiveIntEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, value)
	}
	return parsed, nil
}

func nonNegativeIntEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer, got %q", name, value)
	}
	return parsed, nil
}

func proxyHandler(cfg *config, cache *tokenCache, obs *observer, client *http.Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		class := classifyPath(r.URL.EscapedPath())
		start := time.Now()

		if shouldThrottle(cfg, obs, class) {
			obs.recordSyntheticThrottle("blob_inflight")
			obs.begin(r.Method, class)
			body := "synthetic throttle\n"
			w.Header().Set("Retry-After", strconv.Itoa(cfg.throttleRetryAfterSec))
			w.WriteHeader(http.StatusTooManyRequests)
			n, _ := io.WriteString(w, body)
			obs.finish(r.Method, class, strconv.Itoa(http.StatusTooManyRequests), 0, int64(n), time.Since(start))
			return
		}

		obs.begin(r.Method, class)
		status := "upstream_error"
		var upstreamBytes int64
		var clientBytes int64
		defer func() {
			obs.finish(r.Method, class, status, upstreamBytes, clientBytes, time.Since(start))
		}()

		r.Header.Del("Authorization")
		target := *cfg.upstream
		target.Path = singleJoiningSlash(cfg.upstream.Path, r.URL.EscapedPath())
		target.RawQuery = r.URL.RawQuery

		resp, refreshed, err := doWithAuth(r.Context(), client, cfg, cache, obs, r.Method, &target, r.Header, r.Body)
		if err != nil {
			status = strconv.Itoa(http.StatusBadGateway)
			log.Printf("ERROR method=%s path=%s class=%s upstream=%s err=%v", r.Method, r.URL.Path, class, target.String(), err)
			http.Error(w, "bad gateway: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		copyResponseHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		status = strconv.Itoa(resp.StatusCode)

		countingBody := &countingReader{r: resp.Body}
		countingClient := &countingWriter{w: w}
		buf := make([]byte, copyBufferBytes)
		_, copyErr := io.CopyBuffer(countingClient, countingBody, buf)
		upstreamBytes = countingBody.n
		clientBytes = countingClient.n
		if copyErr != nil {
			status = "client_closed"
		}

		log.Printf("method=%s path=%s class=%s status=%s upstream_bytes=%d client_bytes=%d auth_refreshed=%v latency=%s copy_err=%v",
			r.Method, r.URL.Path, class, status, upstreamBytes, clientBytes, refreshed,
			time.Since(start).Round(time.Millisecond), copyErr)
	})
}

func shouldThrottle(cfg *config, obs *observer, class pathClass) bool {
	return class == pathClassBlob && cfg.throttleBlobInflight > 0 && obs.currentInflight(class) >= cfg.throttleBlobInflight
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func summaryHandler(obs *observer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(obs.snapshot(time.Now()))
	})
}

func classifyPath(rawPath string) pathClass {
	path := rawPath
	if idx := strings.IndexByte(path, '?'); idx >= 0 {
		path = path[:idx]
	}
	if path == "/v2" || path == "/v2/" {
		return pathClassPing
	}
	if !strings.HasPrefix(path, "/v2/") {
		return pathClassOther
	}

	rest := strings.TrimPrefix(path, "/v2/")
	sep, kind := rightmostOCIBoundary(rest)
	if sep <= 0 {
		return pathClassOther
	}

	var ref string
	switch kind {
	case "blobs":
		ref = rest[sep+len("/blobs/"):]
		if ref == "uploads" || strings.HasPrefix(ref, "uploads/") {
			return pathClassOther
		}
		if isDigest(ref) {
			return pathClassBlob
		}
		return pathClassOther
	case "manifests":
		ref = rest[sep+len("/manifests/"):]
		if strings.Contains(ref, "/") || ref == "" {
			return pathClassOther
		}
		if isDigest(ref) {
			return pathClassManifestByDigest
		}
		return pathClassManifestByTag
	default:
		return pathClassOther
	}
}

func rightmostOCIBoundary(rest string) (int, string) {
	blobIndex := strings.LastIndex(rest, "/blobs/")
	manifestIndex := strings.LastIndex(rest, "/manifests/")
	if blobIndex > manifestIndex {
		return blobIndex, "blobs"
	}
	if manifestIndex >= 0 {
		return manifestIndex, "manifests"
	}
	return -1, ""
}

var digestRe = regexp.MustCompile(`(?i)^sha256:[a-f0-9]{64}$`)

func isDigest(ref string) bool {
	return digestRe.MatchString(ref)
}

func doWithAuth(
	ctx context.Context,
	client *http.Client,
	cfg *config,
	cache *tokenCache,
	obs *observer,
	method string,
	target *url.URL,
	headers http.Header,
	body io.Reader,
) (*http.Response, bool, error) {
	scope := guessScope(target.Path)

	req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, false, err
	}
	copyForwardedHeaders(req.Header, headers)
	attachInitialAuth(req.Header, cfg, cache, scope)

	resp, err := client.Do(req)
	if err != nil {
		return nil, false, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, false, nil
	}
	if cfg.authMode == "basic" {
		return resp, false, nil
	}

	challenge := parseBearerChallenge(resp.Header.Get("WWW-Authenticate"))
	if challenge == nil {
		return resp, false, nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	tok, exp, err := exchangeToken(ctx, client, cfg, challenge)
	if err != nil {
		obs.recordAuthRefresh("error")
		return nil, false, fmt.Errorf("token exchange (realm=%s): %w", challenge.realm, err)
	}
	obs.recordAuthRefresh("success")

	cacheScope := challenge.scope
	if cacheScope == "" {
		cacheScope = scope
	}
	if cacheScope != "" {
		lifetimeCap := time.Now().Add(cfg.maxTokenLife)
		if exp.IsZero() || exp.After(lifetimeCap) {
			exp = lifetimeCap
		}
		cache.store(cacheScope, tok, exp)
	}

	req2, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, true, err
	}
	copyForwardedHeaders(req2.Header, headers)
	req2.Header.Set("Authorization", "Bearer "+tok)
	resp, err = client.Do(req2)
	return resp, true, err
}

func attachInitialAuth(header http.Header, cfg *config, cache *tokenCache, scope string) {
	switch cfg.authMode {
	case "basic":
		header.Set("Authorization", "Basic "+basicAuth(cfg.user, cfg.pass))
	case "bearer":
		if tok, ok := cache.lookup(scope); ok {
			header.Set("Authorization", "Bearer "+tok)
		}
	case "auto":
		if tok, ok := cache.lookup(scope); ok {
			header.Set("Authorization", "Bearer "+tok)
		} else {
			header.Set("Authorization", "Basic "+basicAuth(cfg.user, cfg.pass))
		}
	}
}

type bearerChallenge struct {
	realm   string
	service string
	scope   string
}

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

func exchangeToken(ctx context.Context, client *http.Client, cfg *config, c *bearerChallenge) (string, time.Time, error) {
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
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return "", time.Time{}, fmt.Errorf("realm returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
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

func guessScope(path string) string {
	if !strings.HasPrefix(path, "/v2/") {
		return ""
	}
	rest := strings.TrimPrefix(path, "/v2/")
	sep, _ := rightmostOCIBoundary(rest)
	if sep <= 0 {
		return ""
	}
	repo := rest[:sep]
	return "repository:" + repo + ":pull"
}

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
	"Authorization":       true,
}

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
