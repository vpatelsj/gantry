package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestClassifyPath(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	cases := []struct {
		name string
		path string
		want pathClass
	}{
		{name: "ping slash", path: "/v2/", want: pathClassPing},
		{name: "ping no slash", path: "/v2", want: pathClassPing},
		{name: "manifest tag", path: "/v2/acme/team/svc/manifests/v1.2.3", want: pathClassManifestByTag},
		{name: "manifest tag with query", path: "/v2/acme/team/svc/manifests/v1.2.3?ns=demo", want: pathClassManifestByTag},
		{name: "manifest digest", path: "/v2/acme/team/svc/manifests/" + digest, want: pathClassManifestByDigest},
		{name: "blob digest", path: "/v2/acme/team/svc/blobs/" + digest, want: pathClassBlob},
		{name: "uppercase digest accepted", path: "/v2/acme/blobs/sha256:" + strings.Repeat("A", 64), want: pathClassBlob},
		{name: "blob upload", path: "/v2/acme/team/svc/blobs/uploads/123", want: pathClassOther},
		{name: "tag contains slash", path: "/v2/acme/team/svc/manifests/release/candidate", want: pathClassOther},
		{name: "trailing manifest slash", path: "/v2/acme/team/svc/manifests/", want: pathClassOther},
		{name: "unknown", path: "/v2/acme/team/svc/tags/list", want: pathClassOther},
		{name: "not v2", path: "/status", want: pathClassOther},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyPath(tc.path); got != tc.want {
				t.Fatalf("classifyPath(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

func TestProxyCountsHappyPathAndOverridesAuthorization(t *testing.T) {
	upstreamBody := "hello-proxy"
	var upstreamAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, upstreamBody)
	}))
	defer upstream.Close()

	obs, handler := testProxy(t, upstream.URL, "basic")
	req := httptest.NewRequest(http.MethodGet, "/v2/acme/team/svc/blobs/"+testDigest(), nil)
	req.Header.Set("Authorization", "Bearer inbound-token")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if rr.Body.String() != upstreamBody {
		t.Fatalf("body = %q, want %q", rr.Body.String(), upstreamBody)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("demo-user:demo-pass"))
	if upstreamAuth != wantAuth {
		t.Fatalf("upstream Authorization = %q, want %q", upstreamAuth, wantAuth)
	}

	assertMetric(t, obs.started.WithLabelValues(http.MethodGet, string(pathClassBlob)), 1)
	assertMetric(t, obs.completed.WithLabelValues(http.MethodGet, string(pathClassBlob), "200"), 1)
	assertMetric(t, obs.bytesUpstream.WithLabelValues(string(pathClassBlob), "200"), float64(len(upstreamBody)))
	assertMetric(t, obs.bytesToClient.WithLabelValues(string(pathClassBlob), "200"), float64(len(upstreamBody)))
	assertMetric(t, obs.inflight.WithLabelValues(string(pathClassBlob)), 0)

	snap := obs.snapshot(time.Now())
	if snap.Totals.RequestsCompleted != 1 || snap.Totals.BytesToClient != uint64(len(upstreamBody)) {
		t.Fatalf("summary totals = %+v", snap.Totals)
	}
	if got := snap.Totals.ByPathClass[pathClassBlob]; got.Requests != 1 || got.Bytes != uint64(len(upstreamBody)) {
		t.Fatalf("blob summary = %+v", got)
	}
}

func TestProxyRecordsClientClosedOnceAndClearsInflight(t *testing.T) {
	upstreamBody := strings.Repeat("x", copyBufferBytes+17)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, upstreamBody)
	}))
	defer upstream.Close()

	obs, handler := testProxy(t, upstream.URL, "basic")
	req := httptest.NewRequest(http.MethodGet, "/v2/acme/team/svc/blobs/"+testDigest(), nil)
	w := &failingResponseWriter{header: make(http.Header), failAfter: 3}
	handler.ServeHTTP(w, req)

	assertMetric(t, obs.started.WithLabelValues(http.MethodGet, string(pathClassBlob)), 1)
	assertMetric(t, obs.completed.WithLabelValues(http.MethodGet, string(pathClassBlob), "client_closed"), 1)
	assertMetric(t, obs.inflight.WithLabelValues(string(pathClassBlob)), 0)

	toClient := testutil.ToFloat64(obs.bytesToClient.WithLabelValues(string(pathClassBlob), "client_closed"))
	fromUpstream := testutil.ToFloat64(obs.bytesUpstream.WithLabelValues(string(pathClassBlob), "client_closed"))
	if toClient > fromUpstream {
		t.Fatalf("bytes_to_client=%f > bytes_upstream=%f", toClient, fromUpstream)
	}
	if toClient != 3 {
		t.Fatalf("bytes_to_client = %f, want 3", toClient)
	}
}

func TestBearerChallengeRefreshesOnceAndUsesCachedToken(t *testing.T) {
	var tokenCalls int64
	var upstreamCalls int64
	var upstream *httptest.Server

	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth2/token":
			atomic.AddInt64(&tokenCalls, 1)
			if r.Header.Get("Authorization") == "" {
				t.Fatalf("token request missing Authorization")
			}
			if got := r.URL.Query().Get("scope"); got != "repository:acme/team/svc:pull" {
				t.Fatalf("scope = %q", got)
			}
			_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "demo-token", ExpiresIn: 3600})
		default:
			atomic.AddInt64(&upstreamCalls, 1)
			if r.Header.Get("Authorization") != "Bearer demo-token" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="`+upstream.URL+`/oauth2/token",service="`+upstream.URL+`",scope="repository:acme/team/svc:pull"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = io.WriteString(w, "manifest")
		}
	}))
	defer upstream.Close()

	obs, handler := testProxy(t, upstream.URL, "auto")
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v2/acme/team/svc/manifests/"+testDigest(), nil)
		req.Header.Set("Authorization", "Bearer inbound-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200; body=%q", i+1, rr.Code, rr.Body.String())
		}
	}

	if got := atomic.LoadInt64(&tokenCalls); got != 1 {
		t.Fatalf("token calls = %d, want 1", got)
	}
	if got := atomic.LoadInt64(&upstreamCalls); got != 3 {
		t.Fatalf("upstream data calls = %d, want 3 (401+200, cached 200)", got)
	}
	assertMetric(t, obs.authRefresh.WithLabelValues("success"), 1)
	assertMetric(t, obs.completed.WithLabelValues(http.MethodGet, string(pathClassManifestByDigest), "200"), 2)
}

func TestTokenCacheRefreshesWithinSkew(t *testing.T) {
	var tokenCalls int64
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth2/token" {
			call := atomic.AddInt64(&tokenCalls, 1)
			_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: "token-" + strconv.FormatInt(call, 10), ExpiresIn: 1})
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer token-") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+upstream.URL+`/oauth2/token",service="`+upstream.URL+`",scope="repository:acme/team/svc:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	obs, handler := testProxy(t, upstream.URL, "auto")
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v2/acme/team/svc/manifests/latest", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i+1, rr.Code)
		}
	}

	if got := atomic.LoadInt64(&tokenCalls); got != 2 {
		t.Fatalf("token calls = %d, want 2 because token expires within refresh skew", got)
	}
	assertMetric(t, obs.authRefresh.WithLabelValues("success"), 2)
}

func TestSummaryHandlerShape(t *testing.T) {
	obs := newObserver(prometheus.NewRegistry(), time.Now().Add(-2*time.Second))
	obs.begin(http.MethodHead, pathClassPing)
	obs.finish(http.MethodHead, pathClassPing, "200", 0, 0, time.Millisecond)

	rr := httptest.NewRecorder()
	summaryHandler(obs).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/debug/summary", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	var got summary
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if got.Since == "" || got.UptimeSecs < 1 {
		t.Fatalf("unexpected time fields: %+v", got)
	}
	if got.Totals.RequestsCompleted != 1 {
		t.Fatalf("requests_completed = %d, want 1", got.Totals.RequestsCompleted)
	}
	if got.Totals.ByPathClass[pathClassPing].Requests != 1 {
		t.Fatalf("ping totals = %+v", got.Totals.ByPathClass[pathClassPing])
	}
	for _, class := range allPathClasses {
		if _, ok := got.Totals.ByPathClass[class]; !ok {
			t.Fatalf("summary missing path_class %q", class)
		}
	}
}

func TestSyntheticThrottle(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("throttled request should not reach upstream")
	}))
	defer upstream.Close()

	obs, handler := testProxy(t, upstream.URL, "basic")
	obs.mu.Lock()
	obs.inflightByPathClass[pathClassBlob] = 1
	obs.mu.Unlock()

	cfg := testConfig(t, upstream.URL, "basic")
	cfg.throttleBlobInflight = 1
	cfg.throttleRetryAfterSec = 7
	handler = proxyHandler(cfg, newTokenCache(cfg.refreshSkewSecs), obs, upstream.Client())

	req := httptest.NewRequest(http.MethodGet, "/v2/acme/team/svc/blobs/"+testDigest(), nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("Retry-After = %q, want 7", got)
	}
	assertMetric(t, obs.syntheticThrottle.WithLabelValues("blob_inflight"), 1)
	assertMetric(t, obs.completed.WithLabelValues(http.MethodGet, string(pathClassBlob), "429"), 1)
}

func testProxy(t *testing.T, upstreamURL, authMode string) (*observer, http.Handler) {
	t.Helper()
	cfg := testConfig(t, upstreamURL, authMode)
	obs := newObserver(prometheus.NewRegistry(), time.Now())
	return obs, proxyHandler(cfg, newTokenCache(cfg.refreshSkewSecs), obs, http.DefaultClient)
}

func testConfig(t *testing.T, upstreamURL, authMode string) *config {
	t.Helper()
	u, err := url.Parse(upstreamURL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	return &config{
		listen:                defaultListenAddr,
		metricsListen:         defaultMetricsListenAddr,
		upstream:              u,
		user:                  "demo-user",
		pass:                  "demo-pass",
		authMode:              authMode,
		maxTokenLife:          defaultMaxTokenLife,
		refreshSkewSecs:       defaultRefreshSkewSecs,
		throttleRetryAfterSec: 5,
	}
}

func testDigest() string {
	return "sha256:" + strings.Repeat("a", 64)
}

func assertMetric(t *testing.T, collector prometheus.Collector, want float64) {
	t.Helper()
	if got := testutil.ToFloat64(collector); got != want {
		t.Fatalf("metric = %f, want %f", got, want)
	}
}

type failingResponseWriter struct {
	header    http.Header
	status    int
	written   int
	failAfter int
}

func (w *failingResponseWriter) Header() http.Header {
	return w.header
}

func (w *failingResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *failingResponseWriter) Write(p []byte) (int, error) {
	remaining := w.failAfter - w.written
	if remaining <= 0 {
		return 0, errors.New("client closed")
	}
	if len(p) > remaining {
		w.written += remaining
		return remaining, errors.New("client closed")
	}
	w.written += len(p)
	return len(p), nil
}
