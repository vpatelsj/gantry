package mirror_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gantry/gantry/internal/cache"
	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/mirror"
	"github.com/gantry/gantry/internal/origin"
)

func digestOf(b []byte) digest.Digest {
	sum := sha256.Sum256(b)
	d, err := digest.Parse("sha256:" + hex.EncodeToString(sum[:]))
	if err != nil {
		panic(err)
	}
	return d
}

type fixture struct {
	cfg        *config.Config
	cache      *cache.Cache
	upstream   *httptest.Server
	server     *httptest.Server
	originHits *int32
}

func newFixture(t *testing.T, blobs map[digest.Digest][]byte) *fixture {
	t.Helper()
	var originHits int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originHits, 1)
		// Extract digest from /v2/<repo>/blobs/<digest> or /v2/<repo>/manifests/<digest>
		path := r.URL.Path
		var refStart int
		switch {
		case strings.Contains(path, "/blobs/"):
			refStart = strings.LastIndex(path, "/blobs/") + len("/blobs/")
		case strings.Contains(path, "/manifests/"):
			refStart = strings.LastIndex(path, "/manifests/") + len("/manifests/")
		default:
			w.WriteHeader(404)
			return
		}
		ref := path[refStart:]
		d, err := digest.Parse(ref)
		if err != nil {
			w.WriteHeader(404)
			return
		}
		body, ok := blobs[d]
		if !ok {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(up.Close)

	cfg := &config.Config{
		UpstreamRegistries: []config.UpstreamRegistry{
			{Name: "reg.example.com", Endpoint: up.URL},
		},
	}
	c, err := cache.Open(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	oc, err := origin.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	m := mirror.New(cfg, c, oc)
	srv := httptest.NewServer(m.Handler())
	t.Cleanup(srv.Close)

	return &fixture{cfg: cfg, cache: c, upstream: up, server: srv, originHits: &originHits}
}

func TestMirror_V2Root(t *testing.T) {
	f := newFixture(t, nil)
	resp, err := http.Get(f.server.URL + "/v2/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Docker-Distribution-API-Version"); got != "registry/2.0" {
		t.Errorf("api version header = %q", got)
	}
}

func TestMirror_Healthz(t *testing.T) {
	f := newFixture(t, nil)
	resp, err := http.Get(f.server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Errorf("healthz: %d %q", resp.StatusCode, body)
	}
}

func TestMirror_TagManifestReturns503(t *testing.T) {
	f := newFixture(t, nil)
	resp, err := http.Get(f.server.URL + "/v2/library/nginx/manifests/latest")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("tag fallthrough: got %d, want 503", resp.StatusCode)
	}
	if atomic.LoadInt32(f.originHits) != 0 {
		t.Errorf("origin was hit during tag fallthrough; should not be (containerd retries directly)")
	}
}

func TestMirror_BlobMissPullsFromOriginAndCaches(t *testing.T) {
	body := []byte("layer-contents")
	d := digestOf(body)
	f := newFixture(t, map[digest.Digest][]byte{d: body})

	url := f.server.URL + "/v2/library/nginx/blobs/" + d.String()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("first GET: %d %q", resp.StatusCode, got)
	}
	if resp.Header.Get("Docker-Content-Digest") != d.String() {
		t.Errorf("Docker-Content-Digest header missing")
	}

	// Second GET: should hit cache, not origin.
	resp2, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	got2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if string(got2) != string(body) {
		t.Errorf("cache GET: %q", got2)
	}
	if atomic.LoadInt32(f.originHits) != 1 {
		t.Errorf("origin hit %d times; want exactly 1 (second was a cache hit)", *f.originHits)
	}

	// Cache should have it.
	ok, _ := f.cache.Has(context.Background(), d)
	if !ok {
		t.Error("cache.Has(d) = false after stream-cache")
	}
}

func TestMirror_ManifestByDigest(t *testing.T) {
	body := []byte(`{"schemaVersion":2}`)
	d := digestOf(body)
	f := newFixture(t, map[digest.Digest][]byte{d: body})

	url := f.server.URL + "/v2/library/nginx/manifests/" + d.String()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("manifest GET: %d %q", resp.StatusCode, got)
	}
}

func TestMirror_OriginNotFoundIs404(t *testing.T) {
	f := newFixture(t, nil)
	d := digestOf([]byte("missing"))
	resp, err := http.Get(f.server.URL + "/v2/library/nginx/blobs/" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("want 404, got %d", resp.StatusCode)
	}
}

func TestMirror_NSParamSelectsUpstream(t *testing.T) {
	// Two upstreams: ?ns= becomes required.
	body1, body2 := []byte("from-reg-A"), []byte("from-reg-B")
	d := digestOf(body1) // same digest in both for simplicity
	_ = body2

	upA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body1)))
		_, _ = w.Write(body1)
	}))
	defer upA.Close()
	upB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body1)))
		_, _ = w.Write(body1)
	}))
	defer upB.Close()

	cfg := &config.Config{UpstreamRegistries: []config.UpstreamRegistry{
		{Name: "regA.example.com", Endpoint: upA.URL},
		{Name: "regB.example.com", Endpoint: upB.URL},
	}}
	c, _ := cache.Open(t.TempDir(), 1<<20)
	defer c.Close()
	oc, _ := origin.New(cfg)
	m := mirror.New(cfg, c, oc)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	// No ?ns=: must be 404 since there are multiple upstreams.
	resp, _ := http.Get(srv.URL + "/v2/lib/n/blobs/" + d.String())
	if resp.StatusCode != 404 {
		t.Errorf("missing ?ns=: got %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// ?ns= unknown: 404.
	resp, _ = http.Get(srv.URL + "/v2/lib/n/blobs/" + d.String() + "?ns=unknown.example.com")
	if resp.StatusCode != 404 {
		t.Errorf("unknown ?ns=: got %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// ?ns=regA.example.com works.
	resp, _ = http.Get(srv.URL + "/v2/lib/n/blobs/" + d.String() + "?ns=regA.example.com")
	if resp.StatusCode != 200 {
		t.Errorf("ns=regA: got %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestMirror_InvalidDigestRefIs400(t *testing.T) {
	f := newFixture(t, nil)
	resp, err := http.Get(f.server.URL + "/v2/lib/n/blobs/sha256:not-hex")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("invalid digest: got %d, want 400", resp.StatusCode)
	}
}

func TestMirror_HeadRequest(t *testing.T) {
	body := []byte("HEAD-test")
	d := digestOf(body)
	f := newFixture(t, map[digest.Digest][]byte{d: body})

	req, _ := http.NewRequest(http.MethodHead, f.server.URL+"/v2/lib/n/blobs/"+d.String(), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("HEAD status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Docker-Content-Digest"); got != d.String() {
		t.Errorf("HEAD Docker-Content-Digest = %q", got)
	}
	// HEAD must NOT return a body.
	body2, _ := io.ReadAll(resp.Body)
	if len(body2) != 0 {
		t.Errorf("HEAD body should be empty, got %d bytes", len(body2))
	}
}

func TestParseV2Path(t *testing.T) {
	// Direct unit-test coverage lives in internal/oci/path_test.go; this
	// case keeps an end-to-end smoke check that the mirror handler still
	// routes through the shared helper.
	f := newFixture(t, nil)
	// Junk path should be 404.
	resp, _ := http.Get(f.server.URL + "/v2/totally-bogus")
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("bogus v2 path: got %d, want 404", resp.StatusCode)
	}
}

// TestMirror_OriginSuccessMetric_FiresOnlyOnCacheCommit pins the
// eleventh-review contract: p2p_origin_pull_success_total must fire
// EXACTLY when the cluster has gained a usable artifact from the
// origin pull — body fully streamed AND cache commit succeeded.
// Earlier the origin Client fired success on Close(), which also
// fires on HEAD (body never read), on io.Copy interruption (body
// partially read), and on cache-commit failure (commit returns
// error after EOF). The mirror now owns the success hook and must
// fire it only after cw.Commit returns nil (or after the
// direct-stream digest verifier passes when cache is unavailable).
func TestMirror_OriginSuccessMetric_FiresOnlyOnCacheCommit(t *testing.T) {
	t.Run("GET fires success once after cache commit", func(t *testing.T) {
		body := []byte("origin-success-body")
		d := digestOf(body)
		f := newFixture(t, map[digest.Digest][]byte{d: body})

		var successCalls int32
		var successKinds []string
		// Rebuild server with the success hook wired.
		oc, err := origin.New(f.cfg)
		if err != nil {
			t.Fatal(err)
		}
		m := mirror.New(f.cfg, f.cache, oc,
			mirror.WithOriginSuccessMetric(func(kind string, _ int64) {
				atomic.AddInt32(&successCalls, 1)
				successKinds = append(successKinds, kind)
			}),
		)
		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := http.Get(srv.URL + "/v2/lib/n/blobs/" + d.String())
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || string(got) != string(body) {
			t.Fatalf("GET: %d %q", resp.StatusCode, got)
		}
		if n := atomic.LoadInt32(&successCalls); n != 1 {
			t.Fatalf("successCalls = %d, want 1 (one full pull + commit = one success)", n)
		}
		if len(successKinds) != 1 || successKinds[0] != "layer" {
			t.Fatalf("successKinds = %v, want [layer] (blob → layer per design-doc Prometheus vocabulary)", successKinds)
		}
	})

	t.Run("HEAD does not fire success", func(t *testing.T) {
		body := []byte("head-no-success")
		d := digestOf(body)
		f := newFixture(t, map[digest.Digest][]byte{d: body})

		var successCalls int32
		// Also count origin.Pull starts (p2p_origin_pull_total) so
		// we can pin the twelfth-review invariant: HEAD must NOT
		// bump that counter. Before Batch 61 the mirror called
		// s.origin.Pull on HEAD which fired onPullStart even
		// though HEAD never read the body, drifting the pull
		// arithmetic.
		var pullStarts int32
		oc, err := origin.New(f.cfg,
			origin.WithMetrics(
				func(_ string) { atomic.AddInt32(&pullStarts, 1) },
				func(_, _ string) {},
			),
		)
		if err != nil {
			t.Fatal(err)
		}
		m := mirror.New(f.cfg, f.cache, oc,
			mirror.WithOriginSuccessMetric(func(_ string, _ int64) {
				atomic.AddInt32(&successCalls, 1)
			}),
		)
		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		req, _ := http.NewRequest(http.MethodHead, srv.URL+"/v2/lib/n/blobs/"+d.String(), nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("HEAD: %d", resp.StatusCode)
		}
		// HEAD must NOT fire success: the body was never streamed
		// and the cache was never warmed. Counting this as a
		// success was the central case the eleventh-review fix
		// targets.
		if n := atomic.LoadInt32(&successCalls); n != 0 {
			t.Fatalf("successCalls after HEAD = %d, want 0 (HEAD never reads the body so it never produces a real success)", n)
		}
		// Twelfth-review invariant: HEAD must NOT bump
		// p2p_origin_pull_total either. HEAD now routes through
		// origin.Client.Head which deliberately does NOT call
		// onPullStart. If this regresses, started would inflate
		// against operations that fire neither success
		// (no commit) nor downstream-failure (no body copy),
		// breaking the started == success + failure + in_flight
		// arithmetic.
		if n := atomic.LoadInt32(&pullStarts); n != 0 {
			t.Fatalf("pullStarts after HEAD = %d, want 0 (HEAD must take the origin.Head path and NOT bump p2p_origin_pull_total)", n)
		}
	})

	t.Run("origin truncation does not fire success", func(t *testing.T) {
		body := []byte("full-body-but-truncated-on-the-wire")
		d := digestOf(body)
		// Custom upstream that advertises the full Content-Length
		// but writes only the first 5 bytes before closing,
		// forcing io.Copy in serveDigest to return an error
		// before cw.Commit is reached.
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body[:5])
			// httptest will close the connection here.
		}))
		defer up.Close()

		cfg := &config.Config{UpstreamRegistries: []config.UpstreamRegistry{
			{Name: "reg.example.com", Endpoint: up.URL},
		}}
		c, err := cache.Open(t.TempDir(), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()

		var successCalls int32
		// Twelfth-review: terminal counter for downstream failures.
		// Capture the (kind,class) values so we can assert the
		// exact label set this truncation maps to.
		var downstreamCalls int32
		var downstreamKinds, downstreamClasses []string
		oc, err := origin.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		m := mirror.New(cfg, c, oc,
			mirror.WithOriginSuccessMetric(func(_ string, _ int64) {
				atomic.AddInt32(&successCalls, 1)
			}),
			mirror.WithDownstreamFailureMetric(func(kind, class string) {
				atomic.AddInt32(&downstreamCalls, 1)
				downstreamKinds = append(downstreamKinds, kind)
				downstreamClasses = append(downstreamClasses, class)
			}),
		)
		srv := httptest.NewServer(m.Handler())
		defer srv.Close()

		resp, err := http.Get(srv.URL + "/v2/lib/n/blobs/" + d.String())
		if err != nil {
			t.Fatal(err)
		}
		// The mirror has already written headers (200 OK) by the
		// time the upstream truncation surfaces, so the HTTP
		// status is 200 — but the body is short and the cache
		// commit never ran. We don't assert on body length
		// because the truncation may surface as a copy error
		// before the client side sees EOF; what we DO assert is
		// the success metric never fired.
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()

		if n := atomic.LoadInt32(&successCalls); n != 0 {
			t.Fatalf("successCalls after truncated origin = %d, want 0 (io.Copy returned an error before cw.Commit; this is the cache-commit-skipped path the success metric must NOT count)", n)
		}
		// The truncation may surface in EITHER io.Copy
		// (truncated read) OR cw.Commit (digest mismatch on
		// finalize). Both paths fire the downstream failure
		// hook, so we expect exactly 1.
		if n := atomic.LoadInt32(&downstreamCalls); n != 1 {
			t.Fatalf("downstreamCalls after truncated origin = %d, want 1 (origin returned 2xx; the downstream io.Copy or cw.Commit failed; must move arithmetic off in-flight)", n)
		}
		if len(downstreamKinds) != 1 || downstreamKinds[0] != "layer" {
			t.Fatalf("downstreamKinds = %v, want [layer]", downstreamKinds)
		}
		if len(downstreamClasses) != 1 || downstreamClasses[0] != string(ifaces.FailureTransient) {
			t.Fatalf("downstreamClasses = %v, want [transient] (downstream failures reserved class)", downstreamClasses)
		}
		// The cache must also be empty since commit never ran.
		if ok, _ := c.Has(context.Background(), d); ok {
			t.Errorf("cache.Has(d) = true after truncated origin pull; want false (commit must not have run)")
		}
	})
}

// TestMirror_OriginPullArithmeticIdentity pins the twelfth-review
// arithmetic invariant for the mirror direct-origin path:
//
//	p2p_origin_pull_total{kind}  ==  p2p_origin_pull_success_total{kind}
//	                              +  p2p_origin_pull_failure_total{kind,class=any}
//	                              +  (in-flight at scrape time)
//
// Before this batch, downstream failures (io.Copy stall / cw.Commit
// digest mismatch) bumped neither success nor failure, so started
// drifted positive on every cache-commit-skipped path. This test
// drives the mirror through one success and one truncation, and
// asserts started == success + failure == 2 with zero in-flight at
// the end.
func TestMirror_OriginPullArithmeticIdentity(t *testing.T) {
	good := []byte("good-body")
	gd := digestOf(good)
	truncBody := []byte("full-body-but-truncated-here")
	td := digestOf(truncBody)

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/blobs/"+gd.String()):
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(good)))
			_, _ = w.Write(good)
		case strings.HasSuffix(r.URL.Path, "/blobs/"+td.String()):
			// Advertise full length but write only a few bytes
			// then drop the connection so io.Copy in serveDigest
			// returns an error.
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(truncBody)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(truncBody[:5])
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer up.Close()

	cfg := &config.Config{UpstreamRegistries: []config.UpstreamRegistry{
		{Name: "reg.example.com", Endpoint: up.URL},
	}}
	c, err := cache.Open(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	var (
		started    int32
		successes  int32
		failures   int32
		downstream int32
	)
	oc, err := origin.New(cfg,
		origin.WithMetrics(
			func(_ string) { atomic.AddInt32(&started, 1) },
			func(_, _ string) { atomic.AddInt32(&failures, 1) },
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	m := mirror.New(cfg, c, oc,
		mirror.WithOriginSuccessMetric(func(_ string, _ int64) {
			atomic.AddInt32(&successes, 1)
		}),
		mirror.WithDownstreamFailureMetric(func(_, _ string) {
			atomic.AddInt32(&downstream, 1)
		}),
	)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	// Successful pull.
	resp, err := http.Get(srv.URL + "/v2/lib/n/blobs/" + gd.String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Truncated pull.
	resp, err = http.Get(srv.URL + "/v2/lib/n/blobs/" + td.String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Identity: started == success + failure + downstream.
	// In-flight is zero because both requests have returned.
	s := atomic.LoadInt32(&started)
	su := atomic.LoadInt32(&successes)
	fa := atomic.LoadInt32(&failures)
	dn := atomic.LoadInt32(&downstream)
	if s != 2 {
		t.Errorf("started = %d, want 2 (one GET per pull)", s)
	}
	if su != 1 {
		t.Errorf("successes = %d, want 1 (only the good pull committed)", su)
	}
	if fa != 0 {
		t.Errorf("origin-side failures = %d, want 0 (both pulls got 200 from origin)", fa)
	}
	if dn != 1 {
		t.Errorf("downstream failures = %d, want 1 (the truncation)", dn)
	}
	if s != su+fa+dn {
		t.Errorf("arithmetic identity broken: started=%d != success(%d)+failure(%d)+downstream(%d) = %d", s, su, fa, dn, su+fa+dn)
	}
}
