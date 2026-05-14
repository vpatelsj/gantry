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
	// Exercise via the HTTP handler since parseV2Path is unexported.
	f := newFixture(t, nil)
	// Junk path should be 404.
	resp, _ := http.Get(f.server.URL + "/v2/totally-bogus")
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("bogus v2 path: got %d, want 404", resp.StatusCode)
	}
}
