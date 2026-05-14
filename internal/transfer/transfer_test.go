package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/ifaces/fakes"
)

func mustDigest(b []byte) digest.Digest {
	sum := sha256.Sum256(b)
	return digest.MustParse("sha256:" + hex.EncodeToString(sum[:]))
}

func newTestServer(t *testing.T) (*httptest.Server, *fakes.Cache, *int) {
	t.Helper()
	cache := fakes.NewCache()
	served := 0
	s := New(cache, WithMetrics(
		func() { served++ },
		nil,
	))
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, cache, &served
}

func TestV2Root(t *testing.T) {
	ts, _, _ := newTestServer(t)
	resp, err := http.Get(ts.URL + "/v2/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Docker-Distribution-API-Version"); got != "registry/2.0" {
		t.Errorf("API-Version header = %q, want registry/2.0", got)
	}
}

func TestRequiresMirroredHeader(t *testing.T) {
	ts, cache, _ := newTestServer(t)
	body := []byte("hello")
	d := mustDigest(body)
	cache.Put(d, body)

	resp, err := http.Get(ts.URL + "/v2/myrepo/blobs/" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (missing %s header)", resp.StatusCode, MirroredHeader)
	}
}

func TestServeFromCache(t *testing.T) {
	ts, cache, served := newTestServer(t)
	body := []byte("hello world, this is a peer-served blob")
	d := mustDigest(body)
	cache.Put(d, body)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/myrepo/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Docker-Content-Digest"); got != d.String() {
		t.Errorf("Docker-Content-Digest = %q, want %q", got, d)
	}
	if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("Accept-Ranges = %q, want bytes", got)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(body) {
		t.Errorf("body mismatch: got %q, want %q", got, body)
	}
	if *served != 1 {
		t.Errorf("served count = %d, want 1", *served)
	}
}

func TestMiss404(t *testing.T) {
	ts, _, _ := newTestServer(t)
	d := digest.MustParse("sha256:" + strings.Repeat("a", 64))
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/r/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestTagAlways404(t *testing.T) {
	ts, _, _ := newTestServer(t)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/r/manifests/latest", nil)
	req.Header.Set(MirroredHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (tags banned at peer endpoint)", resp.StatusCode)
	}
}

func TestRangeRequest(t *testing.T) {
	ts, cache, _ := newTestServer(t)
	body := []byte("0123456789ABCDEF")
	d := mustDigest(body)
	cache.Put(d, body)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/r/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	req.Header.Set("Range", "bytes=2-5")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want 206", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes 2-5/16" {
		t.Errorf("Content-Range = %q, want bytes 2-5/16", got)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "2345" {
		t.Errorf("body = %q, want 2345", got)
	}
}

func TestSuffixRange(t *testing.T) {
	ts, cache, _ := newTestServer(t)
	body := []byte("abcdefgh")
	d := mustDigest(body)
	cache.Put(d, body)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/r/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	req.Header.Set("Range", "bytes=-3")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status = %d, want 206", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "fgh" {
		t.Errorf("body = %q, want fgh", got)
	}
}

func TestInvalidRange(t *testing.T) {
	ts, cache, _ := newTestServer(t)
	body := []byte("ab")
	d := mustDigest(body)
	cache.Put(d, body)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/r/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	req.Header.Set("Range", "bytes=10-20")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("status = %d, want 416", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes */2" {
		t.Errorf("Content-Range = %q, want bytes */2", got)
	}
}

func TestHeadServesHeadersOnly(t *testing.T) {
	ts, cache, _ := newTestServer(t)
	body := []byte("payload")
	d := mustDigest(body)
	cache.Put(d, body)

	req, _ := http.NewRequest(http.MethodHead, ts.URL+"/v2/r/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Docker-Content-Digest"); got != d.String() {
		t.Errorf("Docker-Content-Digest = %q, want %q", got, d)
	}
	gotBody, _ := io.ReadAll(resp.Body)
	if len(gotBody) != 0 {
		t.Errorf("HEAD body len = %d, want 0", len(gotBody))
	}
}

func TestInvalidDigest400(t *testing.T) {
	ts, _, _ := newTestServer(t)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/r/blobs/sha256:not-hex", nil)
	req.Header.Set(MirroredHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	ts, _, _ := newTestServer(t)
	d := digest.MustParse("sha256:" + strings.Repeat("0", 64))
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v2/r/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != "GET, HEAD" {
		t.Errorf("Allow header = %q, want GET, HEAD", got)
	}
}

// fakeSecondary is a SecondaryBlobSource that returns a fixed body for
// one digest and *ErrNotFound for everything else, used to assert the
// transfer server's miss → secondary fallback (Batch 19 / review #5).
type fakeSecondary struct {
	digest digest.Digest
	body   []byte
}

func (f *fakeSecondary) Open(_ context.Context, d digest.Digest) (io.ReadCloser, int64, error) {
	if d.String() != f.digest.String() {
		return nil, 0, &ifaces.ErrNotFound{Digest: d}
	}
	return io.NopCloser(strings.NewReader(string(f.body))), int64(len(f.body)), nil
}

func TestSecondaryBlobSource_ServesOnCacheMiss(t *testing.T) {
	cache := fakes.NewCache()
	body := []byte("served from containerd content store")
	d := mustDigest(body)
	// Deliberately NOT calling cache.Put — the cache misses, the
	// secondary must serve.
	served := 0
	missed := 0
	s := New(cache,
		WithMetrics(func() { served++ }, func() { missed++ }),
		WithSecondaryBlobSource(&fakeSecondary{digest: d, body: body}),
	)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/myrepo/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (secondary should serve)", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("body = %q, want %q", got, body)
	}
	if served != 1 {
		t.Errorf("served counter = %d, want 1 (secondary hit still bumps peer-serve)", served)
	}
	if missed != 0 {
		t.Errorf("missed counter = %d, want 0 (secondary covered the miss)", missed)
	}
}

func TestSecondaryBlobSource_404WhenBothMiss(t *testing.T) {
	cache := fakes.NewCache()
	body := []byte("not in cache, not in secondary either")
	d := mustDigest(body)
	otherD := mustDigest([]byte("something else entirely"))
	missed := 0
	s := New(cache,
		WithMetrics(nil, func() { missed++ }),
		WithSecondaryBlobSource(&fakeSecondary{digest: otherD, body: []byte("x")}),
	)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v2/myrepo/blobs/"+d.String(), nil)
	req.Header.Set(MirroredHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (both layers miss)", resp.StatusCode)
	}
	if missed != 1 {
		t.Errorf("missed counter = %d, want 1", missed)
	}
}
