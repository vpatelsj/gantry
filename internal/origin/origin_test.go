package origin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
)

func digestOf(b []byte) digest.Digest {
	sum := sha256.Sum256(b)
	d, err := digest.Parse("sha256:" + hex.EncodeToString(sum[:]))
	if err != nil {
		panic(err)
	}
	return d
}

func newClient(t *testing.T, ur config.UpstreamRegistry) *Client {
	t.Helper()
	cfg := &config.Config{UpstreamRegistries: []config.UpstreamRegistry{ur}}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestPullBlob_Success(t *testing.T) {
	body := []byte("layer-bytes")
	d := digestOf(body)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/library/nginx/blobs/"+d.String() {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			_, _ = w.Write(body)
			return
		}
		t.Errorf("unexpected request: %s", r.URL.Path)
		w.WriteHeader(404)
	}))
	defer srv.Close()

	c := newClient(t, config.UpstreamRegistry{Name: "reg", Endpoint: srv.URL})
	rc, size, err := c.Pull(context.Background(), ifaces.OriginRef{
		Registry: "reg", Repository: "library/nginx", Digest: d, Kind: ifaces.KindBlob,
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != string(body) {
		t.Errorf("body = %q", got)
	}
	if size != int64(len(body)) {
		t.Errorf("size = %d, want %d", size, len(body))
	}
}

func TestPullManifest_AcceptHeaderAndPath(t *testing.T) {
	body := []byte(`{"schemaVersion":2}`)
	d := digestOf(body)
	var seenAccept atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAccept.Store(r.Header.Get("Accept"))
		if !strings.Contains(r.URL.Path, "/manifests/") {
			t.Errorf("manifest pull hit wrong path: %s", r.URL.Path)
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	c := newClient(t, config.UpstreamRegistry{Name: "reg", Endpoint: srv.URL})
	rc, _, err := c.Pull(context.Background(), ifaces.OriginRef{
		Registry: "reg", Repository: "library/nginx", Digest: d, Kind: ifaces.KindManifest,
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	rc.Close()
	got, _ := seenAccept.Load().(string)
	if !strings.Contains(got, "manifest.v1+json") {
		t.Errorf("Accept header missing manifest media types: %q", got)
	}
}

func TestPull_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer srv.Close()

	c := newClient(t, config.UpstreamRegistry{Name: "reg", Endpoint: srv.URL})
	d := digestOf([]byte("x"))
	_, _, err := c.Pull(context.Background(), ifaces.OriginRef{
		Registry: "reg", Repository: "r", Digest: d,
	})
	var oe *ifaces.OriginError
	if !errors.As(err, &oe) || oe.Class != ifaces.FailureNotFound {
		t.Fatalf("want FailureNotFound, got %v", err)
	}
}

func TestPull_RateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer srv.Close()

	c := newClient(t, config.UpstreamRegistry{Name: "reg", Endpoint: srv.URL})
	d := digestOf([]byte("x"))
	_, _, err := c.Pull(context.Background(), ifaces.OriginRef{Registry: "reg", Repository: "r", Digest: d})
	var oe *ifaces.OriginError
	if !errors.As(err, &oe) || oe.Class != ifaces.FailureRateLimited {
		t.Fatalf("want FailureRateLimited, got %v", err)
	}
}

func TestPull_TransientOn5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
	}))
	defer srv.Close()

	c := newClient(t, config.UpstreamRegistry{Name: "reg", Endpoint: srv.URL})
	d := digestOf([]byte("x"))
	_, _, err := c.Pull(context.Background(), ifaces.OriginRef{Registry: "reg", Repository: "r", Digest: d})
	var oe *ifaces.OriginError
	if !errors.As(err, &oe) || oe.Class != ifaces.FailureTransient {
		t.Fatalf("want FailureTransient, got %v", err)
	}
}

func TestPull_UnknownRegistry(t *testing.T) {
	c := newClient(t, config.UpstreamRegistry{Name: "reg", Endpoint: "https://reg.example.com"})
	d := digestOf([]byte("x"))
	_, _, err := c.Pull(context.Background(), ifaces.OriginRef{Registry: "other", Repository: "r", Digest: d})
	var oe *ifaces.OriginError
	if !errors.As(err, &oe) || oe.Class != ifaces.FailureNotFound {
		t.Fatalf("want FailureNotFound for unknown registry, got %v", err)
	}
}

func TestPull_BearerTokenFlow(t *testing.T) {
	body := []byte("token-protected")
	d := digestOf(body)
	var authReqs, tokenReqs, dataReqs int32

	// Set up the token endpoint first so we know its URL.
	tokenMux := http.NewServeMux()
	tokenSrv := httptest.NewServer(tokenMux)
	defer tokenSrv.Close()
	tokenMux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenReqs, 1)
		user, pass, ok := r.BasicAuth()
		if !ok || user != "alice" || pass != "secret" {
			t.Errorf("token auth missing/wrong: ok=%v user=%q", ok, user)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "deadbeef"})
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer deadbeef" {
			atomic.AddInt32(&authReqs, 1)
			w.Header().Set("WWW-Authenticate",
				`Bearer realm="`+tokenSrv.URL+`/token",service="reg",scope="repository:library/nginx:pull"`)
			w.WriteHeader(401)
			return
		}
		atomic.AddInt32(&dataReqs, 1)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dir := t.TempDir()
	credsPath := filepath.Join(dir, "creds")
	if err := os.WriteFile(credsPath, []byte("alice:secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newClient(t, config.UpstreamRegistry{Name: "reg", Endpoint: srv.URL, CredentialsPath: credsPath})
	rc, _, err := c.Pull(context.Background(), ifaces.OriginRef{
		Registry: "reg", Repository: "library/nginx", Digest: d,
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(body) {
		t.Errorf("body = %q", got)
	}
	if atomic.LoadInt32(&authReqs) == 0 || atomic.LoadInt32(&tokenReqs) == 0 || atomic.LoadInt32(&dataReqs) == 0 {
		t.Errorf("flow incomplete: auth=%d token=%d data=%d", authReqs, tokenReqs, dataReqs)
	}

	// Second pull should reuse the cached token (no extra token request).
	rc2, _, err := c.Pull(context.Background(), ifaces.OriginRef{
		Registry: "reg", Repository: "library/nginx", Digest: d,
	})
	if err != nil {
		t.Fatalf("Pull (2nd): %v", err)
	}
	io.Copy(io.Discard, rc2)
	rc2.Close()
	if atomic.LoadInt32(&tokenReqs) != 1 {
		t.Errorf("tokenReqs after 2nd pull = %d, want 1 (cached)", tokenReqs)
	}
}

func TestNSAliasResolves(t *testing.T) {
	body := []byte("aliased")
	d := digestOf(body)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	cfg := &config.Config{UpstreamRegistries: []config.UpstreamRegistry{
		{Name: "ghcr.io", Endpoint: srv.URL, NSAlias: "github"},
	}}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Pull by alias.
	rc, _, err := c.Pull(context.Background(), ifaces.OriginRef{
		Registry: "github", Repository: "owner/repo", Digest: d,
	})
	if err != nil {
		t.Fatalf("Pull(alias): %v", err)
	}
	rc.Close()
}

func TestNewRejectsBadCredentialsFile(t *testing.T) {
	dir := t.TempDir()
	credsPath := filepath.Join(dir, "creds")
	if err := os.WriteFile(credsPath, []byte("no-colon-here\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{UpstreamRegistries: []config.UpstreamRegistry{
		{Name: "reg", Endpoint: "https://reg.example.com", CredentialsPath: credsPath},
	}}
	if _, err := New(cfg); err == nil {
		t.Fatal("expected New() to reject malformed credentials")
	}
}

func TestParseChallenge(t *testing.T) {
	got := parseChallenge(`Bearer realm="https://auth.example.com/token",service="reg.example.com",scope="repository:lib/n:pull"`)
	if got["realm"] != "https://auth.example.com/token" {
		t.Errorf("realm = %q", got["realm"])
	}
	if got["service"] != "reg.example.com" {
		t.Errorf("service = %q", got["service"])
	}
	if got["scope"] != "repository:lib/n:pull" {
		t.Errorf("scope = %q", got["scope"])
	}
}
