package mirror_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/gantry/gantry/internal/cache"
	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/ifaces/fakes"
	"github.com/gantry/gantry/internal/mirror"
	"github.com/gantry/gantry/internal/origin"
	"github.com/gantry/gantry/internal/transfer"
)

// startPeerTransfer stands up a real :5001-style h2c transfer server on an
// ephemeral loopback port backed by the given Cache and returns its
// "host:port" address.
func startPeerTransfer(t *testing.T, c ifaces.Cache) string {
	t.Helper()
	s := transfer.New(c)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	h2s := &http2.Server{}
	hsrv := &http.Server{
		Handler:           h2c.NewHandler(s.Handler(), h2s),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = hsrv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = hsrv.Shutdown(ctx)
		_ = ln.Close()
	})
	return ln.Addr().String()
}

func newMirrorWithPeer(t *testing.T, originBlobs map[digest.Digest][]byte, providers map[digest.Digest][]ifaces.Provider) (*httptest.Server, *cache.Cache, *int32, *int32) {
	t.Helper()
	var originHits int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originHits, 1)
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
		body, ok := originBlobs[d]
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

	dht := fakes.NewDHT()
	for d, provs := range providers {
		dht.Inject(d, provs...)
	}

	var peerFetches int32
	client := transfer.NewClient(transfer.WithDialTimeout(time.Second), transfer.WithRequestTimeout(5*time.Second))
	m := mirror.New(cfg, c, oc,
		mirror.WithDiscovery(dht, client),
		mirror.WithPeerBudgets(2*time.Second, 5*time.Second, 3),
		mirror.WithPeerMetrics(
			func(outcome string) {
				if outcome == "hit" {
					atomic.AddInt32(&peerFetches, 1)
				}
			},
			nil,
		),
	)
	srv := httptest.NewServer(m.Handler())
	t.Cleanup(srv.Close)
	return srv, c, &originHits, &peerFetches
}

func TestMirror_PeerFallback_ServesFromPeerNotOrigin(t *testing.T) {
	body := []byte("the canonical bytes for this digest")
	d := digestOf(body)

	peerCache := fakes.NewCache()
	peerCache.Put(d, body)
	peerAddr := startPeerTransfer(t, peerCache)

	srv, _, originHits, peerFetches := newMirrorWithPeer(
		t,
		map[digest.Digest][]byte{d: body},
		map[digest.Digest][]ifaces.Provider{d: {{NodeID: "peer-a", Addr: peerAddr}}},
	)

	resp, err := http.Get(srv.URL + "/v2/r/blobs/" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(body) {
		t.Errorf("body mismatch: got %q want %q", got, body)
	}
	if atomic.LoadInt32(originHits) != 0 {
		t.Errorf("origin hits = %d, want 0 (should have served from peer)", *originHits)
	}
	if atomic.LoadInt32(peerFetches) != 1 {
		t.Errorf("peer fetches = %d, want 1", *peerFetches)
	}
}

func TestMirror_PeerFallback_NoProvidersFallsThroughToOrigin(t *testing.T) {
	body := []byte("only at origin")
	d := digestOf(body)

	srv, _, originHits, peerFetches := newMirrorWithPeer(
		t,
		map[digest.Digest][]byte{d: body},
		nil,
	)

	resp, err := http.Get(srv.URL + "/v2/r/blobs/" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(body) {
		t.Errorf("body mismatch: got %q want %q", got, body)
	}
	if atomic.LoadInt32(originHits) != 1 {
		t.Errorf("origin hits = %d, want 1", *originHits)
	}
	if atomic.LoadInt32(peerFetches) != 0 {
		t.Errorf("peer fetches = %d, want 0", *peerFetches)
	}
}

func TestMirror_PeerFallback_PeerNotFoundExhaustsWarmPath(t *testing.T) {
	body := []byte("origin-only")
	d := digestOf(body)

	// Peer cache is empty but DHT says peer has it (stale DHT record).
	peerCache := fakes.NewCache()
	peerAddr := startPeerTransfer(t, peerCache)

	srv, _, originHits, peerFetches := newMirrorWithPeer(
		t,
		map[digest.Digest][]byte{d: body},
		map[digest.Digest][]ifaces.Provider{d: {{NodeID: "peer-stale", Addr: peerAddr}}},
	)

	resp, err := http.Get(srv.URL + "/v2/r/blobs/" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	// §5.1 v1 transfer policy: warm path exhausted → 5xx, NOT origin pull.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (warm path exhausted)", resp.StatusCode)
	}
	if atomic.LoadInt32(originHits) != 0 {
		t.Errorf("origin hits = %d, want 0 (§5.1: containerd handles origin via hosts.toml)", *originHits)
	}
	if atomic.LoadInt32(peerFetches) != 0 {
		t.Errorf("peer hits = %d, want 0", *peerFetches)
	}
}

func TestMirror_PeerFallback_DialFailureExhaustsWarmPath(t *testing.T) {
	body := []byte("unreachable-peer")
	d := digestOf(body)

	// Provide an unreachable peer addr (port 1 is reliably refused).
	dht := fakes.NewDHT()
	dht.Inject(d, ifaces.Provider{NodeID: "peer-dead", Addr: "127.0.0.1:1"})

	var originHits int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originHits, 1)
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
	var dialFailures int32
	client := transfer.NewClient(transfer.WithDialTimeout(200*time.Millisecond), transfer.WithRequestTimeout(2*time.Second))
	m := mirror.New(cfg, c, oc,
		mirror.WithDiscovery(dht, client),
		mirror.WithPeerBudgets(time.Second, time.Second, 3),
		mirror.WithPeerMetrics(nil, func(success bool) {
			if !success {
				atomic.AddInt32(&dialFailures, 1)
			}
		}),
	)
	ts := httptest.NewServer(m.Handler())
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/v2/r/blobs/" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	// §5.1 v1 transfer policy: warm path exhausted → 5xx.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (warm path exhausted)", resp.StatusCode)
	}
	if atomic.LoadInt32(&originHits) != 0 {
		t.Errorf("origin hits = %d, want 0 (§5.1: agent must NOT origin-pull after warm-path exhaustion)", originHits)
	}
	if atomic.LoadInt32(&dialFailures) == 0 {
		t.Error("expected at least one dial failure metric")
	}
}

// nolint:unused // referenced by helpers below
var errCantHappen = errors.New("test scaffolding error")
