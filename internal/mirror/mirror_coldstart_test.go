package mirror_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gantry/gantry/internal/cache"
	"github.com/gantry/gantry/internal/config"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/ifaces/fakes"
	"github.com/gantry/gantry/internal/mirror"
	"github.com/gantry/gantry/internal/origin"
	"github.com/gantry/gantry/internal/transfer"
)

// stubColdStart returns a canned provider list on any digest. Used to
// simulate the cold-start orchestrator's verdict at the mirror boundary
// without spinning up libp2p hosts.
type stubColdStart struct {
	providers []ifaces.Provider
	err       error
	calls     int32
}

func (s *stubColdStart) Resolve(_ context.Context, _ digest.Digest, _ ifaces.OriginRefKind, _ int64) (*mirror.ColdStartResolution, error) {
	atomic.AddInt32(&s.calls, 1)
	if s.err != nil {
		return nil, s.err
	}
	return &mirror.ColdStartResolution{Providers: s.providers, Outcome: "stub"}, nil
}

// When DHT returns empty, the cold-start orchestrator is consulted, and
// its returned providers must be passed into the peer fetch loop — NOT
// falling through to origin.
func TestMirror_ColdStart_EmptyDHTRoutedThroughColdStartHit(t *testing.T) {
	body := []byte("served via cold-start hit")
	d := digestOf(body)

	peerCache := fakes.NewCache()
	peerCache.Put(d, body)
	peerAddr := startPeerTransfer(t, peerCache)

	cs := &stubColdStart{
		providers: []ifaces.Provider{{NodeID: "cs-peer", Addr: peerAddr}},
	}

	srv, originHits, peerFetches := newMirrorWithColdStart(t,
		map[digest.Digest][]byte{d: body},
		nil, // empty DHT
		cs,
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
		t.Errorf("origin hits = %d, want 0", *originHits)
	}
	if atomic.LoadInt32(peerFetches) != 1 {
		t.Errorf("peer fetches = %d, want 1", *peerFetches)
	}
	if atomic.LoadInt32(&cs.calls) != 1 {
		t.Errorf("cold-start invocations = %d, want 1", cs.calls)
	}
}

// When cold-start returns a sentinel error (rule 1 / rule 4 /
// exhaustion), the mirror must respond 5xx — NOT origin-pull.
func TestMirror_ColdStart_SentinelErrorReturns503(t *testing.T) {
	body := []byte("would-be served")
	d := digestOf(body)
	cs := &stubColdStart{err: errors.New("coldstart: failure short-circuit (auth)")}

	srv, originHits, _ := newMirrorWithColdStart(t,
		map[digest.Digest][]byte{d: body},
		nil, // empty DHT
		cs,
	)

	resp, err := http.Get(srv.URL + "/v2/r/blobs/" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (cold-start sentinel)", resp.StatusCode)
	}
	if atomic.LoadInt32(originHits) != 0 {
		t.Errorf("origin hits = %d, want 0 (cold-start sentinel must not origin-pull)", *originHits)
	}
}

// When the DHT already has providers, cold-start must NOT be invoked
// — the warm path runs directly per §5.1.
func TestMirror_ColdStart_WarmPathSkipsResolver(t *testing.T) {
	body := []byte("warm path bytes")
	d := digestOf(body)

	peerCache := fakes.NewCache()
	peerCache.Put(d, body)
	peerAddr := startPeerTransfer(t, peerCache)

	cs := &stubColdStart{}

	srv, originHits, _ := newMirrorWithColdStart(t,
		map[digest.Digest][]byte{d: body},
		map[digest.Digest][]ifaces.Provider{d: {{NodeID: "p", Addr: peerAddr}}},
		cs,
	)

	resp, err := http.Get(srv.URL + "/v2/r/blobs/" + d.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if atomic.LoadInt32(originHits) != 0 {
		t.Errorf("origin hits = %d, want 0", *originHits)
	}
	if atomic.LoadInt32(&cs.calls) != 0 {
		t.Errorf("cold-start calls = %d, want 0 (warm path must skip resolver)", cs.calls)
	}
}

// newMirrorWithColdStart is a variant of newMirrorWithPeer that wires
// WithColdStart. The cold-start resolver may be nil, in which case
// Phase 1 fallthrough behaviour is preserved.
func newMirrorWithColdStart(t *testing.T, originBlobs map[digest.Digest][]byte, providers map[digest.Digest][]ifaces.Provider, cs mirror.ColdStartResolver) (*httptest.Server, *int32, *int32) {
	t.Helper()
	srv, _, originHits, peerFetches := newMirrorWithPeer(t, originBlobs, providers)
	// Discard the previous httptest.Server and rebuild with the same
	// upstream + cache but adding ColdStart. The simplest way is to
	// repeat the construction.
	t.Cleanup(srv.Close)

	// Re-derive the upstream URL and cache by walking back to the
	// helper's pieces. Simpler: just construct a fresh stack.
	srv2, hits, peerHits := buildColdStartMirror(t, originBlobs, providers, cs)
	_ = originHits
	_ = peerFetches
	return srv2, hits, peerHits
}

// buildColdStartMirror mirrors newMirrorWithPeer but wires WithColdStart.
func buildColdStartMirror(t *testing.T, originBlobs map[digest.Digest][]byte, providers map[digest.Digest][]ifaces.Provider, cs mirror.ColdStartResolver) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var originHits int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originHits, 1)
		path := r.URL.Path
		ref := ""
		for _, sep := range []string{"/blobs/", "/manifests/"} {
			if idx := stringsLastIndex(path, sep); idx >= 0 {
				ref = path[idx+len(sep):]
				break
			}
		}
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
	for d, p := range providers {
		dht.Inject(d, p...)
	}
	var peerFetches int32
	client := transfer.NewClient()
	m := mirror.New(cfg, c, oc,
		mirror.WithDiscovery(dht, client),
		mirror.WithColdStart(cs),
		mirror.WithPeerMetrics(func(o string) {
			if o == "hit" {
				atomic.AddInt32(&peerFetches, 1)
			}
		}, nil),
	)
	srv := httptest.NewServer(m.Handler())
	t.Cleanup(srv.Close)
	return srv, &originHits, &peerFetches
}

// minimal lastIndex helper (avoids importing strings just for this).
func stringsLastIndex(s, sub string) int {
	for i := len(s) - len(sub); i >= 0; i-- {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
