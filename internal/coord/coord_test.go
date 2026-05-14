package coord_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"github.com/gantry/gantry/internal/cache"
	"github.com/gantry/gantry/internal/coord"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/ifaces/fakes"
	"github.com/gantry/gantry/internal/inflight"
)

// helper: build two libp2p hosts that know each other's addresses.
func makeHostPair(t *testing.T) (a, b host.Host) {
	t.Helper()
	mkHost := func() host.Host {
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("libp2p.New: %v", err)
		}
		return h
	}
	a = mkHost()
	b = mkHost()
	a.Peerstore().AddAddrs(b.ID(), b.Addrs(), peerstore.PermanentAddrTTL)
	b.Peerstore().AddAddrs(a.ID(), a.Addrs(), peerstore.PermanentAddrTTL)
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	return a, b
}

func TestPullIntent_NotCachedNotInFlight(t *testing.T) {
	hClient, hServer := makeHostPair(t)

	c, err := cache.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}

	members := fakes.NewMembers(ifaces.NodeID(hServer.ID().String()),
		ifaces.Node{ID: ifaces.NodeID(hServer.ID().String()), Addr: "x"},
		ifaces.Node{ID: ifaces.NodeID(hClient.ID().String()), Addr: "y"},
	)
	infl := inflight.New(inflight.DefaultStalls(), nil)

	srv := coord.NewServer(c, members, infl)
	srv.Bind(hServer)

	cli := coord.NewClient(hClient)

	d := digest.MustParse("sha256:" + rep('a', 64))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	intent, err := cli.PullIntentQuery(ctx, ifaces.NodeID(hServer.ID().String()), d)
	if err != nil {
		t.Fatalf("PullIntentQuery: %v", err)
	}
	if intent.HasCached {
		t.Error("unexpected HasCached=true")
	}
	if intent.InFlight {
		t.Error("unexpected InFlight=true")
	}
	if intent.RecipientRank < 0 {
		t.Errorf("RecipientRank = %d, want >=0", intent.RecipientRank)
	}
}

func TestPullIntent_InFlight(t *testing.T) {
	hClient, hServer := makeHostPair(t)

	c, err := cache.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	members := fakes.NewMembers(ifaces.NodeID(hServer.ID().String()),
		ifaces.Node{ID: ifaces.NodeID(hServer.ID().String()), Addr: "x"},
	)
	infl := inflight.New(inflight.DefaultStalls(), nil)
	d := digest.MustParse("sha256:" + rep('b', 64))
	h, _, _ := infl.Start(d, ifaces.KindBlob, 0)
	defer h.Done()

	srv := coord.NewServer(c, members, infl)
	srv.Bind(hServer)

	cli := coord.NewClient(hClient)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	intent, err := cli.PullIntentQuery(ctx, ifaces.NodeID(hServer.ID().String()), d)
	if err != nil {
		t.Fatalf("PullIntentQuery: %v", err)
	}
	if !intent.InFlight {
		t.Error("InFlight=false, want true")
	}
	if intent.StartedAt.IsZero() {
		t.Error("StartedAt zero, want set")
	}
}

func TestPleasePull_Started(t *testing.T) {
	hClient, hServer := makeHostPair(t)

	c, err := cache.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	members := fakes.NewMembers(ifaces.NodeID(hServer.ID().String()),
		ifaces.Node{ID: ifaces.NodeID(hServer.ID().String()), Addr: "x"},
	)
	infl := inflight.New(inflight.DefaultStalls(), nil)

	var pumpCalls int32
	pump := coord.PullerPump(func(ctx context.Context, registry, repository string, d digest.Digest, kind ifaces.OriginRefKind) (time.Time, bool, *coord.NegativeEntry) {
		atomic.AddInt32(&pumpCalls, 1)
		// Claim in-flight as the real puller would; that gates re-pulls.
		h, _, already := infl.Start(d, kind, 0)
		_ = h // leak intentionally for test brevity
		return time.Now(), already, nil
	})
	srv := coord.NewServer(c, members, infl, coord.WithPullerPump(pump))
	srv.Bind(hServer)

	cli := coord.NewClient(hClient)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d1 := digest.MustParse("sha256:" + rep('1', 64))
	d2 := digest.MustParse("sha256:" + rep('2', 64))
	outs, err := cli.PleasePull(ctx, ifaces.NodeID(hServer.ID().String()), "reg", "repo", []digest.Digest{d1, d2})
	if err != nil {
		t.Fatalf("PleasePull: %v", err)
	}
	if len(outs) != 2 {
		t.Fatalf("len(outs) = %d, want 2", len(outs))
	}
	for _, o := range outs {
		if o.Outcome != ifaces.PleasePullStarted {
			t.Errorf("outcome for %s = %v, want PleasePullStarted", o.Digest, o.Outcome)
		}
	}
	if atomic.LoadInt32(&pumpCalls) != 2 {
		t.Errorf("pumpCalls = %d, want 2", pumpCalls)
	}
}

func TestPleasePull_AlreadyPulling(t *testing.T) {
	hClient, hServer := makeHostPair(t)

	c, err := cache.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	members := fakes.NewMembers(ifaces.NodeID(hServer.ID().String()))
	infl := inflight.New(inflight.DefaultStalls(), nil)

	d := digest.MustParse("sha256:" + rep('c', 64))
	pre, _, _ := infl.Start(d, ifaces.KindBlob, 0)
	defer pre.Done()

	pump := coord.PullerPump(func(_ context.Context, _ string, _ string, d digest.Digest, kind ifaces.OriginRefKind) (time.Time, bool, *coord.NegativeEntry) {
		h, e, already := infl.Start(d, kind, 0)
		_ = h
		return e.StartedAt, already, nil
	})
	srv := coord.NewServer(c, members, infl, coord.WithPullerPump(pump))
	srv.Bind(hServer)

	cli := coord.NewClient(hClient)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outs, err := cli.PleasePull(ctx, ifaces.NodeID(hServer.ID().String()), "reg", "repo", []digest.Digest{d})
	if err != nil {
		t.Fatalf("PleasePull: %v", err)
	}
	if len(outs) != 1 || outs[0].Outcome != ifaces.PleasePullAlreadyPulling {
		t.Fatalf("outs = %+v; want ALREADY_PULLING", outs)
	}
}

// stubNegCache implements coord.NegativeCache for testing §5.8 wiring.
type stubNegCache struct {
	entries map[digest.Digest]coord.NegativeEntry
}

func (s stubNegCache) Lookup(d digest.Digest) (coord.NegativeEntry, bool) {
	e, ok := s.entries[d]
	return e, ok
}

func TestPullIntent_NegativeCacheSurfaced(t *testing.T) {
	hClient, hServer := makeHostPair(t)

	c, err := cache.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	members := fakes.NewMembers(ifaces.NodeID(hServer.ID().String()),
		ifaces.Node{ID: ifaces.NodeID(hServer.ID().String()), Addr: "x"},
	)
	infl := inflight.New(inflight.DefaultStalls(), nil)

	d := digest.MustParse("sha256:" + rep('f', 64))
	cooldownUntil := time.Now().Add(30 * time.Second).UTC().Truncate(time.Microsecond)
	neg := stubNegCache{entries: map[digest.Digest]coord.NegativeEntry{
		d: {CooldownUntil: cooldownUntil, Class: ifaces.FailureRateLimited},
	}}

	srv := coord.NewServer(c, members, infl, coord.WithNegativeCache(neg))
	srv.Bind(hServer)
	cli := coord.NewClient(hClient)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	intent, err := cli.PullIntentQuery(ctx, ifaces.NodeID(hServer.ID().String()), d)
	if err != nil {
		t.Fatalf("PullIntentQuery: %v", err)
	}
	if !intent.RecentlyFailed {
		t.Fatalf("RecentlyFailed = false, want true")
	}
	if intent.FailureClass != ifaces.FailureRateLimited {
		t.Fatalf("FailureClass = %v, want FailureRateLimited", intent.FailureClass)
	}
	if !intent.CooldownUntil.Equal(cooldownUntil) {
		t.Fatalf("CooldownUntil = %v, want %v", intent.CooldownUntil, cooldownUntil)
	}
}

func TestPleasePull_RecentlyFailedShortCircuit(t *testing.T) {
	hClient, hServer := makeHostPair(t)

	c, err := cache.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	members := fakes.NewMembers(ifaces.NodeID(hServer.ID().String()))
	infl := inflight.New(inflight.DefaultStalls(), nil)

	d := digest.MustParse("sha256:" + rep('7', 64))
	cooldownUntil := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)

	// Pump returns *NegativeEntry to short-circuit (real puller would
	// consult its negcache before starting an origin pull).
	var pumpCalls int32
	pump := coord.PullerPump(func(_ context.Context, _ string, _ string, _ digest.Digest, _ ifaces.OriginRefKind) (time.Time, bool, *coord.NegativeEntry) {
		atomic.AddInt32(&pumpCalls, 1)
		return time.Time{}, false, &coord.NegativeEntry{
			CooldownUntil: cooldownUntil,
			Class:         ifaces.FailureAuth,
		}
	})
	srv := coord.NewServer(c, members, infl, coord.WithPullerPump(pump))
	srv.Bind(hServer)
	cli := coord.NewClient(hClient)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outs, err := cli.PleasePull(ctx, ifaces.NodeID(hServer.ID().String()), "reg", "repo", []digest.Digest{d})
	if err != nil {
		t.Fatalf("PleasePull: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("len(outs) = %d, want 1", len(outs))
	}
	o := outs[0]
	if o.Outcome != ifaces.PleasePullRecentlyFailed {
		t.Fatalf("Outcome = %v, want PleasePullRecentlyFailed", o.Outcome)
	}
	if o.FailureClass != ifaces.FailureAuth {
		t.Fatalf("FailureClass = %v, want FailureAuth", o.FailureClass)
	}
	if !o.CooldownUntil.Equal(cooldownUntil) {
		t.Fatalf("CooldownUntil = %v, want %v", o.CooldownUntil, cooldownUntil)
	}
	if got := atomic.LoadInt32(&pumpCalls); got != 1 {
		t.Fatalf("pumpCalls = %d, want 1", got)
	}
}

func TestClient_UnknownNodeReturnsError(t *testing.T) {
	hClient, _ := makeHostPair(t)
	cli := coord.NewClient(hClient)

	// peer.Decode of a non-peerID string fails.
	d := digest.MustParse("sha256:" + rep('d', 64))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := cli.PullIntentQuery(ctx, ifaces.NodeID("not-a-peer-id"), d)
	if err == nil {
		t.Fatal("expected error for unresolvable NodeID")
	}
}

// Ensure peer.ID resolution caching works.
func TestClient_ResolvePeerIDCache(t *testing.T) {
	hClient, hServer := makeHostPair(t)
	c, err := cache.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("cache.Open: %v", err)
	}
	members := fakes.NewMembers(ifaces.NodeID(hServer.ID().String()))
	infl := inflight.New(inflight.DefaultStalls(), nil)
	srv := coord.NewServer(c, members, infl)
	srv.Bind(hServer)

	cli := coord.NewClient(hClient)
	cli.ResolvePeerID("alias", hServer.ID())

	d := digest.MustParse("sha256:" + rep('e', 64))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.PullIntentQuery(ctx, "alias", d); err != nil {
		t.Fatalf("aliased call: %v", err)
	}
}

// silence "imported and not used" for peer in some build matrices.
var _ peer.ID

func rep(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}
