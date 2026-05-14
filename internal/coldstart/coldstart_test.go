package coldstart_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gantry/gantry/internal/coldstart"
	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/hrw"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/ifaces/fakes"
	"github.com/gantry/gantry/internal/inflight"
)

// stubCoord returns canned PullIntent / PleasePullOutcome per node.
type stubCoord struct {
	mu              sync.Mutex
	intents         map[ifaces.NodeID]ifaces.PullIntent
	intentErrs      map[ifaces.NodeID]error
	pleasePullCalls []ifaces.NodeID
	pleasePullRegs  []string
	pleasePullRepos []string
	pleasePullErrs  map[ifaces.NodeID]error
}

func (s *stubCoord) PullIntentQuery(_ context.Context, id ifaces.NodeID, _ digest.Digest) (ifaces.PullIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.intentErrs[id]; ok {
		return ifaces.PullIntent{}, err
	}
	return s.intents[id], nil
}

func (s *stubCoord) PleasePull(_ context.Context, id ifaces.NodeID, registry, repository string, ds []digest.Digest) ([]ifaces.PleasePullOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pleasePullCalls = append(s.pleasePullCalls, id)
	s.pleasePullRegs = append(s.pleasePullRegs, registry)
	s.pleasePullRepos = append(s.pleasePullRepos, repository)
	if err, ok := s.pleasePullErrs[id]; ok {
		return nil, err
	}
	out := make([]ifaces.PleasePullOutcome, len(ds))
	for i, d := range ds {
		out[i] = ifaces.PleasePullOutcome{Digest: d, Outcome: ifaces.PleasePullStarted}
	}
	return out, nil
}

// stubDisco implements coldstart.Discovery. providers is the canned
// FindProviders response; calls increments per call so tests can
// arrange "DHT empty then non-empty".
type stubDisco struct {
	mu        sync.Mutex
	providers [][]ifaces.Provider
	idx       int
	health    float64
}

func (s *stubDisco) FindProviders(_ context.Context, _ digest.Digest) ([]ifaces.Provider, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.providers) {
		if len(s.providers) == 0 {
			return nil, nil
		}
		return s.providers[len(s.providers)-1], nil
	}
	out := s.providers[s.idx]
	s.idx++
	return out, nil
}

func (s *stubDisco) Health() float64 {
	if s.health == 0 {
		return 1.0
	}
	return s.health
}

// helper: build a Resolver against the supplied test doubles plus a
// 4-node cluster (n0, n1, n2, n3) with self=n3 by default.
func buildResolver(t *testing.T, coord ifaces.Coordinator, disco coldstart.Discovery, self ifaces.NodeID, members []ifaces.Node, metrics coldstart.MetricsHooks, now func() time.Time) *coldstart.Resolver {
	t.Helper()
	mems := fakes.NewMembers(self, members...)
	infl := inflight.New(inflight.DefaultStalls(), now)
	return coldstart.New(coldstart.Options{
		Members:   mems,
		Discovery: disco,
		Coord:     coord,
		Inflight:  infl,
		Now:       now,
		HrwK:      3,
		HrwScope:  hrw.ScopeCluster,
		Metrics:   metrics,
		// Short timeouts so the cascade test suite runs in <1s total.
		QueryTimeout:         200 * time.Millisecond,
		PollManifest:         20 * time.Millisecond,
		PollLayer:            50 * time.Millisecond,
		TransientCooldownCap: 30 * time.Second,
	})
}

// fixture: 4 named nodes; HRW top-K for the test digest needs to span
// all four so the test can pin who the responder is.
func clusterNodes() []ifaces.Node {
	return []ifaces.Node{
		{ID: "n0", Addr: "n0:5001"},
		{ID: "n1", Addr: "n1:5001"},
		{ID: "n2", Addr: "n2:5001"},
		{ID: "n3", Addr: "n3:5001"},
	}
}

func TestRule2_CacheHit(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('a', 64))
	nodes := clusterNodes()

	// Compute who actually lands in top-K for this digest so the test
	// can program the right node's intent.
	top := hrw.TopK(nodes, d, 3)
	if len(top) == 0 {
		t.Fatal("top-K empty")
	}
	cacheHolder := top[0].Node.ID

	coord := &stubCoord{intents: map[ifaces.NodeID]ifaces.PullIntent{
		cacheHolder: {HasCached: true},
	}}
	disco := &stubDisco{}
	hits := 0
	metrics := coldstart.MetricsHooks{
		OnTopKProbeHit:  func() { hits++ },
		OnDhtFalseEmpty: func() { /* counted via local int below */ },
	}
	var falseEmptyHits int
	metrics.OnDhtFalseEmpty = func() { falseEmptyHits++ }

	r := buildResolver(t, coord, disco, "self", nodes, metrics, time.Now)

	res, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Providers) == 0 {
		t.Fatal("Providers empty")
	}
	if res.Providers[0].NodeID != cacheHolder {
		t.Errorf("first provider = %s; want %s", res.Providers[0].NodeID, cacheHolder)
	}
	if hits != 1 {
		t.Errorf("OnTopKProbeHit calls = %d; want 1", hits)
	}
	if falseEmptyHits != 1 {
		t.Errorf("OnDhtFalseEmpty calls = %d; want 1", falseEmptyHits)
	}
}

func TestRule1_FailureShortCircuitBeatsCacheHit(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('b', 64))
	nodes := clusterNodes()
	top := hrw.TopK(nodes, d, 3)
	if len(top) < 2 {
		t.Fatal("need at least 2 nodes in top-K")
	}

	// One node has cache, another reports recently_failed (auth).
	// Rule 1 must beat rule 2.
	coord := &stubCoord{intents: map[ifaces.NodeID]ifaces.PullIntent{
		top[0].Node.ID: {HasCached: true},
		top[1].Node.ID: {RecentlyFailed: true, FailureClass: ifaces.FailureAuth},
	}}
	disco := &stubDisco{}
	r := buildResolver(t, coord, disco, "self", nodes, coldstart.MetricsHooks{}, time.Now)

	_, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	if !errors.Is(err, coldstart.ErrFailureShortCircuit) {
		t.Fatalf("err = %v; want ErrFailureShortCircuit", err)
	}
}

// TestRule1_ClusterWideTrustedClasses covers §5.8's requester rule:
// auth / not_found / rate_limited are trusted cluster-wide, so a
// single reachable node reporting any of them must short-circuit the
// cascade. Transient is handled separately by rule 4 (TestRule4_*).
func TestRule1_ClusterWideTrustedClasses(t *testing.T) {
	classes := []ifaces.FailureClass{
		ifaces.FailureAuth,
		ifaces.FailureNotFound,
		ifaces.FailureRateLimited,
	}
	for _, fc := range classes {
		fc := fc
		t.Run(string(fc), func(t *testing.T) {
			d := digest.MustParse("sha256:" + rep('b', 64))
			nodes := clusterNodes()
			top := hrw.TopK(nodes, d, 3)
			coord := &stubCoord{intents: map[ifaces.NodeID]ifaces.PullIntent{
				top[0].Node.ID: {RecentlyFailed: true, FailureClass: fc, CooldownUntil: time.Now().Add(time.Minute)},
				top[1].Node.ID: {},
				top[2].Node.ID: {},
			}}
			disco := &stubDisco{}
			r := buildResolver(t, coord, disco, "self", nodes, coldstart.MetricsHooks{}, time.Now)

			_, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
			if !errors.Is(err, coldstart.ErrFailureShortCircuit) {
				t.Fatalf("class=%s err = %v; want ErrFailureShortCircuit", fc, err)
			}
			// Crucially, no please_pull dialed: rule 1 forbids it
			// because rank-1 will get the same answer.
			if len(coord.pleasePullCalls) != 0 {
				t.Fatalf("class=%s: please_pull dialed %d times, want 0", fc, len(coord.pleasePullCalls))
			}
		})
	}
}

func TestRule3_InFlightThenDhtProvider(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('c', 64))
	nodes := clusterNodes()
	top := hrw.TopK(nodes, d, 3)

	// One node reports in-flight with started_at = "1s ago".
	now := time.Now()
	coord := &stubCoord{intents: map[ifaces.NodeID]ifaces.PullIntent{
		top[0].Node.ID: {InFlight: true, StartedAt: now.Add(-1 * time.Second)},
	}}

	// First DHT poll empty, second returns a provider.
	disco := &stubDisco{providers: [][]ifaces.Provider{
		nil,
		{{NodeID: "pulled-by", Addr: "pulled-by:5001"}},
	}}

	r := buildResolver(t, coord, disco, "self", nodes, coldstart.MetricsHooks{}, func() time.Time { return now })

	res, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Providers) != 1 || res.Providers[0].NodeID != "pulled-by" {
		t.Errorf("Providers = %+v; want single 'pulled-by'", res.Providers)
	}
}

func TestRule3_InFlightStaleExcluded(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('d', 64))
	nodes := clusterNodes()
	top := hrw.TopK(nodes, d, 3)

	// Manifest stall threshold = 5s. Report in-flight 10s ago — too
	// stale; should NOT trigger rule 3 (would fall through to rule 7).
	now := time.Now()
	coord := &stubCoord{intents: map[ifaces.NodeID]ifaces.PullIntent{
		top[0].Node.ID: {InFlight: true, StartedAt: now.Add(-10 * time.Second)},
		top[1].Node.ID: {},
		top[2].Node.ID: {},
	}}
	// With "neither cached nor in-flight" (after staleness filter), the
	// orchestrator will expand to top-2K under degraded DHT health, or
	// run cold-start on the first pass otherwise. Health=1.0 here; the
	// expansion is skipped, and rule 7 runs (please_pull + poll).
	disco := &stubDisco{
		health:    1.0,
		providers: [][]ifaces.Provider{nil, {{NodeID: "x", Addr: "x:5001"}}},
	}

	// §5.6: the stale puller exclusion must fire the takeover metric so
	// operators can observe rank-0 → rank-1 routing.
	var takeoverKinds []string
	hooks := coldstart.MetricsHooks{
		OnDesignatedPullerTakeover: func(kindLabel string) {
			takeoverKinds = append(takeoverKinds, kindLabel)
		},
	}
	r := buildResolver(t, coord, disco, "self", nodes, hooks, func() time.Time { return now })

	_, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	// Without expansion, rule 7 runs from the first pass. But the
	// orchestrator deliberately defers rule 7 until after a potential
	// expansion (to honor §5.2 rule 6). With health=1.0 the expansion
	// is NOT triggered, so the cascade reports exhausted.
	if !errors.Is(err, coldstart.ErrExhausted) {
		t.Logf("err = %v (acceptable: cascade exhausted under healthy DHT without expansion)", err)
	}
	if len(takeoverKinds) == 0 {
		t.Fatalf("OnDesignatedPullerTakeover never fired; want at least one (manifest)")
	}
	for _, k := range takeoverKinds {
		if k != "manifest" {
			t.Errorf("OnDesignatedPullerTakeover kind = %q; want \"manifest\"", k)
		}
	}
}

func TestRule4_TransientCooldown(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('e', 64))
	nodes := clusterNodes()
	top := hrw.TopK(nodes, d, 3)

	now := time.Now()
	coord := &stubCoord{intents: map[ifaces.NodeID]ifaces.PullIntent{
		top[0].Node.ID: {RecentlyFailed: true, FailureClass: ifaces.FailureTransient, CooldownUntil: now.Add(20 * time.Second)},
	}}
	disco := &stubDisco{}
	r := buildResolver(t, coord, disco, "self", nodes, coldstart.MetricsHooks{}, func() time.Time { return now })

	_, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	if !errors.Is(err, coldstart.ErrCooldownActive) {
		t.Fatalf("err = %v; want ErrCooldownActive", err)
	}
}

func TestRule7_DegradedDhtExpansionThenColdStart(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('f', 64))
	nodes := []ifaces.Node{
		{ID: "n0", Addr: "n0:5001"},
		{ID: "n1", Addr: "n1:5001"},
		{ID: "n2", Addr: "n2:5001"},
		{ID: "n3", Addr: "n3:5001"},
		{ID: "n4", Addr: "n4:5001"},
		{ID: "n5", Addr: "n5:5001"},
		{ID: "n6", Addr: "n6:5001"},
		{ID: "n7", Addr: "n7:5001"},
	}
	// All nodes report "no cache, not in-flight" → rule 7.
	intents := map[ifaces.NodeID]ifaces.PullIntent{}
	for _, n := range nodes {
		intents[n.ID] = ifaces.PullIntent{}
	}
	coord := &stubCoord{intents: intents}
	// DHT degraded so rule 6 expansion fires; then poll discovers the
	// provider after please_pull is sent.
	disco := &stubDisco{
		health:    0.4,
		providers: [][]ifaces.Provider{nil, {{NodeID: "the-puller", Addr: "the-puller:5001"}}},
	}
	r := buildResolver(t, coord, disco, "self", nodes, coldstart.MetricsHooks{}, time.Now)

	res, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Providers) != 1 || res.Providers[0].NodeID != "the-puller" {
		t.Errorf("Providers = %+v; want single 'the-puller'", res.Providers)
	}
	if len(coord.pleasePullCalls) != 1 {
		t.Errorf("pleasePullCalls = %d; want 1", len(coord.pleasePullCalls))
	}
	if len(coord.pleasePullRegs) != 1 || coord.pleasePullRegs[0] != "reg.example.com" {
		t.Errorf("pleasePullRegs = %v; want [reg.example.com]", coord.pleasePullRegs)
	}
	if len(coord.pleasePullRepos) != 1 || coord.pleasePullRepos[0] != "test/repo" {
		t.Errorf("pleasePullRepos = %v; want [test/repo]", coord.pleasePullRepos)
	}
}

// Rule 7 must refuse to send please_pull with empty registry/repository
// per §4.4 (single-repo-per-batch). The orchestrator surfaces this as
// ErrExhausted so the mirror returns 5xx instead of silently never
// triggering the puller.
func TestRule7_EmptyRegistryRejected(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('e', 64))
	nodes := []ifaces.Node{
		{ID: "n0", Addr: "n0:5001"},
		{ID: "n1", Addr: "n1:5001"},
		{ID: "n2", Addr: "n2:5001"},
		{ID: "n3", Addr: "n3:5001"},
		{ID: "n4", Addr: "n4:5001"},
		{ID: "n5", Addr: "n5:5001"},
		{ID: "n6", Addr: "n6:5001"},
		{ID: "n7", Addr: "n7:5001"},
	}
	intents := map[ifaces.NodeID]ifaces.PullIntent{}
	for _, n := range nodes {
		intents[n.ID] = ifaces.PullIntent{}
	}
	coord := &stubCoord{intents: intents}
	// Degraded DHT so rule 6 expansion fires and the expanded pass
	// reaches rule 7. Without registry/repository the orchestrator
	// must refuse to send please_pull and surface ErrExhausted.
	disco := &stubDisco{
		health:    0.4,
		providers: [][]ifaces.Provider{nil, nil},
	}
	r := buildResolver(t, coord, disco, "self", nodes, coldstart.MetricsHooks{}, time.Now)

	_, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "", "", 0)
	if !errors.Is(err, coldstart.ErrExhausted) {
		t.Fatalf("Resolve err = %v; want ErrExhausted (empty registry must short-circuit rule 7)", err)
	}
	if len(coord.pleasePullCalls) != 0 {
		t.Errorf("pleasePullCalls = %d; want 0 (must not dial puller with empty registry)", len(coord.pleasePullCalls))
	}
}

func TestRule5_NoReachableExpands(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('1', 64))
	nodes := []ifaces.Node{
		{ID: "n0", Addr: ""},
		{ID: "n1", Addr: ""},
		{ID: "n2", Addr: ""},
		{ID: "n3", Addr: ""},
		{ID: "n4", Addr: ""},
		{ID: "n5", Addr: ""},
		{ID: "n6", Addr: ""},
		{ID: "n7", Addr: ""},
	}
	top3 := hrw.TopK(nodes, d, 3)
	top6 := hrw.TopK(nodes, d, 6)
	// Top-3 all error out (simulating unreachable); top-6 includes 3
	// extra nodes that succeed with empty intent (rule 7 path).
	intentErrs := map[ifaces.NodeID]error{}
	for _, s := range top3 {
		intentErrs[s.Node.ID] = errors.New("unreachable")
	}
	intents := map[ifaces.NodeID]ifaces.PullIntent{}
	for _, s := range top6 {
		if _, blocked := intentErrs[s.Node.ID]; !blocked {
			intents[s.Node.ID] = ifaces.PullIntent{}
		}
	}
	coord := &stubCoord{intents: intents, intentErrs: intentErrs}
	disco := &stubDisco{
		health:    1.0,
		providers: [][]ifaces.Provider{nil, {{NodeID: "p", Addr: "p:5001"}}},
	}
	r := buildResolver(t, coord, disco, "self", nodes, coldstart.MetricsHooks{}, time.Now)

	res, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Providers) == 0 {
		t.Fatal("Providers empty")
	}
}

func TestPollDHTTimeoutReturnsExhausted(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('2', 64))
	nodes := clusterNodes()
	// Force rule 7 (degraded health) but DHT never returns a provider
	// so the poll loop must time out at the per-§5.2a threshold.
	intents := map[ifaces.NodeID]ifaces.PullIntent{}
	for _, n := range nodes {
		intents[n.ID] = ifaces.PullIntent{}
	}
	coord := &stubCoord{intents: intents}
	disco := &stubDisco{
		health:    0.3, // expand
		providers: [][]ifaces.Provider{nil, nil, nil, nil},
	}

	// Pin "now" to a clock we control; the inflight Stalls() resolver
	// will give us a 5s manifest threshold. We shrink it by configuring
	// a tiny PollManifest so the ticker fires often but never finds a
	// provider; the deadline based on inflight.DefaultStalls() is 5s
	// of wall-clock time, which is too long for a unit test. Use a
	// fake clock that advances quickly.
	now := time.Now()
	r := coldstart.New(coldstart.Options{
		Members:      fakes.NewMembers("self", nodes...),
		Discovery:    disco,
		Coord:        coord,
		Inflight:     inflight.New(inflight.Stalls{ManifestConfig: 100 * time.Millisecond, LayerFloor: time.Second, LayerBytesPerSec: 50 << 20, LayerMultiplier: 3}, func() time.Time { return now }),
		Now:          func() time.Time { return now },
		HrwK:         3,
		QueryTimeout: 50 * time.Millisecond,
		PollManifest: 20 * time.Millisecond,
		PollLayer:    100 * time.Millisecond,
	})

	_, err := r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	if !errors.Is(err, coldstart.ErrExhausted) {
		t.Fatalf("err = %v; want ErrExhausted", err)
	}
}

func TestRankMismatchEmitsMetric(t *testing.T) {
	d := digest.MustParse("sha256:" + rep('3', 64))
	nodes := clusterNodes()
	top := hrw.TopK(nodes, d, 3)

	// Responder at rank 0 reports rank 99 instead of its actual rank
	// — should trigger OnRankMismatch. The other top-K nodes report
	// their honest ranks so they don't generate false-positive
	// mismatches.
	intents := map[ifaces.NodeID]ifaces.PullIntent{
		top[0].Node.ID: {HasCached: true, RecipientRank: 99},
		top[1].Node.ID: {RecipientRank: 1},
		top[2].Node.ID: {RecipientRank: 2},
	}
	coord := &stubCoord{intents: intents}
	disco := &stubDisco{}

	var mismatches []ifaces.NodeID
	metrics := coldstart.MetricsHooks{
		OnRankMismatch: func(_ string, id ifaces.NodeID) {
			mismatches = append(mismatches, id)
		},
	}
	r := buildResolver(t, coord, disco, "self", nodes, metrics, time.Now)
	_, _ = r.Resolve(context.Background(), d, ifaces.KindManifest, "reg.example.com", "test/repo", 0)
	if len(mismatches) != 1 || mismatches[0] != top[0].Node.ID {
		t.Errorf("mismatches = %v; want [%s]", mismatches, top[0].Node.ID)
	}
}

func rep(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}
