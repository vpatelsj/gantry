// Package coldstart implements the §5.2 / §5.2a rule cascade that
// decides how an agent resolves a digest when its local cache misses
// and the DHT lookup did not return enough providers.
//
// The orchestrator is invoked by the mirror miss path after a
// FindProviders call returns empty. It runs the following pipeline:
//
//  1. Compute HRW top-K from the local membership snapshot.
//  2. Dial all K in parallel with `pull_intent_query`, collecting
//     responses up to a 2 s timeout (§5.2 step 4).
//  3. Apply the 7-rule cascade in priority order (§5.2 step 5):
//  1. failure short-circuit  →  return ErrFailureShortCircuit (5xx)
//  2. cache hit               →  return the responder's transfer addr
//  3. in-flight piggyback     →  DHT-poll until provider appears
//  4. transient cooldown      →  return ErrCooldownActive (5xx)
//  5. all-unreachable expand  →  re-run step 2 with top-2K
//  6. degraded eager expand   →  re-run step 2 with top-2K
//  7. cold-start              →  please_pull to lowest-rank reachable,
//     then DHT-poll for the provider
//  4. While DHT-polling (rules 3 and 7), bound by the per-digest
//     timeout from §5.2a (manifest/config 5 s; layer
//     max(10 s, size/50 MB/s) × 3) and the per-kind poll interval
//     (200 ms manifest/config; 1 s layer).
//
// The Resolver is stateless across calls; concurrent Resolve invocations
// for the same digest are safe and will independently arrive at the
// same outcome (the inflight map at the puller side dedupes the
// origin pull itself).
package coldstart

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/hrw"
	"github.com/gantry/gantry/internal/ifaces"
	"github.com/gantry/gantry/internal/inflight"
)

// Sentinel errors. Mirror layer maps all of these to 5xx; tests
// distinguish to validate which rule fired.
var (
	// ErrFailureShortCircuit fires rule 1.
	ErrFailureShortCircuit = errors.New("coldstart: failure short-circuit")
	// ErrCooldownActive fires rule 4.
	ErrCooldownActive = errors.New("coldstart: transient cooldown active")
	// ErrExhausted fires when the cascade reaches its terminal state
	// without producing a provider (e.g., expanded-2K exhausted, or
	// please_pull completed but the DHT poll timed out).
	ErrExhausted = errors.New("coldstart: cascade exhausted")
)

// Discovery is the subset of the libp2p discovery host that the
// orchestrator needs. Kept narrow for ease of mocking.
type Discovery interface {
	FindProviders(ctx context.Context, d digest.Digest) ([]ifaces.Provider, error)
	Health() float64
}

// MetricsHooks lets the metrics package wire Prometheus counters
// without coupling the orchestrator to client_golang. All hooks are
// nil-safe.
type MetricsHooks struct {
	// OnRankMismatch fires once per pull_intent response whose
	// reported hrw_rank disagrees with the requester's computed
	// rank for that responder. kindLabel is "manifest" or "layer".
	OnRankMismatch func(kindLabel string, responder ifaces.NodeID)
	// OnDhtFalseEmpty fires when the orchestrator observes the
	// false-empty case: DHT had returned 0 providers, but a
	// pull_intent_query reports has_cached=true.
	OnDhtFalseEmpty func()
	// OnTopKProbeHit fires when any rule before rule 7 (cold-start)
	// resolves the request. Used to track how often the probe saves
	// an origin pull.
	OnTopKProbeHit func()
	// OnColdStartDuration is called once per Resolve with the total
	// elapsed time and the outcome rule that fired ("rule1".."rule7"
	// or "expanded_rule_N").
	OnColdStartDuration func(kindLabel, outcome string, d time.Duration)
	// OnDesignatedPullerTakeover fires when a pull_intent_query
	// responder reports in_flight=true but its started_at is older
	// than the per-§5.2a stall threshold, so the requester excludes
	// it from rule-3 piggyback and routes via the next-ranked node
	// (rule 6 / rule 7). kindLabel is "manifest" or "layer". Maps to
	// §7.6 metric `p2p_designated_puller_takeover_total`.
	OnDesignatedPullerTakeover func(kindLabel string)
	// OnTopKExpansion fires once per expansion pass to top-2K (or
	// top-(K × TopKExpansionFactor) when the factor is configured).
	// reason is "degraded" (rule-6 DHT-degraded expand) or
	// "all_unreachable" (rule-5 expansion). Maps to §7.6 metric
	// `p2p_topk_expansion_total{reason=}`.
	OnTopKExpansion func(reason string)
}

// Options configures a Resolver.
type Options struct {
	Members   ifaces.Members
	Discovery Discovery
	Coord     ifaces.Coordinator
	Inflight  *inflight.Map
	Logger    *slog.Logger
	Metrics   MetricsHooks
	Now       func() time.Time
	HrwK      int       // default 3
	HrwScope  hrw.Scope // default ScopeCluster
	SelfZone  string    // required when HrwScope == ScopeZone

	// Tunables (defaults applied if zero).
	QueryTimeout         time.Duration // default 2s — §5.2 step 5 wait window
	PollManifest         time.Duration // default 200ms — §5.2a
	PollLayer            time.Duration // default 1s — §5.2a
	TransientCooldownCap time.Duration // default 30s — §5.2 rule 4

	// TopKExpansionFactor is the multiplier applied to HrwK on the
	// expansion pass under rule 5 / rule 6 (§5.2 step 5; §7.7
	// `topk_expansion_factor_degraded`). Defaults to 2 when ≤1.
	TopKExpansionFactor int
}

// Resolver runs the §5.2 cascade.
type Resolver struct {
	opts Options

	// honorMu guards honorUntil. honorUntil records the §5.8 requester-
	// side local honor window per digest: when a transient cooldown
	// was last observed, the requester suppresses please_pull (and
	// in fact the whole probe pass) for `min(cooldown_until - now,
	// TransientCooldownCap)`. Evicted on access once the deadline
	// passes; see suppressedByHonorWindow.
	honorMu    sync.Mutex
	honorUntil map[digest.Digest]time.Time
}

// New builds a Resolver. Required fields: Members, Discovery, Coord,
// Inflight.
func New(opts Options) *Resolver {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	opts.Logger = opts.Logger.With(slog.String("subsystem", "coldstart"))
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.HrwK <= 0 {
		opts.HrwK = 3
	}
	if opts.QueryTimeout <= 0 {
		opts.QueryTimeout = 2 * time.Second
	}
	if opts.PollManifest <= 0 {
		opts.PollManifest = 200 * time.Millisecond
	}
	if opts.PollLayer <= 0 {
		opts.PollLayer = 1 * time.Second
	}
	if opts.TransientCooldownCap <= 0 {
		opts.TransientCooldownCap = 30 * time.Second
	}
	if opts.TopKExpansionFactor < 2 {
		opts.TopKExpansionFactor = 2
	}
	return &Resolver{
		opts:       opts,
		honorUntil: make(map[digest.Digest]time.Time),
	}
}

// Resolution carries the orchestrator's verdict.
type Resolution struct {
	// Providers are transfer endpoints (host:port) the caller should
	// fetch from, in priority order. Non-empty on success.
	Providers []ifaces.Provider
	// Outcome names which rule fired. Useful for tests and metrics.
	Outcome string
}

// Resolve runs the cascade for d. The returned error is one of the
// sentinel errors above, or a context cancellation, or a transport
// error. expectedSize is 0 if unknown (e.g., manifest digest before
// parsing); it is used to compute the per-§5.2a stall threshold for
// rule 3 (in_flight) and the DHT-polling deadline overall.
//
// registry+repository identify the upstream and OCI repo for the
// please_pull RPC (§4.4 single-repo-per-batch invariant). Both must be
// non-empty for rule 7 to fire; the orchestrator otherwise falls
// through to ErrExhausted.
func (r *Resolver) Resolve(ctx context.Context, d digest.Digest, kind ifaces.OriginRefKind, registry, repository string, expectedSize int64) (*Resolution, error) {
	start := r.opts.Now()
	kindLabel := kindLabel(kind)
	defer func() {
		// outcome is set by the named return wrapper below.
	}()

	// §5.8 requester-side honor window: if we recently observed a
	// transient cooldown for this digest and the capped window has
	// not yet elapsed, short-circuit without probing the top-K. This
	// matches the design's "apply a local honor window" rule and
	// suppresses redundant probe traffic across kubelet retries.
	if r.suppressedByHonorWindow(d) {
		r.bumpDuration(kindLabel, "rule4_cooldown_honored", start)
		return nil, ErrCooldownActive
	}

	// Step 1 + 2: top-K + parallel pull_intent_query.
	cluster := r.opts.Members.Snapshot()
	scope := r.opts.HrwScope
	scopedZone := r.opts.SelfZone
	candidates := hrw.Candidates(cluster, scope, scopedZone)
	if scope == hrw.ScopeZone && len(candidates) == 0 {
		// Zone-mode degrades to cluster-mode when the zone is empty
		// (e.g., this node has no zone label). Matches the design's
		// §4.3 fallback expectation.
		candidates = cluster
	}

	// Pass 1: top-K.
	res, outcome, err := r.probe(ctx, d, kind, registry, repository, expectedSize, candidates, r.opts.HrwK, "")
	if err == nil {
		r.bumpDuration(kindLabel, outcome, start)
		return res, nil
	}

	// Rules 5/6: expansion to 2K. These apply when:
	//   - rule 5: probe returned ErrNoReachable (no reachable top-K).
	//   - rule 6: probe returned ErrAllNeitherCachedNorInFlight AND
	//     DHT health is below the §7.7 Healthy threshold (0.7).
	//     Below 0.3 the health is "Unhealthy" per §7.7 and the
	//     expansion reason is distinguished from "Degraded".
	expand := false
	expandReason := ""
	expandMetricReason := ""
	switch {
	case errors.Is(err, errNoReachable):
		expand = true
		expandReason = "rule5_all_unreachable"
		expandMetricReason = "all_unreachable"
	case errors.Is(err, errAllNeither):
		health := r.opts.Discovery.Health()
		if health < 0.7 {
			expand = true
			expandReason = "rule6_degraded_expand"
			if health < 0.3 {
				expandMetricReason = "unhealthy_health"
			} else {
				expandMetricReason = "degraded_health"
			}
		}
	}
	if expand {
		factor := r.opts.TopKExpansionFactor
		if factor < 2 {
			factor = 2
		}
		if r.opts.Metrics.OnTopKExpansion != nil {
			r.opts.Metrics.OnTopKExpansion(expandMetricReason)
		}
		res, outcome, err = r.probe(ctx, d, kind, registry, repository, expectedSize, candidates, r.opts.HrwK*factor, expandReason)
		if err == nil {
			r.bumpDuration(kindLabel, outcome, start)
			return res, nil
		}
	}

	// Translate internal errors into the public sentinel set.
	mappedOutcome, out := mapTerminalErr(err)
	r.bumpDuration(kindLabel, mappedOutcome, start)
	return nil, out
}

// errNoReachable / errAllNeither are internal — see probe() and
// mapTerminalErr().
var (
	errNoReachable = errors.New("coldstart: no reachable top-K")
	errAllNeither  = errors.New("coldstart: all reachable report neither cached nor in-flight (rule 7 path)")
)

func mapTerminalErr(err error) (string, error) {
	switch {
	case err == nil:
		return "ok", nil
	case errors.Is(err, ErrFailureShortCircuit):
		return "rule1_failure", ErrFailureShortCircuit
	case errors.Is(err, ErrCooldownActive):
		return "rule4_cooldown", ErrCooldownActive
	case errors.Is(err, errNoReachable), errors.Is(err, errAllNeither), errors.Is(err, ErrExhausted):
		return "exhausted", ErrExhausted
	default:
		return "error", err
	}
}

// probe runs one pass of pull_intent_query against the top-N candidates
// and evaluates the §5.2 rule cascade. expandLabel is non-empty when
// this is the expansion pass (top-2K) and identifies which rule fired
// the expansion (used as the outcome label).
//
// Returns (Resolution, outcomeLabel, nil) on success; or
// (nil, "", err) where err is one of the internal/public sentinels.
func (r *Resolver) probe(ctx context.Context, d digest.Digest, kind ifaces.OriginRefKind, registry, repository string, expectedSize int64, candidates []ifaces.Node, k int, expandLabel string) (*Resolution, string, error) {
	top := hrw.TopK(candidates, d, k)
	if len(top) == 0 {
		return nil, "", errNoReachable
	}

	self := r.opts.Members.Self()
	// Don't pull_intent_query ourselves — we already know our state.
	queryTargets := make([]hrw.Scored, 0, len(top))
	for _, s := range top {
		if s.Node.ID != self {
			queryTargets = append(queryTargets, s)
		}
	}

	probeCtx, cancel := context.WithTimeout(ctx, r.opts.QueryTimeout)
	defer cancel()
	responses := r.fanOut(probeCtx, queryTargets, d)

	// §5.3: emit hrw_rank_mismatch when responder's reported rank
	// disagrees with our locally computed rank.
	r.checkRankMismatches(top, responses, d, kindLabel(kind))

	// Apply rules in strict priority order.
	reachable := reachableResponses(responses)

	// Rule 1: failure short-circuit.
	if v := findFailureShortCircuit(reachable); v != nil {
		return nil, prefix(expandLabel, "rule1_failure"), ErrFailureShortCircuit
	}

	// Rule 2: cache hit.
	if v := findCacheHit(reachable); v != nil {
		// DHT-false-empty marker: we got here because FindProviders
		// returned 0, yet a peer claims has_cached. Emit metric.
		if r.opts.Metrics.OnDhtFalseEmpty != nil {
			r.opts.Metrics.OnDhtFalseEmpty()
		}
		if r.opts.Metrics.OnTopKProbeHit != nil {
			r.opts.Metrics.OnTopKProbeHit()
		}
		return r.providersFor(v, top), prefix(expandLabel, "rule2_cache_hit"), nil
	}

	// Rule 3: in-flight piggyback.
	if v := findInFlight(reachable, r.opts.Inflight, kind, expectedSize, r.opts.Now(), r.opts.Metrics.OnDesignatedPullerTakeover); v != nil {
		if r.opts.Metrics.OnTopKProbeHit != nil {
			r.opts.Metrics.OnTopKProbeHit()
		}
		providers, err := r.pollDHT(ctx, d, kind, expectedSize)
		if err != nil {
			return nil, prefix(expandLabel, "rule3_inflight_poll_exhausted"), ErrExhausted
		}
		return &Resolution{Providers: providers, Outcome: prefix(expandLabel, "rule3_inflight")}, prefix(expandLabel, "rule3_inflight"), nil
	}

	// Rule 4: transient cooldown. Capture the latest cooldown_until so
	// the requester's local honor window can be set, bounded by
	// TransientCooldownCap (§5.8 "apply a local honor window of
	// min(cooldown_until - now, cap)").
	if hit, cooldownUntil := findTransientCooldown(reachable); hit {
		r.recordHonorWindow(d, cooldownUntil)
		return nil, prefix(expandLabel, "rule4_cooldown"), ErrCooldownActive
	}

	// Rule 5/6 are decided one frame up in Resolve() once probe()
	// reports its own diagnosis here:
	//   - 0 reachable → errNoReachable (rule 5 expand)
	//   - all reachable report neither cached nor in-flight → errAllNeither
	//     (rule 6 expand or rule 7 cold-start depending on DHT health)
	if len(reachable) == 0 {
		return nil, "", errNoReachable
	}

	// Rule 7: cold-start. Lowest hrw_rank reachable wins.
	puller := lowestRankReachable(reachable)
	if puller == nil {
		// Defensive: should not be possible — reachable is non-empty
		// but no entry had a valid rank. Treat as exhausted.
		return nil, prefix(expandLabel, "rule7_no_puller"), ErrExhausted
	}

	// If the orchestrator hasn't expanded yet AND none of rules 2/3
	// fired, the design (§5.2 rule 6) says: under Degraded DHT health,
	// expand to top-2K *before* cold-starting. Surface errAllNeither so
	// Resolve() can decide.
	if expandLabel == "" {
		return nil, "", errAllNeither
	}

	// Cold-start: please_pull, then DHT-poll for completion.
	if err := r.sendPleasePull(ctx, *puller, d, registry, repository); err != nil {
		return nil, prefix(expandLabel, "rule7_please_pull_failed"), ErrExhausted
	}
	providers, err := r.pollDHT(ctx, d, kind, expectedSize)
	if err != nil {
		return nil, prefix(expandLabel, "rule7_poll_exhausted"), ErrExhausted
	}
	return &Resolution{Providers: providers, Outcome: prefix(expandLabel, "rule7_cold_start")}, prefix(expandLabel, "rule7_cold_start"), nil
}

// fanOut issues pull_intent_query to every target in parallel. Each
// response (or error) is recorded in the returned slice, indexed by
// target. Targets with nil queryTimeout protection rely on ctx.
func (r *Resolver) fanOut(ctx context.Context, targets []hrw.Scored, d digest.Digest) []response {
	out := make([]response, len(targets))
	var wg sync.WaitGroup
	wg.Add(len(targets))
	for i, s := range targets {
		i, s := i, s
		go func() {
			defer wg.Done()
			intent, err := r.opts.Coord.PullIntentQuery(ctx, s.Node.ID, d)
			out[i] = response{
				node:   s.Node,
				ok:     err == nil,
				err:    err,
				intent: intent,
			}
		}()
	}
	wg.Wait()
	return out
}

type response struct {
	node   ifaces.Node
	ok     bool
	err    error
	intent ifaces.PullIntent
}

func reachableResponses(in []response) []response {
	out := make([]response, 0, len(in))
	for _, r := range in {
		if r.ok {
			out = append(out, r)
		}
	}
	return out
}

func findFailureShortCircuit(rs []response) *response {
	for i := range rs {
		if !rs[i].intent.RecentlyFailed {
			continue
		}
		switch rs[i].intent.FailureClass {
		case ifaces.FailureAuth, ifaces.FailureNotFound, ifaces.FailureRateLimited:
			return &rs[i]
		}
	}
	return nil
}

func findCacheHit(rs []response) *response {
	for i := range rs {
		if rs[i].intent.HasCached {
			return &rs[i]
		}
	}
	return nil
}

func findInFlight(rs []response, infl *inflight.Map, kind ifaces.OriginRefKind, expectedSize int64, now time.Time, onTakeover func(kindLabel string)) *response {
	for i := range rs {
		intent := rs[i].intent
		if !intent.InFlight {
			continue
		}
		// §5.6 stall check: exclude the reporter if started_at is too
		// old for the per-digest timeout.
		if !intent.StartedAt.IsZero() && infl != nil {
			elapsed := now.Sub(intent.StartedAt)
			threshold := infl.Stalls().ResolveStall(kind, expectedSize)
			if elapsed > threshold {
				// Stale puller — emit the §7.6 takeover metric and
				// keep searching. Rank-1 (next entry) may still serve.
				if onTakeover != nil {
					onTakeover(kindLabel(kind))
				}
				continue
			}
		}
		return &rs[i]
	}
	return nil
}

func findTransientCooldown(rs []response) (bool, time.Time) {
	// Returns whether any reachable response reports a transient
	// cooldown and, if so, the latest cooldown_until across all such
	// responses. Picking the latest is conservative: it gives the
	// requester the longest honor window any puller is asking for.
	hit := false
	var until time.Time
	for _, r := range rs {
		if !r.intent.RecentlyFailed || r.intent.FailureClass != ifaces.FailureTransient {
			continue
		}
		hit = true
		if r.intent.CooldownUntil.After(until) {
			until = r.intent.CooldownUntil
		}
	}
	return hit, until
}

func lowestRankReachable(rs []response) *response {
	// Sort by reported hrw_rank ascending; the lowest-numbered rank is
	// the highest-priority puller (§5.2 step 6).
	sorted := append([]response(nil), rs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		ri, rj := sorted[i].intent.RecipientRank, sorted[j].intent.RecipientRank
		// Treat negative (unknown) ranks as +∞ so they're picked last.
		if ri < 0 {
			ri = int32(1<<31 - 1)
		}
		if rj < 0 {
			rj = int32(1<<31 - 1)
		}
		return ri < rj
	})
	if len(sorted) == 0 {
		return nil
	}
	return &sorted[0]
}

func (r *Resolver) checkRankMismatches(top []hrw.Scored, rs []response, d digest.Digest, kindLabel string) {
	if r.opts.Metrics.OnRankMismatch == nil {
		return
	}
	// Build a node-id → expected-rank map from our own scoring.
	idToRank := make(map[ifaces.NodeID]int32, len(top))
	for i, s := range top {
		idToRank[s.Node.ID] = int32(i)
	}
	for _, resp := range rs {
		if !resp.ok {
			continue
		}
		want, known := idToRank[resp.node.ID]
		if !known {
			continue
		}
		if resp.intent.RecipientRank != want {
			r.opts.Logger.Warn("hrw_rank_mismatch",
				slog.String("digest", d.String()),
				slog.String("recipient_node", string(resp.node.ID)),
				slog.Int("our_rank", int(want)),
				slog.Int("their_rank", int(resp.intent.RecipientRank)),
			)
			r.opts.Metrics.OnRankMismatch(kindLabel, resp.node.ID)
		}
	}
}

func (r *Resolver) providersFor(v *response, top []hrw.Scored) *Resolution {
	// Build a list of providers in HRW rank order, with the cache-hit
	// responder first (so the warm-path peer fetch loop tries it
	// before falling back to other reachable top-K members).
	out := []ifaces.Provider{{NodeID: v.node.ID, Addr: v.node.Addr}}
	for _, s := range top {
		if s.Node.ID == v.node.ID {
			continue
		}
		out = append(out, ifaces.Provider{NodeID: s.Node.ID, Addr: s.Node.Addr})
	}
	return &Resolution{Providers: out, Outcome: "rule2_cache_hit"}
}

func (r *Resolver) sendPleasePull(ctx context.Context, puller response, d digest.Digest, registry, repository string) error {
	// §4.4 invariant: please_pull is a single repo per batch. If the
	// caller didn't supply registry+repository, refuse to send a
	// malformed RPC (the server would reject it) and surface a
	// terminal error so the cascade reports rule7_please_pull_failed
	// instead of silently succeeding.
	if registry == "" || repository == "" {
		return fmt.Errorf("coldstart: please_pull requires non-empty registry+repository (got %q/%q)", registry, repository)
	}
	// Single-digest call; batching is the orchestrator-caller's job
	// (the mirror at the layer-fanout level groups by puller).
	ctx, cancel := context.WithTimeout(ctx, r.opts.QueryTimeout)
	defer cancel()
	_, err := r.opts.Coord.PleasePull(ctx, puller.node.ID, registry, repository, []digest.Digest{d})
	return err
}

func (r *Resolver) pollDHT(ctx context.Context, d digest.Digest, kind ifaces.OriginRefKind, expectedSize int64) ([]ifaces.Provider, error) {
	// §5.2a polling interval + per-digest stall threshold.
	interval := r.opts.PollLayer
	if kind == ifaces.KindManifest {
		interval = r.opts.PollManifest
	}
	threshold := r.opts.Inflight.Stalls().ResolveStall(kind, expectedSize)
	deadline := r.opts.Now().Add(threshold)

	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	t := time.NewTicker(interval)
	defer t.Stop()

	// One immediate lookup before the first sleep — minimizes latency
	// when the provider record landed just before we started polling.
	if provs, err := r.opts.Discovery.FindProviders(pollCtx, d); err == nil && len(provs) > 0 {
		return provs, nil
	}
	for {
		select {
		case <-pollCtx.Done():
			return nil, ErrExhausted
		case <-t.C:
			provs, err := r.opts.Discovery.FindProviders(pollCtx, d)
			if err != nil {
				continue
			}
			if len(provs) > 0 {
				return provs, nil
			}
		}
	}
}

func (r *Resolver) bumpDuration(kindLabel, outcome string, start time.Time) {
	if r.opts.Metrics.OnColdStartDuration == nil {
		return
	}
	r.opts.Metrics.OnColdStartDuration(kindLabel, outcome, r.opts.Now().Sub(start))
}

// suppressedByHonorWindow reports whether d is currently in this
// requester's local §5.8 transient honor window. Evicts the entry on
// access if the window has elapsed (lazy GC keeps the map bounded
// without a background goroutine).
func (r *Resolver) suppressedByHonorWindow(d digest.Digest) bool {
	r.honorMu.Lock()
	defer r.honorMu.Unlock()
	until, ok := r.honorUntil[d]
	if !ok {
		return false
	}
	now := r.opts.Now()
	if !now.Before(until) {
		delete(r.honorUntil, d)
		return false
	}
	return true
}

// recordHonorWindow stores a new honor-window deadline for d derived
// from the puller's advertised cooldown_until, capped at
// TransientCooldownCap. A non-positive remaining duration (the
// puller's cooldown already elapsed) is dropped so the next request
// re-probes immediately.
func (r *Resolver) recordHonorWindow(d digest.Digest, cooldownUntil time.Time) {
	maxWindow := r.opts.TransientCooldownCap
	if maxWindow <= 0 {
		return
	}
	now := r.opts.Now()
	remaining := cooldownUntil.Sub(now)
	if remaining <= 0 {
		return
	}
	if remaining > maxWindow {
		remaining = maxWindow
	}
	r.honorMu.Lock()
	r.honorUntil[d] = now.Add(remaining)
	r.honorMu.Unlock()
}

func kindLabel(k ifaces.OriginRefKind) string {
	if k == ifaces.KindManifest {
		return "manifest"
	}
	return "layer"
}

func prefix(p, s string) string {
	if p == "" {
		return s
	}
	return fmt.Sprintf("%s/%s", p, s)
}
