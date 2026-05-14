package coldstart

// Speculative wire-level batching described in detailed-design.md §5.2
// (L332) and archecture.md L180:
//
//	"when multiple cold-start layers all HRW to the same designated
//	 puller (which happens often when K is small relative to the
//	 cluster), the agent may send a single
//	 please_pull([digest1, digest2, …]) carrying all such digests"
//
// Prefetch is intentionally NOT the full §5.2 cascade. It is a
// best-effort warm-up fired by the mirror's manifest serve path: when
// the mirror serves a manifest it knows the layer digests up-front,
// and pre-emptively asking each layer's HRW rank-0 reachable peer to
// start pulling means the cluster is already warm by the time
// containerd issues per-layer GETs. If the rank-0 puller turns out
// to be unreachable or already failed, no harm done — when containerd
// asks for that layer, the full Resolve cascade runs and recovers via
// rules 5/6/7 expansion.
//
// Batching is the entire point: 10 layers landing on 3 pullers
// produces 3 PleasePull RPCs, not 10. The puller's per-digest
// in-flight dedupe is unchanged.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gantry/gantry/internal/digest"
	"github.com/gantry/gantry/internal/hrw"
	"github.com/gantry/gantry/internal/ifaces"
)

// PrefetchLayers groups digests by their HRW rank-0 reachable
// designated puller and issues one PleasePull RPC per puller. Digests
// HRW'ing to self are skipped (the local agent doesn't ask itself).
// Already-cached or otherwise filtered digests should be removed by
// the caller before invoking.
//
// PrefetchLayers blocks until every per-puller RPC has completed or
// errored, but each RPC is bounded by QueryTimeout. Callers run it in
// a goroutine for fire-and-forget semantics. A returned error means
// at least one puller's RPC failed; callers can log it and otherwise
// ignore — the next per-digest Resolve call from containerd will fall
// back to the full §5.2 cascade.
//
// registry and repository identify the upstream and OCI repo. They
// MUST be non-empty (§4.4 single-repo-per-batch invariant); an empty
// value returns ErrPrefetchInvalid without issuing any RPC.
func (r *Resolver) PrefetchLayers(ctx context.Context, digests []digest.Digest, registry, repository string) error {
	if registry == "" || repository == "" {
		return fmt.Errorf("%w: registry=%q repository=%q",
			ErrPrefetchInvalid, registry, repository)
	}
	if len(digests) == 0 {
		return nil
	}

	cluster := r.opts.Members.Snapshot()
	self := r.opts.Members.Self()
	candidates := hrw.Candidates(cluster, r.opts.HrwScope, r.opts.SelfZone)
	if r.opts.HrwScope == hrw.ScopeZone && len(candidates) == 0 {
		// Zone empty → fall back to cluster mode (mirrors Resolve's
		// behaviour, §4.3).
		candidates = cluster
	}
	if len(candidates) == 0 {
		return nil
	}

	// Group digests by HRW rank-0 puller, excluding self. Deterministic
	// digest order inside each group: input order, which is also the
	// order containerd will pull the layers.
	byPuller := make(map[ifaces.NodeID][]digest.Digest)
	skippedSelf := 0
	skippedNoTop := 0
	// Suppress duplicate digests within a single call so a malformed
	// manifest with repeated layer references doesn't inflate the RPC.
	seen := make(map[string]struct{}, len(digests))
	for _, d := range digests {
		key := d.String()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		top := hrw.TopK(candidates, d, 1)
		if len(top) == 0 {
			skippedNoTop++
			continue
		}
		puller := top[0].Node.ID
		if puller == self {
			skippedSelf++
			continue
		}
		byPuller[puller] = append(byPuller[puller], d)
	}
	if len(byPuller) == 0 {
		r.opts.Logger.Debug("coldstart: prefetch had no remote pullers",
			slog.Int("digests", len(digests)),
			slog.Int("skipped_self", skippedSelf),
			slog.Int("skipped_no_top", skippedNoTop),
		)
		return nil
	}

	// Sort puller IDs for deterministic logging / test order.
	pullerIDs := make([]ifaces.NodeID, 0, len(byPuller))
	for id := range byPuller {
		pullerIDs = append(pullerIDs, id)
	}
	sort.Slice(pullerIDs, func(i, j int) bool { return pullerIDs[i] < pullerIDs[j] })

	r.opts.Logger.Debug("coldstart: prefetch batching",
		slog.Int("digests", len(digests)),
		slog.Int("pullers", len(byPuller)),
		slog.Int("skipped_self", skippedSelf),
	)
	if r.opts.Metrics.OnPrefetchBatch != nil {
		r.opts.Metrics.OnPrefetchBatch(len(byPuller), len(digests)-skippedSelf-skippedNoTop)
	}

	var wg sync.WaitGroup
	var failures atomic.Int32
	for _, id := range pullerIDs {
		ds := byPuller[id]
		wg.Add(1)
		go func(node ifaces.NodeID, digests []digest.Digest) {
			defer wg.Done()
			callCtx, cancel := context.WithTimeout(ctx, r.opts.QueryTimeout)
			defer cancel()
			_, err := r.opts.Coord.PleasePull(callCtx, node, registry, repository, digests)
			if err != nil {
				failures.Add(1)
				r.opts.Logger.Debug("coldstart: prefetch please_pull failed",
					slog.String("puller", string(node)),
					slog.Int("batch_size", len(digests)),
					slog.Any("err", err),
				)
			}
		}(id, ds)
	}
	wg.Wait()
	if n := failures.Load(); n > 0 {
		return fmt.Errorf("%w: %d/%d pullers errored", ErrPrefetchPartial, n, len(byPuller))
	}
	return nil
}

// ErrPrefetchInvalid signals a programmer error in the prefetch
// call: registry or repository was empty, which would produce a
// malformed PleasePull RPC.
var ErrPrefetchInvalid = errors.New("coldstart: prefetch invalid arguments")

// ErrPrefetchPartial signals at least one puller's PleasePull RPC
// failed. The other pullers were still asked to pull. Callers can
// errors.Is against this for retry / logging logic; the prefetch is
// best-effort so this is informational rather than fatal.
var ErrPrefetchPartial = errors.New("coldstart: prefetch had per-puller failures")
