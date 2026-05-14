//go:build e2e

package e2e

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestMain centralizes the suite-level boot/teardown so each test
// function gets a ready cluster and the cluster is torn down exactly
// once even if multiple tests are added later.
func TestMain(m *testing.M) {
	if err := guardAssumptions(); err != nil {
		// Fail loud rather than silently skip.
		os.Stderr.WriteString("e2e: " + err.Error() + "\n")
		os.Exit(2)
	}
	os.Exit(m.Run())
}

// TestSmoke_DaemonSetBecomesReady is the canary test. It proves the
// end-to-end pipeline works:
//
//   - kind cluster boots,
//   - the gantry image builds and side-loads,
//   - the deploy manifests apply cleanly,
//   - the DaemonSet rolls out to every node,
//   - each pod's /readyz turns green within the timeout.
//
// This is the foundation every future scenario (cache-hit on second
// pull, NF5 chaos, eviction headroom, etc.) builds on.
func TestSmoke_DaemonSetBecomesReady(t *testing.T) {
	h := newHarness(t)
	h.checkPrereqs()

	// 15-minute overall budget — kind boot can take 90 s on a cold
	// docker pull, then image build ~30 s, rollout ~30 s, with
	// generous slack.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	h.bootCluster(ctx)
	t.Cleanup(func() {
		// Best-effort teardown; use a fresh ctx since the test's
		// may already be cancelled by a Fatal.
		tdCtx, tdCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer tdCancel()
		h.teardown(tdCtx)
	})

	h.buildAndLoadImage(ctx)
	h.applyManifests(ctx)
	h.waitForRollout(ctx)
	h.checkReadyz(ctx)
}
