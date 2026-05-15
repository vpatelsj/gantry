//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPatchDaemonSetForE2E_TargetsGantryContainerOnly is the
// regression test for the twelfth-review finding: the harness's
// previous strings.Replace(..., 1) on the bare "imagePullPolicy:
// IfNotPresent" string patched the FIRST occurrence (the busybox
// initContainer), not the gantry container.
//
// On a fresh kind cluster busybox:1.36 is NOT preloaded, so setting
// it to imagePullPolicy=Never caused ErrImageNeverPull and the
// initContainer never started. Meanwhile the gantry container kept
// imagePullPolicy=IfNotPresent, so kubelet would have tried to pull
// the side-loaded :e2e tag from a registry that doesn't have it.
//
// This test pins the contract that patchDaemonSetForE2E:
//   - sets the gantry container to use the side-loaded e2e image,
//   - sets the gantry container's imagePullPolicy to Never,
//   - leaves the busybox initContainer's image and imagePullPolicy
//     UNCHANGED so kind's containerd can pull busybox normally,
//   - and fails loudly if the anchor stops matching.
func TestPatchDaemonSetForE2E_TargetsGantryContainerOnly(t *testing.T) {
	// Locate the real deploy/daemonset.yaml relative to the e2e
	// package directory. Using the actual on-disk manifest means
	// this test also catches "someone reformatted the gantry
	// container line in deploy/daemonset.yaml" regressions.
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot, "deploy", "daemonset.yaml"))
	if err != nil {
		t.Fatalf("read daemonset.yaml: %v", err)
	}

	const e2eTag = "gantry:e2e-test-fixture"
	patched, err := patchDaemonSetForE2E(string(raw), e2eTag)
	if err != nil {
		t.Fatalf("patchDaemonSetForE2E: %v", err)
	}

	// Gantry container must reference the side-loaded tag with
	// imagePullPolicy=Never.
	if !strings.Contains(patched, "image: "+e2eTag) {
		t.Errorf("patched manifest missing 'image: %s'; gantry container was not retargeted", e2eTag)
	}
	if strings.Contains(patched, "image: ghcr.io/vpatelsj/gantry:latest") {
		t.Errorf("patched manifest still contains the production gantry image reference; the swap did not apply to the gantry container")
	}

	// Busybox initContainer must be UNCHANGED. The reviewer's
	// concrete case: busybox is not preloaded into kind, so it
	// must keep imagePullPolicy=IfNotPresent (kind's containerd
	// pulls it from the public registry on first boot).
	if !strings.Contains(patched, "image: busybox:1.36") {
		t.Errorf("patched manifest is missing the busybox initContainer image; the helper accidentally rewrote it")
	}

	// Count imagePullPolicy occurrences: there must be exactly
	// one "Never" (the gantry container) and one "IfNotPresent"
	// (the busybox initContainer). Anything else means the
	// swap leaked across containers.
	neverCount := strings.Count(patched, "imagePullPolicy: Never")
	ifNotPresentCount := strings.Count(patched, "imagePullPolicy: IfNotPresent")
	if neverCount != 1 {
		t.Errorf("imagePullPolicy=Never count = %d, want exactly 1 (only the gantry container should be Never)", neverCount)
	}
	if ifNotPresentCount != 1 {
		t.Errorf("imagePullPolicy=IfNotPresent count = %d, want exactly 1 (the busybox initContainer must keep IfNotPresent so kind can pull it)", ifNotPresentCount)
	}

	// Specifically: the line that follows `image: busybox:1.36`
	// must still be `imagePullPolicy: IfNotPresent`. This is the
	// load-bearing assertion — it's the exact bug class the
	// twelfth review flagged.
	busyboxIdx := strings.Index(patched, "image: busybox:1.36")
	if busyboxIdx < 0 {
		t.Fatalf("patched: busybox not found")
	}
	after := patched[busyboxIdx:]
	// Find the next imagePullPolicy line after the busybox image
	// line.
	policyIdx := strings.Index(after, "imagePullPolicy:")
	if policyIdx < 0 {
		t.Fatalf("patched: imagePullPolicy not found after busybox")
	}
	// Read the policy value (up to end of line).
	policyLine := after[policyIdx:]
	if nl := strings.IndexByte(policyLine, '\n'); nl >= 0 {
		policyLine = policyLine[:nl]
	}
	if !strings.Contains(policyLine, "IfNotPresent") {
		t.Errorf("busybox initContainer policy line = %q, want it to retain 'IfNotPresent' (the previous bug set busybox to Never which fails on kind because busybox is not preloaded)", policyLine)
	}
}

// TestPatchDaemonSetForE2E_FailsLoudWhenAnchorMissing covers the
// other half of the contract: if deploy/daemonset.yaml's gantry
// container line is reformatted in a way that breaks the anchor,
// the helper must fail loudly rather than silently leaving the
// production image in place. The previous "two independent
// strings.Replace calls" code silently no-op'd in that case; this
// helper returns an explicit error so the harness exits before
// kubectl apply.
func TestPatchDaemonSetForE2E_FailsLoudWhenAnchorMissing(t *testing.T) {
	// A manifest that does NOT contain the gantry container line
	// in the expected layout. Any apply on this should error.
	raw := "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: gantry\nspec: {}\n"

	_, err := patchDaemonSetForE2E(raw, "gantry:e2e")
	if err == nil {
		t.Fatalf("patchDaemonSetForE2E returned nil error on a manifest missing the gantry anchor; want a loud error so the harness aborts before kubectl apply")
	}
	if !strings.Contains(err.Error(), "anchor not found") {
		t.Errorf("error message = %q, want it to mention 'anchor not found' so the operator knows to update gantryContainerAnchor", err.Error())
	}
}
