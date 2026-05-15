//go:build e2e

// Package e2e holds the kind-based integration suite for gantry.
//
// All files in this package are guarded by `//go:build e2e` so the
// default `go test ./...` skips them. Run via `make e2e` or
// `go test -tags=e2e ./e2e/...`.
//
// The harness is intentionally CLI-driven (shell-out to kind, kubectl,
// docker) rather than client-go-driven so the e2e module stays
// dependency-free relative to the root module.
package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	clusterName  = "gantry-e2e"
	imageTag     = "gantry:e2e"
	manifestsDir = "../deploy"
	namespace    = "gantry-system"
	dsName       = "gantry"
)

// harness bundles the setup/teardown lifecycle for one e2e run. One
// instance is shared across the test functions in this package.
type harness struct {
	t           *testing.T
	repoRoot    string
	artifacts   string
	keepCluster bool
}

// newHarness resolves repo paths from the e2e/ directory's location
// at test time.
func newHarness(t *testing.T) *harness {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Dir(wd) // e2e/ → repo root
	artifacts := filepath.Join(wd, ".artifacts")
	if err := os.MkdirAll(artifacts, 0o755); err != nil {
		t.Fatalf("mkdir artifacts: %v", err)
	}
	return &harness{
		t:           t,
		repoRoot:    root,
		artifacts:   artifacts,
		keepCluster: os.Getenv("E2E_KEEP") == "1",
	}
}

// checkPrereqs fails the test fast if any required CLI is missing or
// docker isn't running.
func (h *harness) checkPrereqs() {
	h.t.Helper()
	for _, bin := range []string{"docker", "kind", "kubectl"} {
		if _, err := exec.LookPath(bin); err != nil {
			h.t.Skipf("e2e prereq %q missing on PATH; skipping suite", bin)
		}
	}
	// docker info is the canonical "is the engine actually running?" probe.
	if err := h.run(context.Background(), "docker", "info"); err != nil {
		h.t.Skipf("docker engine unreachable (%v); skipping suite", err)
	}
}

// bootCluster creates the kind cluster declared by kind-config.yaml.
// Idempotent: if a cluster with the same name already exists we use it.
func (h *harness) bootCluster(ctx context.Context) {
	h.t.Helper()
	if h.clusterExists(ctx) {
		h.t.Logf("kind cluster %q already exists; reusing", clusterName)
		return
	}
	cfg := filepath.Join(h.repoRoot, "e2e", "kind-config.yaml")
	if err := h.run(ctx, "kind", "create", "cluster", "--config", cfg, "--wait", "120s"); err != nil {
		h.t.Fatalf("kind create cluster: %v", err)
	}
}

func (h *harness) clusterExists(ctx context.Context) bool {
	out, err := h.runOut(ctx, "kind", "get", "clusters")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == clusterName {
			return true
		}
	}
	return false
}

// buildAndLoadImage builds the gantry container image for the kind
// cluster's host platform and loads it into every kind node.
func (h *harness) buildAndLoadImage(ctx context.Context) {
	h.t.Helper()
	platform := fmt.Sprintf("linux/%s", goArchForDocker(runtime.GOARCH))
	build := filepath.Join(h.repoRoot, "deploy", "build.sh")
	if err := h.run(ctx, build, "-p", platform, "-t", "e2e", "-r", "gantry"); err != nil {
		h.t.Fatalf("build image: %v", err)
	}
	if err := h.run(ctx, "kind", "load", "docker-image", imageTag, "--name", clusterName); err != nil {
		h.t.Fatalf("kind load: %v", err)
	}
}

// applyManifests installs the gantry rollout into the kind cluster,
// rewriting the DaemonSet image to the freshly-loaded e2e tag.
func (h *harness) applyManifests(ctx context.Context) {
	h.t.Helper()
	// `kubectl create ns --dry-run=client` would generate the manifest;
	// the serviceaccount.yaml below contains the namespace declaration
	// so we don't need a separate ns-create call. Documenting here so
	// it doesn't look like an oversight.
	if err := h.run(ctx, "kubectl", "apply", "-f",
		filepath.Join(h.repoRoot, "deploy", "serviceaccount.yaml")); err != nil {
		h.t.Fatalf("apply serviceaccount: %v", err)
	}
	if err := h.run(ctx, "kubectl", "apply", "-f",
		filepath.Join(h.repoRoot, "deploy", "configmap.yaml")); err != nil {
		h.t.Fatalf("apply configmap: %v", err)
	}
	// NetworkPolicy is intentionally NOT part of the default e2e
	// rollout. The hardening manifest lives at
	// deploy/examples/networkpolicy.yaml and is templated — every
	// rule defers CIDR/namespace choices to the operator (see
	// deploy/README.md § Hardening overlays). Applying it as-is here
	// would either fail validation (unresolved placeholders) or
	// silently isolate the agent pods from the kind-cluster control
	// plane and break the smoke test before rollout. A dedicated
	// hardening-overlay e2e variant can opt in via the
	// GANTRY_E2E_NETWORKPOLICY env var once we have a kind-friendly
	// concrete copy to ship.
	if os.Getenv("GANTRY_E2E_NETWORKPOLICY") != "" {
		if err := h.run(ctx, "kubectl", "apply", "-f",
			filepath.Join(h.repoRoot, "deploy", "examples", "networkpolicy.yaml")); err != nil {
			h.t.Fatalf("apply networkpolicy (opt-in): %v", err)
		}
	}
	// Rewrite the DaemonSet image to gantry:e2e using kubectl's
	// -k overlay would require a kustomization file; for now use
	// `sed` via kubectl apply -f - with stdin.
	if err := h.applyDaemonSet(ctx); err != nil {
		h.t.Fatalf("apply daemonset: %v", err)
	}
}

func (h *harness) applyDaemonSet(ctx context.Context) error {
	raw, err := os.ReadFile(filepath.Join(h.repoRoot, "deploy", "daemonset.yaml"))
	if err != nil {
		return err
	}
	// Swap the image reference to our locally-loaded tag.
	patched := strings.Replace(string(raw),
		"ghcr.io/vpatelsj/gantry:latest", imageTag, 1)
	// Force imagePullPolicy=Never so kubelet doesn't try to pull
	// gantry:e2e from a registry — we've side-loaded it via kind.
	patched = strings.Replace(patched,
		"imagePullPolicy: IfNotPresent", "imagePullPolicy: Never", 1)

	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(patched)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kubectl apply: %w (stderr: %s)", err, stderr.String())
	}
	return nil
}

// waitForRollout polls until the DaemonSet reports all desired pods
// ready, or the context fires.
//
// The poll cadence is 5 s — explicitly chosen to honor the
// "never sleep > 5s in one call" project preference: the loop sleeps
// in 5 s steps so the test stays interruptible by Ctrl-C / ctx.
func (h *harness) waitForRollout(ctx context.Context) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		if time.Now().After(deadline) {
			h.dumpDiagnostics(ctx)
			h.t.Fatalf("daemonset %s/%s did not roll out within 5m", namespace, dsName)
		}
		out, err := h.runOut(ctx, "kubectl", "-n", namespace, "rollout", "status",
			"ds/"+dsName, "--timeout=5s")
		if err == nil && strings.Contains(out, "rolled out") {
			return
		}
		select {
		case <-ctx.Done():
			h.t.Fatalf("ctx cancelled while waiting for rollout: %v", ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
}

// checkReadyz execs into one daemon pod and curls its /readyz endpoint.
func (h *harness) checkReadyz(ctx context.Context) {
	h.t.Helper()
	// Pick the first ready pod's name.
	name, err := h.runOut(ctx, "kubectl", "-n", namespace,
		"get", "pods", "-l", "app.kubernetes.io/name=gantry",
		"-o", "jsonpath={.items[0].metadata.name}")
	if err != nil || strings.TrimSpace(name) == "" {
		h.t.Fatalf("get pods: %v", err)
	}
	name = strings.TrimSpace(name)

	// The metrics port is :9095 inside the pod. We use a synthetic
	// "wget -qO- 127.0.0.1:9095/readyz" via kubectl exec rather than
	// port-forwarding, because the latter races with pod readiness in
	// short tests.
	out, err := h.runOut(ctx, "kubectl", "-n", namespace,
		"exec", name, "--",
		"wget", "-qO-", "--timeout=5", "http://127.0.0.1:9095/readyz")
	if err != nil {
		h.t.Fatalf("/readyz probe on pod %q: %v", name, err)
	}
	if !strings.Contains(out, "ok") && !strings.Contains(out, "ready") {
		h.t.Fatalf("/readyz returned %q; expected 'ok' or 'ready'", out)
	}
}

// teardown deletes the kind cluster. Skipped when E2E_KEEP=1.
func (h *harness) teardown(ctx context.Context) {
	if h.keepCluster {
		h.t.Logf("E2E_KEEP=1 — leaving cluster %q running", clusterName)
		return
	}
	if err := h.run(ctx, "kind", "delete", "cluster", "--name", clusterName); err != nil {
		h.t.Logf("kind delete cluster: %v", err)
	}
}

// dumpDiagnostics writes pod logs + describe output into the
// artifacts dir on failure.
func (h *harness) dumpDiagnostics(ctx context.Context) {
	h.t.Helper()
	for _, args := range [][]string{
		{"-n", namespace, "get", "pods", "-o", "wide"},
		{"-n", namespace, "describe", "ds/" + dsName},
		{"-n", namespace, "logs", "ds/" + dsName, "--all-containers", "--tail=200"},
	} {
		out, _ := h.runOut(ctx, "kubectl", args...)
		dst := filepath.Join(h.artifacts, fmt.Sprintf("%s_%s.log", h.t.Name(), strings.Join(args, "_")))
		_ = os.WriteFile(dst, []byte(out), 0o644) //#nosec G306 -- test artifact
		h.t.Logf("diagnostics: wrote %s", dst)
	}
}

// run executes cmd and pipes stdout+stderr to the test log.
// Inherits the parent process environment; per-call env overrides are
// not currently needed.
func (h *harness) run(ctx context.Context, name string, args ...string) error {
	h.t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = h.repoRoot
	cmd.Env = os.Environ()
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, buf.String())
	}
	return nil
}

// runOut is run() but returns stdout for callers that need to parse it.
// stderr is still surfaced on error.
func (h *harness) runOut(ctx context.Context, name string, args ...string) (string, error) {
	h.t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = h.repoRoot
	cmd.Env = os.Environ()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, stderr.String())
	}
	return stdout.String(), nil
}

// goArchForDocker maps Go's GOARCH to Docker's platform flag form.
func goArchForDocker(goarch string) string {
	switch goarch {
	case "amd64", "arm64":
		return goarch
	default:
		return goarch
	}
}

// guardAssumptions panics if the test environment violates contracts
// the harness depends on. Used by TestMain in smoke_e2e_test.go.
func guardAssumptions() error {
	if runtime.GOOS == "windows" {
		return errors.New("e2e suite is unsupported on windows; run from linux or darwin")
	}
	return nil
}
