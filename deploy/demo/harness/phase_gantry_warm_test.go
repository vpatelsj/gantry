//go:build demo

package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPhaseGantryWarm(t *testing.T) {
	if os.Getenv("DEMO_RUN_LIVE") != "1" {
		t.Skip("set DEMO_RUN_LIVE=1 to run the live Gantry warm-cache phase")
	}
	if os.Getenv("DEMO_ALLOW_CONTENT_PURGE") != "1" {
		t.Skip("set DEMO_ALLOW_CONTENT_PURGE=1 to confirm you intend to wipe containerd content stores on every node")
	}
	imageRef := os.Getenv("DEMO_WARM_IMAGE_REF")
	if imageRef == "" {
		t.Fatal("DEMO_WARM_IMAGE_REF is required: pass the same image used for the gantry-cold phase")
	}

	cfg, err := LoadLiveConfig()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := WaitForGantryRollout(ctx, cfg); err != nil {
		t.Fatalf("gantry DaemonSet not converged: %v", err)
	}

	if err := InstallHostsToml(ctx, cfg, gantryHostsMode("gantry")); err != nil {
		t.Fatalf("install gantry hosts.toml: %v", err)
	}

	if _, err := runCommand(ctx, cfg.RepoRoot, nil,
		"deploy/demo/infra/85-purge-containerd-cache.sh", cfg.EnvFile, imageRef,
	); err != nil {
		t.Fatalf("purge containerd cache: %v", err)
	}

	proxyBefore, err := FetchLiveProxySummary(ctx, cfg)
	if err != nil {
		t.Fatalf("fetch proxy summary before phase: %v", err)
	}
	cacheHitsBefore, _ := FetchGantryMetric(ctx, cfg, "sum(p2p_cache_hit_total)")

	phaseStart := time.Now().UTC()
	containerStarts, jobName, err := RunPullJob(ctx, cfg, PhaseGantryWarm, imageRef)
	if err != nil {
		t.Fatalf("run gantry-warm pull job: %v", err)
	}
	t.Logf("completed job %s with %d pod log timestamps", jobName, len(containerStarts))

	proxyAfter, err := FetchLiveProxySummary(ctx, cfg)
	if err != nil {
		t.Fatalf("fetch proxy summary after phase: %v", err)
	}
	delta := DiffProxySummary(proxyBefore, proxyAfter)
	t.Logf("proxy delta: requests=%d bytes_to_client=%d by_path_class=%+v", delta.RequestsCompleted, delta.BytesToClient, delta.ByPathClass)

	digestRequests := delta.ByPathClass["blob"].Requests + delta.ByPathClass["manifest_by_digest"].Requests
	digestBytes := delta.ByPathClass["blob"].Bytes + delta.ByPathClass["manifest_by_digest"].Bytes
	if digestRequests != 0 || digestBytes != 0 {
		t.Errorf("warm cache leaked digest traffic to proxy: requests=%d bytes=%d (expected 0; see plan §6 Phase 3)", digestRequests, digestBytes)
	}

	if hits, err := FetchGantryMetric(ctx, cfg, "sum(p2p_cache_hit_total)"); err == nil {
		t.Logf("gantry p2p_cache_hit_total delta: %.0f", hits-cacheHitsBefore)
	}

	latencies := make([]time.Duration, 0, len(containerStarts))
	for _, stamp := range containerStarts {
		if stamp.After(phaseStart) {
			latencies = append(latencies, stamp.Sub(phaseStart))
		}
	}
	if summary, err := SummarizeLatencies(latencies); err == nil {
		t.Logf("pod-start latency from phase start: p50=%s p95=%s p100=%s", summary.P50, summary.P95, summary.P100)
	}
}
