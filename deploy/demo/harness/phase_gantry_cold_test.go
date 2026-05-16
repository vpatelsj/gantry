//go:build demo

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPhaseGantryCold(t *testing.T) {
	if os.Getenv("DEMO_RUN_LIVE") != "1" {
		t.Skip("set DEMO_RUN_LIVE=1 to build/push an image and run the live Gantry cold-start phase")
	}
	cfg, err := LoadLiveConfig()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := WaitForGantryRollout(ctx, cfg); err != nil {
		t.Fatalf("gantry DaemonSet not converged: %v (run deploy/demo/infra/41-deploy-gantry.sh first)", err)
	}

	preflightImage, err := BuildFreshWorkloadImage(ctx, cfg, PhaseName("preflight"))
	if err != nil {
		t.Fatalf("build preflight image: %v", err)
	}
	repo := strings.TrimPrefix(strings.SplitN(preflightImage, ":", 2)[0], cfg.ACRLoginServer+"/")
	tag := strings.SplitN(preflightImage, ":", 2)[1]
	t.Logf("preflight pull through proxy: %s:%s", repo, tag)
	if err := PreflightWiring(ctx, cfg, repo, tag); err != nil {
		t.Fatalf("wiring preflight failed: %v", err)
	}

	gantryOriginBefore, err := FetchGantryMetric(ctx, cfg, "sum(p2p_origin_pull_total)")
	if err != nil {
		t.Logf("warning: could not query gantry metric before phase: %v", err)
	}
	proxyBefore, err := FetchLiveProxySummary(ctx, cfg)
	if err != nil {
		t.Fatalf("fetch proxy summary before phase: %v", err)
	}

	image, err := BuildFreshWorkloadImage(ctx, cfg, PhaseGantryCold)
	if err != nil {
		t.Fatalf("build cold-start image: %v", err)
	}
	t.Logf("pushed cold-start image %s", image)

	if err := InstallHostsToml(ctx, cfg, gantryHostsMode()); err != nil {
		t.Fatalf("install gantry hosts.toml: %v", err)
	}

	phaseStart := time.Now().UTC()
	containerStarts, jobName, err := RunPullJob(ctx, cfg, PhaseGantryCold, image)
	if err != nil {
		t.Fatalf("run gantry-cold pull job: %v", err)
	}
	t.Logf("completed job %s with %d pod log timestamps", jobName, len(containerStarts))

	proxyAfter, err := FetchLiveProxySummary(ctx, cfg)
	if err != nil {
		t.Fatalf("fetch proxy summary after phase: %v", err)
	}
	delta := DiffProxySummary(proxyBefore, proxyAfter)
	t.Logf("proxy delta: requests=%d bytes_to_client=%d by_path_class=%+v", delta.RequestsCompleted, delta.BytesToClient, delta.ByPathClass)

	gantryOriginAfter, err := FetchGantryMetric(ctx, cfg, "sum(p2p_origin_pull_total)")
	if err == nil {
		t.Logf("gantry p2p_origin_pull_total delta: %.0f", gantryOriginAfter-gantryOriginBefore)
	}
	if peers, err := FetchGantryMetric(ctx, cfg, `sum(p2p_peer_fetch_total{outcome="hit"})`); err == nil {
		t.Logf("gantry p2p_peer_fetch_total{outcome=\"hit\"}: %.0f", peers)
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
