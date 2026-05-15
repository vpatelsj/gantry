//go:build demo

package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPhaseBaseline(t *testing.T) {
	if os.Getenv("DEMO_RUN_LIVE") != "1" {
		t.Skip("set DEMO_RUN_LIVE=1 to build/push an image, install baseline hosts.toml, and run the live baseline Job")
	}
	cfg, err := LoadLiveConfig()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	before, err := FetchLiveProxySummary(ctx, cfg)
	if err != nil {
		t.Fatalf("fetch proxy summary before phase: %v", err)
	}

	image, err := BuildFreshWorkloadImage(ctx, cfg, PhaseBaseline)
	if err != nil {
		t.Fatalf("build fresh workload image: %v", err)
	}
	t.Logf("pushed baseline image %s", image)

	if err := InstallHostsToml(ctx, cfg, "baseline"); err != nil {
		t.Fatalf("install baseline hosts.toml: %v", err)
	}

	phaseStart := time.Now().UTC()
	containerStarts, jobName, err := RunPullJob(ctx, cfg, PhaseBaseline, image)
	if err != nil {
		t.Fatalf("run baseline pull job: %v", err)
	}
	t.Logf("completed job %s with %d pod log timestamps", jobName, len(containerStarts))

	after, err := FetchLiveProxySummary(ctx, cfg)
	if err != nil {
		t.Fatalf("fetch proxy summary after phase: %v", err)
	}
	delta := DiffProxySummary(before, after)
	t.Logf("proxy delta: requests=%d bytes_to_client=%d by_path_class=%+v", delta.RequestsCompleted, delta.BytesToClient, delta.ByPathClass)

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
