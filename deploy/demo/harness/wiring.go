//go:build demo

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PreflightWiring exercises a single image pull through the proxy from
// inside the demo namespace and asserts the proxy counter moved.
//
// This is the "knob (b)" check from plan §6 Phase 2. It does NOT
// exercise containerd or Gantry — that requires the gantry hosts.toml
// being installed on a node, which is a more invasive change. The
// caller is expected to verify the Gantry side separately by inspecting
// p2p_origin_pull_total deltas in Prometheus.
func PreflightWiring(ctx context.Context, cfg LiveConfig, repo, tag string) error {
	before, err := FetchLiveProxySummary(ctx, cfg)
	if err != nil {
		return fmt.Errorf("fetch proxy summary before preflight: %w", err)
	}

	pod := fmt.Sprintf("preflight-%d", time.Now().UnixNano())
	accept := "application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"
	if _, err := runCommand(ctx, cfg.RepoRoot, nil,
		"kubectl", "-n", cfg.GantryNamespace, "run", pod,
		"--restart=Never", "--image=curlimages/curl:8.10.1",
		"--command", "--",
		"sh", "-c",
		fmt.Sprintf("curl -sS -o /dev/null -H 'Accept: %s' http://acr-origin-proxy:5002/v2/%s/manifests/%s", accept, repo, tag),
	); err != nil {
		return fmt.Errorf("preflight curl pod: %w", err)
	}
	defer func() {
		_, _ = runCommand(context.Background(), cfg.RepoRoot, nil,
			"kubectl", "-n", cfg.GantryNamespace, "delete", "pod", pod, "--ignore-not-found=true", "--wait=false")
	}()

	if _, err := runCommand(ctx, cfg.RepoRoot, nil,
		"kubectl", "-n", cfg.GantryNamespace, "wait", "pod/"+pod, "--for=condition=Ready=False", "--timeout=2m",
	); err != nil {
		// The pod completes quickly; treat wait failure as informational.
	}
	if _, err := runCommand(ctx, cfg.RepoRoot, nil,
		"kubectl", "-n", cfg.GantryNamespace, "wait", "pod/"+pod, "--for=jsonpath={.status.phase}=Succeeded", "--timeout=2m",
	); err != nil {
		return fmt.Errorf("preflight pod did not succeed: %w", err)
	}

	after, err := FetchLiveProxySummary(ctx, cfg)
	if err != nil {
		return fmt.Errorf("fetch proxy summary after preflight: %w", err)
	}
	delta := DiffProxySummary(before, after)
	if delta.RequestsCompleted == 0 {
		return fmt.Errorf("proxy counter did not move during preflight (delta=0); proxy is unreachable from %s namespace", cfg.GantryNamespace)
	}
	return nil
}

// WaitForGantryRollout polls until the gantry DaemonSet reports all
// desired pods Ready or the context is cancelled.
func WaitForGantryRollout(ctx context.Context, cfg LiveConfig) error {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("gantry DaemonSet did not converge within deadline")
		}
		out, err := runCommand(ctx, cfg.RepoRoot, nil,
			"kubectl", "-n", "gantry-system", "get", "ds", "gantry",
			"-o", "jsonpath={.status.desiredNumberScheduled} {.status.numberReady}",
		)
		if err == nil {
			parts := strings.Fields(strings.TrimSpace(out))
			if len(parts) == 2 && parts[0] == parts[1] && parts[0] != "0" {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// FetchGantryMetric fetches a single Prometheus metric via the
// Kubernetes Service proxy and returns the numeric sample sum across
// all matching series. Returns 0 and nil when the metric has no
// samples (e.g. before any traffic has flowed).
func FetchGantryMetric(ctx context.Context, cfg LiveConfig, query string) (float64, error) {
	rawPath := "/api/v1/namespaces/monitoring/services/http:kps-kube-prometheus-stack-prometheus:9090/proxy/api/v1/query?query=" + queryEscape(query)
	out, err := runCommand(ctx, cfg.RepoRoot, nil, "kubectl", "get", "--raw", rawPath)
	if err != nil {
		return 0, fmt.Errorf("query prometheus: %w", err)
	}
	var body struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &body); err != nil {
		return 0, fmt.Errorf("decode prometheus response: %w", err)
	}
	if body.Status != "success" {
		return 0, fmt.Errorf("prometheus query status = %q", body.Status)
	}
	var total float64
	for _, r := range body.Data.Result {
		s, ok := r.Value[1].(string)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			continue
		}
		total += v
	}
	return total, nil
}

func queryEscape(q string) string {
	// Minimal URL escape sufficient for promql: spaces, parens, braces,
	// equals, quotes, brackets, semicolons.
	replacer := strings.NewReplacer(
		" ", "%20", "(", "%28", ")", "%29", "{", "%7B", "}", "%7D",
		"\"", "%22", "[", "%5B", "]", "%5D", ";", "%3B", "+", "%2B",
		",", "%2C", "=", "%3D", "/", "%2F",
	)
	return replacer.Replace(q)
}
