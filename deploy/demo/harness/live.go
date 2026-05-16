//go:build demo

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type LiveConfig struct {
	RepoRoot          string
	EnvFile           string
	ACRLoginServer    string
	ProxySummaryURL   string
	GantryNamespace   string
	WorkloadNamespace string
	WorkloadRepo      string
	ImagePlatform     string
	ImageSizeMiB      int
	NodeCount         int
	JobTimeout        time.Duration
}

func LoadLiveConfig() (LiveConfig, error) {
	repoRoot, err := repoRoot()
	if err != nil {
		return LiveConfig{}, err
	}
	cfg := LiveConfig{
		RepoRoot:          repoRoot,
		EnvFile:           envDefault("DEMO_INFRA_ENV", filepath.Join(repoRoot, "deploy/demo/infra/env.local")),
		ACRLoginServer:    os.Getenv("ACR_LOGIN_SERVER"),
		ProxySummaryURL:   os.Getenv("PROXY_SUMMARY_URL"),
		GantryNamespace:   envDefault("GANTRY_DEMO_NAMESPACE", "gantry-demo"),
		WorkloadNamespace: envDefault("DEMO_WORKLOAD_NAMESPACE", "default"),
		WorkloadRepo:      envDefault("DEMO_WORKLOAD_REPO", "gantry-demo-pull"),
		ImagePlatform:     envDefault("IMAGE_PLATFORM", "linux/amd64"),
		ImageSizeMiB:      intEnvDefault("DEMO_IMAGE_SIZE_MB", 16),
		NodeCount:         intEnvDefault("DEMO_NODE_COUNT", 20),
		JobTimeout:        durationEnvDefault("DEMO_JOB_TIMEOUT", 45*time.Minute),
	}
	if cfg.ACRLoginServer == "" {
		return LiveConfig{}, errors.New("ACR_LOGIN_SERVER is required; source deploy/demo/infra/.state.env or set it explicitly")
	}
	if cfg.ImageSizeMiB <= 0 {
		return LiveConfig{}, errors.New("DEMO_IMAGE_SIZE_MB must be > 0")
	}
	if cfg.NodeCount <= 0 {
		return LiveConfig{}, errors.New("DEMO_NODE_COUNT must be > 0")
	}
	return cfg, nil
}

func FetchLiveProxySummary(ctx context.Context, cfg LiveConfig) (ProxySummary, error) {
	if cfg.ProxySummaryURL != "" {
		return FetchProxySummary(ctx, nil, cfg.ProxySummaryURL)
	}
	rawPath := fmt.Sprintf("/api/v1/namespaces/%s/services/http:acr-origin-proxy:9090/proxy/debug/summary", cfg.GantryNamespace)
	out, err := runCommand(ctx, cfg.RepoRoot, nil, "kubectl", "get", "--raw", rawPath)
	if err != nil {
		return ProxySummary{}, err
	}
	var wire summaryWire
	if err := json.Unmarshal([]byte(out), &wire); err != nil {
		return ProxySummary{}, err
	}
	since, err := time.Parse(time.RFC3339, wire.Since)
	if err != nil {
		return ProxySummary{}, fmt.Errorf("parse since %q: %w", wire.Since, err)
	}
	return ProxySummary{
		Since:         since,
		UptimeSeconds: wire.UptimeSeconds,
		Totals:        wire.Totals,
		RawClasses:    wire.Totals.ByPathClass,
	}, nil
}

func BuildFreshWorkloadImage(ctx context.Context, cfg LiveConfig, phase PhaseName) (string, error) {
	tag := fmt.Sprintf("%s-%s", phase, time.Now().UTC().Format("20060102150405"))
	taggedImage := fmt.Sprintf("%s/%s:%s", cfg.ACRLoginServer, cfg.WorkloadRepo, tag)
	tmpdir, err := os.MkdirTemp("", "gantry-demo-image-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpdir)

	dockerfile := []byte(`FROM alpine:3.20
COPY payload.bin /payload.bin
CMD ["sh", "-c", "date -u +%Y-%m-%dT%H:%M:%SZ"]
`)
	if err := os.WriteFile(filepath.Join(tmpdir, "Dockerfile"), dockerfile, 0o644); err != nil {
		return "", err
	}
	if err := writeRandomPayload(filepath.Join(tmpdir, "payload.bin"), int64(cfg.ImageSizeMiB)*1024*1024); err != nil {
		return "", err
	}

	// --metadata-file makes buildx write the pushed manifest's digest to JSON
	// so we can return a digest-pinned ref. The pod spec then references the
	// image by @sha256:... instead of :tag, which makes containerd skip the
	// tag→digest resolution roundtrip at the registry. With a tag in the spec
	// every node would issue a /v2/<repo>/manifests/<tag> request that
	// Gantry deliberately falls through to origin (F9), inflating the
	// proxy-side request count. We don't want F9 noise in the F1 measurement.
	//
	// --output type=image,push=true is the explicit form of --push that lets
	// us pass `oci-mediatypes=true` and override the default behaviour. The
	// key part is that we don't pass --provenance / --sbom (default off when
	// using `type=image`), so buildx pushes a single platform-specific
	// manifest, NOT an OCI image-index. The demo workload only ever runs on
	// linux/amd64 so a multi-arch index buys us nothing — and an index forces
	// containerd to traverse it via /v2/<repo>/blobs/<index-digest> on
	// kubelet pull, which most registries (ACR included) only serve at
	// /v2/<repo>/manifests/<digest>. The /blobs/ → 404 from origin then
	// poisoned the cluster's negative cache (failure_class=not_found) and
	// caused every node's cold-start cascade to short-circuit on §5.2 rule
	// 1, leading to ImagePullBackOff for the entire Job.
	metaPath := filepath.Join(tmpdir, "buildx-meta.json")
	_, err = runCommand(ctx, tmpdir, nil, "docker", "buildx", "build",
		"--platform", cfg.ImagePlatform,
		"-t", taggedImage,
		"--output", "type=image,push=true,oci-mediatypes=true",
		"--provenance=false",
		"--sbom=false",
		"--metadata-file", metaPath,
		".")
	if err != nil {
		return "", err
	}

	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return "", fmt.Errorf("read buildx metadata: %w", err)
	}
	var meta struct {
		Digest string `json:"containerimage.digest"`
	}
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return "", fmt.Errorf("parse buildx metadata: %w", err)
	}
	if meta.Digest == "" {
		return "", fmt.Errorf("buildx metadata did not include containerimage.digest (got %s)", string(metaBytes))
	}
	return fmt.Sprintf("%s/%s@%s", cfg.ACRLoginServer, cfg.WorkloadRepo, meta.Digest), nil
}

func InstallHostsToml(ctx context.Context, cfg LiveConfig, mode string) error {
	_, err := runCommand(ctx, cfg.RepoRoot, nil, filepath.Join(cfg.RepoRoot, "deploy/demo/infra/70-install-hosts-toml.sh"), cfg.EnvFile, mode)
	return err
}

// gantryHostsMode returns the hosts.toml mode the gantry phases install on
// nodes. Defaults to "gantry" (Gantry-first with proxy fallback). Override
// with DEMO_GANTRY_HOSTS_MODE=gantry-strict to attribute proxy traffic
// unambiguously to Gantry's origin client during cold-start measurement
// (no containerd-direct fallback path).
func gantryHostsMode() string {
	if m := os.Getenv("DEMO_GANTRY_HOSTS_MODE"); m != "" {
		return m
	}
	return "gantry"
}

func RunPullJob(ctx context.Context, cfg LiveConfig, phase PhaseName, image string) ([]time.Time, string, error) {
	jobName := fmt.Sprintf("gantry-demo-%s-%s", strings.ReplaceAll(string(phase), "_", "-"), time.Now().UTC().Format("20060102150405"))
	manifest := fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/name: gantry-demo-pull
    gantry.demo/run-label: %s
spec:
  completions: %d
  parallelism: %d
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        app.kubernetes.io/name: gantry-demo-pull
        gantry.demo/run-label: %s
    spec:
      restartPolicy: Never
      containers:
        - name: pull
          image: %s
          imagePullPolicy: Always
`, jobName, cfg.WorkloadNamespace, phase, cfg.NodeCount, cfg.NodeCount, phase, image)
	if _, err := runCommand(ctx, cfg.RepoRoot, []byte(manifest), "kubectl", "apply", "-f", "-"); err != nil {
		return nil, jobName, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, cfg.JobTimeout)
	defer cancel()
	if _, err := runCommand(waitCtx, cfg.RepoRoot, nil, "kubectl", "-n", cfg.WorkloadNamespace, "wait", "--for=condition=complete", "job/"+jobName, "--timeout", fmt.Sprintf("%ds", int(cfg.JobTimeout.Seconds()))); err != nil {
		return nil, jobName, err
	}

	podsOut, err := runCommand(ctx, cfg.RepoRoot, nil, "kubectl", "-n", cfg.WorkloadNamespace, "get", "pods", "-l", "job-name="+jobName, "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return nil, jobName, err
	}
	var stamps []time.Time
	for _, pod := range strings.Fields(podsOut) {
		logs, err := runCommand(ctx, cfg.RepoRoot, nil, "kubectl", "-n", cfg.WorkloadNamespace, "logs", "pod/"+pod)
		if err != nil {
			return nil, jobName, err
		}
		stamp, err := ParseFirstContainerTimestamp(logs)
		if err != nil {
			return nil, jobName, fmt.Errorf("pod %s logs: %w", pod, err)
		}
		stamps = append(stamps, stamp)
	}
	return stamps, jobName, nil
}

func runCommand(ctx context.Context, dir string, stdin []byte, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func writeRandomPayload(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 1024*1024)
	var written int64
	for written < size {
		chunk := buf
		remaining := size - written
		if remaining < int64(len(chunk)) {
			chunk = chunk[:remaining]
		}
		if _, err := rand.Read(chunk); err != nil {
			return err
		}
		n, err := file.Write(chunk)
		if err != nil {
			return err
		}
		written += int64(n)
	}
	return nil
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(wd, "deploy/demo")); err == nil {
				return wd, nil
			}
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			return "", errors.New("could not find repo root")
		}
		wd = parent
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func intEnvDefault(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func durationEnvDefault(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
