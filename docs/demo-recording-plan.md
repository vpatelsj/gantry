# Plan: Record Gantry origin-load demo (AKS 20-node, ~1 GiB)

Goal: produce a screen-recorded video that visibly proves the F1 invariant by running
the three already-wired harness phases (baseline → gantry cold → gantry warm) against
the AKS 20-node demo target, with the counting-proxy "Gantry ACR demo" Grafana
dashboard on screen as the headline visual.

User answers (locked):
- Cluster: AKS 20-node (RUNBOOK target)
- Format: screen-recorded video with Grafana panels
- State: "polish + record" — wiring is in place; need clean end-to-end run
- Image size: ~1 GiB headline (20 nodes × ~1 GiB ≈ ~20 GB baseline egress)
- Proxy/Gantry: not deployed — start from scratch

Out of scope:
- New Go code (harness already covers it via TestPhaseBaseline / Cold / Warm)
- The parallel `demo/acr/` scratch dir from the terminal — we use canonical `deploy/demo/`
- Modifying root Makefile (demo Makefile is strictly local per file header)
- AKS provisioning script changes
- Spark lab / kind paths

## Phases (each independently verifiable)

### Phase A — Provision (one-time, costs $)
1. Author `deploy/demo/infra/env.local` from `env.example` (resource group, ACR name, AKS name, region, GRAFANA_ADMIN_PASSWORD, demo namespace, node count = 20).
2. `make -C deploy/demo infra-provision` (10-provision-azure.sh — AKS + ACR + RG).
3. `make -C deploy/demo infra-images` (20-build-push-images.sh — proxy + gantry images into ACR).
4. `make -C deploy/demo infra-monitoring` (30-install-monitoring.sh — kube-prometheus-stack in `monitoring`).
5. `make -C deploy/demo infra-proxy` (40-deploy-proxy-spike.sh — proxy Deployment + Service in `gantry-demo`).
6. `make -C deploy/demo infra-grafana-dashboard` (80 — auto-import "Gantry ACR demo" dashboard).
7. **Gate**: `make -C deploy/demo infra-smoke` and `infra-node-reachability` MUST both pass before continuing.

### Phase B — Stable Grafana access for recording
The port-forward `kubectl -n monitoring port-forward svc/kps-grafana 3000:80` already
dropped twice during the prior baseline attempt. Recording needs continuous frames.
Pick one (recommendation in §Decisions):
- B1 (recommended): temporarily patch `kps-grafana` Service to `type: LoadBalancer`, capture EXTERNAL-IP, restore to ClusterIP at teardown.
- B2: run a port-forward retry wrapper (`while true; do kubectl ... ; sleep 1; done`) and accept brief blank frames.
- B3: `kubectl proxy` + `/api/v1/namespaces/monitoring/services/kps-grafana:80/proxy/` — works but auth flow is uglier on camera.
Validate by leaving the browser open for ~5 minutes with the dashboard's "Last 1h"
view; no reconnect message should appear.

### Phase C — Pre-record dry run (cheap, mandatory)
Run the entire three-phase sequence with a small image first to validate wiring on
camera path. ~16 MiB workload (`DEMO_IMAGE_SIZE_MB` unset / default):
1. `make -C deploy/demo harness-baseline` — confirm proxy delta and Job complete.
2. `make -C deploy/demo infra-gantry` — `41-deploy-gantry.sh` deploys gantry DS.
3. `kubectl rollout status ds/gantry -n gantry-system` — wait converged.
4. `make -C deploy/demo harness-gantry-cold` — confirm `p2p_peer_fetch_total{outcome="hit"}` > 0; capture the cold image ref printed at `pushed cold-start image ...`.
5. `kubectl -n gantry-demo delete ds/hosts-toml-installer` between phases.
6. `DEMO_WARM_IMAGE_REF=<from step 4> make -C deploy/demo harness-gantry-warm` — confirm the embedded assertion (zero digest traffic) does not fire.
7. If anything fails, fix before burning the 20 GB headline run.

Then reset for the headline recording:
- Delete the baseline/cold/warm jobs and hosts-toml-installer DS (RUNBOOK cleanup).
- Reset proxy Prometheus state by restarting the proxy Deployment (so cumulative-stat panels start near zero on camera).

### Phase D — Recording session
Layout (single 1920×1080 screen):
- Left half: terminal with three tabs / panes labeled "Baseline", "Cold", "Warm". Pre-export `DEMO_IMAGE_SIZE_MB=1024` and source `infra/.state.env` + `infra/env.local` in each.
- Right half: browser at Grafana → "Gantry ACR demo" → "Row 1 — Origin proxy (headline)" panels: requests/sec by path_class, bytes/sec by path_class, "Cumulative digest requests" stat, "Cumulative digest bytes" stat. Set time range to "Last 1 hour" with auto-refresh = 10s.
- Start screen recorder (`obs`, `simplescreenrecorder`, or `peek` for short cuts). Test 30 seconds; verify framerate and audio if narrating.
- Hard checklist before pressing record:
  - Kube context = AKS demo cluster (`kubectl config current-context`).
  - 20 nodes Ready (`kubectl get nodes`).
  - Proxy pod healthy (`kubectl -n gantry-demo get pods`).
  - Gantry DS deployed but **not yet** receiving traffic (baseline hosts.toml not yet installed).
  - Grafana panels show flat lines / zero stats.
  - Two extra terminal windows hidden: one with `kubectl -n gantry-demo logs -f deploy/acr-origin-proxy --tail=0`, one with `watch -n 2 'kubectl get jobs -n default -l gantry.demo/run-label'`.

### Phase E — Record three phases on camera (1 GiB image)
Take ≈ 8–15 min/phase (1 GiB image, 20 nodes). For each phase: narrate intent, run, watch panels rise, show numbers.

1. **Baseline** (`DEMO_IMAGE_SIZE_MB=1024 make -C deploy/demo harness-baseline`):
   - Narration: "Direct path through the counting proxy. Same ACR every phase. Watch the digest-request and bytes panels."
   - When the Job's pod-ready count reaches 20, freeze on the dashboard, then in the terminal scroll up to show `proxy delta: requests=… bytes_to_client=… by_path_class=…`.
   - Note the printed image ref **for the recording overlay only** (baseline image is not reused).
2. **Cleanup cut**: `kubectl -n gantry-demo delete ds/hosts-toml-installer`. Optionally restart proxy to reset cumulative stats for the next stat panel.
3. **Gantry cold-start** (`DEMO_IMAGE_SIZE_MB=1024 make -C deploy/demo harness-gantry-cold`):
   - Narration: "Same proxy. Same ACR. Same 20 nodes. Now containerd talks to Gantry first. F1 says origin sees ≤ 3× the digest count, not 20×."
   - **CRITICAL**: capture the line `pushed cold-start image gantrydemovapa.azurecr.io/gantry-demo-pull:gantry_cold-…` — export it into a shell variable in the third terminal pane immediately (`export DEMO_WARM_IMAGE_REF=…`).
   - Show terminal output: `proxy delta`, `gantry p2p_origin_pull_total delta`, `gantry p2p_peer_fetch_total{outcome="hit"}`.
4. **Cleanup cut**: `kubectl -n gantry-demo delete ds/hosts-toml-installer`.
5. **Gantry warm-cache** (`DEMO_WARM_IMAGE_REF=$DEMO_WARM_IMAGE_REF DEMO_ALLOW_CONTENT_PURGE=1 make -C deploy/demo harness-gantry-warm`):
   - Narration: "Same image, kubelet evicted by the cache-purge DS. Gantry serves from its own cache. Proxy must see zero digest traffic."
   - Highlight the embedded assertion: if digest requests > 0 the test fails — that is the F1 visible failure mode.
6. End on the dashboard with all three phases visible in the "Last 1 hour" time range — three distinct plateaus on the bytes-to-client panel tell the story.

### Phase F — Capture artifacts (offline)
1. Save `proxy /debug/summary` JSON to `deploy/demo/artifacts/recording-{baseline,gantry-cold,gantry-warm}.json` (via `kubectl get --raw …/services/acr-origin-proxy:/debug/summary` or the same path the harness uses).
2. Screenshot Grafana panels at the end of each phase (PNG into `deploy/demo/artifacts/`).
3. Stop screen recorder; trim head/tail; export.

### Phase G — Teardown (bound the cost)
1. Restore `kps-grafana` Service to ClusterIP if patched.
2. `kubectl delete namespace gantry-demo gantry-system`.
3. `kubectl delete --all jobs -n default -l app.kubernetes.io/name=gantry-demo-pull`.
4. `CONFIRM_DESTROY=yes deploy/demo/infra/90-destroy-azure.sh deploy/demo/infra/env.local`.

## Relevant files
- `deploy/demo/RUNBOOK.md` — operator script we are following.
- `deploy/demo/Makefile` — entry points: `infra-*`, `harness-baseline`, `harness-gantry-cold`, `harness-gantry-warm`.
- `deploy/demo/harness/phase_baseline_test.go` / `phase_gantry_cold_test.go` / `phase_gantry_warm_test.go` — what runs each phase; cold-phase logs the image ref needed for warm.
- `deploy/demo/harness/live.go` / `harness.go` / `wiring.go` — `LoadLiveConfig`, `FetchLiveProxySummary`, `BuildFreshWorkloadImage`, `RunPullJob`, `InstallHostsToml`, `WaitForGantryRollout`, `FetchGantryMetric`.
- `deploy/demo/infra/env.example` → `env.local` — credentials & cluster identity, NEVER commit.
- `deploy/demo/infra/{10..90}-*.sh` — driven by Makefile `infra-*` targets.
- `deploy/demo/hosts.toml.baseline.template` / `hosts.toml.gantry.template` — installer DS payload.
- `deploy/demo/grafana-dashboard.json` — "Gantry ACR demo" panels (Row 1 is the headline).
- `deploy/demo/cache-purge-daemonset.yaml` — warm-phase prerequisite.
- `deploy/demo/configmap.gantry-demo.yaml` — applied by `infra-gantry`; points gantry at the proxy.
- `deploy/demo/acr-origin-proxy/main.go` — `/debug/summary` schema (path_class buckets: blob, manifest_by_digest, manifest_by_tag, ping, other).

## Verification (specific, not generic)
1. `kubectl config current-context` resolves to the AKS demo cluster (NOT the Spark lab).
2. `kubectl get nodes -l agentpool` shows 20 Ready nodes.
3. `make -C deploy/demo infra-smoke` exits 0 (auth: ping, manifest, blob).
4. `make -C deploy/demo infra-node-reachability` exits 0 (AKS nodes can curl proxy ClusterIP).
5. Dry-run baseline `proxy delta.by_path_class["blob"].Requests` ≈ 20 × digest_count.
6. Dry-run cold-start: `p2p_peer_fetch_total{outcome="hit"}` > 0 AND `p2p_origin_pull_total` delta ≤ 3 × digest_count.
7. Dry-run warm: `TestPhaseGantryWarm` does NOT fail with "warm cache leaked digest traffic to proxy".
8. Grafana page survives 5 continuous minutes with no reconnect banner.
9. Recorded video file is playable end-to-end; the three plateaus on bytes panel are visually distinct (baseline ≫ cold > 0; warm ≈ 0).
10. Captured `recording-*.json` files contain non-zero `RequestsCompleted` deltas matching what was on camera.

## Decisions
- Demo lives entirely under `deploy/demo/`; no root-Makefile or core-code changes.
- Use `deploy/demo/` (not the unindexed `demo/acr/` scratch dir from the prior terminal). If `demo/acr/` contained drift, we ignore it for the recording.
- Mandatory dry run with the default ~16 MiB image before the 1 GiB headline to keep one ~20 GB ACR-egress run on tape.
- Grafana stability: patch service to LoadBalancer for the recording window (B1). Note: this exposes Grafana publicly briefly — only do it during recording and restore at teardown (Phase G step 1).
- Reset proxy Prometheus state (restart proxy Deployment) between dry run and headline recording so cumulative-stat panels start clean.
- Capture `DEMO_WARM_IMAGE_REF` manually from cold-phase stdout — there is no shared state file between cold and warm phases.

## Further considerations
1. Public LoadBalancer exposure for Grafana during recording: acceptable risk?
   - A) Patch to LoadBalancer, password-only access, restore at teardown (recommended for clean recording).
   - B) Keep port-forward + retry wrapper — accept occasional gaps.
   - C) Pre-record the dashboard separately via headless screenshot and overlay numbers — most production, but more editing.
2. Image size knob: 1 GiB headline matches the published RUNBOOK numbers but costs ~20 GB ACR egress on the baseline run alone.
   - A) Keep 1 GiB (recommended — headline numbers).
   - B) Drop to 256 MiB (~5 GB baseline, still dramatic, 4× faster).
   - C) Both: record 256 MiB first as the takeaway video, run 1 GiB once for the JSON artifacts only.
3. Should narration be live (mic during recording) or post-recorded voiceover?
   - A) Live narration — single take, less polish (recommended for engineering audience).
   - B) Silent recording, voiceover later — more polish, more editing.
