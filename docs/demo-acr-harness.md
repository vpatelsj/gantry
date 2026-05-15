# Demo Harness — Reducing ACR Pull Load with Gantry

Plan for a recorded-video demo showing that **without gantry, an Azure Container
Registry serves a large volume of pull / layer / manifest operations** (and may
be throttled) when many pods pull the same image, and **with gantry deployed,
ACR pull load drops by an order of magnitude** (and any throttling disappears
as a bonus).

> **Framing note**: throttling is a *bonus* signal, not the headline. Basic SKU
> throttle ceilings are not contractually published, so do not promise 429s in
> the recording. The headline is **pull-event reduction** and **egress-byte
> reduction**, both of which are deterministic.

## Locked-in decisions

- **Target**: AKS + real ACR only (no kind variant).
- **Provisioning**: Bash + `az` CLI scripts.
- **Scale**: ~20 AKS worker nodes, **Basic SKU** ACR (tightest documented limits
  → best chance of incidentally tripping 429s, and lowest demo cost).
- **Audience**: Recorded video / async walkthrough.
- **Observability** (all three):
  - ACR-side: Azure Monitor metrics — `TotalPullCount`, `SuccessfulPullCount`
    (coarse image-pull counters; `ThrottlingErrors` is **not** a documented
    `Microsoft.ContainerRegistry/registries` metric, do not query it).
  - ACR-side: Log Analytics `ContainerRegistryRepositoryEvents` — both a
    429-only query (throttling proof, if any) and a grouped-by
    `OperationName` / `ResultType` query (the headline "ACR served X events"
    number, including layer / manifest / blob operations).
  - Cluster-side: gantry `/metrics` via in-cluster Prometheus + Grafana.

## Key codebase facts that shape the plan

- Gantry is registry-agnostic — no ACR-specific code. The demo treats ACR as a
  generic upstream.
- `deploy/daemonset.yaml` (lines 220–249) documents three options for the
  containerd socket permission caveat: (a) fsGroup, (b) relax socket perms,
  (c) clear `containerd_socket` in the ConfigMap to disable cdsub. **The demo
  takes path (c)** — cleanest, keeps gantry at UID 65532 (production-like).
  cdsub being NoOp is fine for this demo because gantry's cache is populated by
  its own pulls and peer transfers — all that's needed to demonstrate F1.
- Verified: `deploy/daemonset.yaml` has only one initContainer
  (`chown-hostpaths`) — it does NOT touch `hosts.toml`. A separate installer
  DaemonSet is necessary.
- `deploy/hosts.toml.template` must be substituted (`${REGISTRY_SERVER}` →
  `https://<acr-name>.azurecr.io`) and written to
  `/etc/containerd/certs.d/<acr-name>.azurecr.io/hosts.toml` on every node.
  AKS containerd auto-reloads `certs.d/`.
- Credentials are read eagerly: `upstream_registries[].credentials_path` → file
  with `username:password`. Missing file = crashloop. For the demo, use the ACR
  admin user (call out as demo-only).
- **Auth paths do not fight**: kubelet uses the AKS-attached AcrPull MSI to pull
  (1) the gantry image and (2) the demo workload image. Gantry's *origin client*
  uses the admin-user Secret to pull from ACR on cache miss. Two different
  consumers on two different code paths — no conflict.
- Pull-metric arithmetic:
  `p2p_origin_pull_started_total == success + (origin_failure + downstream_failure) + in_flight`.
  The expected with-gantry origin-pull count is bounded but **not exactly N**:
  HRW Top-K=3 plus informer divergence during a fresh rollout means up to ~3
  nodes may briefly believe they're rank-0 for the same digest, so the
  realistic floor is `digest_count`–`3 × digest_count` origin pulls per image.
- **Use `digest_count`, not `layer_count`**, for all gantry-counter
  expectations. A real image pull resolves the manifest, the config blob, and
  every layer — gantry sees all of them as distinct content-addressed
  fetches. For a single-platform 30-layer image,
  `digest_count = 1 (manifest) + 1 (config) + 30 (layers) ≈ 32`. Cold-start
  origin pulls expected in **[digest_count, 3 × digest_count]** ≈ 32–96. Warm
  cache hits expected ≈ `node_count × digest_count` ≈ 640.
- Gantry itself must be pulled before it can serve. Push the gantry image to
  the same ACR; kubelet pulls it via the AKS-attached AcrPull MSI.

## Triggering ACR throttling reliably

Basic SKU has the tightest documented limits, but Microsoft does not publish
fixed thresholds. Multi-pronged approach:

- **Big image** (~600 MB–1 GB compressed) so the bandwidth limit bites.
- **Many layers** (~30 thin layers, each ~10–20 MB, produced via a `RUN` loop)
  so the ReadOps limit bites.
- **Unique tag per run AND unique layer digests** (`demo:run-<timestamp>` plus
  `RUN_ID`-seeded random bytes inside each layer). A unique tag alone is
  insufficient: containerd caches by content digest, so if two builds produce
  identical layer digests, containerd will reuse local blobs and bypass
  gantry entirely. The image generator (Phase 2) must mix `RUN_ID` into every
  layer's payload, and Phase 6 must **assert zero overlap between the prior
  run's layer-digest set and the new run's layer-digest set** before applying
  the workload.
- **20 concurrent nodes** via a Job with `parallelism: 20`, `completions: 20`,
  `podAntiAffinity` requiring different nodes (one pod per node).
- **Per-pod `imagePullPolicy: Always`** to ensure manifest re-resolution.
- Backstop narrative if 429s don't materialize: the headline is **repository
  event reduction** from `ContainerRegistryRepositoryEvents` (manifest +
  config + per-layer GET operations), not the coarse Azure Monitor
  `TotalPullCount` counter. Baseline event count ≈
  `node_count × digest_count` operations (≈20×32 = 640 for a 30-layer
  image); with-gantry event count ≈ `digest_count` to `3 × digest_count`
  (32–96); ratio is therefore **~7–20× fewer ACR operations**, with egress
  bytes dropping by a similar factor. `TotalPullCount` itself is an
  image-level pull counter (≈ `node_count` baseline, ≈ 1–3 with gantry) —
  useful as a coarse sanity check, not the headline. Frame the speedup as
  **"faster, often dramatically faster"** with the actual measured number
  inserted in the recording — don't pre-commit to "3–10×" because Basic
  SKU throttle ceilings can produce much larger ratios.

## Phased plan

### Phase 1 — Provisioning (one-time per demo session)

1. `00-prereqs.sh` — checks: `az`, `kubectl`, `helm`, `jq`, `envsubst`,
   `docker buildx`; `az account show` not failing.
2. `10-provision.sh` — `az group create`;
   `az acr create --sku Basic --admin-enabled true`;
   `az monitor log-analytics workspace create`;
   `az aks create --node-count 20 --node-vm-size Standard_D4s_v5 --attach-acr <acr> --workspace-resource-id <law>`;
   `az aks get-credentials`; enable ACR diagnostic settings
   (Log Analytics destination, `ContainerRegistryRepositoryEvents` category).
3. `10b-set-budget-alert.sh` — creates a `$50/day` budget alert on the RG so an
   overnight-forgotten cluster auto-pages someone.

### Phase 2 — Image preparation (re-run between scenarios to defeat cache)

4. `20-push-demo-image.sh` — generates a Dockerfile with ~30 layers via
   deterministic-content `RUN` lines (each ~20 MB random-but-reproducible blob),
   tags `<acr>.azurecr.io/demo:run-${RUN_ID}`, pushes with `docker buildx --push`.
   Writes `RUN_ID` to `.run-id` for later scripts.

### Phase 3 — Observability stack

5. `30-install-prom.sh` — `helm install kube-prometheus-stack` with values:
   ServiceMonitor selector matching gantry's `app=gantry`, persistent volume
   disabled (demo), Grafana exposed via LoadBalancer or `kubectl port-forward`.
6. Apply `manifests/grafana-dashboard-configmap.yaml` containing pre-built
   dashboard JSON (panels: origin-pull rate, peer-hit rate, cache-hit rate,
   in-flight gauge, DHT health, gantry CPU/mem) sidecar-loaded into Grafana.

### Phase 4 — Baseline run (no gantry)

7. `40-baseline.sh` — applies `manifests/workload-job.yaml.tmpl`
   (envsubst with current `RUN_ID`). Job spec: `parallelism: 20`,
   `completions: 20`, `imagePullPolicy: Always`, podAntiAffinity-by-hostname.
   Container's first action is
   `echo "POD_READY $(date -u +%FT%T.%NZ)"` — timestamp captured *inside the
   pod* right when the image-pull-to-container-start transition completes, so
   log scrape gives precise per-pod ready-times without kubectl polling races —
   then sleeps 30s.
8. `41-record-baseline.sh` — polls pod statuses; once Job completes, **sleeps
   ≥5 min to let Azure Monitor + Log Analytics ingest** (documented 2–5 min
   lag). Then: dumps pod logs to extract `POD_READY` timestamps; runs both KQL
   queries (the 429-only query AND `acr-total-events.kql` for all repository
   events grouped by `Repository` / `Tag` / `OperationName` / `ResultType` —
   this is the headline "ACR served X operations" number); pulls Azure
   Monitor metrics `TotalPullCount` and `SuccessfulPullCount` for the ACR
   resource over the run window (coarse image-pull counters only —
   `ThrottlingErrors` is **not** an ACR metric, so 429 evidence comes from
   the KQL path, not Azure Monitor). Writes `artifacts/baseline-<RUN_ID>.json`.

### Phase 5 — Gantry deployment

> **Ordering is critical.** `hosts.toml` must be installed only **after** the
> gantry DaemonSet is fully Ready on every node. If `hosts.toml` lands first,
> kubelet's pull of the gantry image itself can be redirected to
> `127.0.0.1:5000` on a node where gantry isn't running yet → self-bootstrap
> deadlock. Phase 5 is therefore split into 51a (deploy gantry, wait Ready)
> and 51b (install hosts.toml, then preflight).

9. `50-build-gantry.sh` — runs
   `deploy/build.sh -p linux/amd64 -r <acr>.azurecr.io/gantry --push -t demo`.
   **Local push auth**: `az acr login` as the current Azure CLI user (which
   must hold `AcrPush` or Contributor on the registry), or `docker login` with
   the ACR admin credentials for demo simplicity. The AKS-attached AcrPull MSI
   is *pull-only* and is **not** used for the local push — that's a separate
   identity on a separate code path. The MSI is only used later by kubelet
   on each node to pull the gantry image and the demo workload image.
10. `51a-deploy-gantry.sh` — applies a `manifests/overlay/` Kustomize directory
    layered on `deploy/`. Overlay does:
    - Patches DaemonSet image to `<acr>.azurecr.io/gantry:demo`.
    - Patches ConfigMap with
      `upstream_registries: [{name: <acr>.azurecr.io, endpoint: https://<acr>.azurecr.io, credentials_path: /etc/gantry/registry/<acr>.azurecr.io}]`.
    - Patches ConfigMap with `containerd_socket: ""` to disable cdsub
      (path-(c) approach from the daemonset.yaml caveat) — keeps gantry at
      UID 65532, no socket-permission gymnastics.
    - Creates a Secret from `az acr credential show` output (admin-enabled).
      Mounted at `/etc/gantry/registry/<acr>.azurecr.io`.
    - Optional ServiceMonitor for Prometheus discovery.
    - Waits for DaemonSet rollout + `/readyz` green on every node **before
      returning**. Does NOT touch `/etc/containerd/certs.d`.
11. `51b-install-hosts-toml.sh` — only runs after 51a returns clean.
    - Applies `manifests/hosts-toml-installer.yaml`, a tiny DaemonSet whose
      initContainer writes the substituted `hosts.toml` to
      `/etc/containerd/certs.d/<acr>.azurecr.io/hosts.toml` via hostPath mount
      of `/etc/containerd/certs.d`, then the main container `sleep infinity`
      (keeps the file alive if the pod restarts). containerd auto-reloads, no
      node restart.
    - Verifies `hosts.toml` exists on every node (`kubectl exec` into each
      installer pod, `cat` the file).
    - **Single-node pull-through preflight** (mandatory before fleet test):
      pick one node; `kubectl debug node/<n>` (or a privileged toolbox pod)
      and run `crictl pull <acr>.azurecr.io/demo:$PREFLIGHT_TAG`. The
      preflight tag **must** be either the current baseline `RUN_ID` or a
      dedicated `preflight-<timestamp>` `RUN_ID` minted just for this check
      — it must **never** be the upcoming with-gantry cold-start `RUN_ID`,
      because pulling that tag here would warm gantry's cache on one node
      and contaminate the cold-start measurement. Then assert,
      in priority order:
      1. **Primary (must pass)**: gantry on that node observed manifest +
         config + layer activity — `p2p_origin_pull_started_total` and/or
         `p2p_cache_hit_total` deltas non-zero, scoped to that pod. This is
         the source of truth that gantry is on the pull path.
      2. **Best-effort**: `ContainerRegistryRepositoryEvents` for the
         preflight window shows requests with `Identity` matching the ACR
         admin user and/or a gantry-like `UserAgent`, and kubelet/containerd
         MSI-identified pulls are not dominant. ACR log field availability
         varies, so treat this as corroborating, not gating.
      3. **Sanity**: the `crictl pull` itself succeeded.
      Without this preflight, a misconfigured `hosts.toml` (wrong
      capabilities, wrong host) silently bypasses gantry and the 20-node
      test fails confusingly.
12. **cdsub-disabled metric audit (pre-recording dry run)** — before the actual
    baseline+gantry recording, deploy gantry, open Grafana, and confirm every
    panel renders data. The ConfigMap patch sets `containerd_socket: ""`,
    putting cdsub in NoOp mode — any panel whose source metric is fed
    *exclusively* by cdsub-published events (filtered on `source="cdsub"` or
    driven by `p2p_cdsub_*` series) will show "No Data" during the recording.
    Planned panels (origin-pull rate, peer-hit rate, cache-hit rate, in-flight
    gauge, DHT health, gantry CPU/mem) are all mirror/transfer/cache/discovery-
    driven and should render — but verify before going live. If a panel is
    dark, swap it for an equivalent or annotate it "cdsub disabled for demo".

### Phase 6 — With-gantry run (gantry as coordinator)

13. `60-with-gantry.sh` — re-runs `20-push-demo-image.sh` to mint a **fresh**
    `RUN_ID`; then asserts:
    - The new manifest digest exists in ACR and differs from the baseline
      manifest digest (`az acr manifest list-metadata ... --orderby time_desc`).
    - **The new image's layer-digest set has zero overlap with the prior run's
      layer-digest set** (`az acr manifest show-metadata` or `oras manifest
      fetch` → extract `.layers[].digest` → set-diff against
      `.last-layers-baseline`). Without this assertion, identical layer
      digests across runs would let containerd serve from its own content
      store and bypass gantry entirely.
    - Fails loud on either mismatch — these are the linchpins of the demo.
    Writes the new manifest digest to `.last-digest-with-gantry` and the
    layer-digest set to `.last-layers-with-gantry`. Then deletes the previous
    baseline Job and applies the templated workload Job with the new `RUN_ID`.
14. `61-record-with-gantry.sh` — same wait-then-capture pattern as baseline
    (≥5 min Azure Monitor ingest lag, then KQL queries + ACR metrics).
    Additionally:
    - Scrapes Prometheus before+after deltas of `p2p_origin_pull_started_total`,
      `p2p_origin_pull_success_total`, `p2p_peer_hit_total`,
      `p2p_cache_hit_total`.
    - Scrapes cAdvisor for gantry's own footprint:
      `rate(container_cpu_usage_seconds_total{pod=~"gantry-.*"}[1m])` and
      `max_over_time(container_memory_working_set_bytes{pod=~"gantry-.*"}[10m])`.
      Pre-empts the "what does it cost" question.
    - Saves `artifacts/with-gantry-<RUN_ID>.json`.
15. `61b-dashboard-replay.sh` — re-pulls Azure Monitor + Log Analytics metrics
    at +10 min post-run for the clean recording cut.

### Phase 6b — Cache-only run (gantry as cache)

16. `62-cached-rerun.sh`:
    - **Pre-flight on one node (mandatory before fleet-wide Job)**:
      `kubectl exec` into a cleanup-Job pod on one node and run the cleanup
      commands manually, then assert
      `ctr -n k8s.io content ls | grep <demo-digest>` returns **nothing** and
      `crictl images | grep demo` returns **nothing**. `crictl rmi` only
      removes the image *reference*; containerd's content store keeps the
      blobs until GC runs. If blob digests persist, a subsequent
      `imagePullPolicy: Always` re-pull will hit them locally and bypass
      gantry entirely — Phase 6b would silently look like "containerd as
      cache" and gantry counters would stay flat. The pre-flight catches this
      before the recording.
    - Applies `manifests/cleanup-containerd-cache-job.yaml`: a one-shot Job
      (`parallelism: 20`, podAntiAffinity, `securityContext.privileged: true`,
      hostPID + hostPath mounts of `/run/containerd/containerd.sock` and
      `/var/lib/containerd`). Job image contains `ctr` (lower-level containerd
      CLI, since `crictl` doesn't expose content-store ops). Sequence per node:
      1. `ctr -n k8s.io images rm <acr>.azurecr.io/demo:$RUN_ID` — removes
         image reference.
      2. `ctr -n k8s.io leases ls` → delete any lease holding the digest
         (otherwise GC won't reclaim).
      3. `ctr -n k8s.io content prune references` — GC now-unreferenced blobs.
      4. Assertion: `ctr -n k8s.io content ls | grep <digest>` returns empty
         (Job exits non-zero if not — fails loud).
    - Waits for cleanup Job completion. Then re-applies the workload Job with
      the **same** `RUN_ID` (no new push). containerd cache miss +
      content-store miss → must route through gantry → gantry's local cache
      HIT on every node → near-zero origin, near-zero peer.
17. `63-record-cached.sh` — Prometheus scrape with **strict expectations as
    recording gates, plus mandatory raw-evidence capture for forensics**:
    - Expected: `p2p_cache_hit_total` delta ≈ `node_count × digest_count`;
      `p2p_origin_pull_started_total` delta **exactly 0**; `p2p_peer_hit_total`
      delta **exactly 0**.
    - Non-zero on either of the last two means one of: (a) containerd cleanup
      failed and a node served from its own content store, (b) gantry's cache
      evicted under load, (c) tag/resolve routed differently than expected,
      or (d) a real gantry bug. To distinguish, **always** save the raw
      evidence regardless of whether the gates pass:
      - `ctr -n k8s.io content ls` and `ctr -n k8s.io images ls` from every
        node, before *and* after the workload run.
      - Gantry per-node cache inventory (HTTP endpoint or on-disk listing).
      - Full Prometheus delta dump for `p2p_*` series.
      - containerd journald logs from one representative node for the run
        window.
    - Same Azure Monitor + KQL captures (will show essentially no ACR
      activity). Saves `artifacts/cached-<RUN_ID>.json` plus
      `artifacts/cached-<RUN_ID>-forensics/`.

### Phase 7 — Comparison + cleanup

18. `70-compare.sh` — prints a **three-column** side-by-side table with
    explicit headers:

    | metric | `baseline (no gantry)` | `gantry cold-start (coordinator path)` | `gantry warm (cache path)` |
    | --- | --- | --- | --- |
    | total pod-ready time | from in-pod `POD_READY` logs | … | … |
    | ACR `TotalPullCount` (Azure Monitor, coarse) | ≈ `node_count` | ≈ 1–3 | ≈ 0 |
    | ACR repository events (Log Analytics, headline) | ≈ `node_count × digest_count` | `digest_count` – `3 × digest_count` | ≈ 0 |
    | ACR 429 events (Log Analytics) | ≥ 0 (bonus if non-zero) | ≈ 0 | ≈ 0 |
    | `p2p_origin_pull_started_total` delta | n/a | `digest_count` – `3 × digest_count` | 0 |
    | `p2p_peer_hit_total` delta | n/a | ≈ `(node_count - 1) × digest_count` | 0 |
    | `p2p_cache_hit_total` delta | n/a | … | ≈ `node_count × digest_count` |
    | gantry CPU/mem footprint | n/a | … | … |

    For 20 nodes and a 30-layer single-platform image (`digest_count ≈ 32`):
    cold-start origin pulls ≈ 32–96; peer hits ≈ 19 × 32 = 608; warm cache
    hits ≈ 20 × 32 = 640.

    Same labels reused in the chart and recording-checklist narration so a
    viewer immediately reads col-2-vs-col-3 as "two distinct jobs gantry does"
    rather than "gantry got faster the second time it ran".
19. `99-cleanup.sh` — first prints expected dollar cost from a Cost Management
    query for the RG, then `read -p "Type DELETE to confirm: " CONFIRM` and
    aborts unless `CONFIRM == "DELETE"`. Then `az group delete --yes --no-wait`.

## Relevant files (to create)

- `demo/acr/README.md` — run order, narrative, screen-recording checklist,
  cost warnings.
- `demo/acr/env.example.sh` — `SUBSCRIPTION_ID`, `LOCATION`, `RG_NAME`,
  `ACR_NAME`, `AKS_NAME`, `LAW_NAME`, `NODE_COUNT=20`,
  `NODE_VM_SIZE=Standard_D4s_v5`, `DAILY_BUDGET_USD=50`.
- `demo/acr/00-prereqs.sh` … `99-cleanup.sh` — phased scripts above (including
  `10b-set-budget-alert.sh`, `61b-dashboard-replay.sh`, `62-cached-rerun.sh`,
  `63-record-cached.sh`).
- `demo/acr/manifests/workload-job.yaml.tmpl` — 20-pod Job with
  podAntiAffinity, `imagePullPolicy: Always`, `${IMAGE_REF}` placeholder,
  container echoes `POD_READY` timestamp on start.
- `demo/acr/manifests/hosts-toml-installer.yaml` — DaemonSet that writes
  `hosts.toml` per node.
- `demo/acr/manifests/cleanup-containerd-cache-job.yaml` — privileged Job
  that `ctr images rm` + lease drop + `content prune references` on every node
  before Phase 6b.
- `demo/acr/manifests/overlay/kustomization.yaml` + patch files — layers on
  `deploy/` (image swap, ConfigMap `upstream_registries` +
  `containerd_socket: ""`, Secret from ACR admin creds).
- `demo/acr/manifests/grafana-dashboard-configmap.yaml` — pre-built dashboard
  panels (origin-pull / peer-hit / cache-hit / in-flight / DHT health /
  gantry-self CPU+mem).
- `demo/acr/queries/acr-throttling.kql` — Log Analytics query for 429-only
  events.
- `demo/acr/queries/acr-total-events.kql` — all
  `ContainerRegistryRepositoryEvents` grouped by `Repository`, `Tag`,
  `OperationName`, `ResultType` over the run window. This produces the
  headline "ACR served X repository events (operations) baseline vs Y vs Z"
  number — more compelling than the coarse `TotalPullCount` image-level
  counter alone.
- `demo/acr/queries/prom-queries.txt` — PromQL snippets used by `61-*.sh` and
  `63-*.sh` (deltas + cAdvisor footprint).
- `demo/acr/imagegen/Dockerfile.tmpl` — 30-layer image generator.
- `demo/acr/docs/recording-checklist.md` — what to capture on screen,
  including ingest-lag-aware cut points.

## Reused / referenced (no changes)

- `deploy/daemonset.yaml` — base, patched via Kustomize overlay.
- `deploy/configmap.yaml` — base, patched.
- `deploy/hosts.toml.template` — read by `51-*.sh`, substituted into
  `manifests/hosts-toml-installer.yaml`.
- `deploy/serviceaccount.yaml` — applied unchanged.
- `deploy/Dockerfile` + `deploy/build.sh` — used to build the gantry image
  into ACR.

## Verification

1. **Phase 1**: `kubectl get nodes` shows 20 Ready nodes;
   `az acr show -n <acr>` shows `sku.name == Basic`; ACR diagnostic settings
   list includes the Log Analytics destination; daily budget alert exists on
   the RG.
2. **Phase 2**: `az acr repository show-tags --name <acr> --repository demo`
   lists the current `RUN_ID`; manifest digest changes between runs (asserted
   in script).
3. **Phase 3**: `kubectl get servicemonitor` shows the gantry ServiceMonitor;
   the Grafana dashboard loads with no panel errors.
4. **Phase 4 (baseline)**: Job completes (may take minutes if throttling
   bites); `41-record-baseline.sh` reports a high
   `ContainerRegistryRepositoryEvents` count (≈ `node_count × digest_count`
   layer/manifest operations) — the headline pull-load number we want to
   reduce. 429s in the KQL throttling query are a bonus, not required.
5. **Phase 5**: All 20 gantry pods reach `/readyz` green; gantry logs show
   cdsub initialized in NoOp mode (because `containerd_socket: ""`) — sanity
   check that the path-(c) approach took; `kubectl exec` any pod and
   `curl /metrics:9095` shows `p2p_dht_health_score >= 0.7`; `hosts.toml`
   present under `/etc/containerd/certs.d/<acr>.azurecr.io/` on each node
   (`kubectl exec` into the installer DaemonSet to `cat` it).
6. **Phase 6 (with gantry, coordinator case)**: `60-with-gantry.sh` asserts
   the new manifest digest exists in ACR **and** that the layer-digest set
   has zero overlap with the prior run before applying the workload; Job
   completes faster than baseline (record the actual ratio);
   `p2p_origin_pull_started_total` delta in **[`digest_count`,
   `3 × digest_count`]** (≈32–96 for a 30-layer image); `p2p_peer_hit_total`
   delta dominates (≈ `(node_count - 1) × digest_count`); KQL throttling
   query returns no 429s; KQL total-events query shows ~7–20× reduction in
   ACR repository events vs baseline.
7. **Phase 6b (cached)**: pre-flight on one node confirms `ctr content ls` is
   empty for the demo digest after cleanup; full cleanup Job removes the image
   from all 20 containerd content stores; workload re-applied with same
   `RUN_ID` completes very fast; `p2p_cache_hit_total` delta ≈
   `node_count × digest_count`; `p2p_origin_pull_started_total` delta
   **exactly 0**; `p2p_peer_hit_total` delta **exactly 0**; ACR records
   essentially no activity.
8. **Phase 7**: Three-column comparison table with explicit
   `baseline (no gantry)` / `gantry cold-start (coordinator path)` /
   `gantry warm (cache path)` headers — same labels in chart legends and
   narration. Order-of-magnitude reduction baseline → col-2, additional large
   drop col-2 → col-3. Gantry's own CPU/mem footprint shown alongside to
   pre-empt the cost question.

## Implementation order (revised)

Two distinct orderings: a developer-validation order for building the
harness, and the recording order that the actual demo run follows. **They
are not the same** — in particular, the baseline run must execute against a
cluster with no `hosts.toml` redirect on any node, otherwise containerd may
route baseline pulls through gantry and contaminate the comparison.

### Developer validation order (build/test the harness)

1. Phase 1 — provision AKS + ACR + diagnostic settings + budget alert.
2. Phase 2 — build/push demo image **and verify layer-digest uniqueness**
   between two consecutive `RUN_ID`s before proceeding.
3. **Phase 3 (metrics substrate only)** — install kube-prometheus-stack so
   Prometheus is scraping gantry **before** any cold-start or warm-cache
   dry run; `61-record-with-gantry.sh` and `63-record-cached.sh` depend on
   before/after Prometheus deltas, so the substrate must exist first.
   Grafana dashboard polish can be deferred to step 8 below.
4. Phase 5 step 51a — deploy gantry, wait Ready on every node.
5. Phase 5 step 51b — install `hosts.toml`, then run the **single-node
   pull-through preflight**. Do not proceed past this step until the
   preflight confirms gantry is actually on the pull path.
6. Phase 6 dry run — confirm the cold-start path produces the expected
   gantry-counter deltas end-to-end.
7. Phase 6b dry run — confirm cleanup Job + warm-cache rerun behaves.
8. Phase 3 (dashboards) — Grafana dashboard polish on top of the
   already-running Prometheus.
9. **Reset to baseline-clean state** before the recording: delete the
   `hosts-toml-installer` DaemonSet, then on every node remove
   `/etc/containerd/certs.d/<acr>.azurecr.io/hosts.toml` (a one-shot
   `manifests/hosts-toml-uninstaller.yaml` Job mirrors the installer's
   hostPath mount and `rm`s the file; containerd auto-reloads). Verify
   absence on every node before recording.

### Recording order (the actual demo)

1. **Baseline run** — confirm `hosts.toml` is absent on every node
   (`kubectl exec` into a toolbox DaemonSet, `test ! -f`); then run
   Phase 4 (`40-baseline.sh` + `41-record-baseline.sh`).
2. **Deploy gantry** — Phase 5 step 51a; wait Ready.
3. **Install `hosts.toml`** — Phase 5 step 51b.
4. **Single-node pull-through preflight** — same as 51b's preflight; gates
   the rest of the recording.
5. **Gantry cold-start run** — Phase 6 (`60-with-gantry.sh` +
   `61-record-with-gantry.sh` + `61b-dashboard-replay.sh`).
6. **Warm-cache run** — Phase 6b (`62-cached-rerun.sh` +
   `63-record-cached.sh`).
7. **Comparison + cleanup** — Phase 7.

## Decisions / scope

- **In scope**: AKS + Basic ACR; bash + `az` CLI; 20 nodes; recorded-video-
  friendly outputs; three-scenario comparison (baseline / coordinator /
  cache); both ACR-side and cluster-side observability; gantry's own footprint
  capture.
- **Out of scope**: Terraform/Bicep; multi-region; geo-replicated ACR;
  NetworkPolicy hardening; chaos scenarios (NF5 takeover, eviction, cooldown);
  Workload Identity for gantry → ACR (uses ACR admin password for demo
  simplicity); private endpoint ACR (public endpoint only for demo).
- **Security caveats called out in the demo README**:
  - ACR admin user enabled → for demo only; production should use Workload
    Identity + AcrPull MSI.
  - cdsub disabled (`containerd_socket: ""`) → demo doesn't exercise
    containerd-content-store secondary blob source. Production deployments
    wanting cdsub must use path (a) fsGroup or (b) socket-group surgery per
    `deploy/daemonset.yaml` (lines 220–249). Gantry stays at UID 65532 — no
    `runAsUser: 0` override.
  - Cleanup Job in Phase 6b uses `privileged: true` + hostPath of
    `/run/containerd/containerd.sock` — only acceptable because it's a demo
    throwaway cluster.
  - No NetworkPolicy applied; peer ports 5001/4001 open across the cluster.
- **Reset strategy**:
  - Baseline → with-gantry: **new image tag** (`RUN_ID` from epoch)
    invalidates both containerd's per-node cache and gantry's content-
    addressed cache.
  - With-gantry → cached: **same tag**, but the cleanup Job purges containerd's
    per-node cache; gantry's content-addressed cache stays warm, so the third
    run hits gantry's local cache on every node.
- **Cost guard**: 20× `Standard_D4s_v5` runs ~$3.84/hr.
  `10b-set-budget-alert.sh` sets a $50/day RG budget alert; `99-cleanup.sh`
  requires typed `DELETE` confirmation.

## Further considerations

1. **AKS node OS** (default Azure Linux v2 vs Ubuntu 22.04): leave default
   (`certs.d` enabled on both); record OS in `artifacts/` for traceability.
2. **Workload behavior**: keep Option A (`echo POD_READY <ts>` then sleep 30s)
   for demo clarity; mention B (tiny HTTP server) and C (CPU bench) as
   variations in the demo README.
3. **Recorder-friendly ingest lag**: both `41-record-baseline.sh` and
   `61-record-with-gantry.sh` block ≥5 min before scraping Azure Monitor /
   Log Analytics. A separate `61b-dashboard-replay.sh` re-pulls at +10 min for
   the recording cut.
