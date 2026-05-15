# Demo Harness — Reducing ACR Pull Load with Gantry

A bash + `az` CLI harness that records the headline gantry result on a
real cluster: with gantry deployed in front of a Basic-SKU Azure
Container Registry, the registry serves an order of magnitude fewer
pull / manifest / blob events when 20 nodes pull the same large image,
and any rate-limit throttling vanishes as a bonus.

The full design rationale lives in
[../../docs/demo-acr-harness.md](../../docs/demo-acr-harness.md). This
README is an operator guide.

> **Cost warning.** A 20-node `Standard_D4s_v5` AKS cluster + Basic-SKU
> ACR + Log Analytics workspace is on the order of **~$3.84/hr** of
> compute alone. `10b-set-budget-alert.sh` configures a $50/day budget
> alert. `99-cleanup.sh` requires you to type `DELETE` to confirm
> teardown of the resource group. Do not leave the cluster running
> overnight.

> **Demo-only security shortcuts.** The harness uses the ACR admin
> user (a long-lived password kept in a Kubernetes Secret) for gantry's
> origin client, runs a `privileged: true` cleanup Job to purge each
> node's containerd content store, and applies no NetworkPolicy. None
> of these are appropriate for production. See [`Security caveats`](#security-caveats)
> below and §"Decisions / scope" in
> [`docs/demo-acr-harness.md`](../../docs/demo-acr-harness.md) for what
> a production deployment should change.

## What the demo measures

Three scenarios are run back-to-back against the same 20-node cluster:

| scenario                                  | what gantry is doing                                     | expected ACR load                                       |
| ----------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------- |
| `baseline (no gantry)`                    | not deployed; kubelet pulls direct from ACR              | `node_count × digest_count` repository events (≈640)    |
| `gantry cold-start (coordinator path)`    | gantry deployed; one node fetches per digest, peers copy | `digest_count`–`3 × digest_count` events (≈32–96)       |
| `gantry warm (cache path)`                | gantry's cache is warm; containerd cache cleaned         | ≈ 0 ACR events                                          |

The headline number is the **Log Analytics
`ContainerRegistryRepositoryEvents` count** (per-operation, per-blob
granularity), not the coarse Azure Monitor `TotalPullCount`. ACR
throttling (HTTP 429 rows in the same KQL table) is a *bonus* signal
because Microsoft does not contractually publish Basic-SKU ceilings.

## Prerequisites

- Logged-in `az` CLI with rights to create resource groups and AKS.
- `kubectl`, `helm`, `jq`, `envsubst` (`gettext` package), `docker buildx`
  on `PATH`. `00-prereqs.sh` checks all of these.
- An email address for the budget alert.

## Run order

```bash
cd demo/acr
cp env.example.sh env.sh
$EDITOR env.sh                      # set SUBSCRIPTION_ID, ACR_NAME, BUDGET_ALERT_EMAIL
source env.sh

# Phase 1 — provision (one-time per session)
./00-prereqs.sh
./10-provision.sh
./10b-set-budget-alert.sh           # optional; skipped if BUDGET_ALERT_EMAIL is unset

# Phase 2 — build/push the first demo image (40-baseline.sh will mint
# additional fresh tags for each hammer iteration; this initial push
# only exists so 30-install-prom.sh has something to scrape against).
./20-push-demo-image.sh

# Phase 3 — observability stack
./30-install-prom.sh
kubectl apply -f manifests/grafana-dashboard-configmap.yaml

# --- Recording starts here ---

# Phase 4 — baseline (no gantry; loops the workload BASELINE_HAMMER_ITERATIONS
# times to drive enough load to risk Basic-SKU throttling). 41-record
# captures pull-event durations + a real-time containerd-journald 429 scan.
./40-baseline.sh
./41-record-baseline.sh

# Phase 5 — deploy gantry, then install hosts.toml, then preflight
./50-build-gantry.sh
./51a-deploy-gantry.sh
./51b-install-hosts-toml.sh        # includes the single-node pull-through preflight

# Phase 6 — gantry cold-start
./60-with-gantry.sh
./61-record-with-gantry.sh
./61b-dashboard-replay.sh           # re-scrape at +10 min for the recording cut

# Phase 6b — warm-cache rerun
./62-cached-rerun.sh
./63-record-cached.sh

# Phase 7 — comparison + teardown
./70-compare.sh
./99-cleanup.sh                     # prompts for typed "DELETE" confirmation
```

### Re-doing just the recording

If the harness is already validated end-to-end and you want a clean
take, reset to the baseline-clean state with:

```bash
kubectl delete -f manifests/hosts-toml-installer.yaml --ignore-not-found
kubectl apply -f manifests/hosts-toml-uninstaller.yaml
kubectl wait --for=condition=complete job/gantry-hosts-toml-uninstaller -n gantry-system --timeout=120s
kubectl delete daemonset gantry -n gantry-system --ignore-not-found
```

Then re-run from Phase 4.

## Recording checklist

See [`docs/recording-checklist.md`](docs/recording-checklist.md) for the
on-screen capture sequence, including ingest-lag-aware cut points.

## Security caveats

- **ACR admin user** is enabled by `10-provision.sh` so gantry's origin
  client has a username/password to drop into a Secret. Production
  should use Workload Identity + AcrPull MSI; the harness does not
  exercise that path.
- **`containerd_socket: ""`** in the gantry ConfigMap (overlay) puts
  cdsub in NoOp mode. Production deployments wanting cdsub must use
  one of the path-(a)/path-(b) approaches documented in
  [`deploy/daemonset.yaml`](../../deploy/daemonset.yaml) lines 220–249.
  The demo's path-(c) keeps gantry at UID 65532, no socket-permission
  surgery — the cost is no containerd-content-store secondary blob
  source, which the demo doesn't need anyway.
- **Phase 6b cleanup Job** runs `securityContext.privileged: true` with
  hostPath mounts of `/run/containerd/containerd.sock` and
  `/var/lib/containerd`. Acceptable only because this is a throwaway
  demo cluster.
- **No NetworkPolicy** is applied; gantry's peer ports (5001 + libp2p
  4001) are reachable from any pod on the cluster network.
