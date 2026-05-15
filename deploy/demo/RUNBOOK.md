# Demo runbook

Operator-facing recording / runbook for the ACR counting-proxy demo
(plan §10 step 11). Every command below is run from the repository
root.

## What this demo proves

A 20-node Kubernetes Job pulling a fresh ~1 GB image hits the same ACR
through three paths, with the same counting proxy in front of ACR
in every case (plan §1.2 / §2). The demo measures origin requests and
bytes seen by the proxy:

| Phase                | Path                                                                |
| -------------------- | ------------------------------------------------------------------- |
| Baseline             | containerd → proxy → ACR                                            |
| Gantry cold-start    | containerd → 127.0.0.1:5000 (Gantry) → proxy → ACR                  |
| Gantry warm-cache    | containerd → 127.0.0.1:5000 (Gantry, cache hit) → no upstream call |

The proxy is the same instrument across all three phases. ACR audit
logs see exactly one client identity (the proxy's ACR credential, plan
§2.5) — kubelet identity is **not** in play.

> Demo-only credentials. Demo-only AKS / ACR. Tear down at the end.

## One-time prerequisites

Already present after `make -C deploy/demo infra-*` runs:

- AKS cluster with 20 worker nodes, kube-prometheus-stack installed,
  Grafana available.
- ACR with admin credentials.
- `acr-origin-proxy` Deployment + Service in `gantry-demo`.
- Grafana dashboard "Gantry ACR demo" auto-imported via ConfigMap.
- Optional: Gantry DaemonSet in `gantry-system`.

If anything is missing, see [`infra/README.md`](infra/README.md).

## Phase 0.5 — Auth + reachability gate (mandatory)

```bash
make -C deploy/demo infra-smoke              # OCI auth: ping, manifest, blob
make -C deploy/demo infra-node-reachability  # AKS nodes can curl proxy ClusterIP
```

If either fails, **do not proceed**. Triage in plan §6 / §9.

## Phase 1 — Baseline

```bash
# Deploy or refresh the counting proxy (idempotent).
make -C deploy/demo infra-proxy

# Run the live baseline phase. Defaults to a 16 MiB workload image;
# bump DEMO_IMAGE_SIZE_MB to 1024 for the headline 1 GB number.
DEMO_IMAGE_SIZE_MB=1024 make -C deploy/demo harness-baseline
```

Expected proxy delta (plan §1.2):

- `manifest_by_digest` requests ≈ 20 × digest_count
- `blob` requests ≈ 20 × digest_count
- bytes ≈ 20 × compressed_image_size

Open Grafana → "Gantry ACR demo" → Row 1.

```bash
kubectl -n monitoring port-forward svc/kps-grafana 3000:80
# http://localhost:3000  user admin / pass from GRAFANA_ADMIN_PASSWORD
```

Clean up the installer DaemonSet between phases:

```bash
kubectl -n gantry-demo delete ds/hosts-toml-installer
```

## Phase 2 — Gantry cold-start

```bash
make -C deploy/demo infra-gantry             # deploy Gantry pointing at proxy
make -C deploy/demo harness-gantry-cold      # builds fresh image, runs Job
```

The harness:

1. Waits for the Gantry DaemonSet to converge.
2. Runs a wiring preflight pull through the proxy.
3. Builds and pushes a fresh image (no shared layers with baseline).
4. Installs the fail-closed gantry `hosts.toml` on every node.
5. Runs the 20-pod pull Job.
6. Reports proxy and Gantry counter deltas plus pod-start latency.

Save the image ref printed by the harness — the warm phase reuses it.

Expected proxy delta (plan §1.2 / §6 Phase 2):

- digest requests ≈ digest_count to 3 × digest_count (F1 worst case)
- digest bytes ≈ 1 to 3 × compressed_image_size
- `p2p_peer_fetch_total{outcome="hit"}` dominates `p2p_origin_pull_total`

## Phase 3 — Gantry warm-cache

```bash
# Use the image ref from the cold phase output:
DEMO_WARM_IMAGE_REF="gantrydemovapa.azurecr.io/gantry-demo-pull:gantry_cold-..." \
  make -C deploy/demo harness-gantry-warm
```

The harness:

1. Re-installs the gantry `hosts.toml` (idempotent).
2. Purges containerd's content store on every node for the resolved
   digest set (plan §5.2). The verifier fails the phase if any node
   reports a surviving digest.
3. Reruns the 20-pod pull Job against the same image.
4. Asserts proxy `blob` + `manifest_by_digest` deltas are 0.

Expected proxy delta (plan §6 Phase 3):

- `blob` requests = 0
- `manifest_by_digest` requests = 0
- `manifest_by_tag` requests > 0 (Gantry v1 defers tags to origin, F9)

## Recording script outline

1. Show the dashboard's headline panels are at the previous phase's
   numbers.
2. Run the next phase's `make` target and narrate. Mention:
   > Same ACR. Same proxy. Same nodes. Only the pull path changes.
3. After the Job completes, show the dashboard's headline panels
   updated. The digest request and byte panels are the proof.
4. Repeat for each phase.
5. Close with the §2.5 disclaimer:
   > The proxy is the only client of ACR in every phase; AKS-side
   > kubelet MSI is not in play.

## Cleanup

```bash
# Stop the Gantry side and the demo workloads but keep AKS:
kubectl delete namespace gantry-demo gantry-system
kubectl delete --all jobs -n default -l app.kubernetes.io/name=gantry-demo-pull

# Tear down everything in Azure:
CONFIRM_DESTROY=yes deploy/demo/infra/90-destroy-azure.sh deploy/demo/infra/env.local
```

## Failure triage

See plan §9 — every row in that table maps to a concrete kubectl /
Prometheus probe. The two most common failures, with their remediations:

| Symptom                                                                 | First check                                                                                                                                                                                              |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Phase 1 baseline `ImagePullBackOff` on some nodes                       | The `hosts-toml-installer` DaemonSet did not write to that node yet, or the node-level containerd cannot reach the proxy ClusterIP. Re-run `make -C deploy/demo infra-node-reachability` from a fresh proxy. |
| Phase 3 warm-cache shows nonzero `blob` / `manifest_by_digest` requests | The cache purge did not actually remove a digest. Inspect the JSON reports from `kubectl -n gantry-demo logs ds/cache-purge`; any `survivors > 0` line is the first place to look.                       |
