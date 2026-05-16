# Gantry — 1 GiB headline benchmark, 20-node AKS

Recorded **2026-05-16** against AKS `gantry-demo-2`
(20 × `Standard_D4s_v5` worker nodes, k8s 1.34.7, containerd 1.7.31), with
the demo's [counting reverse proxy](../acr-origin-proxy/) sitting between
Gantry and ACR so every origin-egress request is counted at byte
granularity. Workload image: synthetic ~1 GiB single-layer image,
`gantrydemovapa.azurecr.io/gantry-demo-pull:*`. Pull driver: a 20-replica
`Job` in the `default` namespace.

Built from commit `f5db650` (main) — see [docs/known-issues.md](../../../docs/known-issues.md)
I-1 for the cdsub fix that this run depends on.

## Results

| metric | **BASELINE** (no Gantry) | **GANTRY cold-start** | reduction |
|---|---:|---:|---:|
| total proxy requests | 76 | 34 | **55.3 %** |
| **total bytes from origin** | **15.04 GB** | **6.45 GB** | **57.1 %** |
| blob requests | 28 | **10** | **64.3 %** |
| blob bytes | 15.04 GB | 6.45 GB | 57.1 % |
| `manifest_by_digest` requests | 28 | 4 | 85.7 % |
| `manifest_by_tag` requests | 20 | 21 | _by design — F9_ |

Per-pod start latency (median / p95 / p100):
- **Baseline**: 22.6 s / 24.6 s / 26.6 s
- **Gantry cold-start**: 25.5 s / 26.5 s / 31.5 s _(slightly slower —
  cold-cluster peer-discovery overhead, no thundering-herd degradation)_

From Gantry's own metrics during cold-start:
- `p2p_origin_pull_total = 7` cluster-wide (the 7 blobs that genuinely
  reached origin via Gantry's NF5 escape valve)
- `p2p_peer_fetch_total{outcome="hit"} = 95` — 95 of the cluster-wide
  blob fetches were served peer-to-peer over the transfer endpoint

**Origin egress avoided on this single rollout: 8.59 GB.** At cluster-scale
(10 000 nodes, equivalent image), ≈ 850 GB saved per rollout.

`manifest_by_tag` is not a regression: F9 in
[docs/archecture.md](../../../docs/archecture.md) deliberately routes tag
lookups through to the registry so tag → digest resolution stays
authoritative. Only `blob` and `manifest_by_digest` are counted toward F1.

## Raw artifacts

| file | what |
|---|---|
| [headline-before-baseline.json](headline-before-baseline.json) | proxy `/debug/summary` immediately before phase 1 |
| [headline-after-baseline.json](headline-after-baseline.json) | proxy `/debug/summary` after the 20-pod baseline pull |
| [headline-after-cold.json](headline-after-cold.json) | proxy `/debug/summary` after the 20-pod Gantry cold-start pull |
| [headline-baseline.log](headline-baseline.log) | raw `go test -v` output for `TestPhaseBaseline` |
| [headline-cold.log](headline-cold.log) | raw `go test -v` output for `TestPhaseGantryCold` |

Re-derive the table from the JSON snapshots with:

```sh
python3 - <<'PY'
import json
def L(n): return json.load(open(f"deploy/demo/artifacts/headline-{n}.json"))["totals"]
def D(a,b): return {k: b[k]-a[k] for k in ("requests_completed","bytes_to_client")} | \
                  {f"{c}_req":  b["by_path_class"][c]["requests"]-a["by_path_class"][c]["requests"] for c in ("blob","manifest_by_digest","manifest_by_tag")} | \
                  {f"{c}_bytes":b["by_path_class"][c]["bytes"]   -a["by_path_class"][c]["bytes"]    for c in ("blob","manifest_by_digest","manifest_by_tag")}
print("baseline:", D(L("before-baseline"), L("after-baseline")))
print("cold:    ", D(L("after-baseline"),  L("after-cold")))
PY
```

## Reproducing

```sh
# 0. one-time provisioning
make -C deploy/demo infra-provision infra-images infra-monitoring \
                    infra-proxy infra-grafana-dashboard

# 1. clean proxy stats so the snapshot deltas are honest
kubectl -n gantry-demo rollout restart deploy/acr-origin-proxy
kubectl -n gantry-demo rollout status  deploy/acr-origin-proxy

# 2. baseline (no Gantry on the data path)
DEMO_IMAGE_SIZE_MB=1024 make -C deploy/demo harness-baseline

# 3. install Gantry, then cold-start phase
make -C deploy/demo infra-gantry
kubectl -n gantry-demo delete ds/hosts-toml-installer --ignore-not-found
DEMO_IMAGE_SIZE_MB=1024 make -C deploy/demo harness-gantry-cold

# 4. teardown
CONFIRM_DESTROY=yes deploy/demo/infra/90-destroy-azure.sh deploy/demo/infra/env.local
```

The harness prints the proxy delta plus the Gantry counter delta on stdout
at the end of each phase.
