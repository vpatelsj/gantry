# Gantry — 1 GiB headline benchmark, 20-node AKS (clean redeploy)

Recorded **2026-05-16** against AKS `gantry-demo-2` (20 × `Standard_D4s_v5`
worker nodes, k8s 1.34.7, containerd 1.7.31), with the demo's
[counting reverse proxy](../acr-origin-proxy/) sitting between Gantry
and ACR so every byte of origin egress is counted. Workload image:
synthetic ~1 GiB single-layer single-arch image,
`gantrydemovapa.azurecr.io/gantry-demo-pull@sha256:…`, **pulled by
content digest** so containerd skips tag resolution at the registry.

The cluster was redeployed from scratch immediately before this run:
gantry-system namespace deleted, every node's `/var/lib/gantry/{cache,libp2p}`
hostPath wiped via a one-shot DaemonSet, gantry image rebuilt at git
`317c6b5`, then `make -C deploy/demo infra-gantry` deployed a fresh
DaemonSet. Proxy was restarted right before phase 1 so its counters
start at zero.

Built from these commits (see `git log`):

- `0e8ba59` — cdsub: tolerate missing children when walking a containerd image
- `f0066ce` — origin: fall back to /manifests/ when /blobs/<digest> 404s
- `25d504b` — mirror: sniff blob body to label manifest content correctly
- `a7ab3cb` — mirror: set Content-Type on KindManifest responses (HEAD path too)
- `43ae418` — puller-pump: short-circuit when cache.Has(d) before re-pulling
- `317c6b5` — demo: persist DAC_OVERRIDE+DAC_READ_SEARCH caps patch

## Results

| metric                          | **BASELINE** (no Gantry) | **GANTRY cold-start** (first) | **GANTRY cold-start** (reproduce) | reduction (first / reproduce) |
|---|---:|---:|---:|---:|
| total proxy requests            | 121          | 50             | **29**         | **59 % / 76 %** |
| **bytes from origin**           | **27.01 GB** | **14.01 GB**   | **7.00 GB**    | **48 % / 74 %** |
| blob requests                   | 54           | **18**         | **9**          | **67 % / 83 %** |
| blob bytes                      | 27.01 GB     | 14.01 GB       | 7.00 GB        | 48 % / 74 %     |
| `manifest_by_digest` requests   | 67           | 32             | 19             | 52 % / 72 %     |
| `manifest_by_tag` requests      | 0            | 0              | 0              | _digest-pinned, F9 not triggered_ |

Two cold-start measurements are reported because the second one is a
useful complement, not a contradiction:

- **First cold-start** is taken immediately after `make -C deploy/demo
  infra-gantry` deploys a brand-new DaemonSet — 20 fresh gantry pods,
  empty libp2p identity files, empty hostPath caches, members informer
  still catching up. This is the honest pessimistic case.

- **Reproduce cold-start** is the same harness run a few minutes later
  on the same cluster: gantry pods are identical, hostPath cache still
  holds previous-image blobs (different digests but warmer DHT), libp2p
  routing tables have converged, members informer is stable. F1 hits
  its theoretical minimum: **3 cluster-wide origin pulls** for the
  image's 3 unique digests (1 manifest + 1 config + 1 layer), and
  **102 peer-fetch hits** absorb the remaining ~17 nodes × 3 digests of
  blob traffic.

Per-pod start latency is essentially identical across runs (p50 ≈ 25 s,
p100 ≈ 26 s — comparable to the no-Gantry baseline's 23–26 s, with no
thundering-herd tail).

From Gantry's own metrics:

| metric                                          | first cold | reproduce cold |
|---|---:|---:|
| `p2p_origin_pull_total` delta (cluster-wide)    | 7          | **3**          |
| `p2p_peer_fetch_total{outcome="hit"}` delta     | 69         | **102**        |

**13.00 GB origin egress avoided on the first cold-start; 20.01 GB on
the reproduce run.**

The baseline number (27 GB rather than the naive 20 GB) is higher than
ideal because kubelet on AKS retries blob GETs at least once per pod
during the initial pull spike. Gantry's mirror serves 0 of those
retries from origin because the deduplication is upstream of the
kubelet boundary.

## Raw artifacts

First cold-start (clean-redeploy):

| file | what |
|---|---|
| [clean-pre-baseline.json](clean-pre-baseline.json) | proxy `/debug/summary` immediately before phase 1 (counters at zero) |
| [clean-post-baseline.json](clean-post-baseline.json) | after the 20-pod baseline pull |
| [clean-post-cold.json](clean-post-cold.json) | after the 20-pod Gantry cold-start pull |
| [clean-baseline.log](clean-baseline.log) | raw `go test -v` output for `TestPhaseBaseline` |
| [clean-cold.log](clean-cold.log) | raw `go test -v` output for `TestPhaseGantryCold` |
| [clean-run-start.txt](clean-run-start.txt) / [clean-run-end.txt](clean-run-end.txt) | UTC unix timestamps bracketing the run window |

Reproduce cold-start (same cluster, ~5 min later, no redeploy):

| file | what |
|---|---|
| [repro-pre.json](repro-pre.json) | proxy `/debug/summary` immediately before the reproduce phase |
| [repro-post.json](repro-post.json) | after the reproduce 20-pod pull |
| [repro-cold.log](repro-cold.log) | raw `go test -v` output |

Re-derive the table from the JSON snapshots:

```sh
python3 - <<'PY'
import json
def L(n): return json.load(open(f"deploy/demo/artifacts/clean-{n}.json"))["totals"]
def D(a,b):
    cls = lambda c: (b["by_path_class"][c]["requests"]-a["by_path_class"][c]["requests"],
                     b["by_path_class"][c]["bytes"]-a["by_path_class"][c]["bytes"])
    return {k: b[k]-a[k] for k in ("requests_completed","bytes_to_client")} | \
           {f"{c}_req": cls(c)[0] for c in ("blob","manifest_by_digest","manifest_by_tag")} | \
           {f"{c}_bytes": cls(c)[1] for c in ("blob","manifest_by_digest","manifest_by_tag")}
print("baseline:", D(L("pre-baseline"), L("post-baseline")))
print("cold:    ", D(L("post-baseline"), L("post-cold")))
PY
```

## Reproducing

```sh
# 0. one-time provisioning
make -C deploy/demo infra-provision infra-images infra-monitoring \
                    infra-proxy infra-grafana-dashboard

# 1. clean proxy counters
kubectl -n gantry-demo rollout restart deploy/acr-origin-proxy
kubectl -n gantry-demo rollout status  deploy/acr-origin-proxy

# 2. baseline (no Gantry on the data path)
DEMO_IMAGE_SIZE_MB=1024 make -C deploy/demo harness-baseline

# 3. deploy Gantry, then cold-start phase
make -C deploy/demo infra-gantry
kubectl -n gantry-demo delete ds/hosts-toml-installer --ignore-not-found
DEMO_IMAGE_SIZE_MB=1024 make -C deploy/demo harness-gantry-cold

# 4. teardown
CONFIRM_DESTROY=yes deploy/demo/infra/90-destroy-azure.sh deploy/demo/infra/env.local
```

The harness prints the proxy delta plus the Gantry counter delta on
stdout at the end of each phase.
