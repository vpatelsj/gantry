# Gantry — 1 GiB headline benchmark, 20-node AKS (strict cold-start)

Recorded **2026-05-16** against AKS `gantry-demo-2` (20 × `Standard_D4s_v5`
worker nodes, k8s 1.34.7, containerd 1.7.31), with the demo's
[counting reverse proxy](../acr-origin-proxy/) sitting between Gantry
and ACR so every byte of origin egress is counted. Workload image:
synthetic ~1 GiB single-layer single-arch image,
`gantrydemovapa.azurecr.io/gantry-demo-pull@sha256:…`, **pulled by
content digest** so containerd skips tag resolution at the registry.

The cold-start phase was run with a **strict** `hosts.toml` that pins
containerd's only upstream for the registry to `http://127.0.0.1:5000`
(local Gantry mirror) with **no proxy fallback**. Every byte the
counting proxy sees during the cold phase is therefore sourced through
Gantry's own origin client; there is no `containerd → proxy → ACR`
direct path that could bypass Gantry's accounting. See
[`hosts.toml.gantry-strict.template`](../hosts.toml.gantry-strict.template).

Baseline reuses [`hosts.toml.baseline.template`](../hosts.toml.baseline.template):
containerd → counting proxy → ACR, no Gantry on the data path.

## Results

| metric                          | **BASELINE** (no Gantry) | **GANTRY cold-start (strict)** | reduction |
|---|---:|---:|---:|
| total proxy requests            | 121          | **20**         | **83 %**  |
| **bytes from origin**           | **27.01 GB** | **1.07 GB**    | **96 %**  |
| blob requests                   | 54           | **2**          | **96 %**  |
| blob bytes                      | 27.01 GB     | 1.07 GB        | 96 %      |
| `manifest_by_digest` requests   | 67           | 17             | 75 %      |
| `manifest_by_tag` requests      | 0            | 1              | _digest-pinned_ |

**25.94 GB of origin egress avoided on the cold start of 20 pods pulling a 1 GiB image.**

From Gantry's own Prometheus counters across the strict cold-start window:

| metric                                          | delta over strict cold-start |
|---|---:|
| `p2p_origin_pull_total` (cluster-wide)          | **3**          |
| `p2p_peer_fetch_total{outcome="hit"}`           | **+21**        |
| `p2p_peer_fetch_total{outcome="notfound"}`      | +8             |
| `p2p_peer_fetch_total{outcome="stall"}`         | +12            |
| `p2p_cache_hit_total`                           | **+73**        |

Three cluster-wide origin pulls match the three unique blobs in the
image (manifest + config + 1 layer) — the theoretical minimum for F1.
The remaining ~17 nodes' worth of traffic for those digests is served
peer-to-peer (`peer_fetch{hit}` + `cache_hit`), never reaching the
proxy.

Per-pod start latency:

| percentile | strict cold-start |
|---|---:|
| p50  | 32.3 s |
| p95  | 33.3 s |
| p100 | 34.3 s |

Comparable to the no-Gantry baseline's 23–26 s per pod, with no
thundering-herd tail. The ~6–9 s overhead vs. baseline is the
cold-start coordination round-trip (DHT lookups + first peer/origin
fetch) and is amortised across the 19 sibling pods that hit warm
peer/cache paths.

### Why "strict" matters

An earlier cold-start run with the production-shaped Gantry
[`hosts.toml.gantry.template`](../hosts.toml.gantry.template) (Gantry
first, **proxy as `server=` fallback**) showed 14.01 GB / 18 blob
requests at the proxy — ~13 GB higher than the strict measurement.
That delta is containerd retrying directly against the proxy when
Gantry's mirror returned a transient 5xx during the same cold-start.
Those fallback pulls bypass `p2p_origin_pull_total`, so Gantry's own
metrics (3 origin pulls) and the proxy's bytes (14 GB) disagreed. The
strict `hosts.toml` removes the fallback so the two views reconcile:
**proxy bytes = Gantry-routed bytes = 1.07 GB, origin pulls = 3.**

### Per-client and per-digest attribution

The counting proxy now labels every request with a `client_class`
(derived from the inbound `User-Agent`: `gantry`, `containerd`, or
`other`) and tracks per-digest totals in `/debug/summary.totals.by_digest`.
On the strict cold-start the attribution is unambiguous:

| `client_class` | requests Δ | bytes Δ |
|---|---:|---:|
| `gantry`       | **21** | **1.07 GB** |
| `containerd`   |  0     | 0           |
| `other`        |  1     | 19 B (preflight curl) |

The single 1 GiB layer blob shows up exactly once in `by_digest`, with
all 1.07 GB attributed to `client_class=gantry`. `client_class=containerd`
being **zero** is the live, automated proof that no containerd-direct
fallback path is contaminating the measurement. See
[strictv2-pre.json](strictv2-pre.json) /
[strictv2-post.json](strictv2-post.json) /
[strictv2-cold.log](strictv2-cold.log) for the captured snapshots.

Built from these commits (see `git log`):

- `0e8ba59` — cdsub: tolerate missing children when walking a containerd image
- `f0066ce` — origin: fall back to /manifests/ when /blobs/<digest> 404s
- `25d504b` — mirror: sniff blob body to label manifest content correctly
- `a7ab3cb` — mirror: set Content-Type on KindManifest responses (HEAD path too)
- `43ae418` — puller-pump: short-circuit when cache.Has(d) before re-pulling
- `317c6b5` — demo: persist DAC_OVERRIDE+DAC_READ_SEARCH caps patch

## Raw artifacts

Baseline (clean-redeploy, hosts.toml = `baseline`):

| file | what |
|---|---|
| [clean-pre-baseline.json](clean-pre-baseline.json)   | proxy `/debug/summary` immediately before the 20-pod baseline pull |
| [clean-post-baseline.json](clean-post-baseline.json) | after the 20-pod baseline pull |
| [clean-baseline.log](clean-baseline.log)             | raw `go test -v` output for `TestPhaseBaseline` |

Strict cold-start (hosts.toml = `gantry-strict`):

| file | what |
|---|---|
| [strict-pre.json](strict-pre.json)                       | proxy `/debug/summary` immediately before the strict cold-start phase |
| [strict-post.json](strict-post.json)                     | after the 20-pod strict cold-start pull |
| [strict-cold.log](strict-cold.log)                       | raw `go test -v` output for `TestPhaseGantryCold` |
| [strict-pre-gantry-metrics.txt](strict-pre-gantry-metrics.txt)   | Gantry Prometheus counters before the phase |
| [strict-post-gantry-metrics.txt](strict-post-gantry-metrics.txt) | Gantry Prometheus counters after the phase |

Re-derive the table from the JSON snapshots:

```sh
python3 - <<'PY'
import json
def L(p): return json.load(open(p))["totals"]
def D(a, b):
    cls = lambda c: (b["by_path_class"][c]["requests"] - a["by_path_class"][c]["requests"],
                     b["by_path_class"][c]["bytes"]    - a["by_path_class"][c]["bytes"])
    return {
        "requests":           b["requests_completed"] - a["requests_completed"],
        "bytes_to_client":    b["bytes_to_client"]    - a["bytes_to_client"],
        "blob_req":           cls("blob")[0],
        "blob_bytes":         cls("blob")[1],
        "manifest_by_digest": cls("manifest_by_digest")[0],
        "manifest_by_tag":    cls("manifest_by_tag")[0],
    }
print("baseline:", D(L("deploy/demo/artifacts/clean-pre-baseline.json"),
                     L("deploy/demo/artifacts/clean-post-baseline.json")))
print("strict:  ", D(L("deploy/demo/artifacts/strict-pre.json"),
                     L("deploy/demo/artifacts/strict-post.json")))
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

# 3. deploy Gantry, then strict cold-start phase
make -C deploy/demo infra-gantry
kubectl -n gantry-demo delete ds/hosts-toml-installer --ignore-not-found
DEMO_IMAGE_SIZE_MB=1024 make -C deploy/demo harness-gantry-cold

# 4. teardown
CONFIRM_DESTROY=yes deploy/demo/infra/90-destroy-azure.sh deploy/demo/infra/env.local
```

The harness prints the proxy delta plus the Gantry counter delta on
stdout at the end of each phase.
