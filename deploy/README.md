# Gantry deployment artifacts

This directory carries the operator-facing pieces needed to roll out
the gantry agent as a Kubernetes DaemonSet.

## Files

| File | Purpose |
| --- | --- |
| `Dockerfile` | Multi-arch, distroless build (§Phase 6). |
| `build.sh` | Local image build helper. Tags from `git describe`. |
| `daemonset.yaml` | One-pod-per-node DaemonSet (§Phase 6 / §7.5). |
| `serviceaccount.yaml` | ServiceAccount + ClusterRole + Role + PriorityClass. |
| `configmap.yaml` | Default `config.yaml` (mirrors `config.NewDefault()`). |
| `registry-secret.example.yaml` | Template Secret for upstream-registry credentials. |
| `networkpolicy.yaml` | Locks transfer (5001), libp2p (4001), metrics (9095) to inter-agent / monitoring traffic. |
| `hosts.toml.template` | containerd registry mirror config; one file per upstream registry under `/etc/containerd/certs.d/<host>/hosts.toml`. |

## Apply order

```sh
kubectl apply -f deploy/serviceaccount.yaml
kubectl apply -f deploy/configmap.yaml
# Operator: edit registry-secret.example.yaml first.
kubectl apply -f deploy/registry-secret.example.yaml
kubectl apply -f deploy/networkpolicy.yaml
kubectl apply -f deploy/daemonset.yaml
```

## Building the image locally

```sh
# Single-arch into local docker (host arch):
deploy/build.sh

# Multi-arch + push:
deploy/build.sh -p linux/amd64,linux/arm64 -r ghcr.io/your-org/gantry --push

# Explicit tag:
deploy/build.sh -t v0.6.0
```

## Per-node containerd setup

For each upstream registry the cluster pulls from, drop a
`hosts.toml` at:

```
/etc/containerd/certs.d/<registry-host>/hosts.toml
```

derived from `hosts.toml.template` (substitute `${REGISTRY_SERVER}`
with the registry's `https://...` URL). containerd reloads `certs.d`
on its own; no restart needed.

## What to verify after rollout

| Check | How |
| --- | --- |
| Agents are running | `kubectl -n gantry-system get ds gantry` |
| Liveness / readiness | `/livez`, `/readyz` on 9095 per pod |
| Metrics | `curl http://<pod-ip>:9095/metrics` or scrape from Prometheus |
| Routing-table grew | `p2p_dht_health_score` ≥ 0.7 |
| Mirror is being used | `p2p_cache_hit_total` increments while a workload rolls out |
| Origin fallback is rare | `p2p_origin_fallback_total` stays at ~0 |

See `docs/detailed-design.md` §7.6 for the full metric catalogue.
