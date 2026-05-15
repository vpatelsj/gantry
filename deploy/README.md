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

## Production caveats

A few configuration knobs that need operator attention before going
to production:

| Item | Where | What to change |
| --- | --- | --- |
| API server egress CIDR | `networkpolicy.yaml` | The egress to TCP/443 and TCP/6443 defaults to `0.0.0.0/0` because managed control planes (EKS / GKE / AKS) and self-hosted clusters reach the apiserver at IPs that don't match a `namespaceSelector`. Replace with the apiserver's actual CIDR — `kubectl get endpoints kubernetes -n default -o jsonpath='{.subsets[*].addresses[*].ip}'` for self-hosted clusters; the managed-service docs for hosted control planes. |
| Origin registry egress | `networkpolicy.yaml` | The egress to TCP/443 for origin pulls also defaults to `0.0.0.0/0`. If the cluster only pulls from a known set of registry endpoints (your private registry, ghcr.io, etc.), restrict this rule to those IPs or labels. |
| Kubelet probe source | `networkpolicy.yaml` | Metrics ingress on TCP/9095 currently allows `0.0.0.0/0` so kubelet liveness/readiness probes (sourced from the node IP) reach the pod on strict CNIs. Replace with the node CIDR — `kubectl get nodes -o jsonpath='{.items[*].status.addresses[?(@.type=="InternalIP")].address}'`. |
| Mirror port 5000 source | `networkpolicy.yaml` | Ingress on TCP/5000 defaults to a deliberately-narrow `127.0.0.1/32` placeholder. Most CNIs (Calico, Cilium, and managed offerings) SNAT hostPort traffic so the in-pod source-IP after DNAT is the node IP, NOT 127.0.0.1 — the placeholder will then drop containerd's mirror pulls. Replace with the node CIDR (same command as the kubelet probe row). MUST NOT widen to the pod-network CIDR: that bypasses the `hostIP: 127.0.0.1` binding's loopback-only intent. |
| containerd socket access | `daemonset.yaml` | The pod runs as non-root (UID 65532); `containerd.sock` is typically root:root mode 0660. Set `spec.template.spec.securityContext.fsGroup` to a group with socket access, relax socket perms on the node, or clear `containerd_socket` in the ConfigMap to disable cdsub. |
| Kubernetes RBAC scope | `serviceaccount.yaml` | The agent only consumes `pods.list/watch` (informer) plus `pods.patch` (self-announce of libp2p + transfer addresses) in its own namespace, and `nodes.list/watch` cluster-wide for the zone label. There is no `get` on pods — informer events deliver the objects without point reads. Review `ClusterRole/Role` to confirm scope hasn't drifted. Membership setup failure is fatal in production mode (Downward-API env vars set), so an RBAC misconfig surfaces as a CrashLoop on rollout instead of a silent single-node fallback. |

### HEAD semantics on cache miss

`GET /v2/<repo>/blobs/<digest>` on a cache miss warms the cache as a
side effect; `HEAD` on the same URL does NOT. This is intentional
(see the comment block in `internal/mirror/mirror.go` at the HEAD
return after `writeBlobHeaders`) — caching a multi-GB blob just
because a client asked for its size would defeat the bandwidth
amplification fix Gantry exists to provide. A subsequent GET for
the same digest follows the cache-miss path normally and warms
the cache then.

If your client emits HEAD-then-GET patterns where you'd prefer to
amortize the origin metadata round-trip, raise the issue upstream
(containerd's puller, BuildKit's resolver, etc.) — those clients
generally have a one-shot resolve-and-pull mode that skips the
HEAD entirely.
