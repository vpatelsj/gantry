# gantry — end-to-end test suite

This directory holds the kind-based integration suite. It boots a real
Kubernetes cluster on Docker, builds the gantry container image,
deploys the DaemonSet, and exercises a pull through the mirror.

## Status

- ✅ Smoke test — DaemonSet rolls out, all pods reach `/readyz=200`.
- 🟡 Pull-and-cache-hit test — TODO (see `Future scenarios` below).
- 🟡 Chaos scenarios — TODO.

The scaffolding is in place; the smoke test is the only assertion
currently wired in. Additional scenarios listed below should each
land as their own commit.

## Prereqs

The harness shells out to standard CLIs; no extra Go deps. Install:

- [Docker](https://docs.docker.com/get-docker/) (engine running)
- [kind](https://kind.sigs.k8s.io/) ≥ 0.20
- [kubectl](https://kubernetes.io/docs/tasks/tools/) ≥ 1.28
- Go ≥ 1.26 (matching root `go.mod`)

`make tools-e2e` checks every binary is on `$PATH` and fails loud if
anything is missing.

## Running

```sh
make tools-e2e        # one-time prereq check
make e2e              # boot kind, build+load image, deploy, run tests, tear down
```

To keep the cluster running after the test (for debugging), set
`E2E_KEEP=1`:

```sh
E2E_KEEP=1 make e2e
# ...
kind delete cluster --name gantry-e2e
```

Test logs land in `e2e/.artifacts/<test-name>.log`. The harness also
dumps `kubectl describe pods` + container logs on failure.

## How it works

The harness (`harness_e2e.go`) is a small Go-driven wrapper over the
prereq CLIs:

| Step | What it does |
| --- | --- |
| `bootCluster()` | `kind create cluster --config kind-config.yaml` |
| `buildAndLoadImage()` | `deploy/build.sh -t e2e` then `kind load docker-image gantry:e2e` |
| `applyManifests()` | rewrites the DaemonSet image to `gantry:e2e` then `kubectl apply -f deploy/` (NetworkPolicy is opt-in via `GANTRY_E2E_NETWORKPOLICY=1`) |
| `waitForRollout()` | polls `kubectl rollout status ds/gantry -n gantry-system` |
| `checkReadyz()` | `kubectl exec` into one pod and curls `127.0.0.1:9095/readyz` |
| `teardown()` | `kind delete cluster` (skipped when `E2E_KEEP=1`) |

The kind config (`kind-config.yaml`) declares one control-plane + two
worker nodes — enough to exercise multi-peer coord paths in future
scenarios.

## Build tag

All e2e files carry `//go:build e2e`. Default `go test ./...` skips
them. Run with:

```sh
go test -tags=e2e ./e2e/... -v -timeout=10m
```

The `make e2e` target sets `-tags=e2e` and a generous timeout for you.

## Future scenarios

Each item should land as a focused commit; the order suggested below
matches "smallest to largest dependency on other scenarios."

1. **Cache-hit on second pull.** Pull a small image (e.g.
   `registry.k8s.io/pause:3.9`) through the mirror once, then pull
   again from a *different* node, and assert `p2p_cache_hit_total`
   incremented on the second node and `p2p_peer_fetch_total` ≥ 1.
2. **Origin failure → cluster-wide circuit (§5.8).** Inject 401 via a
   mock upstream and assert the second node honors the cooldown.
3. **NF5 pod-kill simulation.** `kubectl delete pod` mid-pull and
   assert the designated-puller takeover metric increments
   (`p2p_designated_puller_takeover_total`).
4. **Forced eviction with `cache_budget_bytes=10MB`.** Pull >budget,
   assert provider count doesn't drop below
   `eviction_provider_count_threshold` until the headroom condition
   forces it.
5. **DHT-empty cold-start hold (§7.7).** Start with one node, pull,
   assert no fallback before `bootstrap_window` elapses.

## Caveats

- The kind cluster boot takes ~60–120 s. The Makefile target reserves
  a 10-minute test timeout to absorb that.
- The default kind containerd uses namespace `k8s.io`, matching the
  gantry `containerd_namespace` default — no extra config needed.
- The DaemonSet socket-permissions caveat from `deploy/daemonset.yaml`
  applies inside kind too. The harness sets `securityContext.fsGroup`
  on the e2e pod template to mirror the workaround.
