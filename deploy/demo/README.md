# `deploy/demo/` — ACR counting-proxy demo

Demo-only artifacts for the same-minute origin-load comparison
documented in [`docs/acr-counting-proxy-demo.md`](../../docs/acr-counting-proxy-demo.md).

## Repo isolation invariant

Everything the demo needs lives **only** under this directory. Per
the plan's §1.1, nothing here may modify:

- root [`go.mod`](../../go.mod)
- root [`Makefile`](../../Makefile)
- production [`deploy/`](..) manifests
- any package under `cmd/`, `internal/`, or `e2e/`

The proxy and harness each ship their own `go.mod` so they build
without touching the root module. **Deleting `deploy/demo/` reverts
the entire demo** with no residual state in the rest of the repo.

## Subtree layout (build plan order, see plan §10)

| Path                                                | Build-plan step | Status |
| --------------------------------------------------- | --------------- | ------ |
| [`acr-origin-proxy/`](acr-origin-proxy/)            | 1 (spike), 2 (full) | step 2 implemented |
| [`infra/`](infra/)                                  | 0 / 0.5         | Azure provisioning scripts implemented |
| [`Makefile`](Makefile)                              | 1               | implemented |
| `hosts.toml.baseline.template`                      | 3               | implemented |
| `hosts.toml.gantry.template`                        | 3               | implemented |
| `hosts-toml-installer.yaml`                         | 3               | implemented |
| `configmap.gantry-demo.yaml`                        | 3               | implemented |
| `harness/`                                          | 4–8             | step 4 skeleton + baseline / gantry-cold / gantry-warm phases implemented |
| `cache-purge-daemonset.yaml`                        | 7               | implemented |
| `RUNBOOK.md`                                        | 11              | implemented |
| `grafana-dashboard.json`                            | 9               | implemented |

## Usage

The local `Makefile` is the **only** entry point — never add a target
to the root Makefile. Invoke as:

```bash
make -C deploy/demo proxy-build      # build the proxy binary
make -C deploy/demo proxy-vet
make -C deploy/demo proxy-image      # build the container image
make -C deploy/demo harness          # run the build-tagged harness skeleton tests
make -C deploy/demo harness-baseline # opt-in live baseline phase
make -C deploy/demo infra-hosts-baseline
make -C deploy/demo infra-hosts-gantry
```

The proxy README explains how to run it locally against a real ACR
and inspect `/metrics` plus `/debug/summary`:
[`acr-origin-proxy/README.md`](acr-origin-proxy/README.md).
