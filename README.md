# Gantry

Cluster-internal peer-to-peer container image distribution for Kubernetes at 10k+ node scale.

See:

- [docs/archecture.md](docs/archecture.md) — system overview, requirements, scenarios.
- [docs/detailed-design.md](docs/detailed-design.md) — protocols, timeouts, failure modes.
- [docs/implementation-plan.md](docs/implementation-plan.md) — phased build plan.

## Status

- **Phase 0** — skeleton, wire schema, interfaces, CI. ✅
- **Phase 0.5** — cross-cutting infra: typed config, structured logger, metrics registry. ✅
- **Phase 1** — single-node mirror + content-addressed cache + multi-registry origin client. ✅
- **Phase 2** — libp2p host, DHT, `:5001` peer transfer, K8s informer, containerd image-event subscription. In progress.

See [docs/implementation-plan.md](docs/implementation-plan.md) for the full phase plan.

## Building

Requires Go and `protoc` on `$PATH`.

```sh
make build        # build cmd/gantry into ./bin/gantry
make test         # run unit tests
make proto        # regenerate protobuf Go bindings
make proto-check  # CI check: bindings match committed .proto files
make lint         # golangci-lint (requires `make tools` first)
```