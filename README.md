# Gantry

**Cluster-internal peer-to-peer container image distribution for Kubernetes at 10k+ node scale.**

Gantry is a per-node daemon that turns every Kubernetes node into both a
client *and* a cache for OCI image content. When a kubelet asks
containerd for an image, containerd is configured to mirror the request
to a local Gantry agent on `127.0.0.1:5000`. The agent serves the bytes
out of its on-disk cache if it has them, otherwise it discovers other
nodes that already have the layer via a libp2p Kademlia DHT and streams
them peer-to-peer over `:5001`. Only on full peer-miss does Gantry fall
back to the upstream registry — so a 10,000-node rollout pulls each
unique blob from the origin a small number of times rather than 10,000
times.

```mermaid
flowchart LR
    kubelet[kubelet] --> containerd[containerd]
    containerd -->|"/v2/ mirror"| mirror["gantry mirror<br/>127.0.0.1:5000<br/>(this node)"]
    mirror -->|cache hit| bytes1([bytes])
    mirror -->|miss| dht{{libp2p DHT lookup}}
    dht -->|provider found| peer["gantry transfer<br/>peer:5001<br/>(another node)"]
    peer -->|bytes streamed P2P| bytes2([bytes])
    dht -.->|all peers down<br/>or unreachable| origin[(upstream registry)]
    origin -.-> bytes3([bytes])

    classDef node fill:#eef,stroke:#447,color:#000;
    classDef sink fill:#efe,stroke:#474,color:#000;
    classDef fallback fill:#fee,stroke:#744,color:#000;
    class mirror,peer node;
    class bytes1,bytes2,bytes3 sink;
    class origin fallback;
```

## Design references

- [docs/archecture.md](docs/archecture.md) — system overview, requirements, scenarios.
- [docs/detailed-design.md](docs/detailed-design.md) — protocols, timeouts, failure modes, §7 metric catalogue.



## Building

Requires Go 1.26+ and `protoc` on `$PATH`.

```sh
make build        # build cmd/gantry into ./bin/gantry
make test         # run unit tests
make proto        # regenerate protobuf Go bindings
make proto-check  # CI check: bindings match committed .proto files
make lint         # golangci-lint (requires `make tools` first)
```

## Running locally

```sh
./bin/gantry \
  --mirror-listen 127.0.0.1:5000 \
  --transfer-listen 0.0.0.0:5001 \
  --metrics-listen 127.0.0.1:9095 \
  --cache-dir /var/lib/gantry/cache
```

A YAML config file matching `internal/config/config.go` is supported:

```sh
./bin/gantry --config /etc/gantry/config.yaml
```

All flags can be set via uppercase env vars too (e.g. `GANTRY_MIRROR_LISTEN`).

On Linux, the agent automatically connects to the local containerd over
`/run/containerd/containerd.sock` (namespace `k8s.io`) to discover
locally-cached images and announce them on the DHT. Override via
`--containerd-socket` / `--containerd-namespace`, or set socket to ""
to disable. Non-Linux builds skip this entirely.

### Endpoints

| Endpoint | Listener | Purpose |
| --- | --- | --- |
| `:5000` | loopback | OCI v2 mirror for containerd. Tag → 503 (forces digest-pinning). |
| `:5001` | peer-facing | OCI v2 subset for peer-to-peer transfer (`Gantry-Mirrored: 1` header). |
| `:4001` | libp2p | TCP + QUIC swarm + `/gantry/coord/1.0.0` stream protocol. |
| `:9095` | ops | `/metrics`, `/livez`, `/healthz`, `/readyz`. |

## Deployment

See [deploy/README.md](deploy/README.md) for the full Kubernetes rollout
recipe: ServiceAccount + RBAC, ConfigMap, NetworkPolicy, DaemonSet,
distroless image, and the per-node `hosts.toml` containerd
configuration.

```sh
kubectl apply -f deploy/serviceaccount.yaml
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/registry-secret.example.yaml   # edit first
kubectl apply -f deploy/networkpolicy.yaml
kubectl apply -f deploy/daemonset.yaml
```

## Contributing

1. `make tools` once to install `protoc-gen-go` and `golangci-lint`.
2. Run `make check` (vet + tests) before every commit.
3. `make proto-check` must be green if any `proto/**/*.proto` changed.
4. Conventional commits (`feat:`, `fix:`, `chore:`, `docs:`) are preferred.
5. Substantive changes should cite the relevant `docs/detailed-design.md`
   section in the commit message or PR description.

## License

[MIT](LICENSE)