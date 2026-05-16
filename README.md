# Gantry

**Peer-to-peer OCI image distribution for Kubernetes. One origin pull per digest per cluster — at any scale.**

Gantry is a per-node daemon that fans image pulls out across the cluster
instead of stampeding the registry. Drop-in for kubelet + containerd; no
workload changes.

## The pitch

- **F1 — Origin pulls scale with image, not cluster.** A 10,000-replica
  rollout of an N-layer image causes ~N+1 registry pulls, not 10,000 ×
  (N+1). Rendezvous hashing (HRW) elects a per-digest puller; everyone
  else dedupes onto its DHT Provide record.
- **P2P hot path.** libp2p Kademlia for discovery, plaintext h2c for
  bandwidth-bound bulk transfer, RFC 7233 range-resume on flaky links,
  in-band digest verification on every received byte.
- **Zero workload change.** Kubelet, containerd, registry secrets, and
  pod specs are untouched. Wired in once via containerd `hosts.toml`
  pointing at `127.0.0.1:5000`. Disabling Gantry falls back to direct
  origin pulls transparently.

## Headline numbers

20 worker nodes, ~1 GiB workload image, every origin request counted by a
reverse proxy in front of ACR. Full methodology + raw JSON in
[deploy/demo/artifacts/headline.md](deploy/demo/artifacts/headline.md).

| metric | **BASELINE** (no Gantry) | **GANTRY cold-start** | **GANTRY** (warmed cluster) |
|---|---:|---:|---:|
| total proxy requests | 121 | 50 (−59 %) | **29 (−76 %)** |
| **bytes from origin** | **27.01 GB** | **14.01 GB (−48 %)** | **7.00 GB (−74 %)** |
| blob requests | 54 | 18 (−67 %) | **9 (−83 %)** |
| `manifest_by_digest` requests | 67 | 32 (−52 %) | 19 (−72 %) |
| `manifest_by_tag` requests | 0 | 0 | 0 _(digest-pinned, F9 not triggered)_ |

From Gantry's own metrics on the warmed-cluster cold-start: only
**3 origin pulls** cluster-wide (`p2p_origin_pull_total`) — exactly one
per unique digest in the image (1 manifest + 1 config + 1 layer) — and
**102 peer-to-peer fetch hits** (`p2p_peer_fetch_total{outcome="hit"}`).
That's the F1 invariant hitting its theoretical optimum.

20 GB of origin egress avoided on the warmed-cluster rollout.

## Data path

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

## Endpoints

| Endpoint | Listener | Purpose |
| --- | --- | --- |
| `:5000` | loopback | OCI v2 mirror for containerd. Tag → 503 (forces digest-pinning). |
| `:5001` | peer-facing | OCI v2 subset for peer-to-peer transfer. |
| `:4001` | libp2p | TCP + QUIC swarm + `/gantry/coord/1.0.0`. |
| `:9095` | ops | `/metrics`, `/livez`, `/healthz`, `/readyz`. |

## Build & run

```sh
make build && make check                              # build + vet + tests
./bin/gantry agent --config /etc/gantry/config.yaml   # production
```

Kubernetes rollout (DaemonSet + RBAC + ConfigMap + containerd
`hosts.toml`): see [deploy/README.md](deploy/README.md).

## Contributing

`make tools` → `make check` before every commit. `make proto-check`
must be green if `proto/**/*.proto` changed. Conventional commits.
Substantive changes cite the relevant `docs/detailed-design.md` section.

## License

[MIT](LICENSE)