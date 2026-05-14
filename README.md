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
./bin/gantry agent \
  --mirror-listen 127.0.0.1:5000 \
  --transfer-listen 0.0.0.0:5001 \
  --metrics-listen 127.0.0.1:9095 \
  --cache-dir /var/lib/gantry/cache
```

A YAML config file matching `internal/config/config.go` is supported:

```sh
./bin/gantry agent --config /etc/gantry/config.yaml
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

## Security model

Gantry is deliberately **cluster-internal** — there is no cross-cluster
federation, and confidentiality + integrity of the on-cluster traffic
rests on three controls layered together. If you peel one layer back you
must replace it with an equivalent control before deploying.

### Mirror endpoint (`:5000`) — loopback by default, hostPort opt-in

The mirror endpoint speaks plain HTTP and is reachable only from
containerd on the same node. Two patterns are supported:

- **Single-host / non-Kubernetes:** bind `mirror_listen: 127.0.0.1:5000`.
  The config validator hard-rejects any non-loopback address. This is
  the safe default for `make run`, local dev, and any deployment outside
  Kubernetes.
- **Kubernetes DaemonSet:** the pod binds `0.0.0.0:5000` inside its
  network namespace, and the DaemonSet exposes it through
  `hostPort: 5000` with `hostIP: 127.0.0.1`. The kubelet's CNI plumbing
  DNATs `127.0.0.1:5000` on the node into the pod, so containerd reaches
  the mirror over loopback even though the pod itself binds widely.
  Because the pod-side bind isn't literally loopback, the operator must
  set `mirror_bind_allow_non_loopback: true` (env
  `GANTRY_MIRROR_BIND_ALLOW_NON_LOOPBACK=1`, flag
  `--mirror-bind-allow-non-loopback`) to disarm the validator. This is
  an **explicit opt-in**: do not enable it without also configuring
  `hostPort.hostIP: 127.0.0.1` (or an equivalent kernel-level barrier),
  because the mirror endpoint is intentionally not authenticated and
  containerd's `skip_verify: true` mirror config trusts whatever serves
  it. The shipped `deploy/daemonset.yaml` + `deploy/configmap.yaml`
  already wire this correctly; the flag exists so non-DaemonSet rollouts
  (e.g. systemd unit on a bare metal node behind a host firewall) can
  consciously take the same shortcut.

### Peer transfer endpoint (`:5001`) — h2c, NetworkPolicy-gated

The peer-to-peer transfer endpoint runs **plaintext HTTP/2 (h2c)**.
That is a deliberate tradeoff:

- Cluster-internal traffic only. Off-node reachability is blocked by the
  shipped `deploy/networkpolicy.yaml` (ingress restricted to peer
  agents and the kubelet). If your CNI does not enforce NetworkPolicy,
  you must replace it with an equivalent firewall before running Gantry
  in production.
- A `Gantry-Mirrored: 1` request header is required on every peer call;
  the handler 400s anything else. This is **not** an authentication
  mechanism — it stops accidental mis-routes (e.g. a misconfigured curl
  hitting the wrong port), nothing more.
- The integrity backstop is in-band digest verification. Every blob
  pulled from a peer is streamed through the digest-pipe in
  `internal/digestpipe` while it is being committed to the local cache;
  a peer that returns wrong bytes fails the commit and the consumer
  retries another provider. A peer **cannot** poison the cache by
  returning attacker-chosen bytes, because the digest the requester
  asked for is hashed independently as the bytes arrive.
- Range requests use standard RFC 7233 semantics (`Range: bytes=N-M` →
  `206 Partial Content` with `Content-Range`); the digest-pipe is
  applied to the full object on commit, not per-range.

What h2c is **not** defending against:

- A malicious workload sharing the cluster network with the agent can
  read peer-to-peer traffic in transit. If that is in your threat model,
  terminate Gantry traffic on a mesh (Istio / Linkerd / Cilium mTLS)
  before deploying, or wait for the post-GA mTLS option (tracked in
  the design doc §4.4 follow-ups). Image bytes are typically already
  public (pulled from a public registry), so this is rarely the right
  knob to turn first.
- A compromised peer that happens to *also* hold a digest can serve that
  digest. NetworkPolicy ingress + a controlled image-pull list is the
  defense; assume every node in the DHT can serve every digest it has
  legitimately pulled.

### Coordination plane (`:4001`)

The libp2p Kademlia DHT and `/gantry/coord/1.0.0` stream protocol are
authenticated by libp2p peer identity (Ed25519 keypair persisted at
`libp2p_identity_path`). Stream traffic is encrypted by libp2p's
built-in TLS / Noise transport — this is independent of the h2c
transfer endpoint and is **not** affected by the section above.

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