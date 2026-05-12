# Gantry

Kubernetes clusters at 10k+ node scale routinely deploy the same container image across many nodes simultaneously. Naive behavior — every node pulling from the upstream registry — produces a thundering herd at the origin: 10,000 simultaneous TLS handshakes, registry-side rate limiting, link saturation between cluster and registry, and slow rollouts. This is the dominant cost of large-scale rollouts and a known operational pain point.

This document proposes a cluster-local peer-to-peer distribution layer: the origin registry is contacted **at most a small constant number of times per unique content digest** (manifest, config, or layer), after which the content propagates through the cluster between peer nodes. The design is fully decentralized, uses libp2p for content discovery only, and uses rendezvous hashing per digest to coordinate cold-start pulls without leader election or central state.

---

## 1. Requirements

### Functional

| ID  | Requirement |
|-----|-------------|
| F1  | The system pulls each unique content **digest** (manifest, config, or layer) from the origin registry **at most a small constant number of times per cluster** under normal operation — typically exactly once, bounded by ≤3 during transient informer-convergence windows (§5.3). Scope is digest-keyed requests only; tag-keyed cold-start is outside F1 (§5.1a, §6). |
| F2  | Image pulls by `kubelet` / `containerd` are served transparently — no changes to pod specs or workload configuration. |
| F3  | The agent runs as a Kubernetes DaemonSet. No PVCs, no per-node persistent identity managed by Kubernetes. |
| F4  | Peer discovery uses libp2p (Kademlia DHT). No central tracker or registry-side coordination is required. |
| F5  | Cold-start coordination (no peer has the digest yet) uses rendezvous hashing (HRW) **per digest** to deterministically select a designated puller. Each digest is coordinated independently. |
| F6  | When a digest is not yet cached anywhere, exactly one node pulls it from origin in the common case; redundant pulls occur only under failure (e.g., partition). |
| F7  | Content received from peers is verified against OCI digests before being served to `containerd`. |
| F8  | The system supports any OCI-compliant upstream registry. |
| F9  | Tag references (`manifests/<tag>`) resolve directly at origin via containerd's `hosts.toml` mirror-fallback chain. The agent maintains no tag→digest cache, no tag-keyed DHT advertisements, and no agent-layer tag freshness logic; only digest-keyed requests are routed through the P2P layer. |

### Non-functional

| ID   | Requirement |
|------|-------------|
| NF1  | Designed for clusters of 10,000+ nodes. |
| NF2  | Cold-start convergence time (time from first request to image available on all requesting nodes) should be bounded by image size and inter-node bandwidth, not by origin bandwidth. |
| NF3  | Recovery from designated-puller failure should not require coordination with a control plane. |
| NF4  | During network partition, the system should prefer liveness (both partitions make progress) over strict pull-once (one extra origin pull is acceptable). |
| NF5  | The system must not introduce hard dependencies that prevent pulls if libp2p discovery is degraded. The agent's HRW top-K probe (§5.2) is the authoritative discovery mechanism on DHT miss; direct origin fallback fires only when both DHT and HRW probe fail and is jittered + rate-limited (§7.7). |
| NF6  | Per-node resource footprint should be modest: <100 MB RAM, <5% CPU under steady state. |

---

### Architecture overview

```
                          ┌───────────────────┐
                          │  Origin registry  │   (external, OCI-compliant)
                          └─┬──────┬──────┬───┘
                            │ D1   │ D2   │ D3
                            │      │      │   each unique digest pulled
                            │      │      │   at most a small constant
                            │      │      │   number of times cluster-wide
        ┌───────────────────┼──────┼──────┼──────────────────────────────┐
        │ Kubernetes        ▼      ▼      ▼                              │
        │ cluster      ┌────────┐┌────────┐┌────────┐                    │
        │              │ Node A ││ Node B ││ Node C │  ...               │
        │              │ puller ││ puller ││ puller │                    │
        │              │ of D1  ││ of D2  ││ of D3  │                    │
        │              └───┬────┘└───┬────┘└───┬────┘                    │
        │              D1  │     D2  │     D3  │                         │
        │                  ▼         ▼         ▼                         │
        │      ┌──────────────────────────────────────────────┐          │
        │      │ Remaining N-3 nodes fetch D1, D2, D3 from    │          │
        │      │ their respective pullers via libp2p          │          │
        │      │ discovery + HTTP/2 transfer on :5001         │          │
        │      └──────────────────────────────────────────────┘          │
        │                                                                │
        │ Per-digest HRW: an image's manifest, config, and layer         │
        │ digests generally HRW to *different* nodes, so cold-start      │
        │ origin contact spreads across the cluster instead of           │
        │ concentrating on one node. No traffic flows between the        │
        │ pullers themselves; each independently fans out its digest     │
        │ to the rest of the cluster.                                    │
        └────────────────────────────────────────────────────────────────┘
```

Per-node layout:

```
┌────────────────────── Node ───────────────────────┐
│                                                    │
│   kubelet → containerd ──► hosts.toml mirror       │
│                              ▼                     │
│                      127.0.0.1:5000                │
│                              ▼                     │
│              ┌──────── P2P agent ────────┐         │
│              │  - Registry mirror server │         │
│              │  - libp2p host + DHT      │         │
│              │  - HTTP transfer server   │         │
│              │  - HRW + K8s informer     │         │
│              └────────────┬──────────────┘         │
│                           ▼                        │
│                 hostPath: /var/lib/p2p-cache       │
│                                                    │
└────────────────────────────────────────────────────┘
```

### Key design decisions

**libp2p for discovery only, not transfer.** Discovery (find peers with content X) is what Kademlia is good at. Bulk content transfer rides on plain HTTP/2 against a dedicated peer-facing port the DaemonSet exposes (see §4.4). This lets transfer benefit from kernel-level TCP optimizations, standard HTTP tooling for debugging, and a clean substrate for future range-based multi-peer striping. v1 ships single-peer fetch (the requester picks one provider and streams the whole blob); range-based parallel striping across multiple peers is supported by the endpoint contract but deferred to v2 — see §5.1.

**Per-digest granularity, end to end.** Provider records, HRW computation, and cold-start coordination all operate on individual OCI digests, not on "images." An image is a manifest digest plus a config digest plus N layer digests; the agent treats these as N+2 independent units. Two images sharing a base layer share providers automatically. Cold-start for a fully-uncached image fans out to up to N+2 different designated pullers in parallel — one per digest — which maximizes ingress parallelism and spreads load across the cluster (popular base layers naturally HRW to different nodes per image-that-uses-them, rather than concentrating on a single "image owner"). Per-chunk granularity (for pipelined distribution of large layers) is deferred until empirical data justifies the additional complexity.

**Tradeoff: per-digest HRW costs N+2 separate origin TLS handshakes.** For a fully-cold N-layer image, origin sees up to N+2 TLS handshakes from up to N+2 distinct source IPs — not 1 handshake reused across N+2 keepalive'd requests as the single-puller-per-image alternative would produce. The puller-side TLS connection reuse described in §5.2a only helps when batched `please_pull` lands multiple digests on the same puller (the exception under uniformly-distributed HRW, not the rule). This is a deliberate tradeoff: image-pull rollouts are bursty events, and a registry that pushes back on N+2 simultaneous handshakes from distinct cluster nodes would push back harder on the thundering-herd alternative the design exists to prevent. Revisit only if real-world registries report per-handshake (rather than per-byte or per-request) cost as the binding constraint.

**Separate content cache, not co-located with containerd's content store.** v1 maintains the agent's content cache on `hostPath` independently of containerd's content store at `/var/lib/containerd/io.containerd.content.v1.content`. Popular content is therefore on disk twice on each node; at the configured 50 GB cache and 10k nodes, worst-case cluster-wide duplication is on the order of 500 TB. The duplication is a deliberate v1 simplification with three justifications: (i) the agent's cache has provider-count-aware lifetime under §7.4, which containerd's GC has no concept of; (ii) the agent does not need read access to containerd's content directories or knowledge of containerd's internal layout (which varies by version and snapshotter); (iii) restart recovery is self-contained — walk one directory tree, advertise everything. Serving directly from containerd's content store would eliminate the duplication and is the natural v2 candidate once v1 is validated at scale; see §8 open question.

**Tags resolve directly at origin; Gantry routes only digests.** Tag references (`registry/repo:tag`) are not content-addressable, and v1 deliberately does not route them through the P2P layer. When containerd asks the local agent for `manifests/<tag>`, the agent returns `5xx` immediately, and containerd's `hosts.toml` mirror-fallback chain reaches origin directly using that node's own credentials. Origin performs the tag→digest resolution; containerd records the binding in its own image table; the agent observes the resulting digests via image events (§7.3) and routes them through the normal digest-keyed P2P path (§5.1 / §5.2, F1-bounded). The agent maintains **no tag→digest cache, no tag-keyed DHT advertisements, and no tag-freshness logic**. This eliminates the tag-cache coherence problem that would otherwise arise from §7.4's separate-cache decision (a separate cache with its own GC would need either a tag-binding TTL/refresh mechanism or some other invalidation discipline to stay consistent with origin), and preserves OCI's "tag is a pointer at origin, resolved on every pull" semantic exactly. The cost is per-node origin contact for the tag-resolution step (small — manifest body only, typically a few KB; layer and config bytes remain F1-bounded via the digest-keyed path) and loss of tag-pull availability under origin partition. §8 documents an HRW-coordinated TTL-refresh mechanism as a v2 candidate for operators who need bounded-staleness peer-served tag resolution.

**Stateless agent (from the orchestration perspective).** The pod itself has no Kubernetes-managed state — no PVCs, no per-node identity managed by Kubernetes. Cache and (optionally) libp2p identity live on `hostPath`. The agent does maintain in-memory state — in-flight pull map, recent-failures negative cache (§5.8), DHT health rolling stats (§7.7), informer cache — but all of it is reconstructible from the on-disk cache and Kubernetes informer on restart. On pod restart, the agent rebuilds its peer view from the Kubernetes informer and re-announces cached content to the DHT.

**Direct RPC for cold-start coordination, with HRW top-K probe as authoritative discovery on DHT miss.** When DHT lookup returns no providers, the agent does **not** treat that as ground truth. `dht.FindProviders` returning empty is ambiguous — it may mean genuinely no provider, slow/timed-out lookup, sparse local routing table, or expired provider records under load. The agent therefore treats the DHT as a fast-path optimization and uses HRW top-K probe as the authoritative discovery mechanism on DHT miss: it dials the K HRW-ranked nodes for the digest with an enriched query that asks both "do you have it cached?" and "are you already pulling it?". Only if all reachable top-K answer "neither" is the request treated as a true cold-start. HRW computation is local and deterministic and does not depend on DHT health, so this routes around DHT degradation entirely. No PubSub layer. No heartbeat protocol. Receiver-side dedupe handles concurrent requests.

**Topology-aware (optional).** HRW can be scoped per availability zone for clusters where cross-zone bandwidth is the bottleneck. This produces one designated puller per zone instead of one per cluster.

### 4.4 Wire protocols

The agent exposes two distinct wire surfaces. They are versioned independently.

**Coordination RPCs (libp2p stream).** `pull_intent_query` and `please_pull` are carried as length-prefixed protobuf messages over libp2p streams. Single protocol ID `/gantry/coord/1.0.0`; one stream per request/response pair, closed after the response is written. Schema lives in `proto/gantry/coord/v1/coord.proto`. Forward-compat policy: additive changes bump the minor (`/gantry/coord/1.1.0`), breaking changes bump the major (`/gantry/coord/2.0.0`). gRPC is intentionally avoided — `go-msgio` length-delimited framing is sufficient and avoids HTTP-over-libp2p complexity. The message envelope uses `oneof` so a single protocol handler dispatches both RPC kinds:

```proto
syntax = "proto3";
package gantry.coord.v1;
import "google/protobuf/timestamp.proto";

enum FailureClass {
  FAILURE_CLASS_UNSPECIFIED  = 0;
  FAILURE_CLASS_AUTH         = 1;
  FAILURE_CLASS_NOT_FOUND    = 2;
  FAILURE_CLASS_RATE_LIMITED = 3;
  FAILURE_CLASS_TRANSIENT    = 4;
}

message Envelope {
  oneof msg {
    PullIntentRequest   pull_intent_request   = 1;
    PullIntentResponse  pull_intent_response  = 2;
    PleasePullRequest   please_pull_request   = 3;
    PleasePullResponse  please_pull_response  = 4;
  }
}

message PullIntentRequest  { string digest = 1; }

message PullIntentResponse {
  bool has_cached                          = 1;
  bool in_flight                           = 2;
  google.protobuf.Timestamp started_at     = 3;
  int32 hrw_rank                           = 4; // recipient's own rank in recipient's view;
                                                // requester compares against its own rank
                                                // computation to detect informer divergence
                                                // (§5.3) and emit `p2p_hrw_rank_mismatch_total`
  bool recently_failed                     = 5;
  google.protobuf.Timestamp cooldown_until = 6;
  FailureClass failure_class               = 7;
}

message PleasePullRequest {
  repeated string digests   = 1;            // batched (§5.2a); all digests in a single batch
                                            // MUST share `upstream_registry` and `repository`.
                                            // Cross-repo digests (e.g., a layer reachable via
                                            // OCI cross-repo blob mount) require separate calls.
  string upstream_registry  = 2;            // e.g. "registry.example.com"
  string repository         = 3;            // e.g. "library/nginx"
}

message PleasePullResponse {
  message Result {
    enum Outcome {
      OUTCOME_UNSPECIFIED     = 0;
      OUTCOME_ALREADY_PULLING = 1;
      OUTCOME_STARTED         = 2;
      OUTCOME_RECENTLY_FAILED = 3;
      reserved 4;                   // OUTCOME_DECLINED: held for per-source
                                    // `please_pull` rate-limiting (§8 open question)
    }
    string digest                            = 1;
    Outcome outcome                          = 2;
    google.protobuf.Timestamp started_at     = 3;
    google.protobuf.Timestamp cooldown_until = 4;
    FailureClass failure_class               = 5;
  }
  repeated Result results = 1;
}
```

**Transfer endpoint (HTTP).** Each agent binds an HTTP/2 server on `0.0.0.0:5001` (peer-facing; the containerd mirror endpoint on `127.0.0.1:5000` is separate and loopback-only). NetworkPolicy restricts `:5001` to inter-node traffic. The endpoint mirrors the OCI Distribution API so peer-side code reuses the same registry-client codepath:

- `GET /v2/` → `200` (capability probe).
- `GET /v2/<repo>/blobs/sha256:<hex>` → blob bytes; supports `Range: bytes=N-M` with `206 Partial Content` + `Content-Range`. Range support is required at the protocol level even though v1 clients do not use it (it unblocks v2 striping without a protocol change).
- `GET /v2/<repo>/manifests/sha256:<hex>` → manifest bytes by digest.
- `GET /v2/<repo>/manifests/<tag>` → tag-keyed; returns `404` unconditionally on this peer-facing endpoint. Peers never request tags in v1 (the DHT carries no tag keys; see §4.3 and §5.1a), so a tag-shaped request arriving on `:5001` indicates a misconfigured peer and `404` is the appropriate response. **This differs from the containerd mirror endpoint on `127.0.0.1:5000`**, which returns `503` for tag-shaped requests so that containerd's `hosts.toml` mirror-fallback promotes the request to the next host in the chain (§5.1a, §7.1). Different endpoints, different audiences, different status codes — `:5001` is peer-to-peer where a tag request is a bug, `:5000` is the containerd mirror where a tag request is the design-intended path that must trigger fallback.
- `GET /healthz` → `200` for Kubernetes liveness/readiness probes.

**`Gantry-Mirrored: 1` header.** The requesting agent sets `Gantry-Mirrored: 1` on every fetch to a peer's `:5001`. Its presence switches the serving peer's handler into peer-fetch mode:

- Serve **only** from local store. Return `404` on miss. Never trigger DHT lookup, HRW probe, `please_pull`, or origin contact in response to a peer fetch.
- Tag-shaped manifest requests: `404` unconditionally (peers do not request tags in v1; the DHT carries no tag keys).
- Increment `p2p_peer_serve_total` rather than `p2p_cache_hit_total` (which counts workload-side hits via the mirror endpoint).

The header is a behavior switch, not an authorization mechanism. Trust comes from NetworkPolicy scoping `:5001` to inter-node traffic. Any peer reachable on that port is treated as trusted; see the `please_pull` abuse open question in §8.

---

## 5. Protocol flows

Manifest requests come in two forms: by digest (`manifests/sha256:...`) and by tag (`manifests/<tag>`). Tag requests are handled separately (§5.1a) because they are not content-addressable. All other requests — manifests by digest, config blobs, and layer blobs — go through the digest-keyed warm path (§5.1) or cold-start path (§5.2).

### 5.1 Warm path — digest-keyed content exists in cluster

1. `containerd` requests `127.0.0.1:5000/v2/<image>/manifests/sha256:abc` (or a blob by digest).
2. Agent checks local cache. **Miss.**
3. Agent calls `dht.FindProviders(CID)` where CID derives from the OCI digest. **Hit** — returns peer multiaddrs.
4. Agent picks the first reachable provider (ordered roughly by Kademlia proximity) and issues a single HTTP `GET` against that peer's `:5001` transfer endpoint (§4.4) with `Gantry-Mirrored: 1`. No `Range` header in v1.
5. Bytes stream into the agent; digest is verified incrementally.
6. Agent streams to `containerd` while also writing to local cache.
7. On completion, agent calls `dht.Provide(CID)` — now this node is a provider too.

**v1 transfer policy (single-peer).** v1 ships single-peer fetch: one provider per blob, whole-blob GET. If the stream stalls (no bytes received for 10 s) or errors, the agent cancels and reissues against the next provider returned by `FindProviders`. After 3 providers fail in succession the agent gives up on the warm path and returns `5xx` to containerd, which falls through the mirror chain to origin (§7.1). Single-peer fetch is the simplest correct implementation and makes the v1 surface easy to reason about; the transfer-endpoint contract (§4.4) preserves `Range` support so v2 can add striping without a protocol change.

**v2 sketch (deferred).** Each blob is split into fixed-size chunks (proposed 4 MiB). Up to 4 providers issue `Range` requests in parallel; a chunk whose progress falls below 0.5× the median chunk-throughput is canceled and reissued against a different provider. Triggered for evaluation only if v1 cold-start convergence times for large layers (>1 GB) prove unacceptable in practice. See §8 open question on chunk-level granularity.

### 5.1a Tag reference path

Containerd resolves `image:tag` to a digest by fetching `manifests/<tag>` before any blob requests. This is the first request on every cold pull. **Gantry v1 does not handle tag resolution**: tag-shaped requests are short-circuited back to containerd's `hosts.toml` mirror chain, which falls through to origin. This eliminates any agent-layer tag→digest cache, any tag advertisement in the DHT, and the tag-rebinding coherence problem that would otherwise arise from the separate-cache decision in §7.4. The flow:

1. `containerd` requests `127.0.0.1:5000/v2/<repo>/manifests/<tag>?ns=<registry>`. The `ns=` query parameter is supplied by containerd's `hosts.toml` mirror configuration (see §7.1) and is required — without it, a bare tag reference cannot be disambiguated to an upstream registry.
2. Agent recognizes a tag-shaped manifest request and returns `503 Service Unavailable` immediately. No DHT lookup, no local-store consultation, no log at WARN level (this is the design-intended path). The response body identifies the case for diagnostics. `503` is chosen because containerd's `Resolve` loop in `core/remotes/docker/resolver.go` falls through to the next host in the `hosts.toml` chain on any response status `> 299` (with `404` special-cased but still falling through; `> 399` falls through with the error recorded for later return if all hosts fail). Verified across the `release/1.6`, `release/1.7`, and `main` (2.x) branches: any 5xx, including `503`, causes containerd to advance to the next host. The in-host retry path (`retryRequest`) differs across branches — 1.x retries only on `408` and `429`, while 2.x additionally retries on `500`/`503`/`504` — but the 2.x extension is gated on `lastHost`, so when the local agent is the first host in the chain (which it always is in this design), the in-host retry never fires and mirror-fallback happens immediately. `503` is also semantically apt: the agent at this endpoint is *not* the authority for tag resolution and is genuinely unavailable for the request, which `503 Service Unavailable` describes more accurately than alternatives like `502 Bad Gateway` (no upstream was attempted) or `500 Internal Server Error` (nothing failed internally). If a future containerd version narrows the cross-host fallback condition to exclude `503`, substitute another in-set 5xx; the choice is not load-bearing beyond “triggers mirror-fallback.”
3. Containerd's `hosts.toml` mirror chain (§7.1) promotes the request to the next host. The terminal entry is origin, which performs the tag→digest resolution and returns the manifest body. Containerd records the tag→digest binding in its own image table; the agent maintains no tag-binding state.
4. The pull then proceeds digest-keyed: containerd requests the config and each layer (and, for multi-arch images, the per-arch manifest) by digest from `127.0.0.1:5000`. Those requests flow through §5.1 / §5.2 and are F1-bounded. The agent's containerd image-event subscription (§7.3) picks up the new digests as they land in containerd's content store and advertises them in the DHT.

**Origin contact bound for tag-keyed pulls.** Every node hits origin once per pod-start for the tag-resolution step. The cost per contact is small: a single `GET /v2/<repo>/manifests/<tag>` returning the manifest body (typically 2–50 KB; ~10 KB for a multi-arch manifest list) plus one TLS handshake. **Layer bytes do not come from origin** for any node beyond the F1-bounded designated puller: the per-node manifest fetch yields a digest, the digest-keyed config and layer pulls go through Gantry, and F1 applies. For a 1000-node rollout of a 2 GB image, origin sees ~10 MB of manifest-resolution traffic plus 1 × 2 GB of layer egress (F1-bounded), not 1000 × 2 GB.

**Steady-state aggregate origin load.** Manifest-resolution traffic scales as `tag-keyed-pod-starts-per-unit-time × manifest-body-size`, with no Gantry-side aggregation, and **the cluster-wide rate does not decrease with cluster size** — adding nodes adds more pod-starts, each of which contacts origin independently for the resolution step. Two reference points to help operators place their workloads:

- *Stable workload (typical).* 10,000 pods restarting once per day produces ~10,000 manifest fetches/day ≈ ~415 fetches/hour ≈ ~4 MB/hour at ~10 KB per manifest. A trickle; origin barely notices.
- *High-churn workload.* 10,000 pods restarting once per hour produces ~10,000 fetches/hour (~100 MB/hour); restarting every 10 minutes produces ~60,000 fetches/hour (~600 MB/hour). Meaningful tag-resolution traffic; size origin capacity accordingly, or pin digests in pod specs (which avoids the tag-resolution path entirely), or wait for §8's v2 HRW-with-TTL mechanism, which is the candidate for bounding this aggregate without operator action.

The boundary between these regimes is where the operational v1/v2 tradeoff bites. Stable workloads can run v1 indefinitely with no concern; high-churn workloads should evaluate the v2 mechanism against their tolerance for TTL-bounded staleness.

**Tag-herd throttling deferred to origin.** Each node's tag-resolution retry rate is throttled by kubelet's `ImagePullBackOff` on failure. Origin's own rate-limiting / per-IP limits apply to repeated tag queries from the cluster. The agent does not add a tag-keyed negative cache or coordination layer; §5.8's negative cache applies to digest-keyed pulls only, where it bounds the cluster-wide retry burst on origin auth / not-found / rate-limited responses for content the agent is pulling.

**Availability under origin partition.** A consequence of v1's deferral: when origin is unreachable, tag-keyed pulls fail across the cluster, even on nodes whose containerd already has the tag binding cached locally (kubelet may still re-resolve depending on `imagePullPolicy`). Digest-keyed pulls — running pods, pre-pinned digests in pod specs, image-restart of already-cached images — continue to succeed peer-to-peer.

**Operator guidance for v1.** Three options for surviving origin partition on tag-keyed workloads, in order of preference:

1. *Pin digests in pod specs* (`image: registry/repo@sha256:...`). The strongest guarantee — no tag-resolution path is taken at all, and pulls succeed peer-to-peer for any digest already in the cluster cache. Recommended for critical workloads.
2. *Set `imagePullPolicy: IfNotPresent`* on workloads referencing tags. When the image is already present on the node, kubelet skips re-resolution and the pod starts from the local content store without contacting origin. **This is already kubelet's default for any tag reference other than `:latest`**, so most workloads need no operator action; the explicit-configuration recommendation applies specifically to workloads using `:latest` or otherwise setting `imagePullPolicy: Always`, which require origin reachability on every pod-start for the tag-resolution step — v1's behavior by design, not a regression. This option protects pod *restarts* on already-cached nodes but does not help fresh schedules onto nodes that have never pulled the image.
3. *Wait for the §8 v2 HRW-with-TTL mechanism*, which restores peer-served tag resolution within a bounded staleness window.

### 5.2 Cold-start path — no provider exists

The interesting case. Walked through in detail because every failure mode in §6 references this flow. **Applies to digest-keyed requests only** — manifests by digest, config blobs, and layer blobs. Tag references never enter this path; they are handled by §5.1a.

**HRW is per digest, not per image.** Each digest is coordinated independently. The flow below describes the cold-start for a single digest. Containerd requests digests in dependency order (manifest, then config, then layers in parallel), so the agent will run this flow N+2 times for a fully-cold image — possibly concurrently for the blob digests once the manifest has been parsed (see §5.2a).

**DHT-empty is not ground truth.** A `FindProviders` returning empty can mean (a) genuinely no provider, (b) the lookup was slow or timed out, (c) the local routing table is sparse (e.g., during bootstrap or after partition heal), or (d) provider records expired under DHT load before refresh. The agent does not distinguish these cases at the DHT layer; instead, it treats DHT as a fast-path optimization and uses HRW top-K probe as the authoritative cold-start arbiter (steps 3–6 below). HRW is local-only, deterministic, and unaffected by DHT health, so this routes around DHT degradation by construction.

1. `containerd` requests a manifest or blob by digest. Agent has cache miss.
2. Agent calls `dht.FindProviders(CID)`. **Hit:** proceed to §5.1 warm path. **Empty (or below quorum):** continue — do not infer cold-start yet.
3. Agent computes HRW: `score(node, digest) = SHA256(node_id || digest)` for every node in the local Kubernetes informer cache. Selects the top-K (K=3 by default) by score using a partial sort / heap (not a full sort over all nodes). The top-K are the designated pullers for **this digest** in priority order. Different digests of the same image will generally HRW to different top-K sets — that is the desired behavior.
4. Agent dials all K in parallel with `pull_intent_query(digest)`. The response carries authoritative state for that node, with field names and types matching the `PullIntentResponse` proto in §4.4: `has_cached: bool`, `in_flight: bool`, `started_at: Timestamp`, `hrw_rank: int32`, `recently_failed: bool`, `cooldown_until: Timestamp`, `failure_class: FailureClass` (enum, not string — see §4.4). The first four fields drive normal discovery; the last three carry origin-failure circuit-breaker state from §5.8 and are meaningful only when `recently_failed` is true. This RPC is the discovery mechanism; the DHT result was advisory.
5. Agent collects responses with a 2-second timeout. Constructs the set of **reachable top-K nodes**, then evaluates the response set against the following rules **in priority order** (first matching rule wins; do not fall through):
   1. **Cluster-wide failure short-circuit.** If **any** reachable node reports `recently_failed` with `failure_class` in `{auth, not_found, rate_limited}`, return 5xx to containerd immediately. These classes are cluster-wide-trusted per §5.8: asking rank-1 or polling an in-flight pull on rank-0 is futile because rank-0's pull will fail identically (`auth` and `not_found` are deterministic; `rate_limited` requires backoff regardless). This must precede the `has_cached` / `in_flight` rules below because an `in_flight` pull from a node that *also* reports a recent failure in the same response is racing toward the same `401` / `404` / `429`. Note: a node that has `has_cached` for the digest cannot simultaneously report `recently_failed` for it (the cache entry was produced by a successful pull, which clears `recent_failures[digest]` per §5.8); the priority is only relevant across *different* reachable nodes.
   2. **Cache hit.** If any reachable node reports `has_cached`, fetch from that node via the warm-path transfer (§5.1 from step 4 onward). Do not invoke `please_pull`. The DHT lookup was a false-empty; the digest exists in cluster.
   3. **In-flight piggyback.** If any reachable node reports `in_flight` with a fresh `started_at` (`now - started_at` is within the per-digest timeout from §5.2a — 5 s for manifest/config; `expected_pull_seconds × 3` for layers), **poll the local DHT** for providers of the CID at the per-digest interval (200 ms manifest/config, 1 s layer; see §5.2a). The digest is being fetched; piggyback on the puller's eventual `dht.Provide`. If `now - started_at` exceeds the per-digest timeout, treat the pull as stalled (§5.6) and exclude the reporter from `please_pull` candidates.
   4. **Transient cooldown.** If any reachable node reports `recently_failed` with `failure_class = transient`, apply the local **honor window** of `min(cooldown_until - now, 30 s)` before retrying. Do not proceed to step 6 within the honor window.
   5. **All-unreachable expansion.** If no top-K node responds within the timeout, expand the probe to top-2K and re-run step 4 against the new candidates. This rule applies regardless of DHT health score: a fully-unreachable top-K is itself a partition-or-failure symptom, and very likely a provider exists at rank-K+1..rank-2K. NF5 (§7.7) fires only if the expanded probe also fails.
   6. **Degraded-health eager expansion.** If DHT health is Degraded (§7.7) and all reachable top-K report `neither cached nor in-flight`, expand to top-2K before declaring cold-start. Under healthy DHT the honest "neither" answer is trusted; under degraded DHT it may be wrong (the top-K node may have evicted because `dht.Provide` is failing cluster-wide).
   7. **Cold-start.** Only if **all reachable top-K** report `has_cached: false`, `in_flight: false`, and `recently_failed: false`, **and** none of the expansion rules above apply, proceed to step 6. This is the only condition that justifies a true cold-start.
6. Agent selects the **lowest-ranked reachable node** and sends it `please_pull(digest)`.
7. The chosen designated puller checks its in-flight map for the digest:
   - If already pulling, responds `already_pulling(started_at)`. No-op.
   - If not, starts pulling from origin, adds to in-flight map.
8. The requesting agent polls the **local DHT** for providers of the CID at the per-digest interval defined in §5.2a, bounded by the per-digest timeout. Requesters do **not** poll the puller's `:5001` directly — see §5.2a for the rationale.
9. When the puller completes, it calls `dht.Provide(CID)` and removes the digest from its in-flight map.
10. All polling requesters discover the provider and pull via the warm path (§5.1).

**Why this matters under DHT pathology.** With DHT degraded (false-empty rate non-trivial), the previous design would conclude cold-start on every false-empty and either (a) pile up redundant `please_pull` calls or (b) eventually trip NF5 fallback and produce a cluster-wide thundering herd against origin. Under the revised flow, a false DHT-empty is caught at step 5: the top-K node almost always has the digest cached (rank-0 was the original puller and remains a provider for the lifetime of its cache entry), and the request is served peer-to-peer with no origin contact. DHT degradation now produces only a small RPC-overhead penalty, not an origin storm.

**Cost.** K extra dials on every DHT miss. With K=3 and dials in parallel, this is bounded by the 2-second `pull_intent_query` timeout and adds no serialized latency.

**Residual hole.** A digest cached only on nodes *not* currently in the top-K (e.g., a node was rank-2 yesterday but is now rank-47 because the cluster grew) is invisible to this probe. Two safeguards: (i) the eviction policy in §7.4 already defers eviction when the local node is one of few providers, so historical pullers tend to remain providers; (ii) the top-2K expansion rules in step 5 — unconditionally on all-unreachable, eagerly under Degraded DHT health — widen the probe before declaring cold-start (see §7.7).

### 5.2a Per-digest cold-start in practice

Containerd's pull sequence is fixed: manifest → config → layers (in parallel). The agent runs §5.2 once per digest as containerd asks for it, with these specifics per digest type:

- **Manifest digest.** Cold-start with a **fixed short timeout** (default 5 s) and a **local-DHT polling interval of 200 ms** while waiting for the puller's `dht.Provide` to land. Manifests are kB-scale; if the puller hasn't produced one in 5 s, treat it as stalled and run §5.6's takeover. The fixed timeout avoids a chicken-and-egg with size-aware timeouts (the size information lives in the manifest itself). "Manifest" here includes OCI manifest lists / Docker schema-2 manifest lists: the agent treats each as a separate digest, and platform selection (picking the platform-specific manifest digest after the list is fetched) happens entirely inside containerd — the agent sees the list digest and the platform manifest digest as two independent §5.2 runs.
- **Config digest.** Same as manifest — kB-scale, fixed short timeout, 200 ms local-DHT polling interval.
- **Layer digests.** Once the manifest has been received and parsed, the agent knows every layer's size. Cold-start for layer digests uses a size-aware timeout derived from layer size and a configured floor bandwidth assumption (default `expected_pull_seconds × 3`, with `expected_pull_seconds = max(10, layer_bytes / 50 MB/s)`). The local-DHT polling interval is 1 s for layers (lower frequency than manifest/config: layers take longer; the polling cost is amortized). Layer cold-starts run **in parallel** for all layers that miss the DHT — they do not serialize.

**Polling targets the local DHT, not the puller's transfer endpoint.** Each requester polls its own `dht.FindProviders(CID)` at the interval above. The puller publishes via `dht.Provide` on completion; that record propagates through the DHT and surfaces in requesters' local lookups within seconds. Requesters do **not** poll the puller's `:5001` directly — a 200 ms-interval poll across 10,000 requesters during a thundering-herd cold-start would generate ~50,000 inbound HTTP RPS at the puller, on top of its actual transfer load. The local-DHT polling cost is bounded by libp2p's per-node lookup caching and does not concentrate on any one node.

**Batched `please_pull` for layers.** Once layer digests are known, the agent fans out per-digest `please_pull` calls in parallel. As an optimization, when multiple cold-start layers all HRW to the same designated puller (which happens often when K is small relative to the cluster), the agent may send a single `please_pull([digest1, digest2, …])` carrying all such digests. The receiver's per-digest in-flight dedupe is unchanged — batching is purely a wire-level RPC reduction.

**Origin connection reuse on the puller side.** A designated puller that receives multiple `please_pull` requests for digests of the same image (manifest + config + layers, or several layers from a batched call) SHOULD reuse a single TLS connection to origin. This is a local implementation concern, not a protocol-visible behavior, but it materially reduces TLS handshake cost and cooperates with origin keepalive.

**Origin pulls per fully-cold image.** Up to N+2 origin pulls (manifest + config + N layers), each from a *potentially different* designated puller. This is the F1 invariant: one origin pull per unique digest, not one per image. For an image with 10 layers, expect ~12 distinct origin connections cluster-wide on first pull, generally distributed across ~12 different nodes. This is bounded, parallel, and load-balanced by construction; no node becomes a bottleneck.

### 5.3 Concurrent cold-start requests (thundering herd)

The exact scenario the design must handle: 10,000 nodes all want image Y in the same second.

- All 10,000 independently run §5.2 steps 1–3 **per digest**. Because HRW is deterministic and inputs (node list + digest) are identical across the cluster, **all 10,000 arrive at the same top-K for any given digest**. Different digests of the image generally produce different top-K sets, spreading the inbound RPC load across many nodes rather than concentrating on three.
- For each digest, all 10,000 dial that digest's top-3 with `pull_intent_query`. Across an image with N+2 digests, this is ~3(N+2) distinct nodes receiving RPCs from the cluster, each receiving ~10,000 inbound queries for its assigned digests. Manageable (single-RPC, no state writes), but consider rate-limiting if hot-spotting on a particular node becomes an issue.
- For each digest, all 10,000 select the same designated puller and send `please_pull`. The puller's per-digest in-flight dedupe handles this: first request starts the pull, the other 9,999 get `already_pulling` immediately.
- All 10,000 poll the DHT per digest. As each digest's puller finishes and calls `Provide`, the warm path activates per digest and the swarm distributes that digest P2P-style.

**Origin sees one pull per unique digest.** For an image with N+2 unique digests, that is N+2 origin pulls cluster-wide — not N+2 per node. This is the property the system exists to provide.

**Caveat — informer divergence (accepted limitation).** This claim assumes every agent has the same node list at the same instant. In reality, Kubernetes informer caches lag during membership changes (rolling node addition/eviction); the convergence window is **expected** to be <5 s in typical environments based on Kubernetes informer behavior, but this should be validated empirically at scale. When agents disagree on the node set, their HRW rankings differ and multiple designated pullers may be selected concurrently for the same digest. Receiver-side dedupe bounds the damage at each puller, but cross-puller dedupe does not exist — so the property degrades from "one origin pull" to "a small number, bounded by the number of distinct rank-0 selections across divergent views." The **anticipated** bound during a rolling-update window is ≤3 origin pulls per affected digest (hypothesis pending scale validation; the requester compares its own HRW computation to each `PullIntentResponse.hrw_rank` reported by the recipient and emits `p2p_hrw_rank_mismatch_total` when they disagree, giving operators a direct view of the actual divergence rate). The design accepts this as a known limitation rather than introducing a synchronized membership protocol (which would be exactly the kind of side-channel coordinator the design forbids — see §2 non-goals). F1 reflects this with the "small constant" wording.

### 5.4 Designated puller has no local demand

The case where rank-0's HRW says "you pull this digest," but no pod on rank-0 is asking for the digest.

This is handled automatically by §5.2 step 6: the requesting node sends an explicit `please_pull` RPC to rank-0. Rank-0 starts pulling **even though it has no local pod demand for that digest**, because it has been explicitly asked to. After completion it serves the content to peers; it can later evict the content if it remains the only consumer (see §7.4).

### 5.5 Designated puller is down or unreachable

`pull_intent_query` to rank-0 (for this digest) times out within 2 seconds.

- The requesting agent's "reachable top-K" set excludes rank-0.
- Rank-1 becomes the lowest-ranked reachable node for this digest.
- `please_pull` goes to rank-1. Rank-1 starts pulling.

The takeover is **bounded by the dial timeout** (~2s), not by content size. This is the central reason for fan-out over single-target dial.

### 5.6 Designated puller stalls mid-pull

The puller responded "starting" but its origin connection is now hung. Provider record never appears in the DHT.

- Requesters polling the DHT eventually hit their per-digest max-wait timer (the timeout values from §5.2a are the concrete "implausibly long" thresholds: 5 s for manifest/config, `expected_pull_seconds × 3` for layers).
- Each requester re-runs §5.2 from step 3 **for that digest only**. The stall is per-digest; other digests of the same image are unaffected and may be progressing normally on different designated pullers.
- The new `pull_intent_query` may still see rank-0 as alive (TCP-wise), but its response includes the in-flight state with `started_at` — if `(now - started_at)` exceeds the same per-digest timeout from §5.2a, requesters treat the pull as stalled and exclude rank-0 from `please_pull` candidates.
- Rank-1 receives `please_pull`, starts pulling from origin in parallel with rank-0's stalled attempt.
- Whichever finishes first calls `Provide`. The slower one eventually completes (or times out) and also calls `Provide`. No correctness issue.

Origin sees two pulls of that digest. Acceptable price for liveness under failure.

### 5.7 Network partition

Cluster splits into partition A and partition B. Rank-0 is in A; some requesters are in B.

- Partition A: rank-0 is reachable; behaves like §5.2.
- Partition B: rank-0 is unreachable. The reachable top-K (from B's view of cluster membership) excludes rank-0. The lowest-ranked reachable node — say rank-1 in B's view — becomes the designated puller for partition B.
- Both partitions make independent progress. Origin sees two pulls.
- When the partition heals, both rank-0 and rank-1 are providers. The DHT merges naturally; subsequent requesters find both.

**Liveness is preserved at the cost of one extra origin pull per partition.** Acceptable.

**Negative cache after partition heal.** Negative cache entries (§5.8) are puller-local and do not merge across partitions. After heal, each former-puller carries its own `recent_failures` history, applied only to digests on that puller. There is no cross-partition reconciliation; the worst case after heal is that a digest-and-puller pair carries a stale cooldown that delays the next attempt by at most the configured cooldown ceiling (default 10 min). Acceptable.

**Eviction during partition.** §7.4's eviction-deferral logic queries the local DHT for provider count, which during partition reflects only the asking node's partition view. From partition B's perspective, only B-side providers count; the deferral may be over-conservative (under-evicts when partition A independently holds many copies) or under-protective (over-evicts toward the §7.4 forced-eviction headroom when B-side replication is sparse). After heal, DHT records from both partitions converge and provider counts re-stabilize within the TTL refresh interval; operators may observe a transient eviction-rate spike around partition events as deferral decisions re-evaluate against the merged provider set. The 5%-headroom escape hatch (§7.4) backstops any pathological under-eviction during the partition itself.

### 5.8 Origin is down or rejecting pulls

If origin is unreachable or returns errors, the designated puller's pull fails. After exhausting retries, it must avoid both (a) returning failure indefinitely while origin recovers, and (b) allowing every subsequent requester to re-trigger a fresh designated-puller cascade against a known-broken origin. The puller therefore maintains a **per-digest negative cache** with circuit-breaker semantics.

**Failure classification.** On terminal failure of an origin pull, the puller classifies the cause:

- `auth` — 401/403 from origin. Credentials are wrong or revoked. Same credentials will fail identically on every node, so the failure is cluster-relevant.
- `not_found` — 404 from origin. The digest does not exist at the configured upstream registry. Cluster-relevant: rank-1 will get the same answer.
- `rate_limited` — 429 from origin. Origin is back-pressuring; respect it.
- `transient` — connection refused/reset, 5xx, timeout, DNS failure. May be intermittent; may be flapping.

**Negative cache structure (puller-local, in-memory):**

```
recent_failures[digest] = {
    last_failure: time,
    failure_count: int,
    failure_class: "auth" | "not_found" | "rate_limited" | "transient",
    cooldown_until: time,
}
```

**Cooldown schedule (exponential, capped):** 1st failure → 10 s, 2nd → 30 s, 3rd → 2 min, 4th+ → 10 min cap. The first successful pull of the digest clears the entry. Configurable knobs in §7.7.

**Signal propagation via the existing probe RPCs (no new RPCs).** While a digest is in cooldown, the puller's responses change:

- `pull_intent_query(digest)` returns `{has_cached: false, in_flight: false, recently_failed: true, cooldown_until: T, failure_class: X, hrw_rank: R}`.
- `please_pull(digest)` returns `recently_failed(cooldown_until=T, failure_class=X)` instead of starting a pull.

**Requester behavior on `recently_failed`.** When a requester receives `recently_failed` from any reachable top-K node during the §5.2 step 5 probe:

- `auth`, `not_found`: trust cluster-wide. Asking rank-1, rank-2, or any other node is futile — same credentials and same digest produce the same answer everywhere. Requester returns 5xx to `containerd` immediately. No `please_pull` to anyone for this digest.
- `rate_limited`: trust cluster-wide. Requester returns 5xx; `kubelet`'s exponential retry naturally surfaces a new attempt later, after origin's rate window has likely reset.
- `transient`: trust per-digest, but apply a local **honor window** of `min(cooldown_until - now, 30 s)` before sending `please_pull` for this digest to *any* top-K node. A flapping origin will fail rank-1 the same way it failed rank-0; sequential retries within the cooldown window only generate origin pressure. After the honor window expires, the requester re-probes; by then rank-0's own cooldown may also have expired and the next attempt is single-shot.

**Cluster-wide effect.** Origin pull rate is bounded to roughly **one attempt per cooldown interval per affected digest cluster-wide**, regardless of how many requesters are waiting. Under a sustained outage, that is ≤ 6 origin attempts/hour for a given digest at the 10-min cooldown ceiling, no matter how many pods want it.

**Self-healing.** When origin recovers, the next attempt after cooldown succeeds. The puller clears `recent_failures[digest]`, calls `dht.Provide`, and serves peers via the warm path. No operator action required.

**Why the negative cache is local-only, not propagated via DHT.** Same rationale as gap #4's fix (§7.7): a stale cluster-wide "this digest failed" marker that outlived an actual recovery would be a serious bug. The puller-plus-honor-window pair already bounds the cascade adequately without introducing eventual-consistency hazards.

**For images that have never been pulled into the cluster** and where origin is also down, the agent returns 5xx to `containerd`. `kubelet` retries naturally on its own backoff. The system does not invent content it cannot fetch.

**Distinguishing puller failure from origin failure.** A puller that has crashed or is unreachable returns no `recently_failed` response — the requester sees a TCP-level timeout, and the existing §5.5 (puller down) takeover applies, routing to rank-1. The negative cache is only consulted when the puller is alive and able to respond. This separation is intentional: puller failures should reroute; origin failures should back off.

### 5.9 Node joins / leaves cluster

- **Join:** the agent starts up, the Kubernetes informer reports the new node to all other agents within a few seconds, HRW rankings update naturally. The new node bootstraps its libp2p host using peers from the informer's existing pod list and joins the DHT.
- **Leave:** existing provider records held by the departed node expire from the DHT (TTL, default 24h with 12h refresh). HRW rankings update on all surviving agents as the informer removes the departed node. If the departed node was a designated puller for an in-flight pull, the stall-detection path in §5.6 recovers.

---
