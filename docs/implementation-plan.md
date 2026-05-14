# Gantry — Implementation Plan

**Status:** Draft, derived from [archecture.md](archecture.md) and [detailed-design.md](detailed-design.md).
**Scope:** Phased engineering plan to build the v1 P2P agent described in the design docs.

---

## Summary

A Go-based Kubernetes DaemonSet (`gantry`) that:

- Serves containerd's mirror endpoint on `127.0.0.1:5000` (OCI Distribution API).
- Serves peers on `0.0.0.0:5001` (HTTP/2 OCI subset, `Gantry-Mirrored` peer-fetch mode).
- Speaks coordination RPCs over libp2p (`/gantry/coord/1.0.0`, length-prefixed protobuf).
- Joins a cluster-scoped Kademlia DHT in server mode for digest discovery.
- Computes per-digest HRW over a Kubernetes informer view of cluster membership.
- Maintains an LRU `hostPath` cache with provider-count-aware eviction.

## Tech stack (forced by the spec)

| Concern | Choice | Why |
|---|---|---|
| Language | Go | go-libp2p, containerd client, K8s informer all native |
| libp2p | `github.com/libp2p/go-libp2p` + `go-libp2p-kad-dht` | spec calls these out by name |
| Wire framing | `github.com/libp2p/go-msgio` length-delimited | spec §4.4 explicitly rejects gRPC |
| Protobuf | `google.golang.org/protobuf` (`protoc-gen-go`) | schema in `proto/gantry/coord/v1/coord.proto` |
| HTTP server | `net/http` + `golang.org/x/net/http2` h2c on `:5001` | h2c for in-cluster traffic |
| K8s client | `k8s.io/client-go` informer over `Pods`/`Nodes` | §7.3 |
| Containerd | `github.com/containerd/containerd/v2/client` | image-event subscription §7.3 |
| Metrics | `prometheus/client_golang` | §7.6 metric names dictated |
| Tests | stdlib + `testcontainers-go` for containerd integration | — |

## Proposed repo layout

```
cmd/gantry/                 main entrypoint, flag/env wiring
internal/agent/             top-level Agent struct + run loop
internal/config/            typed config struct + flag/env/file parsing
internal/log/               structured logger setup (slog) + per-subsystem fields
internal/digest/            OCI digest type, parsing + validation
internal/ifaces/            cross-cutting interfaces (Cache, Members, OriginPuller, …)
internal/ifaces/fakes/      in-memory fakes for tests
internal/mirror/            :5000 containerd-facing OCI server
internal/transfer/          :5001 peer-facing OCI server (Gantry-Mirrored)
internal/cache/             hostPath content store, LRU, replication-aware eviction
internal/coord/             libp2p stream handler, pull_intent_query / please_pull
internal/discovery/         DHT lookup + advertise, health score, NF5 gating
internal/hrw/               rendezvous hashing + top-K probe orchestration
internal/origin/            origin-pull client, negative cache (§5.8)
internal/members/           K8s informer + node view (incl. zone label)
internal/cdsub/             containerd image-event subscription
internal/digestpipe/        digest-verifying stream tee (containerd ↔ peer ↔ cache)
internal/inflight/          per-digest in-flight pull map (puller + requester)
internal/metrics/           Prometheus registry + per-subsystem registration helpers
proto/gantry/coord/v1/      coord.proto + generated bindings
deploy/                     DaemonSet manifest, Dockerfile, hosts.toml, NetworkPolicy
test/                       integration suites (kind-based)
```

---

## Phase 0 — Skeleton & wire schema (✅ landed)

- `go mod init`, set up linting (`golangci-lint`), `Makefile`.
- Write `coord.proto` from §4.4 verbatim; generate Go bindings; commit them.
- Define internal interfaces: `Cache`, `Members`, `OriginPuller`, `PeerDialer`, `DHT`, `Coordinator`. Wire them up with fakes so tests don't need a real cluster.
- Introduce typed `Digest`, typed `OriginError` with `FailureClass`, and typed `ErrNotFound` — required by §5.8 propagation and §5.1 fail-over distinction; landed in Phase 0 rather than retrofitted.
- CI: build + unit tests + `proto-check` (regenerate and diff `.pb.go`).

## Phase 0.5 — Cross-cutting infrastructure

These are cross-cutting concerns the design implies but never names in one place; they need to exist before Phase 1 starts wiring real subsystems together.

- `internal/config`: a single typed `Config` struct fed from flags + env + (optionally) a YAML file. Schema covers every operator-tunable knob the design enumerates:
  - **Cache:** `cache_dir`, `cache_budget_bytes` (default 50 GB), `cache_forced_eviction_headroom_pct` (§7.4 default 5%), `eviction_provider_count_threshold` (default 3).
  - **Origin / upstream:** `upstream_registries[]` — each with `name`, `endpoint`, `credentials_path`, optional `ns_alias`. Drives both the mirror's `?ns=<registry>` routing (§7.1) and the cdsub event filter (§7.3).
  - **HRW / coordination:** `hrw_k` (default 3, §8 open question), `hrw_topology_scope` (`cluster` | `zone`, §4.3 / §8 open question), `zone_label_key` (default `topology.kubernetes.io/zone`).
  - **DHT / NF5:** `nf5_jitter_base` (3 s), `nf5_per_node_rate_limit` (2/min), `bootstrap_window` (30 s), `bootstrap_routing_table_pct` (25%), `topk_expansion_factor_degraded` (2).
  - **§5.8 origin-failure circuit breaker:** `origin_failure_cooldown_initial` (10 s), `origin_failure_cooldown_max` (10 min), `origin_failure_cooldown_multiplier` (3×), `origin_failure_honor_window_cap` (30 s), `origin_failure_classes_trusted_cluster_wide` (default `{auth, not_found, rate_limited}`).
  - **Listeners:** `mirror_listen` (`127.0.0.1:5000`), `transfer_listen` (`0.0.0.0:5001`), `metrics_listen`, libp2p listen / identity-key paths.
- `internal/log`: thin wrapper over `log/slog` with JSON handler in production; structured fields `subsystem`, `digest`, `peer`, `registry`, `repo` standardised. Log-level conventions documented inline: WARN for forced-eviction (§7.4) and HRW-rank-mismatch (§5.3); INFO for state transitions; DEBUG for per-RPC traces.
- `internal/metrics`: a thin registration helper so subsystems land their metrics in their own phase. Phase 0.5 introduces:
  - A package-private Prometheus `Registerer` and `/metrics` handler.
  - Constructor helpers (`NewCounter`, `NewGaugeFunc`, …) that record metric name → subsystem mapping so Phase 6 can audit completeness.
  - No metric instruments yet — they are owned by the subsystem that emits them.

**Exit criterion:** `cmd/gantry --help` prints the full configuration surface; a `--config` flag accepts a YAML file; the logger emits one structured INFO line on startup with the parsed config (secrets redacted).

## Phase 1 — Single-node mirror + cache (no P2P yet)  (✅ landed)

- Implement `internal/cache`: content-addressed `hostPath` layout, write-then-rename for atomicity, digest-verifying writer. **Eviction policy in Phase 1 is a simple bounded LRU** at `cache_budget_bytes`; provider-count deferral (§7.4) lands in Phase 6 when the DHT exists. Document the v1-Phase-1 vs v1-Phase-6 cache semantics inline.
- Implement `internal/origin`: OCI client. Credentials sourced per-upstream-registry from `credentials_path` (Phase 0.5 config). Multi-registry support is real from day one: every request carries a resolved `OriginRef{registry,repository,digest}`.
- Implement `internal/mirror` on `:5000` (containerd-facing, loopback only):
  - Parse the `?ns=<registry>` query parameter from hosts.toml's `server =` directive (§7.1); reject requests with no `ns` once the upstream-registry list has more than one entry. With a single configured registry, `ns` is optional and defaults to it.
  - `GET /v2/` → 200.
  - `GET /v2/<repo>/blobs/sha256:<hex>` → cache hit serves; miss → origin → tee through digest-verifying writer into cache and response (§F7).
  - `GET /v2/<repo>/manifests/sha256:<hex>` → same.
  - `GET /v2/<repo>/manifests/<tag>` → **503** immediately (§5.1a is load-bearing — implement first and test against real containerd). No log at WARN level; this is the design-intended path.
  - `GET /healthz` → 200.
- Integration test: real `containerd` + `hosts.toml` (`server = ...` + `[host."http://127.0.0.1:5000"]` + `capabilities = ["pull","resolve"]`) pointing to the agent; verify tag-fallback 503 promotes to origin and the `?ns=` parameter is present on the digest-keyed requests we receive. **Deferred (cross-platform):** the Phase 1 unit tests use `httptest`-driven OCI upstream fixtures covering tag 503, multi-upstream `?ns=` rejection/match, blob/manifest cache-miss-and-stream, HEAD, failure-class mapping, and digest-mismatch abort. A real-containerd-in-Linux-VM integration test will land alongside the Phase 2 2-node `kind` warm-path scenario so both can share the same test infrastructure.
- **Metrics owned by this phase** (register in Phase 0.5's helper):
  - `p2p_cache_hit_total` / `p2p_cache_miss_total` (`internal/cache`).
  - `p2p_origin_pull_total{kind="manifest|config|layer"}` (`internal/origin`).
  - `p2p_origin_failure_total{class=}` (`internal/origin`).

**Exit criterion:** single-node containerd successfully pulls images via the agent; tag requests fall through; layer/manifest digest pulls cache locally; metrics increment as expected.

## Phase 2 — libp2p host, DHT, peer transfer (warm path)

- Bring up `go-libp2p` host with TCP+QUIC+Noise (§7.2).
- DHT in **server mode**; persistent libp2p identity on `hostPath`.
- Implement `internal/members`: K8s informer over DaemonSet pods (label-selector configurable); produce a stable hashable identity per node; surface `topology.kubernetes.io/zone` (`zone_label_key` from config) in `Node.Zone` so Phase 3's HRW can scope by zone without re-reading the informer.
- Bootstrap: random 8 peers → dial in parallel; if fewer than 4 respond within 5 s, draw another random subset of 8 from the remaining pool; total dials capped at 32 (§7.2). After cap, proceed with whatever routing-table state we have.
- Implement `internal/transfer` on `:5001` (peer-facing, h2c):
  - Same OCI subset; `Range: bytes=N-M` returns `206 Partial Content` with `Content-Range` even though v1 callers always fetch whole blobs (contract preserved for v2 striping).
  - Require `Gantry-Mirrored: 1` for peer-fetch semantics: serve only from local store, 404 on miss, never recurse into DHT lookup / HRW probe / `please_pull` / origin contact.
  - Tag-shaped manifest requests → **404** unconditionally (peer endpoint differs from mirror endpoint — see §4.4).
  - Increment `p2p_peer_serve_total` (not `p2p_cache_hit_total`).
- Implement `internal/discovery`: `dht.FindProviders(CID)` + `dht.Provide(CID)`; CID derivation `Multihash(sha256, raw_digest_bytes)` documented in code; expose a `Health()` stub returning `1.0` (real health math lands in Phase 5).
- Wire the mirror's blob/manifest miss path to: DHT lookup → pick reachable provider → HTTP/2 GET against `:5001` with header → digest-verify → cache → respond. Fail over across up to 3 providers; stall = 10 s no-bytes; on exhaustion return 5xx so containerd falls back via hosts.toml.
- Re-announce all cached digests via `dht.Provide` on startup (cache directory walk).
- Implement `internal/cdsub` containerd image-event subscription (§7.3):
  - Subscribe to `Image/Create`, `Image/Update`, `Image/Delete` filtered to the configured upstream-registry list.
  - On `Create` / `Update`: walk the manifest tree (incl. multi-arch lists → per-arch manifests) and `dht.Provide` each resulting digest.
  - On `Delete`: no immediate agent-side action; rely on cache eviction (Phase 6) for cleanup.
  - **Stream-loss recovery:** reconnect with exponential backoff; on reconnect run a full reconciliation against `ImageService().List(ctx)` and re-advertise every digest currently in containerd's content store.
  - Document and accept duplicate `Provide` from self-events (intentionally not deduped; §7.3).
- NetworkPolicy stub committed to `deploy/`: `:5001` + libp2p ports inter-node only. Full manifest lands in Phase 6.
- **Metrics owned by this phase:** `p2p_peer_serve_total`, `p2p_peer_dial_success_total` / `p2p_peer_dial_failure_total`, `p2p_dht_lookup_duration_seconds`, `p2p_dht_lookup_total{outcome=}`, `p2p_dht_advertise_total`.

**Exit criterion:** 2-node kind cluster — node A pulls cold, node B's pull is served by node A via DHT discovery, with zero origin contact on B; killing and restarting the containerd event stream on either node leaves the DHT correctly populated after reconcile.

## Phase 3 — HRW, coordination RPCs, cold-start path

- Implement `internal/hrw`: `score = SHA256(node_id || digest)`, partial-sort top-K via heap (do not full-sort 10k entries). Byte ordering and concatenation rules documented inline so all agents compute identical scores.
- **Topology-aware HRW** (§4.3, §8 open question): when `hrw_topology_scope == "zone"`, the candidate set is filtered to nodes in the requester's zone before scoring. When `"cluster"` (default), the full membership view is used. The toggle is a single config knob; both modes share the same scoring code.
- Implement `internal/inflight`: per-digest in-flight map shared between the puller-side `please_pull` handler and the requester-side piggyback path. Tracks `{started_at, expected_class, expected_size}` and exposes:
  - `Start(digest) (Handle, alreadyPulling bool)` for the puller side.
  - `LookupForIntent(digest) (started_at, in_flight bool)` for the puller answering its own `pull_intent_query`.
  - `IsStale(digest, now)` for the requester-side §5.6 stall check.
- Implement `internal/coord` libp2p stream handler under protocol ID `/gantry/coord/1.0.0`:
  - One stream per request/response pair; close after reply.
  - Dispatch via `Envelope.oneof`.
  - `pull_intent_query`: report `has_cached` (from cache), `in_flight` + `started_at` (from `internal/inflight`), this node's own `hrw_rank` in **its** view of membership, plus §5.8 fields when the negative-cache entry is present.
  - `please_pull`: validates the single-repo-per-batch invariant from §4.4; per-digest receiver-side dedupe via `inflight.Start`; returns `STARTED` / `ALREADY_PULLING` / `RECENTLY_FAILED` per result.
- Cold-start orchestrator (§5.2 step-by-step):
  1. Cache miss → DHT FindProviders.
  2. On empty: compute top-K (K=`hrw_k`, default 3), parallel `pull_intent_query` with 2 s timeout.
  3. **Wait for all K responses or 2 s timeout, then evaluate** — the rule cascade is not eagerly evaluable as responses arrive because the failure-short-circuit rule must observe all reachable responses before declaring cache-hit a winner.
  4. Evaluate **in priority order** the 7 rules from §5.2 step 5. Implement as `match() { case failure_short_circuit; case cache_hit; case in_flight; case transient_cooldown; case all_unreachable_expand; case degraded_expand; case cold_start }` — first match wins, no fall-through.
  5. Lowest-ranked reachable node receives `please_pull`.
  6. Poll local DHT at per-digest interval (200 ms manifest/config, 1 s layers); not the puller.
- Per-digest timeouts (§5.2a): 5 s for manifest/config; `max(10, layer_bytes/50MB/s) × 3` for layers.
- Batched `please_pull` for layers HRW'ing to the same puller; puller-side TLS reuse to origin (single `http.Client` keyed by upstream-registry).
- **Metrics owned by this phase:** `p2p_hrw_rank_mismatch_total{digest_kind=}` (with WARN log carrying `recipient_node` unconditionally per §7.6), `p2p_dht_false_empty_total`, `p2p_topk_probe_hit_total`, `p2p_in_flight_pulls`, `p2p_cold_start_duration_seconds`.

**Exit criterion:** 10-node kind cluster, fresh image: origin sees exactly `N+2` pulls (≤3 under induced informer skew), with pullers distributed across distinct nodes (verified by metrics).

## Phase 4 — Failure handling (§5.5, §5.6, §5.7, §5.8)

- Stall detection: in-flight pulls with `now - started_at > per_digest_timeout` are treated as stalled; rank-1 takeover.
- §5.8 negative cache (puller-local, in-memory):
  - Classifier: `auth` (401/403), `not_found` (404), `rate_limited` (429), `transient` (everything else terminal).
  - Cooldown 10 s → 30 s → 2 min → 10 min cap (all from config). Cleared on first success.
  - Propagation via existing RPCs only (no new RPCs).
  - Requester rules: failure classes in `origin_failure_classes_trusted_cluster_wide` → 5xx immediately, no `please_pull`. `transient` → `min(cooldown_until - now, origin_failure_honor_window_cap)` honor window.
- Partition behavior: nothing to code beyond making sure HRW uses *reachable* view; document negative-cache local-only invariant.
- **Metrics owned by this phase:** `p2p_negative_cache_entries`, `p2p_negative_cache_hit_total{class=}`, `p2p_designated_puller_takeover_total`.

**Exit criterion:** failure-injection tests for each row in §6's failure-modes table, verifying cluster-wide origin pull rate stays inside the stated bound.

## Phase 5 — DHT health gating + NF5 fallback

- `internal/discovery/health.go`: rolling routing-table coverage, p95 lookup latency over 5 min, 60 s self-test (`Provide(self_id)` → bootstrap-peer `FindProviders(self_id)`); geometric mean → `p2p_dht_health_score`.
- States Healthy / Degraded / Unhealthy with thresholds 0.7 / 0.3.
- Eager top-2K expansion under Degraded + always under all-top-K-unreachable (factor from `topk_expansion_factor_degraded`).
- Bootstrap-window suppression: first `bootstrap_window` (30 s) OR routing table < `bootstrap_routing_table_pct` (25%).
- NF5 direct-origin fallback (last resort): jitter `[0, nf5_jitter_base × ln(N))`, per-node token bucket `nf5_per_node_rate_limit`, ≤1 in-flight per digest, re-check DHT+probe after jitter (cancel if found).
- **Metrics owned by this phase:** `p2p_dht_health_score`, `p2p_origin_fallback_total`, `p2p_topk_expansion_total{reason=}`.

**Exit criterion:** chaos test that kills the DHT (drops UDP between most nodes) — origin contact does not spike; top-K probe carries the load; `p2p_origin_fallback_total` stays near 0.

## Phase 6 — Cache lifecycle, deployment, ops surface

- Upgrade `internal/cache` from Phase 1's plain bounded LRU to the full §7.4 policy:
  - LRU at layer granularity; provider-count deferral (`eviction_provider_count_threshold`, default 3) via `dht.FindProviders` with a short-interval local cache of the count to avoid eviction-time DHT storms (§8 open question).
  - Forced-eviction headroom: free disk < `cache_forced_eviction_headroom_pct` of budget → evict regardless; emit `p2p_cache_forced_eviction_total` with WARN log carrying CID and provider count.
- **Final metric audit:** verify all 19 names from §7.6 are registered by some subsystem; close any remaining gaps. Phase 6 owns only the audit + the `/metrics` HTTP endpoint binding (`metrics_listen`).
- **Graceful shutdown:** SIGTERM handler in `internal/agent` drains in-flight transfers (bounded grace), stops accepting new mirror requests with `503`, flushes DHT `Provide` for any newly committed entries, then closes the libp2p host and informer.
- **Container image + build pipeline:** `deploy/Dockerfile` (multi-arch, scratch or distroless), `deploy/build.sh` for local image build, image tag derived from `git describe`. Image registry choice is operator-supplied via Make variable.
- Deployment artifacts in `deploy/`:
  - DaemonSet with `hostPath` mounts for cache + libp2p key; `hostNetwork: false`; resource requests within NF6 budget pending validation; Secret mount for per-registry credentials.
  - `hosts.toml` template (§7.1) with `server = "https://..."` + `[host."http://127.0.0.1:5000"]` + `capabilities = ["pull","resolve"]` + `skip_verify = true` (loopback only).
  - NetworkPolicy restricting `:5001` and libp2p ports to inter-node.
- `/healthz` includes liveness (process up) + readiness (informer synced, DHT bootstrapped, cache scan complete).
- **Metrics owned by this phase:** `p2p_cache_forced_eviction_total` (the only metric inherently tied to Phase 6's eviction policy).

---

## Testing strategy

- **Unit:** cache, HRW, classifier, priority-ordered rule matcher (table-driven), negative-cache cooldown ladder, health-score math.
- **Component:** coord-stream protocol round-trip against a fake peer; mirror server against a recorded containerd HTTP trace.
- **Integration (kind):** 2-node warm path, 10-node cold start, induced partition, induced informer skew, origin returning 401/404/429/5xx, DHT degradation by dropping UDP.
- **Scale (deferred):** ≥1k-node synthetic cluster for §8 open questions on K, RAM budget, inbound-RPC fan-in.

---

## Cross-cutting risks (tracked from §8)

1. NF6 RAM budget — likely under-stated for server-mode DHT at 10k nodes; validate before committing.
2. Eviction-time DHT lookups become a load source — needs local cached estimate.
3. `please_pull` abuse surface (untrusted in-cluster — single namespace assumption) — `OUTCOME_DECLINED` reserved in proto, leave field tag `4` reserved.
4. Per-node credentials break §5.8 cluster-wide-trust assumption — v2 only; document in code where assumed.
5. Inbound `pull_intent_query` fan-in during thundering herd — needs scale validation.
