# ACR counting-proxy demo plan

Status: **proposed**. Owner: TBD. Target: demo-only; not a production
deployment artifact.

## 1. Goal

Produce a reproducible, same-minute origin-load comparison between a
baseline AKS image pull and Gantry-mediated pulls — using a real Azure
Container Registry (ACR) as the storage backend — by inserting a small
counting reverse proxy directly in front of ACR. The proxy is the
authoritative origin-side counter for the demo; ACR-side analytics
(`ContainerRegistryRepositoryEvents`) are used only for after-the-fact
verification.

The demo proves Gantry's F1 invariant (≤ a small constant origin
contacts per unique digest cluster-wide, see
[archecture.md](archecture.md) §Requirements) on a live registry without
depending on ACR throttling, ACR rate-limit headers, or the multi-minute
delay of Azure Log Analytics.

### 1.1 Non-goals

- Production-grade auth, HA, or scaling for the proxy.
- Caching, deduplication, or pull-through behavior in the proxy. **The
  proxy must be a counting passthrough only.**
- Replacing ACR throttling as a real-world stressor. Synthetic
  back-pressure in the proxy (§7) is explicitly labeled as such on
  screen and in the recording.
- **Any modification of existing Gantry code or build files.** All
  demo artifacts — the proxy Go binary, the harness, `hosts.toml`
  templates, Kubernetes manifests, and the local Makefile — live
  under a single isolated `deploy/demo/` subtree (§4). The proxy and
  harness each have their own `go.mod`; the demo never touches the
  root [go.mod](../go.mod), the root [Makefile](../Makefile), the
  production [deploy/](../deploy/) manifests, or any package under
  `internal/`, `cmd/`, or `e2e/`. Deleting `deploy/demo/` fully
  reverts the demo — there is no demo state anywhere else in the
  repo.

### 1.2 Headline numbers the demo must produce

For a 20-node Job pulling a fresh ~1 GB compressed image with ~32
unique digests. Digest-class and tag-class proxy traffic are reported
separately because Gantry v1 routes only digest-keyed requests through
the P2P layer; tag resolution always reaches origin
([archecture.md](archecture.md) F9), so even a perfectly-cached warm
run shows some `manifest_by_tag` traffic at the proxy.

| Metric                          | Baseline via proxy | Gantry cold-start | Gantry warm-cache |
| ------------------------------- | -----------------: | ----------------: | ----------------: |
| Origin **digest** requests (`blob`+`manifest_by_digest`) | ~640–900 | ~32–96 | **0** |
| Origin **digest** bytes         |             ~20 GB |           ~1–3 GB |             **0** |
| Origin **tag** requests (`manifest_by_tag`) | ~20–60 |          ~20–60 |          ~20–60 |
| Gantry peer hits (`p2p_peer_fetch_total{outcome="hit"}`) | n/a | ~600 | 0 |
| Gantry cache hits (`p2p_cache_hit_total`) | n/a |             some |              ~640 |
| Pod start P50/P95/P100 (secondary; see §4.1 / §8) | measured | measured | measured |
| Synthetic 429s (optional, §7)   |        likely/high |                 0 |                 0 |

The headline proof is the **digest rows**. The tag row is included for
honesty; it should be roughly constant across the three phases and is
not evidence of demo failure.

These come straight from the proxy's `/metrics` and Gantry's existing
`p2p_*` counters (verified against
[cmd/gantry/main.go](../cmd/gantry/main.go) — see §4.3); no new Gantry
instrumentation is required.

---

## 2. Architecture

### 2.1 Baseline path

```text
AKS node containerd
  → counting proxy (in-cluster Service)
  → ACR (https://<acr>.azurecr.io)
```

containerd's `hosts.toml` for the demo registry hostname points its
first `[host."..."]` entry at the proxy Service. No Gantry on the node.

### 2.2 Gantry path (cold and warm)

```text
AKS node containerd
  → 127.0.0.1:5000 (local Gantry mirror)
  → Gantry origin client
  → counting proxy (in-cluster Service)
  → ACR
```

`hosts.toml` reverts to the Gantry default (the
[hosts.toml.template](../deploy/hosts.toml.template) shape). Gantry's
[`UpstreamRegistry.Endpoint`](../internal/config/config.go) for the
demo ACR is patched to the proxy Service URL instead of
`https://<acr>.azurecr.io`. The proxy attaches ACR credentials upstream,
so Gantry runs with `credentials_path: ""` for this registry in the
demo path.

### 2.3 Why the proxy sits in front of ACR for **both** scenarios

The comparison is only meaningful if the counter is the same instrument
in both runs:

```text
Same ACR.        Same image.       Same nodes.
Same proxy.      Same /metrics.    Only the pull path changes.
```

containerd's per-registry `hosts.toml` mirror chain is the documented
control point for swapping the pull path without touching workloads or
pod specs.

### 2.4 What the proxy is forbidden from doing

- **No caching.** Not in-memory, not on disk, not via HTTP
  conditional-request collapsing. Each upstream byte must be billed
  once per client request.
- **No pull-through registry behavior.** Do not run distribution-spec
  registries (Harbor, Distribution, etc.) configured as pull-through
  caches.
- **No request dedup / single-flight.** Two simultaneous identical
  requests must produce two upstream requests.
- **No response rewriting** beyond what HTTP requires (e.g. `Host`,
  `Authorization`).

Counting integrity is the single load-bearing property; any of the
above silently breaks it.

### 2.5 Auth identity for both paths

Once the proxy is inserted, **the proxy is the only client that
authenticates to ACR**. AKS-side identities (kubelet MSI / managed
identity, ACR-attached pull credentials, per-namespace pull secrets)
are not the ACR identity for either demo path:

```text
Baseline:        containerd → proxy → ACR        ← proxy holds the ACR creds
Gantry cold:     containerd → Gantry → proxy → ACR ← proxy holds the ACR creds
Gantry warm:     containerd → Gantry (cache) → never reaches proxy/ACR for digests
```

This is intentional and **makes the two paths symmetrical**: the same
credential, the same upstream identity, the same audit-log subject in
ACR. The demo cannot accidentally compare "kubelet identity vs. Gantry
identity" because neither is actually used. Recording narration should
name this explicitly so viewers don't assume kubelet MSI is in play.

---

## 3. Counting-proxy specification

### 3.1 Language and shape

A small Go binary under `deploy/demo/acr-origin-proxy/` — single
`main.go` plus its test file, **and its own `go.mod`** so the proxy
is built and tested as a standalone module without touching the root
[go.mod](../go.mod). Reasons:

- Exact byte and request counting per OCI path class is easier in
  `net/http` middleware than in nginx config.
- Same Go toolchain (Go ≥ 1.26 per the root
  [go.mod](../go.mod)) and standard Docker conventions, but the
  module boundary keeps the demo deletable as a single subtree (§1.1).
  Cross-module imports from `github.com/gantry/gantry/...` are
  forbidden — if the proxy needs a helper, copy or rewrite it under
  `deploy/demo/`. Production Gantry never imports anything from the
  demo either.
- One container image, distroless, < 20 MB.

### 3.2 Request flow

1. Listen on `:5002` (plain HTTP) for in-cluster traffic.
2. For each request:
   1. Classify path (§3.4).
   2. Increment `origin_requests_started_total{method,path_class}`
      and `origin_inflight_requests{path_class}`.
   3. Build upstream URL by replacing the request's host and scheme
      with `UPSTREAM_REGISTRY` (e.g. `https://<acr>.azurecr.io`).
   4. Copy hop-by-hop-safe headers; drop `Authorization` from the
      inbound request and set the proxy's own ACR credential (Basic
      or Bearer per §3.5).
   5. Issue the upstream request with a streaming HTTP/1.1 or
      HTTP/2 client (Go's default is fine).
   6. If the response is `401` with a `WWW-Authenticate: Bearer ...`
      challenge, perform the OCI token exchange (§3.5) and reissue
      **once** with the resulting Bearer token. The reissue does
      **not** start a new `started_total`/`inflight` cycle — it is
      part of the same client request from the counter's point of
      view.
   7. Stream the response body to the client through an
      `io.CopyBuffer` wrapped in a counting `io.Writer`.
   8. On completion, record:
      - `origin_bytes_to_client_total{path_class,status}` += bytes
        written to client.
      - `origin_bytes_upstream_total{path_class,status}` += bytes
        read from upstream (may differ from above only if the client
        disconnects mid-stream).
      - `origin_latency_seconds{path_class,status}` histogram
        observation.
      - `origin_requests_completed_total{method,path_class,status}`.
   9. Decrement `origin_inflight_requests{path_class}`.

The proxy **does not retry** on upstream errors; that is the client's
job. Retrying upstream would inflate the request counter on the wrong
side of the measurement. The single Bearer-challenge reissue in step
6 is an exception, and it is invisible to the counters by design: a
`401` followed by the reissued `200` reports a single
`requests_completed_total{status="200"}`.

### 3.3 Metrics surface

Exposed on `:9090/metrics` (Prometheus text format). The started /
completed split is deliberate: a single Prometheus metric name cannot
appear with two different label sets, so the "increment on entry" and
"increment on exit" sites are two distinct counters. The difference
`started - sum(completed)` equals `inflight` at any instant; this also
makes the client-disconnect case (`started` ticks, `completed` ticks
with `status="client_closed"`, bytes-to-client under-counts vs.
bytes-upstream) cleanly observable. **`status="client_closed"` is a
proxy-synthesized label, not an HTTP status returned by ACR**; any
label value matching `[0-9]{3}` is a real upstream status, anything
else (e.g. `client_closed`, `upstream_error`) is proxy-generated.

```text
origin_requests_started_total{method,path_class}
origin_requests_completed_total{method,path_class,status}
origin_bytes_upstream_total{path_class,status}
origin_bytes_to_client_total{path_class,status}
origin_latency_seconds{path_class,status}      # histogram
origin_inflight_requests{path_class}           # gauge
origin_auth_token_refresh_total{result}        # counter (§3.5)
origin_synthetic_throttle_total{reason}        # counter (§7, optional)
```

**HEAD / zero-body note.** A `HEAD` request (containerd's manifest
existence check, the registry-spec ping) counts as one
`origin_requests_started_total` and one `origin_requests_completed_total`
but contributes 0 to `origin_bytes_to_client_total`. This is correct
behaviour, not a counter bug: HEAD is a request from the origin's
perspective even though no body is returned. The dashboard will
therefore show a small steady-state stream of requests with no
corresponding bytes during repeated tag-existence probes; do not
flag that as anomalous.

Plus a `/debug/summary` JSON endpoint for quick scripted assertions
(used by the harness in §5). `requests` here means *completed*
requests:

```json
{
  "since": "2026-05-15T12:00:00Z",
  "uptime_seconds": 123,
  "totals": {
    "requests_completed": 850,
    "bytes_to_client": 20070103040,
    "by_path_class": {
      "blob":              { "requests": 640, "bytes": 19998000000 },
      "manifest_by_digest":{ "requests": 192, "bytes":    71000000 },
      "manifest_by_tag":   { "requests":  18, "bytes":     1100000 },
      "ping":              { "requests":   0, "bytes":           0 },
      "other":             { "requests":   0, "bytes":           0 }
    }
  }
}
```

### 3.4 Path classification

Do **not** parse with a single regex that assumes a fixed number of
repository segments — OCI repositories are slash-separated and may
contain arbitrarily many segments, and tags may contain characters
(`.`, `_`, `-`, digits) that confuse naive splits. The reliable rule:

```text
1. If path == "/v2/" or path == "/v2" → ping.
2. Else find the rightmost occurrence of "/manifests/" or "/blobs/"
   in the path.
     - Everything before that separator (after the leading "/v2/")
       is the repository name.
     - Everything after that separator, up to the first "?" or end
       of path, is the reference.
3. For /blobs/<ref>:
     - If ref == "uploads" or starts with "uploads/" → other (push).
     - If ref matches ^sha256:[a-f0-9]{64}$ (case-insensitive) → blob.
     - Else → other.
4. For /manifests/<ref>:
     - If ref matches ^sha256:[a-f0-9]{64}$ → manifest_by_digest.
     - Else → manifest_by_tag (tags do NOT contain "/"; any "/" in
       the reference position means the classifier mis-split and
       must return other so the bug is visible in metrics).
5. Anything else → other.
```

Resulting classes:

| Path                                     | `path_class`         |
| ---------------------------------------- | -------------------- |
| `/v2/`                                   | `ping`               |
| `/v2/<repo>/manifests/<tag>`             | `manifest_by_tag`    |
| `/v2/<repo>/manifests/sha256:<hex>`      | `manifest_by_digest` |
| `/v2/<repo>/blobs/sha256:<hex>`          | `blob`               |
| `/v2/<repo>/blobs/uploads/...`           | `other` (push; should not occur in demo) |
| everything else                          | `other`              |

The two demo-critical aggregations:

```promql
sum(origin_requests_completed_total{path_class=~"manifest_by_digest|blob"})
sum(origin_bytes_to_client_total{path_class=~"manifest_by_digest|blob"})
```

`manifest_by_tag` traffic is reported separately because (a) Gantry v1
defers tag resolution to origin per
[archecture.md](archecture.md) F9, so even Gantry runs will show some
tag-class traffic, and (b) the headline thundering-herd story is about
the digest-class requests, not the constant-cost tag lookups.

### 3.5 Auth handling

**This is the highest-risk part of the design and must be validated
first (see Phase 0.5 in §6 and step 1 of the build plan in §10).**
ACR exposes admin username/password, but Docker Registry v2 pulls
typically run a Bearer-token dance per request: the first request
returns `401 Unauthorized` with `WWW-Authenticate: Bearer
realm="...",service="...",scope="repository:<repo>:pull"`, and the
client is expected to exchange the user credential for a short-lived
Bearer token at the realm endpoint, then retry the request with
`Authorization: Bearer <token>`. Direct Basic on the data-plane URL
is **not** guaranteed to work on `/v2/.../blobs/<digest>`. The plan
must support both flows.

`ACR_USERNAME` / `ACR_PASSWORD` are read from a Kubernetes Secret and
projected into the proxy Pod as env vars.

**Auth state machine in the proxy:**

```text
For each upstream request:
  1. If a cached Bearer token exists for the request's scope and is
     not within REFRESH_SKEW_SECONDS of expiry → attach it and send.
  2. Else if AUTH_MODE == "basic" → attach
     Authorization: Basic <base64(user:pass)>, send.
  3. Else (AUTH_MODE == "bearer" with no token, or step 1/2 returned
     401 with a WWW-Authenticate: Bearer challenge):
       a. Parse realm / service / scope from the challenge.
       b. GET <realm>?service=<service>&scope=<scope> with
          Authorization: Basic <base64(user:pass)>.
       c. Parse {"access_token" | "token", "expires_in"} from the
          response.
       d. Cache the token keyed by scope, with expiry = now +
          min(expires_in, MAX_TOKEN_LIFETIME).
       e. Reissue the original upstream request with the new token.
       f. Record origin_auth_token_refresh_total{result="success"}
          (or {result="error"} on failure to obtain token).
```

`AUTH_MODE` is an env var with values `basic`, `bearer`, or `auto`
(default `auto` = start in `basic`, fall back to `bearer` on first
401-with-challenge and stay there). The token cache is in-memory,
keyed by scope string; LRU-capped at e.g. 1024 entries (in practice
the demo will hit one or two scopes).

ACR's admin-account flow is what Azure documents for this
username/password pattern; it is acceptable here because:

- The credential is scoped to a single demo ACR.
- The demo recording explicitly calls out "demo-only credentials."
- The demo namespace is deleted at the end of each run.

Production deployments must use workload identity / service principals;
that is out of scope for the demo plan.

The proxy strips any inbound `Authorization` header before applying its
own. This guarantees that no test client can accidentally smuggle a
different identity past the proxy and contaminate the ACR-side audit
log. Combined with §2.5, this means the ACR audit log shows a single
subject (the proxy's credential) for every demo path.

### 3.6 Streaming and buffer sizing

Use `io.CopyBuffer` with a 64 KiB buffer. Do **not** wrap the response
body in a buffering layer (`bufio.Reader` with `Peek`, full-body
`ioutil.ReadAll`, etc.) — those break the streaming guarantee and can
cause apparent bytes/requests mismatches when clients disconnect
mid-blob.

Set `http.Server.ReadHeaderTimeout = 10s` and
`http.Server.WriteTimeout = 0` (unbounded — large blob streams legitimately
take minutes on slow nodes). Idle conns: `http.Transport` defaults are
fine.

### 3.7 Failure modes the proxy must surface

- Upstream DNS / TLS failure: return 502 to the client; record
  `status="502"` so it's visible in the metrics.
- Upstream auth failure (401 from ACR): forward 401 verbatim; do not
  retry. (If this fires during a demo, the demo is broken — fix
  credentials.)
- Client disconnect mid-stream: record bytes copied so far on
  `origin_bytes_to_client_total{status="client_closed"}` and
  `origin_bytes_upstream_total{...}` with whatever was actually
  drained from upstream; complete the request with
  `origin_requests_completed_total{...,status="client_closed"}`. Per
  §3.3, `client_closed` is a proxy-synthesized label — do not pretend
  the upstream returned an HTTP status it never sent.

### 3.8 Test coverage

Unit tests (`main_test.go`):

- Path classifier produces correct `path_class` for every row in
  §3.4, plus the negative-case examples in §3.4 itself (trailing
  slashes, query strings, a tag-like reference containing `/` →
  must return `other`, multi-segment repository names such as
  `acme/team/svc`).
- `origin_requests_started_total` increments on entry; matching
  `origin_requests_completed_total` increments exactly once even when
  the client disconnects mid-stream.
- `origin_inflight_requests` returns to its prior value after every
  request (success, upstream error, mid-stream disconnect).
- `origin_bytes_to_client_total` ≤ `origin_bytes_upstream_total` and
  equal in the happy path.
- `Authorization` from inbound is dropped and overridden with the
  proxy's credential (Basic or Bearer).
- **Bearer-challenge flow:** a fake upstream that returns 401 with a
  `WWW-Authenticate: Bearer` challenge, then 200 once a Bearer token
  is presented. Verify the proxy: parses the challenge, performs the
  token exchange against the realm URL, caches the token, reissues
  with the token, increments
  `origin_auth_token_refresh_total{result="success"}` exactly once,
  and reports a single `requests_completed_total{status="200"}`.
- Token cache: a second request for the same scope reuses the cached
  token; refresh fires within `REFRESH_SKEW_SECONDS` of expiry.
- `/debug/summary` JSON shape matches §3.3 (note `requests_completed`
  field name).
- Synthetic-throttle path (§7) returns 429 with `Retry-After` and
  bumps `origin_synthetic_throttle_total`.

No e2e test against real ACR in CI — the demo runs are manual; CI runs
the unit tests only.

---

## 4. Deployment topology

All demo-only artifacts live under `deploy/demo/` so they cannot be
applied accidentally on top of a production install and so the entire
demo is reverted by deleting one directory (§1.1). Nothing outside
this tree is modified — no root `Makefile` target, no root `go.mod`
require, no edits under `cmd/`, `internal/`, or `e2e/`:

```text
deploy/demo/                          # all demo-only artifacts; deletable as a single unit
  README.md
  Makefile                            # demo-local; never the root Makefile (§10)
  acr-origin-proxy/
    go.mod                            # standalone Go module; no root go.mod changes
    go.sum
    main.go
    main_test.go
    Dockerfile
    deployment.yaml
    service.yaml
    secret.example.yaml
  harness/
    go.mod                            # standalone Go module; no imports from github.com/gantry/gantry/...
    go.sum
    harness.go
    phases.go
    phase_baseline_test.go
    phase_gantry_cold_test.go
    phase_gantry_warm_test.go
  hosts.toml.baseline.template
  hosts.toml.gantry.template
  configmap.gantry-demo.yaml
  prometheus-values.yaml              # Helm values overlay
  grafana-dashboard.json
```

`deploy/demo/Makefile` is the single entry point for the demo build
and test surface (`make proxy-image`, `make proxy-test`, `make
harness`, etc.). It is invoked as `cd deploy/demo && make <target>`
or `make -C deploy/demo <target>`. The root [Makefile](../Makefile)
is never edited.

### 4.1 Namespace and Service

```text
namespace:   gantry-demo
deployment:  acr-origin-proxy    (replicas: 1)
service:     acr-origin-proxy    (ClusterIP, ports: 5002 proxy, 9090 metrics)
secret:      acr-admin-creds     (keys: username, password)
```

One replica is deliberate — a single counter makes the demo
unambiguous. The proxy is stateless apart from in-memory counters, so
replicas would each hold a fragment of the truth and need
sum-by-instance aggregation in Grafana. Avoid.

**The single replica is a potential bottleneck and we accept that.**
For the baseline 20-node × 1 GB run, ~20 GB of body bytes funnel
through one Pod's NIC and one Go HTTP server. That can saturate proxy
CPU or pod-network bandwidth before ACR itself pushes back. The
implications:

- **Origin request and byte counts remain valid.** Each request is
  still counted exactly once at the proxy regardless of how slowly
  it streams. The headline proof of the demo — request reduction
  and byte reduction — is unaffected by proxy throughput.
- **Pod-start latency becomes a secondary metric.** If the proxy is
  the bottleneck, baseline pod-start P95 will reflect proxy-side
  queuing, not the AKS↔ACR link. Report pod-start numbers honestly
  with that caveat.
- **Dashboard Row 4 (§8) must show proxy CPU and proxy network
  saturation** so the operator can see when this matters and
  optionally rerun with a higher CPU/network SKU.

Resource requests/limits in `deployment.yaml`:

```yaml
resources:
  requests: { cpu: 500m,   memory: 256Mi }
  limits:   { cpu: 4,      memory: 1Gi   }
```

The proxy only streams bytes; CPU is mostly TLS + HTTP framing on the
upstream side. 1 Gi memory is generous headroom for buffers and the
Prometheus client's histogram series. If proxy CPU pins at the limit
during baseline, raise `limits.cpu` and rerun; do not lower `limits`
below 4 cores even though the typical demand is well under that.

### 4.2 Network policy

Optional, deferred to a follow-up. The demo namespace is short-lived
and the proxy speaks only to ACR upstream and to in-cluster clients on
:5002/:9090; locking that down is straightforward but not required to
get the headline numbers.

### 4.3 Prometheus and Grafana

Reuse the cluster's existing Prometheus if available. The proxy is
already labeled for kube-prometheus-stack auto-discovery (annotations
in `deployment.yaml`):

```yaml
prometheus.io/scrape: "true"
prometheus.io/port:   "9090"
prometheus.io/path:   "/metrics"
```

`grafana-dashboard.json` is a four-row dashboard (§8). It uses only
metrics from the proxy and the existing Gantry `p2p_*` series, so no
new exporters are needed.

**Gantry metric names used by this plan have been verified against
[cmd/gantry/main.go](../cmd/gantry/main.go) as of the date in this
doc's status line.** The names in scope:

| Used in plan                               | Registered in main.go | Notes |
| ------------------------------------------ | --------------------- | ----- |
| `p2p_origin_pull_total`                    | yes                   | total origin pulls attempted; companion success/failure counters exist |
| `p2p_peer_fetch_total{outcome}`            | yes                   | `outcome="hit"` is the "peer hit" headline metric |
| `p2p_cache_hit_total`                      | yes                   | local-cache hits at the mirror |
| `p2p_dht_health_score`                     | yes                   | gauge |
| `p2p_hrw_rank_mismatch_total`              | yes                   | F1-divergence diagnostic |
| `p2p_cache_forced_eviction_total`          | yes                   | triage §9 |

If any name in this table no longer matches `cmd/gantry/main.go` at
implementation time, fix the plan and dashboard rather than
introducing aliases — stale names render as silent "No data" panels.
Names such as `p2p_origin_pull_started_total` or `p2p_peer_hit_total`
appeared in earlier drafts and are **not** registered; do not use
them.

### 4.4 hosts.toml installation on AKS nodes

containerd on AKS reads per-registry config from
`/etc/containerd/certs.d/<host>/hosts.toml`. For the demo, install the
file via a DaemonSet that writes to a hostPath mount of
`/etc/containerd/certs.d` and then exits (one-shot, with a sleep loop
to keep the Pod present for `kubectl logs`/`describe`). Two templates
ship in `deploy/demo/`. **Both are fail-closed by design — see
§4.6.**

`hosts.toml.baseline.template` (rendered, not applied verbatim — see
§4.5):

```toml
# Demo baseline: proxy is the ONLY upstream. If the proxy is
# unreachable, the pull must fail — we want loud failure, not silent
# fallback to ACR (§4.6).
server = "http://${PROXY_CLUSTER_IP}:5002"

[host."http://${PROXY_CLUSTER_IP}:5002"]
  capabilities = ["pull", "resolve"]
```

`hosts.toml.gantry.template` (demo-only; **NOT** the same as the
production [hosts.toml.template](../deploy/hosts.toml.template) — the
production template falls back to ACR if Gantry is unhealthy, which
is the right production behaviour but the wrong demo behaviour):

```toml
# Demo Gantry path: local Gantry first; fallback is the proxy, not
# ACR. If Gantry AND the proxy are both unreachable, the pull fails
# loudly. There is no path that reaches ACR without crossing the
# proxy counter (§4.6).
server = "http://${PROXY_CLUSTER_IP}:5002"

[host."http://127.0.0.1:5000"]
  capabilities = ["pull", "resolve"]
  skip_verify = true
```

Gantry itself is configured (in `configmap.gantry-demo.yaml`) with the
Service DNS name — Gantry runs inside a Pod and **does** have cluster
DNS, so it can resolve the Service:

```yaml
upstream_registries:
  - name: "${ACR_NAME}.azurecr.io"
    endpoint: "http://acr-origin-proxy.gantry-demo.svc.cluster.local:5002"
    credentials_path: ""
```

This matches the `UpstreamRegistry` schema in
[internal/config/config.go](../internal/config/config.go).

containerd reloads `hosts.toml` on demand per registry — no daemon
restart required — but operators should `kubectl rollout restart
ds/gantry` after switching files to make sure the agent picks up the
new ConfigMap. See [deploy/README.md](../deploy/README.md) for the
normal install order.

### 4.5 Reachability from node-level containerd

The baseline path is `node containerd → proxy → ACR`. **Node-level
containerd is a process on the node, not inside a Pod**, so it does
*not* use cluster DNS by default and cannot resolve
`acr-origin-proxy.gantry-demo.svc.cluster.local`. Using that name in
`hosts.toml.baseline.template` will produce silent fallback to the
origin `server =` URL on every pull, which then bypasses the proxy
entirely and breaks the counter. This is the single most likely
operational blocker in this plan.

The Gantry path is different: Gantry runs as a Pod and inherits the
default `ClusterFirst` DNS policy, so `*.svc.cluster.local` resolves
for Gantry's origin client. **Only the baseline path is affected.**

**The plan uses the ClusterIP-literal approach for the baseline path.**
The installer DaemonSet renders the template at apply time:

```bash
PROXY_CLUSTER_IP=$(kubectl -n gantry-demo get svc acr-origin-proxy \
  -o jsonpath='{.spec.clusterIP}')
envsubst < hosts.toml.baseline.template \
  > /etc/containerd/certs.d/${ACR_NAME}.azurecr.io/hosts.toml
```

Why ClusterIP literal first:

- Service ClusterIPs are *expected* to be routable from AKS nodes in
  the target CNI modes (kube-proxy / Cilium / Azure CNI), and the
  kernel-level Service → endpoint resolution is independent of
  cluster DNS — but the Phase 0.5 reachability gate (§6) is
  authoritative, not this assumption. If `ip route` / `curl` from a
  node fails, fall back to one of the alternatives below; do not
  argue with the cluster.
- One replica, one counter, one address — matches the rest of §4.1.
- No NodePort port-allocation negotiation, no internal LoadBalancer
  provisioning latency.

**Caveat:** Service ClusterIPs change if the Service is recreated. The
installer must re-render the template whenever the proxy Service is
recreated; treat "Service ClusterIP changed" as "redeploy the
installer DaemonSet." The harness should re-resolve the ClusterIP
before Phase 1 even if the installer ran earlier.

Alternatives, in order of preference if ClusterIP routing fails on a
target cluster (some node-network configurations route Pod CIDR but
not Service CIDR — verify with `ip route` on a node):

1. **Internal LoadBalancer** for the proxy Service
   (`service.beta.kubernetes.io/azure-load-balancer-internal: "true"`).
   Gives a stable VNet IP routable from every node; costs one ILB.
2. **NodePort** Service. Stable port across nodes; address is
   `127.0.0.1:<nodePort>` or `<node-private-IP>:<nodePort>` in
   `hosts.toml`. Avoids exposing the port outside the VNet.
3. **hostNetwork + hostPort** on the proxy Pod itself. Simplest
   address (`127.0.0.1:5002` from every node) but loses the
   one-counter property unless the Pod is pinned to a single node;
   for the 20-node demo, pinning is acceptable.

Whichever is picked, document the choice in the Phase 0.5 run log
(§6) alongside the auth-mode decision. The `hosts.toml.baseline.template`
rendering step is the only thing that changes; the proxy Deployment
stays the same.

### 4.6 Fail-closed routing (no silent fallback to ACR)

containerd's per-registry config tries `[host."..."]` entries first
and uses `server = "..."` as the fallback if every preceding host
returns a non-2xx (`> 299`) status or is unreachable. The production
[hosts.toml.template](../deploy/hosts.toml.template) deliberately
sets `server` to the canonical upstream (e.g. ACR) so a degraded
Gantry doesn't break image pulls in production. **The demo needs the
opposite property:** no pull may reach ACR without crossing the
proxy, because a path that bypasses the proxy bypasses the counter,
and that contaminates the headline numbers without any visible
failure.

Failure modes the demo templates explicitly forbid:

```text
Baseline:    proxy unreachable → containerd falls back to ACR → pull succeeds
             → proxy counter stays low → baseline measurement looks better than reality.

Gantry path: local Gantry on 127.0.0.1:5000 returns 5xx (config error, OOM, cold mesh)
             → containerd falls back to ACR → pull succeeds without crossing proxy
             → cold-start "succeeds" with proxy counters at zero → Gantry's bypass
             looks indistinguishable from a perfect peer-fetch.
```

Both templates in §4.4 prevent these silently-bad outcomes:

- **Baseline:** `server` is the proxy. There is exactly one upstream
  in the chain (the proxy, listed twice, on purpose). If the proxy is
  down, containerd exhausts the chain and the pull errors. The
  workload Pod hits `ImagePullBackOff`, which is loud and obvious in
  the demo — the desired behaviour.
- **Gantry path:** `server` is the proxy, not ACR. If local Gantry
  fails, containerd falls back to the proxy (still counted, still
  visible). If the proxy also fails, the pull errors. There is no
  configured path to ACR that doesn't cross the proxy.

The single counter, both phases, no silent escape hatch. The cost is
that operator error (e.g. forgetting to install the proxy before
applying baseline `hosts.toml`) becomes an `ImagePullBackOff` instead
of a slow degraded pull — acceptable for a demo, and triaged by §9.

---

## 5. Harness

A thin Go test/runner under `deploy/demo/harness/` driving the three
demo phases end-to-end. It is a **separate Go module** (its own
`go.mod`) and imports nothing from `github.com/gantry/gantry/...` —
it talks to the proxy and Gantry only over HTTP (`/debug/summary`,
`/metrics`) and to the cluster via `kubectl` / a standalone
client-go, so it has no compile-time dependency on Gantry internals:

```text
deploy/demo/harness/
  README.md
  go.mod
  go.sum
  harness.go
  phases.go
  phase_baseline_test.go
  phase_gantry_cold_test.go
  phase_gantry_warm_test.go
```

Build-tagged `//go:build demo` to mirror the existing `//go:build
e2e` convention documented in [e2e/README.md](../e2e/README.md), even
though it lives in a different module — the tag is a belt-and-braces
guard against `go test ./...` picking the suite up by accident. The
harness is run via `make -C deploy/demo harness`; no `make demo`
target is added to the root Makefile.

### 5.1 What the harness owns

1. Snapshot proxy counters via `GET /debug/summary` before and after
   each phase.
2. Apply the right `hosts.toml.*` and (for Gantry phases)
   `configmap.gantry-demo.yaml`.
3. Push a fresh demo image with a unique tag/digest per phase. Layer
   contents must be *fresh* so the two caches the demo cares about —
   containerd's local content store and Gantry's content-addressed
   cache — cannot satisfy the pull without reaching the proxy. Layer
   reuse between phases must also be defeated so the second phase's
   pull is not trivially served by content already in the cluster.
   (ACR-internal CDN behavior is irrelevant: any client request that
   reaches the proxy is counted there regardless of how ACR itself
   sources the bytes.)
4. Run a 20-replica Kubernetes Job pulling the image with
   `imagePullPolicy: Always`.
5. Wait for all 20 pods to terminate successfully.
6. Collect pod-start latency primarily from an **in-container
   timestamp**: the workload command begins with `date -u
   +%Y-%m-%dT%H:%M:%S.%NZ` (or an equivalent millisecond-precision
   `printf` in Go) written to stdout as the first line, captured by
   `kubectl logs`. This timestamp marks the moment after image pull
   and container startup with no kube-apiserver polling lag. The
   harness also records the Kubernetes pod-status timestamps
   (`Pending` → `Running` → first container `Ready`) as a sanity
   cross-check, but the in-container line is the authoritative
   number.
7. Diff proxy `/debug/summary`; emit pod-start latency samples;
   write a CSV row.
8. For Gantry phases, also scrape the Gantry `/metrics` for
   `p2p_origin_pull_total`, `p2p_peer_fetch_total{outcome}`,
   `p2p_cache_hit_total` deltas (already exposed by
   [cmd/gantry/main.go](../cmd/gantry/main.go); see §4.3 for the
   verified-name table).

### 5.2 Cache hygiene between phases

Cache hygiene is the most error-prone part of the demo. The harness
must enforce it explicitly:

- **Between baseline and Gantry-cold:** rebuild the image with a new
  random layer so no digest is shared; new tag; restart the Job. Also
  `kubectl rollout restart ds/gantry` to ensure no carryover state.
- **Between Gantry-cold and Gantry-warm:** keep the Gantry cache
  intact, but purge containerd's content store on every node so that
  every Pod actually re-resolves through `hosts.toml`. The harness
  does this via a privileged one-shot DaemonSet that runs
  `ctr -n k8s.io content prune` (or, more bluntly, deletes
  `/var/lib/containerd/io.containerd.content.v1.content` then restarts
  containerd via systemd). The blunt path is acceptable on demo nodes;
  it is destructive and must not be available outside `gantry-demo`.

**The purge step is the second-highest implementation risk after auth
(§3.5). The harness must verify it before relying on it for Phase 3.**
The verification is *not* "check the manifest digest is gone" — that
is easy to satisfy without actually purging layers. The verification
is the full digest set:

```text
For each node in the Job's nodeSelector:
  Resolve the demo image's manifest → list of {manifest digest,
    config digest, layer digest × N}.
  ctr -n k8s.io content ls | grep -F <digest>
  Every digest must be absent.
```

If any digest survives the purge, Phase 3 will silently pull from the
local content store instead of from Gantry's cache, and the
zero-bytes-at-proxy claim becomes false without any visible failure.
The purge DaemonSet must therefore emit a per-node JSON report
listing which digests it observed and which it removed; the harness
fails Phase 3 setup if any node reports a surviving digest from the
digest set.

This verification must be implemented and run end-to-end against an
AKS node OS (Mariner / Ubuntu, whatever the cluster uses) before
Phase 3 is consumed by recordings. See step 7 of the build plan in
§10.

### 5.3 What the harness does **not** do

- Provision ACR, AKS, or Prometheus. Those are an operator-driven
  one-time setup; see §6.
- Drive Grafana. Screenshots come from the operator.
- Assert numeric thresholds. The CSV output is the deliverable; the
  expected ranges in §1.2 are guidance for sanity-checking, not
  test assertions. (Hard thresholds make the demo brittle against
  ACR-side variance — e.g. an extra `manifest_by_tag` round-trip from
  containerd's auth dance.)

---

## 6. Recommended demo order

The order maps 1:1 to the harness phases above.

### Phase 0 — Provisioning (manual, one-time)

1. Create ACR (`<acr>.azurecr.io`); enable admin user **for demo
   only**; store creds in `Secret/acr-admin-creds`.
2. Create AKS with ≥ 20 worker nodes. Reasonable node SKU; the
   bottleneck in baseline is ACR or the proxy itself (§4.1), not
   node NIC.
3. Install kube-prometheus-stack (or equivalent Prometheus +
   Grafana). Apply `deploy/demo/grafana-dashboard.json`.
4. Push a baseline demo image to ACR. This image is *only* used to
   verify that the proxy works (Phase 0.5); the real demo images are
   freshly built per phase.
5. Apply `deploy/demo/acr-origin-proxy/` — Secret, Deployment, Service.

### Phase 0.5 — Auth spike (mandatory gate)

**This is a blocking gate. Do not build the harness, the dashboard,
or anything in Phase 1+ until Phase 0.5 passes for every path class
below.** It exists because the §3.5 auth design is the single most
likely place for the demo to fail in a way that masquerades as Gantry
results.

From inside the `gantry-demo` namespace, against the proxy Service,
verify a successful 200 (or 200-via-401-then-Bearer) for each of (use
a known image whose manifest you have inspected with `oras manifest
fetch` or `crane manifest` to get the manifest-digest, config-blob
digest, and a representative layer-blob digest):

```text
# Run from a debug Pod in the gantry-demo namespace (cluster DNS works there):
GET  /v2/                                          → ping
HEAD /v2/<repo>/manifests/<tag>                    → manifest_by_tag
GET  /v2/<repo>/manifests/<tag>                    → manifest_by_tag
GET  /v2/<repo>/manifests/sha256:<manifest-digest> → manifest_by_digest
GET  /v2/<repo>/blobs/sha256:<config-digest>       → blob  (image config JSON)
GET  /v2/<repo>/blobs/sha256:<layer-digest>        → blob  (one representative layer)

# Run on a node via crictl (must use the node-reachable proxy address
# from §4.5, NOT the Service DNS name, because crictl drives node-level
# containerd which has no cluster DNS):
crictl pull ${PROXY_CLUSTER_IP}:5002/<repo>:<tag>  → end-to-end through containerd
```

For each call:

- The proxy must return a 2xx status to the client.
- `origin_requests_completed_total{path_class=...}` must increment by
  exactly 1 (per call) regardless of whether the upstream returned
  401-then-200 (one logical request from the counter's view, per
  §3.2).
- If `AUTH_MODE=auto` and any path required a Bearer token,
  `origin_auth_token_refresh_total{result="success"}` must be > 0
  and the second call to the same scope must **not** increment it.

If any of these fails, the proxy's §3.5 implementation is wrong and
must be fixed before proceeding. Document the actual ACR challenge
response (realm, service, scope strings) in the run log so the
decision between `basic`, `bearer`, or `auto` modes is recorded.
Also record which **node-reachability option** from §4.5
(ClusterIP literal / internal LB / NodePort / hostNetwork) was
chosen, and whether `crictl pull` succeeded against that address —
this is the second gate, independent of auth.

### Phase 1 — Baseline

Apply `hosts.toml.baseline.template` cluster-wide. **Do not deploy
Gantry.** Build a fresh image with zero shared layers. Run the
20-replica Job. Record proxy counters and pod-start latencies.

The baseline `hosts.toml` is fail-closed (§4.6): if the proxy is
unreachable on a node, that node's Pods enter `ImagePullBackOff`
rather than silently bypassing the proxy and pulling from ACR
directly. Treat any `ImagePullBackOff` as evidence to investigate
(see §9), not as noise to retry past.

Expected (sanity check, not assertion):

```text
proxy digest requests   ≈ node_count × digest_count
                          (= 20 × (manifest_by_digest + config_blob + layer_blobs))
proxy tag requests      ≈ node_count × (tag-class + resolve overhead)
proxy bytes-to-client   ≈ node_count × compressed_image_size
```

The headline is the first row; the tag row is informational and is
expected to be roughly the same across all three phases (see §1.2).

### Phase 2 — Gantry cold-start

Deploy Gantry per [deploy/README.md](../deploy/README.md) with the
demo ConfigMap. Apply `hosts.toml.gantry.template`. Wait for the
DaemonSet to roll out and the libp2p mesh to converge (the agent
exposes a readiness gate that already covers this; see
[e2e/README.md](../e2e/README.md) §How it works). Push a **new** image
with zero layer overlap with the baseline image.

**Wiring preflight (mandatory before the 20-replica run).** The
Gantry path has two independent knobs that both have to be right:

```text
(a) hosts.toml on every node points containerd → local Gantry mirror,
    with proxy as the fail-closed fallback (§4.4 / §4.6).
(b) Gantry's UpstreamRegistry.Endpoint for the ACR points Gantry → proxy.
```

With the fail-closed template from §4.4 in place, a misconfigured (b)
cannot escape to ACR — the worst it can do is route around Gantry
through the proxy fallback, which the proxy counter will record. The
preflight still exists so the operator sees that **Gantry itself is
handling the pull**, not just the proxy fallback path. Pull a
**dedicated single preflight tag** from one node and assert:

```text
- proxy origin_requests_completed_total{path_class=~"blob|manifest_by_digest"} > 0
- Gantry p2p_origin_pull_total{...} > 0   (Gantry attempted an origin pull)
```

Both must increment on the same preflight pull. If only Gantry's
counter moves but the proxy's does not, knob (b) is misconfigured —
check `configmap.gantry-demo.yaml` and `kubectl rollout restart
ds/gantry`. If only the proxy's counter moves but Gantry's does not,
knob (a) is misconfigured — the wrong `hosts.toml` is on the node, or
containerd never tried the Gantry mirror.

**Fail-closed verification (mandatory, one-time, before any
recording).** Both demo templates are designed so that no pull can
reach ACR without crossing the proxy (§4.6). Prove that on a real
node before the recording rather than after a contaminated run:

```text
1. Install hosts.toml.gantry.template on every node; confirm Gantry
   is running and the wiring preflight above passes.
2. On exactly one node, stop or block local Gantry on 127.0.0.1:5000
   (e.g. `systemctl stop` the Gantry container via `crictl`, or add
   a node-local iptables/nftables DROP for 127.0.0.1:5000).
3. From that node, `crictl pull ${PROXY_CLUSTER_IP}:5002/<repo>:<preflight-tag>`
   against a fresh tag with zero shared layers.
4. Assert: proxy `origin_requests_completed_total{path_class=~"blob|manifest_by_digest"}`
   increments. The fallback hit the proxy, not ACR.
5. Assert (negative): no direct-to-ACR traffic appears in ACR's
   `ContainerRegistryRepositoryEvents` (or whatever upstream-side
   log is available) during the test window. The proxy is the only
   client of ACR.
6. Restore local Gantry; re-run the wiring preflight to confirm the
   node is healthy again.
```

If step 4 fails, the Gantry template is not actually fail-closed on
this containerd version — fix it before continuing. If step 5 shows
direct ACR traffic, something on the node bypassed `hosts.toml`
entirely (e.g. a stale config file, kubelet image-credential-provider
shortcut, or out-of-band `docker pull`); investigate before any
recording. Record the result of this verification alongside the
Phase 0.5 auth-mode and reachability decisions.

Once the wiring preflight and the fail-closed verification both
pass, run the same 20-replica Job against a separate cold-start
image with zero shared layers.

Expected:

```text
proxy requests          ≈ digest_count  to  3 × digest_count
proxy bytes             ≈ 1 to 3 × compressed_image_size
Gantry p2p_peer_fetch_total dominates p2p_origin_pull_total
```

The "3×" upper bound on origin requests is the F1 worst case from
[archecture.md](archecture.md) §F1 (≤3 origin pulls per digest under
transient informer divergence). If the observed number exceeds that,
the demo has surfaced a real Gantry bug; see §9 for triage.

### Phase 3 — Gantry warm-cache

Purge containerd's content stores on every node (§5.2), including the
full-digest-set verification step. Leave Gantry's own cache intact.
Re-run the same Job against the same image.

Expected:

```text
proxy origin_requests_completed_total{path_class="blob"}              = 0
proxy origin_requests_completed_total{path_class="manifest_by_digest"} = 0
proxy origin_bytes_to_client_total{path_class=~"blob|manifest_by_digest"} = 0
proxy origin_requests_completed_total{path_class="manifest_by_tag"}    > 0   (expected; Gantry v1 defers tag resolution to origin, §1.2 / archecture.md F9)
Gantry p2p_cache_hit_total ≈ 20 × digest_count
```

If the proxy sees *any* `blob` or `manifest_by_digest` traffic in this
phase, Gantry is bypassing its cache; this is the most diagnostic
single number in the demo. See §9. Nonzero `manifest_by_tag` traffic
is not a failure.

---

## 7. Optional synthetic back-pressure

For demos that want the visual drama of 429s without depending on real
ACR throttling:

```text
If inflight_requests{path_class="blob"} > THROTTLE_BLOB_INFLIGHT:
  return 429 with Retry-After: 5
  increment origin_synthetic_throttle_total{reason="blob_inflight"}
```

or

```text
If origin_bytes_upstream_total rate > THROTTLE_BPS:
  return 429 with Retry-After: 5
  increment origin_synthetic_throttle_total{reason="bps_cap"}
```

Both are off by default. Turning them on must be visible on the
dashboard (counter goes nonzero) **and** in the on-screen narration:

> This is a controlled origin limit, not an Azure ACR production
> throttle. It demonstrates what happens when an origin has finite
> request or byte capacity.

The threshold values live in env vars on the proxy Deployment; the
harness does not toggle them — operator decision.

---

## 8. Dashboard layout

Single Grafana dashboard, four rows, all panels live in
`deploy/demo/grafana-dashboard.json`:

**Row 1 — Origin proxy (headline)**

- Origin completed requests/sec by `path_class`
- Origin bytes-to-client/sec by `path_class`
- Cumulative digest requests
  (`sum(origin_requests_completed_total{path_class=~"blob|manifest_by_digest"})`)
- Cumulative digest bytes
  (`sum(origin_bytes_to_client_total{path_class=~"blob|manifest_by_digest"})`)
- Tag requests (single panel; expected nonzero in every phase,
  including warm-cache — see §1.2 / §6 Phase 3)
- Synthetic 429 count (visible only when §7 is enabled)

**Row 2 — Gantry**

- `rate(p2p_origin_pull_total[1m])`
- `rate(p2p_peer_fetch_total[1m])` by `outcome`
- `rate(p2p_cache_hit_total[1m])`
- `p2p_dht_health_score`

**Row 3 — Rollout (secondary)**

- Pods completed (gauge)
- P50/P95/P100 pod-start time from the harness CSV (in-container
  timestamp; see §5.1). Annotate the row "secondary: may reflect
  proxy-side queuing if Row 4 shows proxy saturation" (§4.1).

**Row 4 — Cost / overhead (always-on, used to qualify Row 3)**

- Proxy CPU (per-core usage / limit)
- Proxy network throughput (rx and tx, per-Pod)
- `origin_inflight_requests` by `path_class`
- Gantry CPU / memory (per-pod, from cAdvisor)

All Gantry metric names match those already registered in
[cmd/gantry/main.go](../cmd/gantry/main.go); no new exporters are
needed.

---

## 9. Failure / triage matrix

| Observation                                                 | Likely cause                                       | First check |
| ----------------------------------------------------------- | -------------------------------------------------- | ----------- |
| Phase 1 origin requests far below `20 × digest_count`       | Workload pods landed on a node whose containerd cache already held the image (image not fresh enough) | Verify image was rebuilt with a new random layer; check `crictl images` on a sample node before the run |
| Phase 2 origin requests > 3 × digest_count                  | HRW divergence or DHT not converged before pull surge | `p2p_hrw_rank_mismatch_total` rate, `p2p_dht_health_score`, [archecture.md §F1 / §3.x](archecture.md) |
| Phase 3 nonzero `blob` or `manifest_by_digest` origin traffic | Gantry cache miss (eviction or wrong digest) or `hosts.toml` reverted to baseline | `p2p_cache_forced_eviction_total`; `cat /etc/containerd/certs.d/<host>/hosts.toml` on a sample node |
| Proxy `/metrics` empty after a pull                         | With fail-closed templates (§4.6), the pull should have errored rather than reached this state. Most common: node-level containerd cannot resolve the proxy address in `hosts.toml` (see §4.5 — do **not** use Service DNS here), the ClusterIP was re-allocated, the `hosts.toml` was reverted, or `hosts.toml` was never installed on the node the Pod landed on. | On the node: `cat /etc/containerd/certs.d/<host>/hosts.toml`, then `getent hosts <proxy-host>` and `curl -v http://<proxy-host>:5002/v2/`. Cross-check the ClusterIP against `kubectl -n gantry-demo get svc acr-origin-proxy`. |
| `ImagePullBackOff` during baseline or Gantry cold-start     | Fail-closed behaviour (§4.6): proxy unreachable from the node, or (Gantry path) both Gantry **and** proxy fallback unreachable. This is the intended outcome — do not work around it. | Same first checks as the previous row, plus `kubectl -n gantry-demo logs deploy/acr-origin-proxy` and `kubectl -n gantry-system describe ds/gantry`. |
| Phase 1 P95 pod-start time looks normal                     | Image is too small to expose the herd, or AKS-to-ACR link is unusually fat | Use the 1 GB demo image; enable §7 synthetic throttle |
| `origin_bytes_to_client_total` ≪ `origin_bytes_upstream_total` | Many clients disconnected mid-stream (probably ImagePullBackOff loop) | `kubectl describe` a sample failing Pod |
| Proxy CPU pinned at the limit                               | Wrong Go runtime tuning or buffer sizing           | Verify `GOMAXPROCS` matches the CPU limit; check `io.CopyBuffer` is not allocating per-call |

---

## 10. Build / delivery plan

In order, each landable as its own PR. None of these touch
production [deploy/](../deploy/) artifacts. **Do not build dashboard
polish or harness phases before step 1 (auth spike) passes against
real ACR.**

1. **Auth + reachability spike against real ACR (gate).** Smallest
   possible Go binary under `deploy/demo/acr-origin-proxy/` that does
   only §3.5: takes `ACR_USERNAME` / `ACR_PASSWORD` env, handles the
   Bearer-challenge flow, and proxies a single request. Deploy it,
   pick the node-reachability option per §4.5 (default: ClusterIP
   literal; record the verified `ip route`/`curl` evidence), run the
   Phase 0.5 checklist (§6) by hand against a real ACR with a known
   image, record which `AUTH_MODE` (`basic` / `bearer` / `auto`)
   succeeded for which path class, and pin both decisions in the
   plan. **Stop and resolve any failure here before continuing** —
   this single step gates both blockers (auth and node reachability).
2. **Counting proxy (full).** Extend (1) with classification (§3.4),
   the started/completed metric pair (§3.3), streaming (§3.6),
   failure handling (§3.7), `/debug/summary`, and the unit-test
   suite (§3.8) — including the Bearer-challenge unit test against
   a fake upstream. Image build wired into `deploy/demo/Makefile`'s
   `proxy-image` target. **Do not add a target to the root
   [Makefile](../Makefile)** — invoke via `make -C deploy/demo
   proxy-image` from CI or manual runs. The proxy's `go.mod` stays
   under `deploy/demo/acr-origin-proxy/`; the root `go.mod` is not
   modified.
3. **hosts.toml templates and demo ConfigMap.**
   `deploy/demo/hosts.toml.baseline.template`,
   `deploy/demo/hosts.toml.gantry.template`,
   `deploy/demo/configmap.gantry-demo.yaml`. Includes a one-shot
   `hosts-toml-installer` DaemonSet manifest.
4. **Harness skeleton.**
   `deploy/demo/harness/{harness.go,phases.go,README.md,go.mod}`.
   Build-tagged `//go:build demo`. `deploy/demo/Makefile` gains a
   `harness` target invoked as `make -C deploy/demo harness`. No
   phase logic yet. Wires in the in-container-timestamp collection
   helper (§5.1). Harness `go.mod` declares zero imports from
   `github.com/gantry/gantry/...`.
5. **Baseline phase.** `phase_baseline_test.go` plus its image-build
   helper. Produces the first CSV row end-to-end.
6. **Gantry cold-start phase.** `phase_gantry_cold_test.go`.
   Depends on (4)+(5).
7. **Cache purge with full-digest verification.** Standalone PR
   adding the privileged content-store purge DaemonSet plus its
   verification report (§5.2). Run it manually against AKS nodes
   before consuming it in step 8. Gated behind an explicit
   `DEMO_ALLOW_CONTENT_PURGE=1` env var so it can't run in CI by
   accident.
8. **Gantry warm-cache phase.** `phase_gantry_warm_test.go`. Depends
   on (6)+(7). Fails fast if the purge verification reports any
   surviving digest.
9. **Grafana dashboard.** `deploy/demo/grafana-dashboard.json`.
   Hand-built against the four rows in §8 using the metric names
   verified in §4.3. Building this after the harness exists lets us
   confirm panels actually render real data before the recording.
10. **Optional: synthetic throttle.** §7 wired up; defaulted off.
11. **Recording / runbook.** Operator-facing notes for screen
    recording, captions, and the demo-only credential disclaimer
    (including the §2.5 "proxy owns the ACR identity" caveat so
    viewers don't assume kubelet MSI is in play).

Each step is independently testable (proxy unit tests for 2; live
manual run for 5–8) and independently revertable; nothing in this
list modifies Gantry itself, the production deploy manifests, or the
existing [e2e/](../e2e) suite.

---

## 11. Open questions

1. **Tag-class noise.** Should the demo numbers exclude
   `manifest_by_tag` from the headline aggregation, or include it
   with the caveat that Gantry v1 defers tag resolution
   ([archecture.md](archecture.md) F9)? Current plan: exclude from
   headline; report separately in Row 1 of the dashboard.
2. **Image freshness mechanism.** Embedding a UUID file produces a
   new layer but inflates by exactly one file; do we instead use
   `--build-arg CACHEBUST=$(uuidgen)` to invalidate the whole image,
   or rebuild the bottom layer to invalidate everything above it?
   Either works; we will pick once we have a real demo image in hand.
3. **Should the proxy support a passthrough auth mode** (forwarding
   the client's `Authorization` instead of replacing it) so the demo
   can also exercise Gantry's per-registry credential path?
   Tentatively no — it complicates the counter story without a clear
   payoff for v1.
4. **Multi-replica proxy** for very large clusters (>200 nodes). Out
   of scope for the initial demo; if needed, add an instance label
   to all counters and sum-by in Grafana.
