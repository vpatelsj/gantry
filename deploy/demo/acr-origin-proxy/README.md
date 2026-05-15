# `acr-origin-proxy` — ACR counting proxy

Demo-only counting reverse proxy in front of an Azure Container
Registry for the Gantry demo plan
([`docs/acr-counting-proxy-demo.md`](../../../docs/acr-counting-proxy-demo.md)).

## Status: full counting proxy (build-plan step 2 of plan §10)

This is the demo-only reverse proxy that sits in front of ACR for both
the baseline and Gantry paths. It implements:

- ACR Basic and Bearer-challenge auth handling (plan §3.5)
- OCI path classification (`blob`, `manifest_by_digest`,
  `manifest_by_tag`, `ping`, `other`) (plan §3.4)
- Prometheus `/metrics` on port 9090 (plan §3.3)
- `/debug/summary` JSON on port 9090 for scripted assertions
- Streaming response body accounting with a 64 KiB copy buffer
- Optional synthetic blob-inflight throttling, disabled by default

It is still a demo artifact, not production infrastructure.

## Environment

| Env var                       | Required | Default | Meaning                                          |
| ----------------------------- | -------- | ------- | ------------------------------------------------ |
| `UPSTREAM_REGISTRY`           | yes      |         | e.g. `https://myacr.azurecr.io`                  |
| `ACR_USERNAME`                | yes      |         | ACR admin user (DEMO ONLY)                       |
| `ACR_PASSWORD`                | yes      |         | ACR admin password (DEMO ONLY)                   |
| `AUTH_MODE`                   | no       | `auto`  | `basic` \| `bearer` \| `auto` (plan §3.5)        |
| `LISTEN_ADDR`                 | no       | `:5002` | Bind address.                                    |
| `METRICS_LISTEN_ADDR`         | no       | `:9090` | `/metrics`, `/debug/summary`, `/healthz`.        |
| `MAX_TOKEN_LIFETIME_SECONDS`  | no       | `1800`  | Hard cap on a cached Bearer token's lifetime.    |
| `REFRESH_SKEW_SECONDS`        | no       | `30`    | Refresh tokens this many seconds before expiry.  |
| `THROTTLE_BLOB_INFLIGHT`      | no       | `0`     | Optional synthetic 429 threshold; 0 disables it. |
| `THROTTLE_RETRY_AFTER_SECONDS`| no       | `5`     | `Retry-After` value for synthetic 429s.           |

## Run locally (Phase 0.5 manual gate)

```bash
ACR_NAME=myacr
ACR_USERNAME=...     # from `az acr credential show --name $ACR_NAME`
ACR_PASSWORD=...

UPSTREAM_REGISTRY=https://${ACR_NAME}.azurecr.io \
ACR_USERNAME=${ACR_USERNAME} \
ACR_PASSWORD=${ACR_PASSWORD} \
LISTEN_ADDR=127.0.0.1:5002 \
go run .
```

In another shell, walk the Phase 0.5 checklist (see plan §6):

```bash
curl -sv -o /dev/null -w "%{http_code}\n" \
  http://127.0.0.1:5002/v2/
curl -sv -o /dev/null -w "%{http_code}\n" \
  http://127.0.0.1:5002/v2/<repo>/manifests/<tag>
curl -sv -o /dev/null -w "%{http_code}\n" \
  http://127.0.0.1:5002/v2/<repo>/manifests/sha256:<digest>
curl -sv -o /dev/null -w "%{http_code}\n" \
  http://127.0.0.1:5002/v2/<repo>/blobs/sha256:<digest>
```

Watch the proxy's log lines: each request prints `method`, `path`,
path class, upstream/client byte counts, and
`auth_refreshed=true|false`. Then inspect the counters:

```bash
curl -fsS http://127.0.0.1:9090/debug/summary | jq .
curl -fsS http://127.0.0.1:9090/metrics | grep '^origin_'
```

For a Bearer challenge, the first request to a given scope should
increment `origin_auth_token_refresh_total{result="success"}`; a
second request to the same scope should reuse the cached token until
it enters the refresh-skew window.

## Deploy to `gantry-demo` namespace

```bash
kubectl create namespace gantry-demo

cp secret.example.yaml secret.yaml          # fill in real creds
kubectl apply -f secret.yaml

# Edit UPSTREAM_REGISTRY in deployment.yaml first, or kustomize-patch it.
kubectl apply -f service.yaml
kubectl apply -f deployment.yaml
```

The Service exposes port 5002 for proxy traffic and port 9090 for
metrics/debug endpoints inside the cluster. Pick the node-reachable
address for port 5002 per plan §4.5 (default: the Service's ClusterIP
literal — record the verified `ip route`/`curl` evidence from a node
before committing to that path).
