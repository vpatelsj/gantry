# `acr-origin-proxy` — auth + reachability spike

Demo-only counting reverse proxy in front of an Azure Container
Registry for the Gantry demo plan
([`docs/acr-counting-proxy-demo.md`](../../../docs/acr-counting-proxy-demo.md)).

## Status: SPIKE (build-plan step 1 of plan §10)

This is the **smallest possible** implementation of plan §3.5 (auth)
and §3.6 (streaming). It can be deployed against a real ACR to
validate, by hand, the two blockers that gate the rest of the demo:

1. ACR Bearer-challenge handling (token exchange at the realm
   endpoint and one-shot reissue per scope).
2. Node-reachability of the proxy from AKS nodes via the chosen
   address (ClusterIP literal, internal LoadBalancer, NodePort, or
   hostNetwork+hostPort) — see plan §4.5.

It **deliberately does not** implement:

- Path classification (§3.4)
- Prometheus `/metrics`, `/debug/summary`, started/completed counter
  pair (§3.3)
- Unit tests (§3.8)
- Synthetic-throttle support (§7)

Those land in build-plan step 2 onward.

## Environment

| Env var                       | Required | Default | Meaning                                          |
| ----------------------------- | -------- | ------- | ------------------------------------------------ |
| `UPSTREAM_REGISTRY`           | yes      |         | e.g. `https://myacr.azurecr.io`                  |
| `ACR_USERNAME`                | yes      |         | ACR admin user (DEMO ONLY)                       |
| `ACR_PASSWORD`                | yes      |         | ACR admin password (DEMO ONLY)                   |
| `AUTH_MODE`                   | no       | `auto`  | `basic` \| `bearer` \| `auto` (plan §3.5)        |
| `LISTEN_ADDR`                 | no       | `:5002` | Bind address.                                    |
| `MAX_TOKEN_LIFETIME_SECONDS`  | no       | `1800`  | Hard cap on a cached Bearer token's lifetime.    |
| `REFRESH_SKEW_SECONDS`        | no       | `30`    | Refresh tokens this many seconds before expiry.  |

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
upstream `status`, `bytes`, and `auth_refreshed=true|false`. The
first request to a given scope should print
`auth_refreshed=true` followed by a `token-refresh:` line; subsequent
requests to the same scope (until the cached token expires) must
print `auth_refreshed=false`.

## Deploy to `gantry-demo` namespace

```bash
kubectl create namespace gantry-demo

cp secret.example.yaml secret.yaml          # fill in real creds
kubectl apply -f secret.yaml

# Edit UPSTREAM_REGISTRY in deployment.yaml first, or kustomize-patch it.
kubectl apply -f service.yaml
kubectl apply -f deployment.yaml
```

The Service exposes port 5002 inside the cluster; pick the
node-reachable address per plan §4.5 (default: the Service's
ClusterIP literal — record the verified `ip route`/`curl` evidence
from a node before committing to that path).
