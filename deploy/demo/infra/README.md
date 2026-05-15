# Greenfield Azure infra scripts

These scripts create the Azure foundation for the ACR counting-proxy
demo from scratch:

- Resource group
- ACR with demo-only admin credentials enabled
- AKS node pool sized for the 20-node pull comparison
- kube-prometheus-stack for cluster visibility
- The current `acr-origin-proxy` Phase 0.5 spike deployed in Kubernetes

They intentionally live under `deploy/demo/infra/` so the whole demo
surface remains isolated from production manifests.

## Current implementation boundary

The proxy in `deploy/demo/acr-origin-proxy/` is build-plan step 1 only:
auth and reachability. It exposes `/healthz` and request logs, but it
does not yet expose `/metrics`, `/debug/summary`, path-class counters,
the hosts.toml installer, or the baseline/cold/warm harness. Use these
scripts to reach and validate Phase 0.5 before building the rest.

## Quick start

1. Create `deploy/demo/infra/env.local` using `env.example` as the
   template. At minimum, set a globally unique lowercase `ACR_NAME`.

2. Provision Azure:

   ```bash
   deploy/demo/infra/10-provision-azure.sh deploy/demo/infra/env.local
   ```

3. Build and push images to the new ACR:

   ```bash
   deploy/demo/infra/20-build-push-images.sh deploy/demo/infra/env.local
   ```

4. Install monitoring:

   ```bash
   deploy/demo/infra/30-install-monitoring.sh deploy/demo/infra/env.local
   ```

5. Deploy the proxy spike:

   ```bash
   deploy/demo/infra/40-deploy-proxy-spike.sh deploy/demo/infra/env.local
   ```

6. Run Phase 0.5 helpers:

   ```bash
   deploy/demo/infra/50-smoke-proxy-auth.sh deploy/demo/infra/env.local
   deploy/demo/infra/60-check-node-reachability.sh deploy/demo/infra/env.local
   ```

The scripts write non-secret generated state to `.state.env`. Secrets
remain in Azure and Kubernetes; the ACR password is not written to disk
by these scripts.

## Grafana

After monitoring installs:

```bash
kubectl -n monitoring port-forward svc/kps-grafana 3000:80
```

Open `http://localhost:3000`, user `admin`, password from
`GRAFANA_ADMIN_PASSWORD` in `env.local`.

## Cleanup

Set `CONFIRM_DESTROY=yes` in the environment or `env.local`, then run:

```bash
deploy/demo/infra/90-destroy-azure.sh deploy/demo/infra/env.local
```

This deletes the entire resource group, including AKS, ACR, disks,
load balancers, and public IPs.
