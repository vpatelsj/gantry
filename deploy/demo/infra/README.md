# Greenfield Azure infra scripts

These scripts create the Azure foundation for the ACR counting-proxy
demo from scratch:

- Resource group
- ACR with demo-only admin credentials enabled
- AKS node pool sized for the 20-node pull comparison
- kube-prometheus-stack for cluster visibility
- The current `acr-origin-proxy` counting proxy deployed in Kubernetes

They intentionally live under `deploy/demo/infra/` so the whole demo
surface remains isolated from production manifests.

## Current implementation boundary

The proxy in `deploy/demo/acr-origin-proxy/` implements build-plan
step 2: auth, streaming, OCI path classification, Prometheus metrics,
and `/debug/summary`. The fail-closed hosts.toml templates, installer
DaemonSet, and demo Gantry ConfigMap template are also present. The
build-tagged harness skeleton is present; baseline/cold/warm phase
logic and the Grafana dashboard remain follow-up build-plan steps.

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

5. Deploy the proxy:

   ```bash
   deploy/demo/infra/40-deploy-proxy-spike.sh deploy/demo/infra/env.local
   ```

6. Run Phase 0.5 helpers:

   ```bash
   deploy/demo/infra/50-smoke-proxy-auth.sh deploy/demo/infra/env.local
   deploy/demo/infra/60-check-node-reachability.sh deploy/demo/infra/env.local
   ```

7. Install fail-closed node routing when you are ready to run a phase:

   ```bash
   deploy/demo/infra/70-install-hosts-toml.sh deploy/demo/infra/env.local baseline
   deploy/demo/infra/70-install-hosts-toml.sh deploy/demo/infra/env.local gantry
   ```

   The installer DaemonSet stays alive for log inspection. Delete it
   after confirming every node wrote the expected file:

   ```bash
   kubectl -n gantry-demo delete ds/hosts-toml-installer
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
