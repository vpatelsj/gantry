#!/usr/bin/env bash
# Deploy Gantry into the gantry-system namespace using the demo ConfigMap.
#
# Wiring:
#   containerd → 127.0.0.1:5000 (Gantry mirror)
#   Gantry     → http://acr-origin-proxy.gantry-demo.svc.cluster.local:5002
#   proxy      → ACR
#
# This script does NOT install the gantry hosts.toml on nodes — that
# is the job of:
#   deploy/demo/infra/70-install-hosts-toml.sh ENV gantry
# Run it AFTER this script and AFTER you have verified the wiring
# preflight in the harness (phase_gantry_cold_test.go).

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd az kubectl sed
require_env ACR_NAME
validate_acr_name
select_subscription

login_server="$(acr_login_server)"
gantry_image="${login_server}/${GANTRY_IMAGE_REPO}:${IMAGE_TAG}"
namespace="gantry-system"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

log "Applying gantry ServiceAccount, RBAC, and PriorityClass"
kubectl apply -f "${REPO_ROOT}/deploy/serviceaccount.yaml"

log "Rendering demo ConfigMap (upstream → proxy)"
sed \
    -e "s|@@ACR_LOGIN_SERVER@@|${login_server}|g" \
    -e "s|@@GANTRY_DEMO_NAMESPACE@@|${GANTRY_DEMO_NAMESPACE}|g" \
    "${DEMO_ROOT}/configmap.gantry-demo.yaml" \
    >"${tmpdir}/configmap.yaml"
kubectl apply -f "${tmpdir}/configmap.yaml"

log "Granting ACR pull to the gantry namespace via image pull secret"
acr_user="${ACR_USERNAME:-}"
acr_pass="${ACR_PASSWORD:-}"
if [[ -z "${acr_user}" || -z "${acr_pass}" ]]; then
    acr_user="$(az acr credential show --resource-group "${AZ_RESOURCE_GROUP}" --name "${ACR_NAME}" --query username -o tsv)"
    acr_pass="$(az acr credential show --resource-group "${AZ_RESOURCE_GROUP}" --name "${ACR_NAME}" --query 'passwords[0].value' -o tsv)"
fi
kubectl -n "${namespace}" create secret docker-registry gantry-acr-pull \
    --docker-server="${login_server}" \
    --docker-username="${acr_user}" \
    --docker-password="${acr_pass}" \
    --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${namespace}" patch serviceaccount gantry \
    --type merge \
    -p '{"imagePullSecrets":[{"name":"gantry-acr-pull"}]}'

log "Rendering Gantry DaemonSet with image ${gantry_image}"
sed \
    -e "s|image: ghcr.io/vpatelsj/gantry:latest|image: ${gantry_image}|" \
    "${REPO_ROOT}/deploy/daemonset.yaml" \
    >"${tmpdir}/daemonset.yaml"
kubectl apply -f "${tmpdir}/daemonset.yaml"

log "Waiting for Gantry DaemonSet rollout"
kubectl -n "${namespace}" rollout status ds/gantry --timeout=10m

log "Gantry pods"
kubectl -n "${namespace}" get pods -l app.kubernetes.io/name=gantry -o wide

cat <<EOF

Gantry is deployed in namespace ${namespace}.
Image: ${gantry_image}
Demo ConfigMap upstream endpoint:
  http://acr-origin-proxy.${GANTRY_DEMO_NAMESPACE}.svc.cluster.local:5002

Next steps:
  # 1. Run wiring preflight via the harness (Phase 2 §6).
  # 2. Install fail-closed gantry hosts.toml on nodes:
  deploy/demo/infra/70-install-hosts-toml.sh deploy/demo/infra/env.local gantry
EOF
