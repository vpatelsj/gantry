#!/usr/bin/env bash
# Deploy the demo ACR counting proxy.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd az kubectl sed
require_env ACR_NAME
validate_acr_name
select_subscription

login_server="$(acr_login_server)"
proxy="${login_server}/${PROXY_IMAGE_REPO}:${IMAGE_TAG}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

if [[ -z "${ACR_USERNAME:-}" || -z "${ACR_PASSWORD:-}" ]]; then
    log "Fetching ACR admin credential for Kubernetes Secret"
    ACR_USERNAME="$(az acr credential show --resource-group "${AZ_RESOURCE_GROUP}" --name "${ACR_NAME}" --query username -o tsv)"
    ACR_PASSWORD="$(az acr credential show --resource-group "${AZ_RESOURCE_GROUP}" --name "${ACR_NAME}" --query 'passwords[0].value' -o tsv)"
fi

log "Creating namespace ${GANTRY_DEMO_NAMESPACE}"
kubectl create namespace "${GANTRY_DEMO_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

log "Applying ACR admin Secret acr-admin-creds"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" create secret generic acr-admin-creds \
    --from-literal="username=${ACR_USERNAME}" \
    --from-literal="password=${ACR_PASSWORD}" \
    --dry-run=client -o yaml | kubectl apply -f -

log "Applying proxy Service"
kubectl apply -f "${REPO_ROOT}/deploy/demo/acr-origin-proxy/service.yaml"

log "Rendering proxy Deployment with image ${proxy} and upstream https://${login_server}"
sed \
    -e "s|image: acr-origin-proxy:dev|image: ${proxy}|" \
    -e "s|value: \"https://REPLACE_ME.azurecr.io\"|value: \"https://${login_server}\"|" \
    "${REPO_ROOT}/deploy/demo/acr-origin-proxy/deployment.yaml" \
    >"${tmpdir}/deployment.yaml"

kubectl apply -f "${tmpdir}/deployment.yaml"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" rollout restart deploy/acr-origin-proxy
kubectl -n "${GANTRY_DEMO_NAMESPACE}" rollout status deploy/acr-origin-proxy --timeout=5m

cluster_ip="$(kubectl -n "${GANTRY_DEMO_NAMESPACE}" get svc acr-origin-proxy -o jsonpath='{.spec.clusterIP}')"
write_state_file
printf 'export PROXY_CLUSTER_IP="%s"\n' "${cluster_ip}" >>"${DEMO_INFRA_DIR}/.state.env"

log "Proxy objects"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" get deploy,svc,pods -l app.kubernetes.io/name=acr-origin-proxy -o wide

log "Checking /healthz from inside the cluster"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" run "acr-proxy-health-$RANDOM" \
    --rm -i --restart=Never \
    --image=curlimages/curl:8.10.1 \
    --command -- curl -fsS "http://acr-origin-proxy:5002/healthz"

cat <<EOF

Proxy is deployed.
ClusterIP: ${cluster_ip}

Metrics and debug endpoints:
  kubectl -n ${GANTRY_DEMO_NAMESPACE} port-forward svc/acr-origin-proxy 9090:9090
  curl -fsS http://127.0.0.1:9090/debug/summary | jq .
  curl -fsS http://127.0.0.1:9090/metrics | grep '^origin_'

Next validation helpers:
  deploy/demo/infra/50-smoke-proxy-auth.sh ${1:-${DEMO_INFRA_DIR}/env.local}
  deploy/demo/infra/60-check-node-reachability.sh ${1:-${DEMO_INFRA_DIR}/env.local}
EOF
