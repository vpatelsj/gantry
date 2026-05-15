#!/usr/bin/env bash
# Provision the greenfield Azure foundation for the ACR counting-proxy demo.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd az kubectl
require_env ACR_NAME
validate_acr_name
select_subscription

log "Creating or updating resource group ${AZ_RESOURCE_GROUP} in ${AZ_LOCATION}"
az group create \
    --name "${AZ_RESOURCE_GROUP}" \
    --location "${AZ_LOCATION}" \
    -o none

if az acr show --resource-group "${AZ_RESOURCE_GROUP}" --name "${ACR_NAME}" >/dev/null 2>&1; then
    log "ACR ${ACR_NAME} already exists; ensuring admin user is enabled for the demo"
    az acr update \
        --resource-group "${AZ_RESOURCE_GROUP}" \
        --name "${ACR_NAME}" \
        --admin-enabled true \
        -o none
else
    log "Creating ACR ${ACR_NAME} (${ACR_SKU})"
    az acr create \
        --resource-group "${AZ_RESOURCE_GROUP}" \
        --name "${ACR_NAME}" \
        --sku "${ACR_SKU}" \
        --admin-enabled true \
        -o none
fi

acr_id="$(acr_resource_id)"

if az aks show --resource-group "${AZ_RESOURCE_GROUP}" --name "${AKS_NAME}" >/dev/null 2>&1; then
    log "AKS cluster ${AKS_NAME} already exists; leaving node pool shape unchanged"
else
    log "Creating AKS ${AKS_NAME} with ${AKS_NODE_COUNT} x ${AKS_NODE_VM_SIZE} nodes"
    aks_args=(
        --resource-group "${AZ_RESOURCE_GROUP}"
        --name "${AKS_NAME}"
        --location "${AZ_LOCATION}"
        --node-count "${AKS_NODE_COUNT}"
        --node-vm-size "${AKS_NODE_VM_SIZE}"
        --node-osdisk-size "${AKS_NODE_OS_DISK_GB}"
        --network-plugin "${AKS_NETWORK_PLUGIN}"
        --enable-managed-identity
        --generate-ssh-keys
        --attach-acr "${acr_id}"
    )
    if [[ -n "${AKS_KUBERNETES_VERSION:-}" ]]; then
        aks_args+=(--kubernetes-version "${AKS_KUBERNETES_VERSION}")
    fi
    if [[ -n "${AKS_CREATE_EXTRA_ARGS:-}" ]]; then
        # Intentional word-splitting for operator-provided az aks flags.
        # shellcheck disable=SC2206
        extra_args=(${AKS_CREATE_EXTRA_ARGS})
        aks_args+=("${extra_args[@]}")
    fi
    az aks create "${aks_args[@]}" -o none
fi

log "Ensuring AKS has AcrPull on ${ACR_NAME}"
az aks update \
    --resource-group "${AZ_RESOURCE_GROUP}" \
    --name "${AKS_NAME}" \
    --attach-acr "${acr_id}" \
    -o none

ensure_kube_credentials
write_state_file

log "Cluster nodes"
kubectl get nodes -o wide

cat <<EOF

Provisioning foundation is ready.
Next:
  deploy/demo/infra/20-build-push-images.sh ${1:-${DEMO_INFRA_DIR}/env.local}
EOF
