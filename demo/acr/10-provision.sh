#!/usr/bin/env bash
# 10-provision.sh — create RG, ACR (Basic, admin enabled), Log Analytics
# workspace, AKS cluster (--attach-acr), enable ACR diagnostic settings to
# Log Analytics for ContainerRegistryRepositoryEvents.
#
# Idempotent-ish: re-running is mostly safe; `az ... create` is no-op if
# the resource already exists with matching parameters.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh

if [[ -n "${SUBSCRIPTION_ID}" ]]; then
    az account set --subscription "${SUBSCRIPTION_ID}"
fi
SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
echo "Using subscription: ${SUBSCRIPTION_ID}"

echo "==> Resource group: ${RG_NAME} (${LOCATION})"
az group create --name "${RG_NAME}" --location "${LOCATION}" -o none

echo "==> ACR: ${ACR_NAME} (Basic, admin enabled)"
az acr create \
    --resource-group "${RG_NAME}" \
    --name "${ACR_NAME}" \
    --sku Basic \
    --admin-enabled true \
    -o none
ACR_ID="$(az acr show -g "${RG_NAME}" -n "${ACR_NAME}" --query id -o tsv)"
ACR_LOGIN_SERVER="$(az acr show -g "${RG_NAME}" -n "${ACR_NAME}" --query loginServer -o tsv)"
echo "  ACR_ID=${ACR_ID}"
echo "  ACR_LOGIN_SERVER=${ACR_LOGIN_SERVER}"

echo "==> Log Analytics workspace: ${LAW_NAME}"
az monitor log-analytics workspace create \
    --resource-group "${RG_NAME}" \
    --workspace-name "${LAW_NAME}" \
    --location "${LOCATION}" \
    -o none
LAW_ID="$(az monitor log-analytics workspace show \
    -g "${RG_NAME}" -n "${LAW_NAME}" --query id -o tsv)"
echo "  LAW_ID=${LAW_ID}"

echo "==> ACR diagnostic settings → Log Analytics"
# ContainerRegistryRepositoryEvents is the headline category; LoginEvents
# is included for completeness (cheap, useful when debugging auth).
az monitor diagnostic-settings create \
    --name "acr-to-law" \
    --resource "${ACR_ID}" \
    --workspace "${LAW_ID}" \
    --logs '[
        {"category":"ContainerRegistryRepositoryEvents","enabled":true},
        {"category":"ContainerRegistryLoginEvents","enabled":true}
    ]' \
    --metrics '[{"category":"AllMetrics","enabled":true}]' \
    -o none || echo "  (diagnostic setting may already exist; continuing)"

echo "==> AKS: ${AKS_NAME} (${NODE_COUNT}× ${NODE_VM_SIZE})"
az aks create \
    --resource-group "${RG_NAME}" \
    --name "${AKS_NAME}" \
    --location "${LOCATION}" \
    --node-count "${NODE_COUNT}" \
    --node-vm-size "${NODE_VM_SIZE}" \
    --enable-managed-identity \
    --attach-acr "${ACR_NAME}" \
    --workspace-resource-id "${LAW_ID}" \
    --enable-addons monitoring \
    --generate-ssh-keys \
    -o none

echo "==> Fetching AKS credentials"
az aks get-credentials \
    --resource-group "${RG_NAME}" \
    --name "${AKS_NAME}" \
    --overwrite-existing

echo "==> Sanity check"
kubectl get nodes -o wide

# Stash resolved values for downstream scripts.
cat > .provision-state <<EOF
SUBSCRIPTION_ID=${SUBSCRIPTION_ID}
RG_NAME=${RG_NAME}
ACR_NAME=${ACR_NAME}
ACR_ID=${ACR_ID}
ACR_LOGIN_SERVER=${ACR_LOGIN_SERVER}
LAW_ID=${LAW_ID}
LAW_NAME=${LAW_NAME}
AKS_NAME=${AKS_NAME}
EOF
echo
echo "Provisioning complete. State written to demo/acr/.provision-state."
