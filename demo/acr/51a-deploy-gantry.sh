#!/usr/bin/env bash
# 51a-deploy-gantry.sh — render the demo overlay (substitute
# ${ACR_LOGIN_SERVER} / ${GANTRY_IMAGE_TAG}), apply, wait for the
# DaemonSet to be Ready on every node.
#
# Does NOT install hosts.toml — that's 51b. If hosts.toml landed first,
# kubelet's pull of the gantry image itself could be redirected to
# 127.0.0.1:5000 on a node where gantry isn't running yet → bootstrap
# deadlock.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh
# shellcheck disable=SC1091
source ./.provision-state

# 1. Materialise the registry-credentials Secret from `az acr credential show`.
echo "==> Fetching ACR admin credentials"
ACR_USER="$(az acr credential show -n "${ACR_NAME}" --query username -o tsv)"
ACR_PASS="$(az acr credential show -n "${ACR_NAME}" --query 'passwords[0].value' -o tsv)"

kubectl get namespace gantry-system >/dev/null 2>&1 \
    || kubectl create namespace gantry-system

# Per the ConfigMap, credentials_path points at /etc/gantry/registry/${ACR_LOGIN_SERVER},
# and the agent reads the file as "username:password".
SECRET_KEY="${ACR_LOGIN_SERVER}"
echo "==> Creating Secret gantry-registry-credentials"
kubectl -n gantry-system create secret generic gantry-registry-credentials \
    --from-literal="${SECRET_KEY}=${ACR_USER}:${ACR_PASS}" \
    --dry-run=client -o yaml | kubectl apply -f -

# 2. Render overlay placeholders into a temp directory and apply.
WORKDIR="$(mktemp -d -t overlay-XXXXXX)"
trap 'rm -rf "${WORKDIR}"' EXIT
cp -R manifests/overlay/. "${WORKDIR}/"
# Rewrite the kustomization base paths to absolute (we moved out of
# manifests/overlay/, so ../../../../deploy/* is no longer correct).
DEPLOY_DIR="$(cd ../../deploy && pwd)"
sed -i.bak \
    -e "s|../../../../deploy/|${DEPLOY_DIR}/|g" \
    "${WORKDIR}/kustomization.yaml"
rm -f "${WORKDIR}/kustomization.yaml.bak"
# Substitute the ACR placeholders in the patch files.
for f in "${WORKDIR}/daemonset-image.yaml" "${WORKDIR}/configmap-patch.yaml"; do
    ACR_LOGIN_SERVER="${ACR_LOGIN_SERVER}" \
        GANTRY_IMAGE_TAG="${GANTRY_IMAGE_TAG}" \
        envsubst '${ACR_LOGIN_SERVER} ${GANTRY_IMAGE_TAG}' < "${f}" > "${f}.sub"
    mv "${f}.sub" "${f}"
done

echo "==> kubectl apply -k ${WORKDIR}"
kubectl apply -k "${WORKDIR}"

echo "==> Waiting for DaemonSet rollout"
kubectl -n gantry-system rollout status ds/gantry --timeout=10m

echo "==> Waiting for /readyz on every gantry pod (sample-checked via 'ready' status)"
for i in $(seq 1 60); do
    desired="$(kubectl -n gantry-system get ds/gantry -o jsonpath='{.status.desiredNumberScheduled}')"
    ready="$(kubectl -n gantry-system get ds/gantry -o jsonpath='{.status.numberReady}')"
    if [[ "${desired}" -gt 0 && "${desired}" == "${ready}" ]]; then
        echo "  Ready ${ready}/${desired}"
        break
    fi
    echo "  ($i) ${ready}/${desired} ready, sleeping 5s"
    sleep 5
done

echo
echo "Gantry DaemonSet Ready on every node."
echo "Next: ./51b-install-hosts-toml.sh"
