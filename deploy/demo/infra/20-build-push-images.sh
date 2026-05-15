#!/usr/bin/env bash
# Build and push the demo proxy image, plus the Gantry image used later.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd az docker
require_env ACR_NAME
validate_acr_name
select_subscription

login_server="$(acr_login_server)"
proxy="${login_server}/${PROXY_IMAGE_REPO}:${IMAGE_TAG}"
gantry="${login_server}/${GANTRY_IMAGE_REPO}:${IMAGE_TAG}"

log "Logging in to ACR ${ACR_NAME} (${login_server})"
az acr login --name "${ACR_NAME}" -o none

log "Building and pushing proxy image ${proxy} for ${IMAGE_PLATFORM}"
docker buildx build \
    --platform="${IMAGE_PLATFORM}" \
    --tag "${proxy}" \
    --push \
    "${REPO_ROOT}/deploy/demo/acr-origin-proxy"

if [[ "${BUILD_GANTRY_IMAGE}" == "true" ]]; then
    log "Building and pushing Gantry image ${gantry} for ${IMAGE_PLATFORM}"
    docker buildx build \
        --platform="${IMAGE_PLATFORM}" \
        --build-arg "GANTRY_VERSION=${IMAGE_TAG}" \
        --file "${REPO_ROOT}/deploy/Dockerfile" \
        --tag "${gantry}" \
        --push \
        "${REPO_ROOT}"
else
    warn "BUILD_GANTRY_IMAGE=false; skipping Gantry image build"
fi

write_state_file

cat <<EOF

Images pushed:
  proxy:  ${proxy}
  gantry: ${gantry}

Next:
  deploy/demo/infra/30-install-monitoring.sh ${1:-${DEMO_INFRA_DIR}/env.local}
  deploy/demo/infra/40-deploy-proxy-spike.sh ${1:-${DEMO_INFRA_DIR}/env.local}
EOF
