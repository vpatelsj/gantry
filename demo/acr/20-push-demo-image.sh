#!/usr/bin/env bash
# 20-push-demo-image.sh — generate a fat, unique-per-run multi-layer
# image and push it to ACR.
#
# Each invocation:
#   1. Mints a new RUN_ID (epoch seconds).
#   2. Generates DEMO_LAYER_COUNT layer payload files of DEMO_LAYER_BYTES
#      each, seeded by RUN_ID so layer digests differ across runs.
#   3. Renders demo/acr/imagegen/Dockerfile.tmpl → workdir/Dockerfile.
#   4. `docker buildx build --push` to <acr>/<DEMO_REPO>:<RUN_ID>.
#   5. Records RUN_ID + manifest digest + layer-digest set to disk so
#      Phase 6 can assert no overlap with the previous run.
#
# Auth: relies on `az acr login -n ${ACR_NAME}` for buildx push. Caller
# (the Azure CLI user) must hold AcrPush or Contributor on the ACR.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh
# shellcheck disable=SC1091
source ./.provision-state

RUN_ID="${RUN_ID_OVERRIDE:-$(date -u +%s)}"
TAG="${RUN_ID}"
IMAGE="${ACR_LOGIN_SERVER}/${DEMO_REPO}:${TAG}"

# Roll over .last-layers-baseline ↔ .last-layers-with-gantry tracking.
# Each call writes .run-id (the latest) and appends to .run-history.
# Phase 6 will set RUN_HISTORY_ROLE=with-gantry to label the new run.
ROLE="${RUN_HISTORY_ROLE:-baseline}"

WORKDIR="$(mktemp -d -t gantry-imagegen-XXXXXX)"
trap 'rm -rf "${WORKDIR}"' EXIT

echo "==> RUN_ID=${RUN_ID}  TAG=${TAG}  ROLE=${ROLE}  IMAGE=${IMAGE}"
echo "==> Workdir: ${WORKDIR}"

mkdir -p "${WORKDIR}/layers"
echo "==> Generating ${DEMO_LAYER_COUNT} layers × ${DEMO_LAYER_BYTES} bytes (seeded by RUN_ID)"
for ((i = 0; i < DEMO_LAYER_COUNT; i++)); do
    fname="$(printf '%02d' "$i")"
    # openssl enc with a derived key+iv produces deterministic but
    # RUN_ID-unique pseudo-random bytes. Falls back to /dev/urandom
    # if openssl isn't present (loses determinism but keeps uniqueness).
    if command -v openssl >/dev/null 2>&1; then
        seed="${RUN_ID}-${i}"
        # Stretch the seed to 32 bytes (key) + 16 bytes (iv).
        key="$(printf '%s' "${seed}-key" | openssl dgst -sha256 -binary | xxd -p -c 64)"
        iv="$(printf '%s' "${seed}-iv"  | openssl dgst -sha256 -binary | head -c 16 | xxd -p -c 32)"
        # AES-256-CTR over /dev/zero gives us a deterministic stream.
        head -c "${DEMO_LAYER_BYTES}" /dev/zero \
            | openssl enc -aes-256-ctr -K "${key}" -iv "${iv}" \
            > "${WORKDIR}/layers/${fname}.bin"
    else
        head -c "${DEMO_LAYER_BYTES}" /dev/urandom \
            > "${WORKDIR}/layers/${fname}.bin"
    fi
done

echo "==> Rendering Dockerfile"
LAYER_COPY_LINES=""
for ((i = 0; i < DEMO_LAYER_COUNT; i++)); do
    fname="$(printf '%02d' "$i")"
    LAYER_COPY_LINES+="COPY layers/${fname}.bin /payload/${fname}.bin"$'\n'
done
export RUN_ID LAYER_COPY_LINES
envsubst '${RUN_ID} ${LAYER_COPY_LINES}' \
    < imagegen/Dockerfile.tmpl > "${WORKDIR}/Dockerfile"

echo "==> az acr login -n ${ACR_NAME}"
az acr login -n "${ACR_NAME}" >/dev/null

echo "==> docker buildx build --push ${IMAGE}"
# Single platform: linux/amd64 to match Standard_D4s_v5 nodes. Use a
# named builder so we don't pick up an unrelated default with surprises.
BUILDER="gantry-demo-builder"
docker buildx inspect "${BUILDER}" >/dev/null 2>&1 \
    || docker buildx create --name "${BUILDER}" --driver docker-container >/dev/null
docker buildx use "${BUILDER}"
docker buildx build \
    --builder "${BUILDER}" \
    --platform linux/amd64 \
    --tag "${IMAGE}" \
    --push \
    "${WORKDIR}"

echo "==> Capturing manifest + layer digests"
# `az acr manifest show-metadata` is gone in newer az; use the v2 API
# directly via `az acr repository show-manifests`.
MANIFEST_JSON="$(az acr manifest list-metadata \
    --registry "${ACR_NAME}" --name "${DEMO_REPO}" \
    --orderby time_desc --top 1 -o json | jq '.[0]')"
MANIFEST_DIGEST="$(echo "${MANIFEST_JSON}" | jq -r '.digest')"

# Pull layer digests from the actual manifest (not the metadata doc).
ACR_TOKEN="$(az acr login --name "${ACR_NAME}" --expose-token --output tsv --query accessToken)"
LAYERS_JSON="$(curl -fsSL \
    -H "Authorization: Bearer ${ACR_TOKEN}" \
    -H "Accept: application/vnd.oci.image.manifest.v1+json" \
    -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
    "https://${ACR_LOGIN_SERVER}/v2/${DEMO_REPO}/manifests/${TAG}")"
echo "${LAYERS_JSON}" | jq -r '.layers[].digest' | sort -u > ".last-layers-${ROLE}.txt"

echo "${RUN_ID}"           > ".run-id-${ROLE}"
echo "${MANIFEST_DIGEST}"  > ".last-digest-${ROLE}"
# .run-id is the most-recent (used by Phase 4/Phase 6 helpers that don't
# care about role).
echo "${RUN_ID}"           > .run-id

cat <<EOF

Push complete.
  IMAGE=${IMAGE}
  RUN_ID=${RUN_ID}
  ROLE=${ROLE}
  MANIFEST_DIGEST=${MANIFEST_DIGEST}
  layer-digest count: $(wc -l < ".last-layers-${ROLE}.txt")
  layer digests file: demo/acr/.last-layers-${ROLE}.txt
EOF
