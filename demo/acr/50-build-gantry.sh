#!/usr/bin/env bash
# 50-build-gantry.sh — build the gantry image and push it to the demo
# ACR. Uses the existing deploy/build.sh helper.
#
# Auth: `az acr login -n ${ACR_NAME}` writes a short-lived token into
# ~/.docker/config.json that buildx picks up. Caller must hold AcrPush
# or Contributor on the registry. The AKS-attached AcrPull MSI is *not*
# used here (it's pull-only and only seen by kubelet on each node).

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh
# shellcheck disable=SC1091
source ./.provision-state

echo "==> az acr login -n ${ACR_NAME}"
az acr login -n "${ACR_NAME}" >/dev/null

REPO="${ACR_LOGIN_SERVER}/gantry"
TAG="${GANTRY_IMAGE_TAG}"

echo "==> Building + pushing ${REPO}:${TAG} (linux/amd64)"
( cd ../.. && \
  ./deploy/build.sh \
      --platform linux/amd64 \
      --repo "${REPO}" \
      --tag "${TAG}" \
      --push )

echo
echo "Pushed ${REPO}:${TAG}."
