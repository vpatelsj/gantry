#!/usr/bin/env bash
# 00-prereqs.sh — sanity-check tooling and Azure auth before provisioning.
#
# Run before 10-provision.sh. Purely read-only: no resources are created.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh

err=0

require_cmd() {
    local cmd="$1"
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        echo "MISSING: ${cmd}" >&2
        err=1
    else
        echo "  ok: ${cmd} ($(command -v "${cmd}"))"
    fi
}

echo "==> Checking required tools"
require_cmd az
require_cmd kubectl
require_cmd helm
require_cmd jq
require_cmd envsubst
require_cmd docker

echo "==> Checking docker buildx"
if ! docker buildx version >/dev/null 2>&1; then
    echo "MISSING: docker buildx (install Docker Desktop or buildx plugin)" >&2
    err=1
else
    echo "  ok: docker buildx"
fi

echo "==> Checking az login"
if ! az account show >/dev/null 2>&1; then
    echo "az not logged in. Run 'az login' first." >&2
    err=1
else
    sub_now="$(az account show --query id -o tsv)"
    echo "  ok: subscription ${sub_now}"
    if [[ -n "${SUBSCRIPTION_ID}" && "${SUBSCRIPTION_ID}" != "${sub_now}" ]]; then
        echo "  note: env SUBSCRIPTION_ID=${SUBSCRIPTION_ID} differs from active sub; will switch in 10-provision.sh"
    fi
fi

echo "==> Checking ACR_NAME shape"
if [[ ! "${ACR_NAME}" =~ ^[a-z0-9]{5,50}$ ]]; then
    echo "ACR_NAME='${ACR_NAME}' must be 5-50 lowercase alphanumerics" >&2
    err=1
else
    echo "  ok: ${ACR_NAME}"
fi

echo "==> Checking BUDGET_ALERT_EMAIL"
if [[ "${BUDGET_ALERT_EMAIL}" == "you@example.com" || -z "${BUDGET_ALERT_EMAIL}" ]]; then
    echo "  WARN: BUDGET_ALERT_EMAIL is unset or the placeholder; 10b-set-budget-alert.sh will skip the budget alert (cluster cost will not be auto-paged)."
else
    echo "  ok: ${BUDGET_ALERT_EMAIL}"
fi

if (( err != 0 )); then
    echo
    echo "Prereq check FAILED — fix the above before running 10-provision.sh." >&2
    exit 1
fi

echo
echo "All prereqs satisfied."
