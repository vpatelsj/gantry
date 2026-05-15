#!/usr/bin/env bash
# 40-baseline.sh — apply the workload Job WITHOUT gantry deployed.
# Prereq: hosts.toml must be ABSENT on every node, otherwise containerd
# would route this run through gantry and contaminate the baseline.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

if [[ ! -f .run-id-baseline ]]; then
    echo "Mint a baseline RUN_ID first: ./20-push-demo-image.sh" >&2
    exit 1
fi
RUN_ID="$(cat .run-id-baseline)"
IMAGE_REF="${ACR_LOGIN_SERVER}/${DEMO_REPO}:${RUN_ID}"

# Hard-fail if hosts.toml is present anywhere — otherwise the "baseline"
# would secretly route through gantry's mirror.
assert_no_hosts_toml "${ACR_LOGIN_SERVER}"

# Record the start timestamp; 41-record-baseline.sh uses it for
# Azure Monitor / KQL windowing.
START_ISO="$(date -u +%FT%TZ)"
echo "${START_ISO}" > .baseline-start

apply_workload_job "${IMAGE_REF}" baseline >/dev/null
wait_workload_job baseline 30m

END_ISO="$(date -u +%FT%TZ)"
echo "${END_ISO}" > .baseline-end

echo
echo "Baseline workload Job complete."
echo "  RUN_ID=${RUN_ID}  start=${START_ISO}  end=${END_ISO}"
echo "  Next: ./41-record-baseline.sh"
