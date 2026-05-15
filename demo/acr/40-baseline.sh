#!/usr/bin/env bash
# 40-baseline.sh — hammer the workload WITHOUT gantry to drive ACR
# pull load high enough to risk Basic-SKU throttling.
#
# Prereqs:
#   - hosts.toml absent on every node (asserted; otherwise containerd
#     would route through gantry and contaminate the baseline).
#   - 20-push-demo-image.sh has been run (need .run-id-baseline).
#
# Behaviour: applies the 20-pod podAntiAffinity Job
# BASELINE_HAMMER_ITERATIONS times back-to-back. Each iteration:
#   - Mints a fresh image tag via 20-push-demo-image.sh so layer
#     digests differ run-to-run (containerd can't serve from its own
#     content store on retry).
#   - Applies the Job, waits for completion, deletes it.
#
# Each iteration produces NODE_COUNT × digest_count ≈ 640 ACR
# repository events. With the default 5 iterations that's ~3200
# events fanned across ~5 minutes — much more likely to bite the
# Basic-SKU rate limit than a single 44-second pull burst.
#
# Override: set BASELINE_HAMMER_ITERATIONS=1 to keep the original
# single-run behaviour.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

ITERATIONS="${BASELINE_HAMMER_ITERATIONS:-5}"

# Hard-fail if hosts.toml is present anywhere — otherwise the "baseline"
# would secretly route through gantry's mirror.
assert_no_hosts_toml "${ACR_LOGIN_SERVER}"

# Record the start timestamp covering ALL iterations; 41-record-baseline.sh
# uses it to scope the containerd-journald 429 scan.
START_ISO="$(date -u +%FT%TZ)"
echo "${START_ISO}" > .baseline-start
> .baseline-run-ids

echo "==> Baseline hammer: ${ITERATIONS} iteration(s)"
for i in $(seq 1 "${ITERATIONS}"); do
    echo
    echo "==> Iteration ${i}/${ITERATIONS}: minting fresh image"
    # Each iteration gets its own RUN_ID; .run-id-baseline is overwritten
    # to point at the latest, which is what 41-record-baseline.sh reports.
    RUN_HISTORY_ROLE=baseline ./20-push-demo-image.sh >/dev/null

    RUN_ID="$(cat .run-id-baseline)"
    echo "${RUN_ID}" >> .baseline-run-ids
    IMAGE_REF="${ACR_LOGIN_SERVER}/${DEMO_REPO}:${RUN_ID}"
    echo "  IMAGE=${IMAGE_REF}"

    # Apply, wait, delete to free the Job name for the next iteration.
    apply_workload_job "${IMAGE_REF}" baseline >/dev/null
    wait_workload_job baseline 30m
    kubectl delete job gantry-demo-workload-baseline -n default --ignore-not-found --wait=false
done

END_ISO="$(date -u +%FT%TZ)"
echo "${END_ISO}" > .baseline-end

echo
echo "Baseline hammer complete."
echo "  iterations: ${ITERATIONS}"
echo "  run-id list: $(tr '\n' ' ' < .baseline-run-ids)"
echo "  window: ${START_ISO} .. ${END_ISO}"
echo "  Next: ./41-record-baseline.sh"
