#!/usr/bin/env bash
# 40-baseline.sh — hammer the workload WITHOUT gantry to drive ACR
# pull load high enough to risk Basic-SKU throttling.
#
# Prereqs:
#   - hosts.toml absent on every node (asserted; otherwise containerd
#     would route through gantry and contaminate the baseline).
#   - 20-push-demo-image.sh has been run (need .run-id-baseline).
#
# Behaviour:
#   Phase A — pre-build BASELINE_HAMMER_ITERATIONS fresh image tags
#     (each with a distinct RUN_ID so layer digests differ run-to-run;
#     containerd can't serve from its own content store on retry).
#   Phase B — fire ALL Jobs IN PARALLEL. Each Job has its own
#     gantry.demo/job-id and a podAntiAffinity scoped to that id, so
#     all N Jobs can each place 20 pods (one per node) on the cluster
#     simultaneously. Total concurrent pod pulls = NODE_COUNT × N.
#
# With the default 10 iterations × 20 nodes that's 200 simultaneous
# image pulls against a single Basic-SKU ACR — enough to bite ReadOps
# rate limits, which is the throttling signal we're trying to capture.
#
# Override: set BASELINE_HAMMER_ITERATIONS=1 to keep the original
# single-run behaviour.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

ITERATIONS="${BASELINE_HAMMER_ITERATIONS:-10}"

# Hard-fail if hosts.toml is present anywhere — otherwise the "baseline"
# would secretly route through gantry's mirror.
assert_no_hosts_toml "${ACR_LOGIN_SERVER}"

# ---------- Phase A: pre-build all images ----------
> .baseline-run-ids
echo "==> Pre-building ${ITERATIONS} fresh image tag(s)"
for i in $(seq 1 "${ITERATIONS}"); do
    echo "  build ${i}/${ITERATIONS}"
    RUN_HISTORY_ROLE=baseline ./20-push-demo-image.sh >/dev/null
    cat .run-id-baseline >> .baseline-run-ids
    echo "" >> .baseline-run-ids
done
sed -i '/^$/d' .baseline-run-ids
echo "==> Pre-build complete: $(wc -l < .baseline-run-ids) tags ready"

# ---------- Phase B: fire all workload Jobs IN PARALLEL ----------
# Each iteration = one Job, distinct gantry.demo/job-id, same fan-out
# (NODE_COUNT pods/Job, podAntiAffinity scoped per Job so multiple
# Jobs can each place one pod per node simultaneously). Total
# concurrent pods = NODE_COUNT × ITERATIONS = 20 × 10 = 200 — enough
# parallel pulls to actually punch through Basic-SKU rate limits.
START_ISO="$(date -u +%FT%TZ)"
echo "${START_ISO}" > .baseline-start

PULL_APPEND="${ARTIFACTS_DIR}/baseline-pull-events.append.json"
POD_READY_LOG="${ARTIFACTS_DIR}/baseline-pod-ready.log"
mkdir -p "${ARTIFACTS_DIR}"
echo '[]' > "${PULL_APPEND}"
> "${POD_READY_LOG}"

# Pre-clean any stale baseline Jobs from previous runs.
kubectl get jobs -n default -l gantry.demo/run-label=baseline -o name 2>/dev/null \
    | xargs -r kubectl delete -n default --wait=false

i=0
echo "==> Firing ${ITERATIONS} Jobs in parallel"
while IFS= read -r RUN_ID; do
    i=$(( i + 1 ))
    IMAGE_REF="${ACR_LOGIN_SERVER}/${DEMO_REPO}:${RUN_ID}"
    apply_workload_job "${IMAGE_REF}" baseline "${i}" >/dev/null
done < .baseline-run-ids

echo "==> Waiting for all ${ITERATIONS} Jobs to complete"
wait_jobs_by_label gantry.demo/run-label=baseline 60m

echo "==> Scraping pod events + POD_READY logs (before Job delete)"
scrape_pull_events_append baseline "${PULL_APPEND}" 0
for pod in $(kubectl get pods -l gantry.demo/run-label=baseline -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl logs --tail=20 "${pod}" 2>/dev/null \
        | grep -E '^POD_READY ' \
        | sed "s|^|${pod} |" >> "${POD_READY_LOG}"
done

# Cleanup.
kubectl get jobs -n default -l gantry.demo/run-label=baseline -o name 2>/dev/null \
    | xargs -r kubectl delete -n default --wait=false

END_ISO="$(date -u +%FT%TZ)"
echo "${END_ISO}" > .baseline-end

echo
echo "Baseline hammer complete."
echo "  iterations: ${ITERATIONS}"
echo "  run-id list: $(tr '\n' ' ' < .baseline-run-ids)"
echo "  window: ${START_ISO} .. ${END_ISO}"
echo "  pull events captured: $(jq 'length' "${PULL_APPEND}")"
echo "  POD_READY rows captured: $(wc -l < "${POD_READY_LOG}")"
echo "  Next: ./41-record-baseline.sh"
