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
# image pulls against a single Basic-SKU ACR. For aggressive throttling
# probes, BASELINE_HAMMER_REPLICAS_PER_IMAGE can replay each already-built
# tag multiple times without paying another image-build round.
#
# Override: set BASELINE_HAMMER_ITERATIONS=1 to keep the original
# single-run behaviour.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

ITERATIONS="${BASELINE_HAMMER_ITERATIONS:-10}"
REPLICAS_PER_IMAGE="${BASELINE_HAMMER_REPLICAS_PER_IMAGE:-1}"
REUSE_RUN_IDS="${BASELINE_REUSE_RUN_IDS:-0}"

# Hard-fail if hosts.toml is present anywhere — otherwise the "baseline"
# would secretly route through gantry's mirror.
assert_no_hosts_toml "${ACR_LOGIN_SERVER}"

# ---------- Phase A: pre-build all images ----------
if [[ "${REUSE_RUN_IDS}" == "1" && -s .baseline-run-ids ]]; then
    echo "==> Reusing $(wc -l < .baseline-run-ids) pre-built image tag(s) from .baseline-run-ids"
else
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
fi

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
TOTAL_JOBS=$(( $(wc -l < .baseline-run-ids) * REPLICAS_PER_IMAGE ))
echo "==> Firing ${TOTAL_JOBS} Jobs in parallel (${REPLICAS_PER_IMAGE} replica(s) per image tag)"
while IFS= read -r RUN_ID; do
    IMAGE_REF="${ACR_LOGIN_SERVER}/${DEMO_REPO}:${RUN_ID}"
    for _ in $(seq 1 "${REPLICAS_PER_IMAGE}"); do
        i=$(( i + 1 ))
        apply_workload_job "${IMAGE_REF}" baseline "${i}" >/dev/null
    done
done < .baseline-run-ids

echo "==> Waiting for all ${TOTAL_JOBS} Jobs to complete"
wait_jobs_by_label gantry.demo/run-label=baseline 60m

echo "==> Scraping pod events + POD_READY logs (before Job delete)"
scrape_pull_events_bulk baseline "${PULL_APPEND}" 0
if [[ "${BASELINE_CAPTURE_POD_READY_LOGS:-0}" == "1" ]]; then
    kubectl get pods -l gantry.demo/run-label=baseline -o name 2>/dev/null \
        | sed 's|^pod/||' \
        | xargs -r -n1 -P20 sh -c '
            pod="$1"
            kubectl logs --tail=20 "${pod}" 2>/dev/null \
                | grep -E "^POD_READY " \
                | sed "s|^|${pod} |"
        ' sh >> "${POD_READY_LOG}"
else
    echo "  skipping POD_READY log scrape; set BASELINE_CAPTURE_POD_READY_LOGS=1 to collect it"
fi

# Cleanup.
kubectl get jobs -n default -l gantry.demo/run-label=baseline -o name 2>/dev/null \
    | xargs -r kubectl delete -n default --wait=false

END_ISO="$(date -u +%FT%TZ)"
echo "${END_ISO}" > .baseline-end

echo
echo "Baseline hammer complete."
echo "  image tags: $(wc -l < .baseline-run-ids)"
echo "  replicas per tag: ${REPLICAS_PER_IMAGE}"
echo "  jobs: ${TOTAL_JOBS}"
echo "  run-id list: $(tr '\n' ' ' < .baseline-run-ids)"
echo "  window: ${START_ISO} .. ${END_ISO}"
echo "  pull events captured: $(jq 'length' "${PULL_APPEND}")"
echo "  POD_READY rows captured: $(wc -l < "${POD_READY_LOG}")"
echo "  Next: ./41-record-baseline.sh"
