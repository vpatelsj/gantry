#!/usr/bin/env bash
# 41-record-baseline.sh — fast-path baseline recorder.
#
# Skips Azure Log Analytics (5+ min ingest lag) entirely. The headline
# pull-load proof comes from:
#   1. Kubelet Pulling/Pulled events per pod (instant; per-pod
#      duration distribution).
#   2. Containerd journald 429 scan via a transient DaemonSet
#      (instant; ground truth for "did ACR throttle us").
#   3. Azure Monitor TotalPullCount / SuccessfulPullCount (~1 min
#      ingest lag; coarse but cheap to call).
#
# Log Analytics ContainerRegistryRepositoryEvents can be re-pulled
# later by 70-compare.sh once natural elapsed time has passed.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

RUN_ID="$(cat .run-id-baseline)"
START_ISO="$(cat .baseline-start)"
END_ISO="$(cat .baseline-end)"
ARTIFACT="${ARTIFACTS_DIR}/baseline-${RUN_ID}.json"
PULL_EVENTS="${ARTIFACTS_DIR}/baseline-${RUN_ID}-pull-events.json"
THROTTLE_SUMMARY="${ARTIFACTS_DIR}/baseline-${RUN_ID}-throttle.json"
THROTTLE_RAW_DIR="${ARTIFACTS_DIR}/baseline-${RUN_ID}-throttle-raw"

echo "==> POD_READY timestamps from pod logs"
POD_READY_LOG="${ARTIFACTS_DIR}/baseline-${RUN_ID}-pod-ready.log"
: > "${POD_READY_LOG}"
for pod in $(kubectl get pods -l gantry.demo/run-label=baseline -o jsonpath='{.items[*].metadata.name}'); do
    kubectl logs --tail=20 "${pod}" 2>/dev/null \
        | grep -E '^POD_READY ' \
        | sed "s|^|${pod} |" >> "${POD_READY_LOG}"
done
echo "  $(wc -l < "${POD_READY_LOG}") POD_READY rows → ${POD_READY_LOG}"

echo "==> Kubelet Pulling/Pulled events per pod"
scrape_pull_events baseline "${PULL_EVENTS}"

echo "==> Containerd journald 429/throttle scan (real-time, no Azure ingest)"
scrape_containerd_429s "${START_ISO}" "${END_ISO}" "${THROTTLE_SUMMARY}" "${THROTTLE_RAW_DIR}"

echo "==> Azure Monitor: TotalPullCount + SuccessfulPullCount (best-effort, ~1 min lag)"
TPC="$(safe_az_json acr_metric TotalPullCount      "${START_ISO}" "${END_ISO}")"
SPC="$(safe_az_json acr_metric SuccessfulPullCount "${START_ISO}" "${END_ISO}")"

jq -n \
    --arg run_id "${RUN_ID}" \
    --arg window_start "${START_ISO}" \
    --arg window_end   "${END_ISO}" \
    --slurpfile pull_events "${PULL_EVENTS}" \
    --slurpfile throttle    "${THROTTLE_SUMMARY}" \
    --argjson tpc "${TPC}" \
    --argjson spc "${SPC}" \
    '{
        scenario: "baseline",
        run_id: $run_id,
        window: { start: $window_start, end: $window_end },
        pull_events: $pull_events[0],
        throttling: $throttle[0],
        azure_monitor: { total_pull_count: $tpc, successful_pull_count: $spc }
    }' > "${ARTIFACT}"

echo
echo "Baseline artifact: ${ARTIFACT}"
echo
echo "=== headline ==="
jq -r '
    "  pull duration p50/p95/max: \(.pull_events.summary.p50)s / \(.pull_events.summary.p95)s / \(.pull_events.summary.max)s",
    "  ACR throttling: \(.throttling.total_429_lines) hit lines across \(.throttling.nodes_with_429)/\(.throttling.nodes_checked) nodes",
    "  Azure Monitor TotalPullCount in window: \([.azure_monitor.total_pull_count.value[]?.timeseries[]?.data[]?.total // 0] | add // 0)"
' "${ARTIFACT}"
