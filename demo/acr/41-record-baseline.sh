#!/usr/bin/env bash
# 41-record-baseline.sh — sleep ≥ ingest lag, then capture pod-ready
# timestamps + KQL events + Azure Monitor metrics for the baseline run.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

RUN_ID="$(cat .run-id-baseline)"
START_ISO="$(cat .baseline-start)"
END_ISO="$(cat .baseline-end)"
ARTIFACT="${ARTIFACTS_DIR}/baseline-${RUN_ID}.json"

echo "==> Sleeping ${AZ_INGEST_LAG_SECONDS}s for Azure Monitor / Log Analytics ingest"
sleep "${AZ_INGEST_LAG_SECONDS}"

echo "==> Scraping POD_READY timestamps from pod logs"
POD_READY_LOG="${ARTIFACTS_DIR}/baseline-${RUN_ID}-pod-ready.log"
: > "${POD_READY_LOG}"
for pod in $(kubectl get pods -l gantry.demo/run-label=baseline -o jsonpath='{.items[*].metadata.name}'); do
    kubectl logs --tail=20 "${pod}" 2>/dev/null \
        | grep -E '^POD_READY ' \
        | sed "s|^|${pod} |" >> "${POD_READY_LOG}"
done
echo "  $(wc -l < "${POD_READY_LOG}") POD_READY rows captured → ${POD_READY_LOG}"

echo "==> Running KQL: total repository events (headline)"
TOTAL_KQL_FILE="queries/acr-total-events.kql"
TOTAL_KQL="$(envsubst < "${TOTAL_KQL_FILE}")"
TOTAL_EVENTS="$(run_kql "${TOTAL_KQL}")"

echo "==> Running KQL: 429-only events (bonus throttling signal)"
THR_KQL="$(envsubst < queries/acr-throttling.kql)"
THR_EVENTS="$(run_kql "${THR_KQL}")"

echo "==> Azure Monitor: TotalPullCount + SuccessfulPullCount"
TPC="$(acr_metric TotalPullCount      "${START_ISO}" "${END_ISO}")"
SPC="$(acr_metric SuccessfulPullCount "${START_ISO}" "${END_ISO}")"

# Compose a single artifact JSON.
jq -n \
    --arg run_id "${RUN_ID}" \
    --arg start  "${START_ISO}" \
    --arg end    "${END_ISO}" \
    --argjson total_events "${TOTAL_EVENTS}" \
    --argjson throttling   "${THR_EVENTS}" \
    --argjson tpc          "${TPC}" \
    --argjson spc          "${SPC}" \
    '{
        scenario: "baseline",
        run_id: $run_id,
        window: { start: $start, end: $end },
        acr_repository_events: $total_events,
        acr_throttling: $throttling,
        azure_monitor: { total_pull_count: $tpc, successful_pull_count: $spc }
    }' > "${ARTIFACT}"

echo
echo "Baseline artifact written: ${ARTIFACT}"
