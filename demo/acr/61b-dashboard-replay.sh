#!/usr/bin/env bash
# 61b-dashboard-replay.sh — re-pull Azure Monitor + Log Analytics at
# +AZ_INGEST_REPLAY_SECONDS (default 10 min) for the recording cut.
# Useful when the original 5-min wait still missed late-ingest rows.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

RUN_ID="$(cat .run-id-with-gantry)"
START_ISO="$(cat .with-gantry-start)"
END_ISO="$(cat .with-gantry-end)"

ARTIFACT="${ARTIFACTS_DIR}/with-gantry-${RUN_ID}-replay.json"

# Re-poll for any late-arriving rows; cap at 5 min beyond what 61
# already waited for.
echo "==> Polling Log Analytics again for late-arriving rows"
wait_for_kql_ingest "${START_ISO}" "${END_ISO}" 300 15
TOTAL_EVENTS="$(run_kql "$(envsubst < queries/acr-total-events.kql)")"
THR_EVENTS="$(run_kql "$(envsubst < queries/acr-throttling.kql)")"
TPC="$(acr_metric TotalPullCount      "${START_ISO}" "${END_ISO}")"
SPC="$(acr_metric SuccessfulPullCount "${START_ISO}" "${END_ISO}")"

jq -n \
    --arg run_id "${RUN_ID}" \
    --argjson total_events "${TOTAL_EVENTS}" \
    --argjson throttling   "${THR_EVENTS}" \
    --argjson tpc          "${TPC}" \
    --argjson spc          "${SPC}" \
    '{
        scenario: "gantry-cold-start-replay",
        run_id: $run_id,
        acr_repository_events: $total_events,
        acr_throttling: $throttling,
        azure_monitor: { total_pull_count: $tpc, successful_pull_count: $spc }
    }' > "${ARTIFACT}"

echo "Replay artifact: ${ARTIFACT}"
