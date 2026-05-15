#!/usr/bin/env bash
# 61-record-with-gantry.sh — wait for ingest, scrape Prometheus deltas
# + Azure Monitor + Log Analytics, capture gantry's own footprint.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

RUN_ID="$(cat .run-id-with-gantry)"
START_ISO="$(cat .with-gantry-start)"
END_ISO="$(cat .with-gantry-end)"
ARTIFACT="${ARTIFACTS_DIR}/with-gantry-${RUN_ID}.json"
PROM_BEFORE="${ARTIFACTS_DIR}/with-gantry-${RUN_ID}-prom-before.json"

echo "==> Sleeping ${AZ_INGEST_LAG_SECONDS}s for Azure Monitor / Log Analytics ingest"
sleep "${AZ_INGEST_LAG_SECONDS}"

echo "==> Scraping POD_READY timestamps"
POD_READY_LOG="${ARTIFACTS_DIR}/with-gantry-${RUN_ID}-pod-ready.log"
: > "${POD_READY_LOG}"
for pod in $(kubectl get pods -l gantry.demo/run-label=cold -o jsonpath='{.items[*].metadata.name}'); do
    kubectl logs --tail=20 "${pod}" 2>/dev/null \
        | grep -E '^POD_READY ' \
        | sed "s|^|${pod} |" >> "${POD_READY_LOG}"
done
echo "  $(wc -l < "${POD_READY_LOG}") POD_READY rows captured → ${POD_READY_LOG}"

echo "==> Snapshotting Prometheus counters (after)"
prom_after_origin="$(prom_query_scalar 'sum(p2p_origin_pull_total)')"
prom_after_origin_succ="$(prom_query_scalar 'sum(p2p_origin_pull_success_total)')"
prom_after_peer="$(prom_query_scalar 'sum(p2p_peer_fetch_total)')"
prom_after_cache="$(prom_query_scalar 'sum(p2p_cache_hit_total)')"

prom_before_origin=$(jq '.origin_pull_total' "${PROM_BEFORE}")
prom_before_origin_succ=$(jq '.origin_pull_success_total' "${PROM_BEFORE}")
prom_before_peer=$(jq '.peer_fetch_total' "${PROM_BEFORE}")
prom_before_cache=$(jq '.cache_hit_total' "${PROM_BEFORE}")

origin_delta=$(awk "BEGIN{print ${prom_after_origin} - ${prom_before_origin}}")
origin_succ_delta=$(awk "BEGIN{print ${prom_after_origin_succ} - ${prom_before_origin_succ}}")
peer_delta=$(awk "BEGIN{print ${prom_after_peer} - ${prom_before_peer}}")
cache_delta=$(awk "BEGIN{print ${prom_after_cache} - ${prom_before_cache}}")

echo "  origin Δ=${origin_delta}  origin_success Δ=${origin_succ_delta}"
echo "  peer Δ=${peer_delta}      cache Δ=${cache_delta}"

echo "==> Gantry self CPU/mem (cAdvisor)"
gantry_cpu="$(prom_query_scalar 'sum(rate(container_cpu_usage_seconds_total{namespace="gantry-system",pod=~"gantry-.*",container="gantry"}[1m]))')"
gantry_mem_max="$(prom_query_scalar 'max_over_time(sum(container_memory_working_set_bytes{namespace="gantry-system",pod=~"gantry-.*",container="gantry"})[10m:])')"

echo "==> KQL: total repository events"
TOTAL_EVENTS="$(run_kql "$(envsubst < queries/acr-total-events.kql)")"
echo "==> KQL: 429-only"
THR_EVENTS="$(run_kql "$(envsubst < queries/acr-throttling.kql)")"

echo "==> Azure Monitor: TotalPullCount + SuccessfulPullCount"
TPC="$(acr_metric TotalPullCount      "${START_ISO}" "${END_ISO}")"
SPC="$(acr_metric SuccessfulPullCount "${START_ISO}" "${END_ISO}")"

jq -n \
    --arg run_id "${RUN_ID}" \
    --arg start  "${START_ISO}" \
    --arg end    "${END_ISO}" \
    --argjson total_events "${TOTAL_EVENTS}" \
    --argjson throttling   "${THR_EVENTS}" \
    --argjson tpc          "${TPC}" \
    --argjson spc          "${SPC}" \
    --arg origin_delta      "${origin_delta}" \
    --arg origin_succ_delta "${origin_succ_delta}" \
    --arg peer_delta        "${peer_delta}" \
    --arg cache_delta       "${cache_delta}" \
    --arg gantry_cpu        "${gantry_cpu}" \
    --arg gantry_mem_max    "${gantry_mem_max}" \
    '{
        scenario: "gantry-cold-start",
        run_id: $run_id,
        window: { start: $start, end: $end },
        gantry_prom_deltas: {
            p2p_origin_pull_total: $origin_delta,
            p2p_origin_pull_success_total: $origin_succ_delta,
            p2p_peer_fetch_total: $peer_delta,
            p2p_cache_hit_total: $cache_delta
        },
        gantry_footprint: {
            cpu_cores_avg: $gantry_cpu,
            memory_working_set_max_bytes: $gantry_mem_max
        },
        acr_repository_events: $total_events,
        acr_throttling: $throttling,
        azure_monitor: { total_pull_count: $tpc, successful_pull_count: $spc }
    }' > "${ARTIFACT}"

echo
echo "Cold-start artifact written: ${ARTIFACT}"
