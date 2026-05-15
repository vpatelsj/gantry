#!/usr/bin/env bash
# 61-record-with-gantry.sh — fast-path cold-start recorder.
#
# Skips Log Analytics. Headline data:
#   1. Prometheus gantry deltas (instant; the WHOLE story when gantry
#      is on the path — origin pulls vs peer fetches vs cache hits).
#   2. Kubelet pull-event durations.
#   3. Containerd journald 429 scan (instant).
#   4. Azure Monitor TotalPullCount (~1 min lag, best-effort).
#   5. Gantry self CPU/mem from cAdvisor.

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
PULL_EVENTS="${ARTIFACTS_DIR}/with-gantry-${RUN_ID}-pull-events.json"
THROTTLE_SUMMARY="${ARTIFACTS_DIR}/with-gantry-${RUN_ID}-throttle.json"
THROTTLE_RAW_DIR="${ARTIFACTS_DIR}/with-gantry-${RUN_ID}-throttle-raw"

echo "==> POD_READY timestamps"
POD_READY_LOG="${ARTIFACTS_DIR}/with-gantry-${RUN_ID}-pod-ready.log"
: > "${POD_READY_LOG}"
for pod in $(kubectl get pods -l gantry.demo/run-label=cold -o jsonpath='{.items[*].metadata.name}'); do
    kubectl logs --tail=20 "${pod}" 2>/dev/null \
        | grep -E '^POD_READY ' \
        | sed "s|^|${pod} |" >> "${POD_READY_LOG}"
done
echo "  $(wc -l < "${POD_READY_LOG}") POD_READY rows → ${POD_READY_LOG}"

echo "==> Kubelet Pulling/Pulled events per pod"
scrape_pull_events cold "${PULL_EVENTS}"

echo "==> Prometheus counters (after)"
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

echo "==> Containerd journald 429/throttle scan"
scrape_containerd_429s "${START_ISO}" "${END_ISO}" "${THROTTLE_SUMMARY}" "${THROTTLE_RAW_DIR}"

echo "==> Gantry self CPU/mem (cAdvisor)"
gantry_cpu="$(prom_query_scalar 'sum(rate(container_cpu_usage_seconds_total{namespace="gantry-system",pod=~"gantry-.*",container="gantry"}[1m]))')"
gantry_mem_max="$(prom_query_scalar 'max_over_time(sum(container_memory_working_set_bytes{namespace="gantry-system",pod=~"gantry-.*",container="gantry"})[10m:])')"

echo "==> Azure Monitor TotalPullCount + SuccessfulPullCount (best-effort)"
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
    --arg origin_delta      "${origin_delta}" \
    --arg origin_succ_delta "${origin_succ_delta}" \
    --arg peer_delta        "${peer_delta}" \
    --arg cache_delta       "${cache_delta}" \
    --arg gantry_cpu        "${gantry_cpu}" \
    --arg gantry_mem_max    "${gantry_mem_max}" \
    '{
        scenario: "gantry-cold-start",
        run_id: $run_id,
        window: { start: $window_start, end: $window_end },
        pull_events: $pull_events[0],
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
        throttling: $throttle[0],
        azure_monitor: { total_pull_count: $tpc, successful_pull_count: $spc }
    }' > "${ARTIFACT}"

echo
echo "Cold-start artifact: ${ARTIFACT}"
echo
echo "=== headline ==="
jq -r '
    "  pull duration p50/p95/max: \(.pull_events.summary.p50)s / \(.pull_events.summary.p95)s / \(.pull_events.summary.max)s",
    "  gantry origin Δ=\(.gantry_prom_deltas.p2p_origin_pull_total)  peer Δ=\(.gantry_prom_deltas.p2p_peer_fetch_total)  cache Δ=\(.gantry_prom_deltas.p2p_cache_hit_total)",
    "  ACR throttling: \(.throttling.total_429_lines) hit lines across \(.throttling.nodes_with_429)/\(.throttling.nodes_checked) nodes",
    "  Azure Monitor TotalPullCount: \([.azure_monitor.total_pull_count.value[]?.timeseries[]?.data[]?.total // 0] | add // 0)"
' "${ARTIFACT}"
