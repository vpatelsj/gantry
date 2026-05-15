#!/usr/bin/env bash
# 70-compare.sh — print the three-column comparison table from the
# Phase 4 / 6 / 6b artifacts.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh

BL_RUN="$(cat .run-id-baseline 2>/dev/null || echo missing)"
WG_RUN="$(cat .run-id-with-gantry 2>/dev/null || echo missing)"

bl="${ARTIFACTS_DIR}/baseline-${BL_RUN}.json"
wg="${ARTIFACTS_DIR}/with-gantry-${WG_RUN}.json"
ca="${ARTIFACTS_DIR}/cached-${WG_RUN}.json"

for f in "$bl" "$wg" "$ca"; do
    if [[ ! -f "$f" ]]; then
        echo "Missing artifact: $f" >&2
        exit 1
    fi
done

# Helpers.
sum_events() {
    jq '[.acr_repository_events[]?.Events // .acr_repository_events[]?[1] // 0] | add // 0' "$1"
}
sum_throttle() {
    jq '[.acr_throttling[]?.Events // .acr_throttling[]?[1] // 0] | add // 0' "$1"
}
sum_metric() {
    # Azure Monitor metrics list returns
    #   .value[].timeseries[].data[].total
    jq '[.azure_monitor.'"$2"'.value[]?.timeseries[]?.data[]?.total // 0] | add // 0' "$1"
}
prom() {
    jq -r ".gantry_prom_deltas.\"$2\" // \"n/a\"" "$1"
}

bl_events="$(sum_events "$bl")"
wg_events="$(sum_events "$wg")"
ca_events="$(sum_events "$ca")"

bl_429="$(sum_throttle "$bl")"
wg_429="$(sum_throttle "$wg")"
ca_429="$(sum_throttle "$ca")"

bl_tpc="$(sum_metric "$bl" total_pull_count)"
wg_tpc="$(sum_metric "$wg" total_pull_count)"
ca_tpc="$(sum_metric "$ca" total_pull_count)"

bl_spc="$(sum_metric "$bl" successful_pull_count)"
wg_spc="$(sum_metric "$wg" successful_pull_count)"
ca_spc="$(sum_metric "$ca" successful_pull_count)"

# Pod-ready spread.
ready_span() {
    local lbl="$1" run="$2"
    local f="${ARTIFACTS_DIR}/${lbl}-${run}-pod-ready.log"
    [[ -f "$f" ]] || { echo "n/a"; return; }
    awk '{print $3}' "$f" \
        | sort \
        | awk 'NR==1{first=$0} END{print first" → "$0}'
}

bl_ready="$(ready_span baseline    "$BL_RUN")"
wg_ready="$(ready_span with-gantry "$WG_RUN")"
ca_ready="$(ready_span cached      "$WG_RUN")"

# Gantry footprint (only present in with-gantry artifact, but show
# the value in both gantry columns since the same DS is up).
gantry_cpu="$(jq -r '.gantry_footprint.cpu_cores_avg // "n/a"' "$wg")"
gantry_mem="$(jq -r '.gantry_footprint.memory_working_set_max_bytes // "n/a"' "$wg")"

printf '\n=== Demo comparison ===\n\n'
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'metric' 'baseline (no gantry)' 'gantry cold-start (coordinator)' 'gantry warm (cache)'
printf '|%s|%s|%s|%s|\n' \
    "$(printf '=%.0s' {1..52})" \
    "$(printf '=%.0s' {1..27})" \
    "$(printf '=%.0s' {1..37})" \
    "$(printf '=%.0s' {1..27})"

printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'pod-ready window (POD_READY logs)' "$bl_ready" "$wg_ready" "$ca_ready"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'ACR repo events (Log Analytics, headline)' "$bl_events" "$wg_events" "$ca_events"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'ACR TotalPullCount (Azure Monitor, coarse)' "$bl_tpc" "$wg_tpc" "$ca_tpc"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'ACR SuccessfulPullCount (Azure Monitor)' "$bl_spc" "$wg_spc" "$ca_spc"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'ACR 429 events (KQL, bonus)' "$bl_429" "$wg_429" "$ca_429"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'p2p_origin_pull_total Δ' 'n/a' "$(prom "$wg" p2p_origin_pull_total)" "$(prom "$ca" p2p_origin_pull_total)"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'p2p_peer_fetch_total Δ' 'n/a' "$(prom "$wg" p2p_peer_fetch_total)" "$(prom "$ca" p2p_peer_fetch_total)"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'p2p_cache_hit_total Δ' 'n/a' "$(prom "$wg" p2p_cache_hit_total)" "$(prom "$ca" p2p_cache_hit_total)"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'gantry CPU avg (cores)' 'n/a' "$gantry_cpu" "$gantry_cpu"
printf '| %-50s | %-25s | %-35s | %-25s |\n' \
    'gantry mem max (bytes)' 'n/a' "$gantry_mem" "$gantry_mem"
printf '\n'
printf 'Note: TotalPullCount is image-level and coarse; the headline ratio\n'
printf '      uses the ACR repo-events row above (per-operation granularity).\n'

# If everything looks healthy, print the headline ratio for the script
# to be useful in the recording.
if [[ "$bl_events" =~ ^[0-9]+$ ]] && (( bl_events > 0 )); then
    if [[ "$wg_events" =~ ^[0-9]+$ ]] && (( wg_events > 0 )); then
        ratio=$(awk "BEGIN{printf \"%.1fx\", ${bl_events}/${wg_events}}")
        printf '\nHeadline: ACR served %s fewer repository events with gantry (cold start).\n' "$ratio"
    fi
fi
