#!/usr/bin/env bash
# 63-record-cached.sh — fast-path warm-cache recorder with strict gates
# AND mandatory forensics capture.
#
# Skips Log Analytics. Headline data is the Prometheus deltas (origin
# and peer should both be 0; cache should account for everything) plus
# pull-event durations + containerd 429 scan.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

RUN_ID="$(cat .run-id-with-gantry)"
START_ISO="$(cat .cached-start)"
END_ISO="$(cat .cached-end)"
ARTIFACT="${ARTIFACTS_DIR}/cached-${RUN_ID}.json"
FORENSICS_DIR="${ARTIFACTS_DIR}/cached-${RUN_ID}-forensics"
PROM_BEFORE="${ARTIFACTS_DIR}/cached-${RUN_ID}-prom-before.json"
PULL_EVENTS="${ARTIFACTS_DIR}/cached-${RUN_ID}-pull-events.json"
THROTTLE_SUMMARY="${ARTIFACTS_DIR}/cached-${RUN_ID}-throttle.json"
THROTTLE_RAW_DIR="${ARTIFACTS_DIR}/cached-${RUN_ID}-throttle-raw"

mkdir -p "${FORENSICS_DIR}"

# 1. Forensics (capture FIRST so a later assertion failure still leaves them).
echo "==> Forensics: gantry /metrics + cache-dir listing per node"
for ds in $(kubectl -n gantry-system get pods -l app.kubernetes.io/name=gantry -o jsonpath='{.items[*].metadata.name}'); do
    node="$(kubectl -n gantry-system get pod "${ds}" -o jsonpath='{.spec.nodeName}')"
    {
        echo "# ${node} :: ${ds}"
        echo "## /metrics (gantry, head)"
        kubectl -n gantry-system exec "${ds}" -- \
            wget -qO- http://127.0.0.1:9095/metrics | grep -E '^p2p_(cache|origin|peer)' | head -50 || true
    } > "${FORENSICS_DIR}/${node}-gantry.txt"
done
for pod in $(kubectl -n gantry-system get pods -l app.kubernetes.io/name=gantry -o jsonpath='{.items[*].metadata.name}'); do
    node="$(kubectl -n gantry-system get pod "${pod}" -o jsonpath='{.spec.nodeName}')"
    kubectl -n gantry-system exec "${pod}" -- \
        sh -c 'find /var/lib/gantry/cache -maxdepth 4 -type f 2>/dev/null | head -200' \
        > "${FORENSICS_DIR}/${node}-cache-listing.txt" || true
done

echo "==> Kubelet Pulling/Pulled events per pod"
scrape_pull_events warm "${PULL_EVENTS}"

echo "==> Prometheus deltas"
prom_after_origin="$(prom_query_scalar 'sum(p2p_origin_pull_total)')"
prom_after_peer="$(prom_query_scalar 'sum(p2p_peer_fetch_total)')"
prom_after_cache="$(prom_query_scalar 'sum(p2p_cache_hit_total)')"

prom_before_origin=$(jq '.origin_pull_total' "${PROM_BEFORE}")
prom_before_peer=$(jq '.peer_fetch_total' "${PROM_BEFORE}")
prom_before_cache=$(jq '.cache_hit_total' "${PROM_BEFORE}")

origin_delta=$(awk "BEGIN{print ${prom_after_origin} - ${prom_before_origin}}")
peer_delta=$(awk "BEGIN{print ${prom_after_peer}   - ${prom_before_peer}}")
cache_delta=$(awk "BEGIN{print ${prom_after_cache}  - ${prom_before_cache}}")
echo "  origin Δ=${origin_delta}  peer Δ=${peer_delta}  cache Δ=${cache_delta}"

echo "==> Containerd journald 429/throttle scan"
scrape_containerd_429s "${START_ISO}" "${END_ISO}" "${THROTTLE_SUMMARY}" "${THROTTLE_RAW_DIR}"

echo "==> Azure Monitor TotalPullCount + SuccessfulPullCount (best-effort)"
TPC="$(safe_az_json acr_metric TotalPullCount      "${START_ISO}" "${END_ISO}")"
SPC="$(safe_az_json acr_metric SuccessfulPullCount "${START_ISO}" "${END_ISO}")"

jq -n \
    --arg run_id "${RUN_ID}" \
    --arg window_start "${START_ISO}" \
    --arg window_end   "${END_ISO}" \
    --arg origin_delta "${origin_delta}" \
    --arg peer_delta "${peer_delta}" \
    --arg cache_delta "${cache_delta}" \
    --slurpfile pull_events "${PULL_EVENTS}" \
    --slurpfile throttle    "${THROTTLE_SUMMARY}" \
    --argjson tpc "${TPC}" \
    --argjson spc "${SPC}" \
    '{
        scenario: "gantry-warm-cache",
        run_id: $run_id,
        window: { start: $window_start, end: $window_end },
        pull_events: $pull_events[0],
        gantry_prom_deltas: {
            p2p_origin_pull_total: $origin_delta,
            p2p_peer_fetch_total: $peer_delta,
            p2p_cache_hit_total: $cache_delta
        },
        throttling: $throttle[0],
        azure_monitor: { total_pull_count: $tpc, successful_pull_count: $spc }
    }' > "${ARTIFACT}"

echo "Artifact: ${ARTIFACT}"
echo "Forensics: ${FORENSICS_DIR}"
echo
echo "=== headline ==="
jq -r '
    "  pull duration p50/p95/max: \(.pull_events.summary.p50)s / \(.pull_events.summary.p95)s / \(.pull_events.summary.max)s",
    "  gantry origin Δ=\(.gantry_prom_deltas.p2p_origin_pull_total)  peer Δ=\(.gantry_prom_deltas.p2p_peer_fetch_total)  cache Δ=\(.gantry_prom_deltas.p2p_cache_hit_total)",
    "  ACR throttling: \(.throttling.total_429_lines) hit lines across \(.throttling.nodes_with_429)/\(.throttling.nodes_checked) nodes",
    "  Azure Monitor TotalPullCount: \([.azure_monitor.total_pull_count.value[]?.timeseries[]?.data[]?.total // 0] | add // 0)"
' "${ARTIFACT}"
echo

# Recording gates — fail loud if violated, but the artifact + forensics
# are already on disk for diagnosis.
fail=0
if awk "BEGIN{exit !(${origin_delta} > 0)}"; then
    echo "GATE FAIL: p2p_origin_pull_total delta should be 0 (got ${origin_delta}) — possible cleanup gap" >&2
    fail=1
fi
if awk "BEGIN{exit !(${peer_delta} > 0)}"; then
    echo "GATE FAIL: p2p_peer_fetch_total delta should be 0 (got ${peer_delta}) — content cache eviction or routing surprise" >&2
    fail=1
fi
if awk "BEGIN{exit !(${cache_delta} == 0)}"; then
    echo "GATE WARN: p2p_cache_hit_total delta is 0; expected ≈ node_count × digest_count" >&2
fi

(( fail == 0 )) || exit 1
echo "Warm-cache gates PASSED."
