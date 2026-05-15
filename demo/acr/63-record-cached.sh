#!/usr/bin/env bash
# 63-record-cached.sh — record warm-cache run with strict gates AND
# always capture raw forensics, regardless of whether the gates pass.

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

mkdir -p "${FORENSICS_DIR}"

echo "==> Sleeping ${AZ_INGEST_LAG_SECONDS}s for ingest"
sleep "${AZ_INGEST_LAG_SECONDS}"

# 1. Forensics (capture FIRST so a later assertion failure still leaves them).
echo "==> Forensics: ctr content/images ls per node"
for ds in $(kubectl -n gantry-system get pods -l app.kubernetes.io/name=gantry -o jsonpath='{.items[*].metadata.name}'); do
    node="$(kubectl -n gantry-system get pod "${ds}" -o jsonpath='{.spec.nodeName}')"
    {
        echo "# ${node} :: ${ds}"
        echo "## /metrics (gantry, head)"
        kubectl -n gantry-system exec "${ds}" -- \
            wget -qO- http://127.0.0.1:9095/metrics | grep -E '^p2p_(cache|origin|peer)' | head -50 || true
    } > "${FORENSICS_DIR}/${node}-gantry.txt"
done

# `ctr content ls` from each node via the cleanup-Job-style pod is too
# heavy for forensics; instead use the existing gantry pods to list
# their cache dir as a proxy for what each node serves locally.
for pod in $(kubectl -n gantry-system get pods -l app.kubernetes.io/name=gantry -o jsonpath='{.items[*].metadata.name}'); do
    node="$(kubectl -n gantry-system get pod "${pod}" -o jsonpath='{.spec.nodeName}')"
    kubectl -n gantry-system exec "${pod}" -- \
        sh -c 'find /var/lib/gantry/cache -maxdepth 4 -type f 2>/dev/null | head -200' \
        > "${FORENSICS_DIR}/${node}-cache-listing.txt" || true
done

echo "==> Prometheus deltas (raw dump)"
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

echo "==> KQL: total events + 429"
TOTAL_EVENTS="$(run_kql "$(envsubst < queries/acr-total-events.kql)")"
THR_EVENTS="$(run_kql "$(envsubst < queries/acr-throttling.kql)")"
TPC="$(acr_metric TotalPullCount      "${START_ISO}" "${END_ISO}")"
SPC="$(acr_metric SuccessfulPullCount "${START_ISO}" "${END_ISO}")"

# Compose the artifact unconditionally so post-mortems have data even
# if the gates fail.
jq -n \
    --arg run_id "${RUN_ID}" \
    --arg start "${START_ISO}" \
    --arg end   "${END_ISO}" \
    --arg origin_delta "${origin_delta}" \
    --arg peer_delta "${peer_delta}" \
    --arg cache_delta "${cache_delta}" \
    --argjson total_events "${TOTAL_EVENTS}" \
    --argjson throttling   "${THR_EVENTS}" \
    --argjson tpc          "${TPC}" \
    --argjson spc          "${SPC}" \
    '{
        scenario: "gantry-warm-cache",
        run_id: $run_id,
        window: { start: $start, end: $end },
        gantry_prom_deltas: {
            p2p_origin_pull_total: $origin_delta,
            p2p_peer_fetch_total: $peer_delta,
            p2p_cache_hit_total: $cache_delta
        },
        acr_repository_events: $total_events,
        acr_throttling: $throttling,
        azure_monitor: { total_pull_count: $tpc, successful_pull_count: $spc }
    }' > "${ARTIFACT}"

echo "Artifact: ${ARTIFACT}"
echo "Forensics: ${FORENSICS_DIR}"

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
echo
echo "Warm-cache gates PASSED."
