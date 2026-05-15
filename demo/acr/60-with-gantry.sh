#!/usr/bin/env bash
# 60-with-gantry.sh — Phase 6 cold-start.
#
# Steps:
#   1. Mint a fresh RUN_ID via 20-push-demo-image.sh (role=with-gantry).
#   2. Assert the new manifest digest differs from the prior baseline
#      manifest digest.
#   3. Assert ZERO overlap between the new image's layer-digest set and
#      the prior run's layer-digest set — without this, identical layer
#      digests would let containerd serve from its own content store
#      and bypass gantry entirely.
#   4. Delete the previous baseline Job; apply the templated workload
#      Job with the new RUN_ID.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

echo "==> Minting a fresh image (role=with-gantry)"
RUN_HISTORY_ROLE=with-gantry ./20-push-demo-image.sh

NEW_RUN_ID="$(cat .run-id-with-gantry)"
NEW_DIGEST="$(cat .last-digest-with-gantry)"
NEW_LAYERS=".last-layers-with-gantry.txt"

if [[ ! -s .last-digest-baseline || ! -s .last-layers-baseline.txt ]]; then
    echo "Missing baseline state files — run Phase 4 first." >&2
    exit 1
fi
PREV_DIGEST="$(cat .last-digest-baseline)"
PREV_LAYERS=".last-layers-baseline.txt"

echo "==> Manifest digest changed?"
echo "  baseline:    ${PREV_DIGEST}"
echo "  with-gantry: ${NEW_DIGEST}"
if [[ "${PREV_DIGEST}" == "${NEW_DIGEST}" ]]; then
    echo "FAIL: manifest digests identical — containerd would skip the pull entirely." >&2
    exit 1
fi
echo "  ok: differ."

echo "==> Layer-digest set has ZERO overlap?"
overlap_count="$(comm -12 \
    <(sort -u "${PREV_LAYERS}") \
    <(sort -u "${NEW_LAYERS}") | wc -l)"
echo "  overlap count: ${overlap_count}"
if (( overlap_count > 0 )); then
    echo "FAIL: layer digests overlap — image generator did not seed RUN_ID into payloads." >&2
    comm -12 <(sort -u "${PREV_LAYERS}") <(sort -u "${NEW_LAYERS}") | head >&2
    exit 1
fi
echo "  ok: zero overlap."

# Garbage-collect baseline workload (the hammer leaves N labeled Jobs).
echo "==> Deleting baseline Jobs"
kubectl get jobs -n default -l gantry.demo/run-label=baseline -o name 2>/dev/null \
    | xargs -r kubectl delete -n default --ignore-not-found --wait=false

# Snapshot Prom counters BEFORE the run for later delta computation.
echo "==> Snapshotting Prometheus counters (before)"
PROM_BEFORE="${ARTIFACTS_DIR}/with-gantry-${NEW_RUN_ID}-prom-before.json"
{
    echo '{'
    printf '"origin_pull_total": %s,\n' "$(prom_query_scalar 'sum(p2p_origin_pull_total)')"
    printf '"origin_pull_success_total": %s,\n' "$(prom_query_scalar 'sum(p2p_origin_pull_success_total)')"
    printf '"peer_fetch_total": %s,\n' "$(prom_query_scalar 'sum(p2p_peer_fetch_total)')"
    printf '"cache_hit_total": %s\n' "$(prom_query_scalar 'sum(p2p_cache_hit_total)')"
    echo '}'
} > "${PROM_BEFORE}"

START_ISO="$(date -u +%FT%TZ)"
echo "${START_ISO}" > .with-gantry-start

IMAGE_REF="${ACR_LOGIN_SERVER}/${DEMO_REPO}:${NEW_RUN_ID}"
apply_workload_job "${IMAGE_REF}" cold >/dev/null
wait_workload_job cold 30m

END_ISO="$(date -u +%FT%TZ)"
echo "${END_ISO}" > .with-gantry-end

echo
echo "Cold-start workload Job complete."
echo "  RUN_ID=${NEW_RUN_ID}  start=${START_ISO}  end=${END_ISO}"
echo "  Next: ./61-record-with-gantry.sh"
