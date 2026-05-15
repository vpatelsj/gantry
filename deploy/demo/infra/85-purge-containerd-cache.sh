#!/usr/bin/env bash
# Purge the containerd content store on every AKS node for a known
# digest set, then verify nothing survived.
#
# Usage:
#   DEMO_ALLOW_CONTENT_PURGE=1 \
#   deploy/demo/infra/85-purge-containerd-cache.sh deploy/demo/infra/env.local <image-ref>
#
# The image-ref is required; the script resolves it through the demo
# proxy to the manifest digest, the config digest, and every layer
# digest, then renders cache-purge-daemonset.yaml with that digest set.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

env_arg="${1:-}"
shift || true
image_ref="${1:-${PURGE_IMAGE_REF:-}}"
load_demo_env "${env_arg}"

if [[ "${DEMO_ALLOW_CONTENT_PURGE:-0}" != "1" ]]; then
    die "set DEMO_ALLOW_CONTENT_PURGE=1 to confirm you intend to wipe containerd content stores on every node"
fi

if [[ -z "${image_ref}" ]]; then
    die "image reference is required (positional arg or PURGE_IMAGE_REF env)"
fi

require_cmd kubectl jq sed
require_env ACR_NAME

repo="${image_ref##*/}"
repo="${repo%%:*}"
repo="${repo%%@*}"
case "${image_ref}" in
    "${ACR_NAME}".azurecr.io/*)
        repo_path="${image_ref#"${ACR_NAME}".azurecr.io/}"
        repo_path="${repo_path%%:*}"
        repo_path="${repo_path%%@*}"
        ;;
    *)
        repo_path="${image_ref##*/}"
        repo_path="${repo_path%%:*}"
        repo_path="${repo_path%%@*}"
        ;;
esac
tag="${image_ref##*:}"
if [[ "${tag}" == "${image_ref}" ]]; then
    tag="latest"
fi

log "Resolving digests for ${repo_path}:${tag} via the demo proxy"
pod="cache-purge-resolver-$RANDOM"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" run "${pod}" \
    --restart=Never \
    --image=curlimages/curl:8.10.1 \
    --command -- sh -c 'sleep 600' >/dev/null
cleanup_resolver() {
    kubectl -n "${GANTRY_DEMO_NAMESPACE}" delete pod "${pod}" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
}
trap cleanup_resolver EXIT
kubectl -n "${GANTRY_DEMO_NAMESPACE}" wait --for=condition=Ready "pod/${pod}" --timeout=2m >/dev/null

base="http://acr-origin-proxy:5002"
accept="application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

manifest_json="$(kubectl -n "${GANTRY_DEMO_NAMESPACE}" exec "${pod}" -- curl -fsSL -H "Accept: ${accept}" "${base}/v2/${repo_path}/manifests/${tag}")"
manifest_digest="$(kubectl -n "${GANTRY_DEMO_NAMESPACE}" exec "${pod}" -- curl -fsSI -H "Accept: ${accept}" "${base}/v2/${repo_path}/manifests/${tag}" | tr -d '\r' | awk 'BEGIN{IGNORECASE=1} /^Docker-Content-Digest:/ {print $2}' | tail -1)"
if jq -e '.manifests? | type == "array"' <<<"${manifest_json}" >/dev/null; then
    child_digest="$(jq -r '.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64") | .digest' <<<"${manifest_json}" | head -1)"
    if [[ -z "${child_digest}" ]]; then
        die "no linux/amd64 child manifest in index for ${repo_path}:${tag}"
    fi
    child_json="$(kubectl -n "${GANTRY_DEMO_NAMESPACE}" exec "${pod}" -- curl -fsSL -H "Accept: ${accept}" "${base}/v2/${repo_path}/manifests/${child_digest}")"
    config_digest="$(jq -r '.config.digest' <<<"${child_json}")"
    layer_digests="$(jq -r '.layers[].digest' <<<"${child_json}")"
    digests_set=( "${manifest_digest}" "${child_digest}" "${config_digest}" )
    while IFS= read -r d; do digests_set+=( "${d}" ); done <<<"${layer_digests}"
else
    config_digest="$(jq -r '.config.digest' <<<"${manifest_json}")"
    layer_digests="$(jq -r '.layers[].digest' <<<"${manifest_json}")"
    digests_set=( "${manifest_digest}" "${config_digest}" )
    while IFS= read -r d; do digests_set+=( "${d}" ); done <<<"${layer_digests}"
fi

unique_digests="$(printf '%s\n' "${digests_set[@]}" | awk 'NF' | sort -u)"
log "Resolved $(printf '%s\n' "${unique_digests}" | wc -l) unique digests"

tmpdir="$(mktemp -d)"
trap 'cleanup_resolver; rm -rf "${tmpdir}"' EXIT
printf '%s\n' "${unique_digests}" >"${tmpdir}/digests"

log "Publishing cache-purge-digests ConfigMap"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" create configmap cache-purge-digests \
    --from-file=digests="${tmpdir}/digests" \
    --dry-run=client -o yaml | kubectl apply -f -

log "Rendering cache-purge DaemonSet"
sed \
    -e "s|@@GANTRY_DEMO_NAMESPACE@@|${GANTRY_DEMO_NAMESPACE}|g" \
    "${DEMO_ROOT}/cache-purge-daemonset.yaml" \
    >"${tmpdir}/cache-purge.yaml"

# Force fresh DaemonSet run by deleting any prior incarnation first.
kubectl -n "${GANTRY_DEMO_NAMESPACE}" delete ds/cache-purge --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
kubectl apply -f "${tmpdir}/cache-purge.yaml"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" rollout status ds/cache-purge --timeout=10m

log "Collecting per-node purge reports"
node_count="$(kubectl -n "${GANTRY_DEMO_NAMESPACE}" get pods -l app.kubernetes.io/name=cache-purge --no-headers | wc -l)"
log "Reports from ${node_count} cache-purge pods"

reports="${tmpdir}/reports.jsonl"
: >"${reports}"
for pod in $(kubectl -n "${GANTRY_DEMO_NAMESPACE}" get pods -l app.kubernetes.io/name=cache-purge -o jsonpath='{.items[*].metadata.name}'); do
    log "  ${pod}"
    kubectl -n "${GANTRY_DEMO_NAMESPACE}" logs "${pod}" --tail=200 \
        | awk '/^\{.*\}$/ {print}' >>"${reports}" || true
done

survivor_total="$(jq -s 'map(select(.survivors!=null) | .survivors) | add' "${reports}")"
purge_lines="$(jq -s 'map(select(.digests!=null)) | length' "${reports}")"
log "Per-node purge JSON lines: ${purge_lines}"
log "Total survivor digests across nodes: ${survivor_total}"

if [[ "${survivor_total}" != "0" ]]; then
    log "Survivor digests by node:"
    jq -s '.[] | select(.survivors!=null) | select(.survivors > 0) | {node, survivors}' "${reports}"
    die "containerd content store still holds ${survivor_total} digest(s); see logs above"
fi

log "All ${purge_lines} nodes report 0 survivor digests"

cat <<EOF

Cache purge complete.
DaemonSet pods are still alive (sleeping) so you can re-inspect logs.
Delete them when done:
  kubectl -n ${GANTRY_DEMO_NAMESPACE} delete ds/cache-purge
  kubectl -n ${GANTRY_DEMO_NAMESPACE} delete configmap cache-purge-digests
EOF
