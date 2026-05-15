#!/usr/bin/env bash
# Exercise the deployed proxy from inside the cluster against an image in ACR.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd kubectl jq

log "Running proxy auth smoke for ${PROXY_SMOKE_REPO}:${PROXY_SMOKE_TAG}"
pod="acr-proxy-smoke-$RANDOM"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" run "${pod}" \
    --restart=Never \
    --image=curlimages/curl:8.10.1 \
    --command -- sh -c 'sleep 3600' >/dev/null

cleanup() {
    kubectl -n "${GANTRY_DEMO_NAMESPACE}" delete pod "${pod}" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

kubectl -n "${GANTRY_DEMO_NAMESPACE}" wait --for=condition=Ready "pod/${pod}" --timeout=2m >/dev/null

base="http://acr-origin-proxy:5002"
accept_all="application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"
accept_manifest="application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

kcurl() {
    kubectl -n "${GANTRY_DEMO_NAMESPACE}" exec "${pod}" -- curl "$@"
}

echo "GET /v2/"
kcurl -fsS -o /dev/null -w "  status=%{http_code}\n" "${base}/v2/"

echo "HEAD manifest by tag"
headers="$(kcurl -fsS -I -H "Accept: ${accept_all}" "${base}/v2/${PROXY_SMOKE_REPO}/manifests/${PROXY_SMOKE_TAG}" | tr -d '\r')"
printf '%s\n' "${headers}" | awk 'BEGIN{IGNORECASE=1} /^HTTP\// || /^Docker-Content-Digest:/ || /^Content-Type:/ {print "  " $0}'

tag_digest="$(printf '%s\n' "${headers}" | awk 'BEGIN{IGNORECASE=1} /^Docker-Content-Digest:/ {print $2}' | tail -1)"
if [[ -z "${tag_digest}" ]]; then
    die "missing Docker-Content-Digest header for ${PROXY_SMOKE_REPO}:${PROXY_SMOKE_TAG}"
fi

echo "GET manifest by tag"
tag_manifest="$(kcurl -fsSL -H "Accept: ${accept_all}" "${base}/v2/${PROXY_SMOKE_REPO}/manifests/${PROXY_SMOKE_TAG}")"

echo "GET manifest by digest ${tag_digest}"
kcurl -fsS -o /dev/null -w "  status=%{http_code}\n" -H "Accept: ${accept_all}" "${base}/v2/${PROXY_SMOKE_REPO}/manifests/${tag_digest}"

media_type="$(jq -r '.mediaType // ""' <<<"${tag_manifest}")"
manifest_json="${tag_manifest}"
manifest_digest="${tag_digest}"

if jq -e '.manifests? | type == "array"' <<<"${tag_manifest}" >/dev/null; then
    manifest_digest="$(jq -r '.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64") | .digest' <<<"${tag_manifest}" | head -1)"
    if [[ -z "${manifest_digest}" ]]; then
        die "${PROXY_SMOKE_REPO}:${PROXY_SMOKE_TAG} is an index (${media_type}) but has no linux/amd64 child manifest"
    fi
    echo "GET linux/amd64 child manifest ${manifest_digest}"
    manifest_json="$(kcurl -fsSL -H "Accept: ${accept_manifest}" "${base}/v2/${PROXY_SMOKE_REPO}/manifests/${manifest_digest}")"
fi

config_digest="$(jq -r '.config.digest // empty' <<<"${manifest_json}")"
layer_digest="$(jq -r '.layers[0].digest // empty' <<<"${manifest_json}")"

if [[ -z "${config_digest}" ]]; then
    die "no config digest found in selected manifest ${manifest_digest}"
fi

echo "GET config blob ${config_digest}"
kcurl -fsS -o /dev/null -w "  status=%{http_code}\n" "${base}/v2/${PROXY_SMOKE_REPO}/blobs/${config_digest}"

if [[ -n "${layer_digest}" ]]; then
    echo "GET first byte of layer blob ${layer_digest}"
    kcurl -fsS -r 0-0 -o /dev/null -w "  status=%{http_code}\n" "${base}/v2/${PROXY_SMOKE_REPO}/blobs/${layer_digest}"
else
    echo "no layer digest parsed; skipping layer blob smoke"
fi

log "Recent proxy logs"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" logs deploy/acr-origin-proxy --tail=80

echo "Proxy debug summary"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" exec "${pod}" -- \
    curl -fsS "http://acr-origin-proxy:9090/debug/summary" | jq .

echo "Proxy origin metric names"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" exec "${pod}" -- \
    curl -fsS "http://acr-origin-proxy:9090/metrics" | awk '/^origin_/ {print $1}' | sort -u
