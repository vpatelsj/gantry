#!/usr/bin/env bash
# Install the demo fail-closed hosts.toml on every AKS node.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

env_arg="${1:-}"
mode="${2:-${HOSTS_TOML_MODE:-baseline}}"
if [[ "${env_arg}" == "baseline" || "${env_arg}" == "gantry" ]]; then
    mode="${env_arg}"
    env_arg=""
fi

load_demo_env "${env_arg}"

require_cmd az kubectl sed
require_env ACR_NAME
validate_acr_name
select_subscription

case "${mode}" in
    baseline|gantry) ;;
    *) die "mode must be baseline or gantry, got: ${mode}" ;;
esac

login_server="$(acr_login_server)"
proxy_cluster_ip="${PROXY_CLUSTER_IP:-}"
if [[ -z "${proxy_cluster_ip}" ]]; then
    proxy_cluster_ip="$(kubectl -n "${GANTRY_DEMO_NAMESPACE}" get svc acr-origin-proxy -o jsonpath='{.spec.clusterIP}')"
fi
if [[ -z "${proxy_cluster_ip}" ]]; then
    die "could not resolve proxy ClusterIP from svc/acr-origin-proxy"
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

log "Creating namespace ${GANTRY_DEMO_NAMESPACE}"
kubectl create namespace "${GANTRY_DEMO_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

log "Publishing hosts.toml templates"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" create configmap hosts-toml-templates \
    --from-file=baseline="${DEMO_ROOT}/hosts.toml.baseline.template" \
    --from-file=gantry="${DEMO_ROOT}/hosts.toml.gantry.template" \
    --dry-run=client -o yaml | kubectl apply -f -

log "Rendering hosts-toml-installer for ${mode} mode (${login_server} -> ${proxy_cluster_ip})"
sed \
    -e "s|@@GANTRY_DEMO_NAMESPACE@@|${GANTRY_DEMO_NAMESPACE}|g" \
    -e "s|@@ACR_LOGIN_SERVER@@|${login_server}|g" \
    -e "s|@@HOSTS_TOML_MODE@@|${mode}|g" \
    -e "s|@@PROXY_CLUSTER_IP@@|${proxy_cluster_ip}|g" \
    "${DEMO_ROOT}/hosts-toml-installer.yaml" \
    >"${tmpdir}/hosts-toml-installer.yaml"

kubectl apply -f "${tmpdir}/hosts-toml-installer.yaml"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" rollout status ds/hosts-toml-installer --timeout=5m

log "Installed hosts.toml on nodes"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" get pods -l app.kubernetes.io/name=hosts-toml-installer -o wide

log "Sample installer logs"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" logs \
    -l app.kubernetes.io/name=hosts-toml-installer \
    --tail=80 \
    --prefix=true

cat <<EOF

Installed ${mode} hosts.toml for ${login_server} using proxy ${proxy_cluster_ip}.

The installer DaemonSet stays alive for log inspection. Delete it when
you are done inspecting logs:
  kubectl -n ${GANTRY_DEMO_NAMESPACE} delete ds/hosts-toml-installer
EOF