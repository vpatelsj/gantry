#!/usr/bin/env bash
# 51b-install-hosts-toml.sh — write hosts.toml on every node, verify
# presence, then run a single-node pull-through preflight that proves
# gantry actually intercepts ACR pulls.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

# 1. Render hosts.toml from deploy/hosts.toml.template
echo "==> Rendering hosts.toml"
HOSTS_TOML_FILE="$(mktemp -t hosts-toml-XXXXXX)"
trap 'rm -f "${HOSTS_TOML_FILE}"' EXIT
REGISTRY_SERVER="https://${ACR_LOGIN_SERVER}" \
    envsubst '${REGISTRY_SERVER}' \
    < ../../deploy/hosts.toml.template > "${HOSTS_TOML_FILE}"

# 2. Render the installer DS template (envsubst can't easily inject a
#    multi-line file, so we sed-replace on a sentinel line).
INSTALLER_FILE="$(mktemp -t installer-XXXXXX.yaml)"
{
    awk -v acr="${ACR_LOGIN_SERVER}" '
        { gsub(/\$\{ACR_LOGIN_SERVER\}/, acr); print }
    ' manifests/hosts-toml-installer.yaml \
        | awk -v file="${HOSTS_TOML_FILE}" '
            BEGIN {
                # Read the file into payload; preserve indentation by
                # the surrounding YAML (the heredoc body is flush-left
                # because busybox `sh` cat reads stdin literally).
                while ((getline line < file) > 0) {
                    payload = payload line "\n"
                }
                close(file)
                sub(/\n$/, "", payload)
            }
            /\$\{HOSTS_TOML_CONTENTS\}/ {
                print payload
                next
            }
            { print }
        '
} > "${INSTALLER_FILE}"

echo "==> Applying hosts-toml installer DaemonSet"
kubectl apply -f "${INSTALLER_FILE}"
kubectl -n gantry-system rollout status ds/gantry-hosts-toml-installer --timeout=5m

echo "==> Verifying hosts.toml on every node"
for pod in $(kubectl -n gantry-system get pods -l app.kubernetes.io/name=gantry-hosts-toml-installer -o name); do
    if ! kubectl -n gantry-system exec "${pod}" -c keepalive -- \
            cat "/host-certs.d/${ACR_LOGIN_SERVER}/hosts.toml" >/dev/null; then
        echo "MISSING: ${pod} cannot read hosts.toml" >&2
        exit 1
    fi
    echo "  ok: ${pod}"
done

# 3. Single-node pull-through preflight.
#
# Mints a *dedicated* preflight tag (preflight-<epoch>) — never the
# upcoming with-gantry RUN_ID — so the preflight pull doesn't warm
# gantry's cache on one node and contaminate the cold-start
# measurement.
echo
echo "==> Single-node pull-through preflight"
PREFLIGHT_RUN_ID="preflight-$(date -u +%s)"
( cd . && \
  RUN_ID_OVERRIDE="${PREFLIGHT_RUN_ID}" \
  RUN_HISTORY_ROLE=preflight \
  ./20-push-demo-image.sh )

PREFLIGHT_IMAGE="${ACR_LOGIN_SERVER}/${DEMO_REPO}:${PREFLIGHT_RUN_ID}"

# Pick the first scheduled node.
NODE="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
echo "  preflight node: ${NODE}"

# Find the gantry pod on that node.
GANTRY_POD="$(kubectl -n gantry-system get pods \
    -l app.kubernetes.io/name=gantry \
    --field-selector "spec.nodeName=${NODE}" \
    -o jsonpath='{.items[0].metadata.name}')"

prom_before_origin="$(kubectl -n gantry-system exec "${GANTRY_POD}" -- \
    wget -qO- http://127.0.0.1:9095/metrics | awk '/^p2p_origin_pull_total\{/ {sum+=$2} END {print sum+0}')"
prom_before_cache="$(kubectl -n gantry-system exec "${GANTRY_POD}" -- \
    wget -qO- http://127.0.0.1:9095/metrics | awk '/^p2p_cache_hit_total / {print $2}')"
prom_before_cache="${prom_before_cache:-0}"

# Pull via crictl in a privileged debugger pod on the chosen node.
echo "  triggering crictl pull on ${NODE}"
kubectl debug "node/${NODE}" -it --rm \
    --image=busybox:1.36 \
    --profile=sysadmin \
    --quiet \
    -- /bin/sh -c "chroot /host crictl pull ${PREFLIGHT_IMAGE}" \
    || { echo "preflight crictl pull FAILED" >&2; exit 1; }

prom_after_origin="$(kubectl -n gantry-system exec "${GANTRY_POD}" -- \
    wget -qO- http://127.0.0.1:9095/metrics | awk '/^p2p_origin_pull_total\{/ {sum+=$2} END {print sum+0}')"
prom_after_cache="$(kubectl -n gantry-system exec "${GANTRY_POD}" -- \
    wget -qO- http://127.0.0.1:9095/metrics | awk '/^p2p_cache_hit_total / {print $2}')"
prom_after_cache="${prom_after_cache:-0}"

origin_delta=$(( prom_after_origin - prom_before_origin ))
cache_delta=$(( prom_after_cache  - prom_before_cache  ))

echo "  gantry on ${NODE}: origin Δ=${origin_delta}, cache Δ=${cache_delta}"
if (( origin_delta == 0 && cache_delta == 0 )); then
    cat >&2 <<EOF
PREFLIGHT FAIL: gantry on ${NODE} saw no pull activity for ${PREFLIGHT_IMAGE}.
hosts.toml is present but containerd is NOT routing through gantry. Check:
  - hosts.toml host directory matches the registry hostname exactly.
  - capabilities = ["pull","resolve"] (deploy/hosts.toml.template).
  - containerd auto-reload picked up the file (kubectl logs the
    installer pod for the dump).
EOF
    exit 1
fi

echo
echo "Preflight PASSED — gantry is on the pull path."
echo "Next: ./60-with-gantry.sh"
