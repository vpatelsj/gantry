#!/usr/bin/env bash
# Install the Gantry ACR demo Grafana dashboard as a labeled ConfigMap.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd kubectl

dashboard="${DEMO_ROOT}/grafana-dashboard.json"
if [[ ! -f "${dashboard}" ]]; then
    die "dashboard JSON not found: ${dashboard}"
fi

log "Publishing Grafana dashboard ConfigMap in namespace ${MONITORING_NAMESPACE}"
kubectl -n "${MONITORING_NAMESPACE}" create configmap gantry-acr-demo-dashboard \
    --from-file=gantry-acr-demo.json="${dashboard}" \
    --dry-run=client -o yaml \
    | kubectl label --local -f - --dry-run=client -o yaml \
        grafana_dashboard=1 \
        app.kubernetes.io/part-of=gantry-demo \
    | kubectl apply -f -

log "Dashboard ConfigMap labels"
kubectl -n "${MONITORING_NAMESPACE}" get cm gantry-acr-demo-dashboard --show-labels

cat <<EOF

Grafana sidecar will pick up the ConfigMap within ~30s.
Open Grafana and look for the dashboard "Gantry ACR demo":

  kubectl -n ${MONITORING_NAMESPACE} port-forward svc/${KPS_RELEASE}-grafana 3000:80
  # then http://localhost:3000  (admin / ${GRAFANA_ADMIN_PASSWORD})
EOF
