#!/usr/bin/env bash
# 30-install-prom.sh — install kube-prometheus-stack with values that
# pick up gantry's ServiceMonitor automatically.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh

echo "==> helm repo add"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update >/dev/null

echo "==> Namespace ${PROM_NAMESPACE}"
kubectl create namespace "${PROM_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

VALUES_FILE="$(mktemp -t kps-values-XXXXXX.yaml)"
trap 'rm -f "${VALUES_FILE}"' EXIT
cat > "${VALUES_FILE}" <<'EOF'
# Demo-friendly values: no PVCs (cluster is throwaway), Grafana
# dashboard sidecar enabled so manifests/grafana-dashboard-configmap.yaml
# is picked up automatically.
prometheus:
  prometheusSpec:
    # Ignore namespace boundaries so we discover gantry's ServiceMonitor
    # in gantry-system.
    serviceMonitorNamespaceSelector: {}
    serviceMonitorSelector: {}
    podMonitorNamespaceSelector: {}
    podMonitorSelector: {}
    storageSpec: {}
    retention: 6h
    resources:
      requests:
        cpu: 200m
        memory: 512Mi
grafana:
  adminPassword: "gantry-demo"
  service:
    type: ClusterIP
  persistence:
    enabled: false
  sidecar:
    dashboards:
      enabled: true
      label: grafana_dashboard
      labelValue: "1"
      searchNamespace: ALL
alertmanager:
  enabled: false
EOF

echo "==> helm install/upgrade ${PROM_RELEASE}"
helm upgrade --install "${PROM_RELEASE}" prometheus-community/kube-prometheus-stack \
    -n "${PROM_NAMESPACE}" \
    -f "${VALUES_FILE}" \
    --wait

echo
echo "==> Forwarding hints"
cat <<EOF
Prometheus UI:
  kubectl -n ${PROM_NAMESPACE} port-forward svc/${PROM_RELEASE}-kube-prom-prometheus 9090:9090

Grafana (admin / gantry-demo):
  kubectl -n ${PROM_NAMESPACE} port-forward svc/${PROM_RELEASE}-grafana 3000:80

Apply the dashboard ConfigMap:
  kubectl apply -f manifests/grafana-dashboard-configmap.yaml
EOF
