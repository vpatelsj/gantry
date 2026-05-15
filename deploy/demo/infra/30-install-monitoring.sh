#!/usr/bin/env bash
# Install kube-prometheus-stack for demo visibility.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd kubectl helm

values_file="$(mktemp)"
trap 'rm -f "${values_file}"' EXIT

cat >"${values_file}" <<'EOF'
prometheus:
  prometheusSpec:
    serviceMonitorSelectorNilUsesHelmValues: false
    podMonitorSelectorNilUsesHelmValues: false
    additionalScrapeConfigs:
      - job_name: kubernetes-pods-annotations
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - action: keep
            source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
            regex: "true"
          - action: replace
            source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
            target_label: __metrics_path__
            regex: (.+)
          - action: replace
            source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
            target_label: __address__
            regex: ([^:]+)(?::\d+)?;(\d+)
            replacement: $1:$2
          - action: labelmap
            regex: __meta_kubernetes_pod_label_(.+)
          - action: replace
            source_labels: [__meta_kubernetes_namespace]
            target_label: namespace
          - action: replace
            source_labels: [__meta_kubernetes_pod_name]
            target_label: pod
EOF

log "Adding prometheus-community Helm repo"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update >/dev/null

log "Installing kube-prometheus-stack release ${KPS_RELEASE} in ${MONITORING_NAMESPACE}"
helm upgrade --install "${KPS_RELEASE}" prometheus-community/kube-prometheus-stack \
    --namespace "${MONITORING_NAMESPACE}" \
    --create-namespace \
    --wait \
    --timeout 20m \
    --values "${values_file}" \
    --set-string "grafana.adminPassword=${GRAFANA_ADMIN_PASSWORD}"

kubectl -n "${MONITORING_NAMESPACE}" get pods,svc

cat <<EOF

Monitoring is installed.
Grafana port-forward:
  kubectl -n ${MONITORING_NAMESPACE} port-forward svc/${KPS_RELEASE}-grafana 3000:80

Grafana login:
  user: admin
  pass: ${GRAFANA_ADMIN_PASSWORD}
EOF
