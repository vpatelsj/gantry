#!/usr/bin/env bash
# Check whether node host networking can reach the proxy ClusterIP.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd kubectl

cluster_ip="$(kubectl -n "${GANTRY_DEMO_NAMESPACE}" get svc acr-origin-proxy -o jsonpath='{.spec.clusterIP}')"
tmpfile="$(mktemp)"
trap 'rm -f "${tmpfile}"' EXIT

cat >"${tmpfile}" <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: acr-proxy-node-reachability
  namespace: ${GANTRY_DEMO_NAMESPACE}
  labels:
    app.kubernetes.io/name: acr-proxy-node-reachability
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: acr-proxy-node-reachability
  template:
    metadata:
      labels:
        app.kubernetes.io/name: acr-proxy-node-reachability
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      tolerations:
        - operator: Exists
      containers:
        - name: curl
          image: curlimages/curl:8.10.1
          command:
            - sh
            - -ceu
            - |
              node="\${NODE_NAME:-unknown}"
              echo "node=\${node} target=http://${cluster_ip}:5002/healthz"
              curl -fsS --connect-timeout 5 "http://${cluster_ip}:5002/healthz"
              echo "node=\${node} ok"
              sleep 3600
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
EOF

log "Applying host-network reachability DaemonSet against ${cluster_ip}"
kubectl apply -f "${tmpfile}"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" rollout status ds/acr-proxy-node-reachability --timeout=5m

log "Per-node reachability logs"
for pod in $(kubectl -n "${GANTRY_DEMO_NAMESPACE}" get pods -l app.kubernetes.io/name=acr-proxy-node-reachability -o jsonpath='{.items[*].metadata.name}'); do
    echo "--- ${pod} ---"
    kubectl -n "${GANTRY_DEMO_NAMESPACE}" logs "${pod}" --tail=20
done

log "Deleting reachability DaemonSet"
kubectl -n "${GANTRY_DEMO_NAMESPACE}" delete ds acr-proxy-node-reachability --wait=true
kubectl -n "${GANTRY_DEMO_NAMESPACE}" delete pod \
  -l app.kubernetes.io/name=acr-proxy-node-reachability \
  --ignore-not-found=true \
  --wait=false >/dev/null

cat <<EOF

ClusterIP reachability from host-network pods passed.
This validates the likely baseline routing choice, but the later hosts.toml
installer still must be verified with containerd before recording demo runs.
EOF
