#!/usr/bin/env bash
# Shared helper sourced by the per-phase scripts. Defines:
#   - load_state            — sources env.sh + .provision-state.
#   - apply_workload_job    — render the workload Job template and apply.
#   - wait_workload_job     — block until the named Job is complete.
#   - assert_no_hosts_toml  — fail if hosts.toml is present on any node
#                              (used by 40-baseline.sh).
#   - prom_query / prom_query_scalar
#                            — port-forward Prometheus once and run a query.
#   - run_kql               — run a KQL query against the LAW.
#   - acr_metric            — fetch an Azure Monitor metric for the ACR.

set -euo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

load_state() {
    cd "${LIB_DIR}/.."
    # shellcheck disable=SC1091
    source ./env.sh
    if [[ ! -f .provision-state ]]; then
        echo ".provision-state missing — run 10-provision.sh first" >&2
        exit 1
    fi
    # shellcheck disable=SC1091
    source ./.provision-state
}

apply_workload_job() {
    local image_ref="$1" run_label="$2"
    local rendered
    rendered="$(mktemp -t workload-XXXXXX.yaml)"
    IMAGE_REF="${image_ref}" \
        NODE_COUNT="${NODE_COUNT}" \
        RUN_LABEL="${run_label}" \
        envsubst '${IMAGE_REF} ${NODE_COUNT} ${RUN_LABEL}' \
        < manifests/workload-job.yaml.tmpl > "${rendered}"
    echo "==> Applying workload Job (run-label=${run_label}, image=${image_ref})"
    kubectl apply -f "${rendered}"
    echo "${rendered}"
}

wait_workload_job() {
    local run_label="$1" timeout="${2:-30m}"
    local job="gantry-demo-workload-${run_label}"
    echo "==> Waiting for Job/${job} (timeout ${timeout})"
    kubectl wait --for=condition=complete \
        "job/${job}" -n default --timeout="${timeout}"
}

assert_no_hosts_toml() {
    local acr_login="$1"
    echo "==> Asserting hosts.toml is ABSENT on every node (baseline must be uncontaminated)"
    # Use a transient privileged DaemonSet (toolbox) that mounts
    # /etc/containerd/certs.d read-only and runs `test ! -f`.
    local toolbox
    toolbox="$(mktemp -t toolbox-XXXXXX.yaml)"
    cat > "${toolbox}" <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: gantry-demo-hoststoml-check
  namespace: default
spec:
  selector:
    matchLabels: { app: gantry-demo-hoststoml-check }
  template:
    metadata:
      labels: { app: gantry-demo-hoststoml-check }
    spec:
      tolerations: [{ operator: Exists }]
      hostPID: false
      containers:
        - name: check
          image: busybox:1.36
          command: ["/bin/sh","-c"]
          args:
            - |
              if [ -f /host-certs.d/${acr_login}/hosts.toml ]; then
                echo "FAIL: hosts.toml present on \$(hostname)"
                exit 1
              fi
              echo "OK: hosts.toml absent on \$(hostname)"
              sleep 3600
          securityContext:
            runAsUser: 0
          volumeMounts:
            - name: certs-d
              mountPath: /host-certs.d
              readOnly: true
      volumes:
        - name: certs-d
          hostPath: { path: /etc/containerd/certs.d, type: DirectoryOrCreate }
EOF
    kubectl apply -f "${toolbox}"
    # Wait for the DS to be fully scheduled.
    kubectl rollout status ds/gantry-demo-hoststoml-check -n default --timeout=120s
    # Any pod that exited non-zero would show up as not-Ready.
    sleep 5
    if kubectl get pods -l app=gantry-demo-hoststoml-check -n default \
            -o jsonpath='{.items[*].status.containerStatuses[*].ready}' \
            | tr ' ' '\n' | grep -q '^false$'; then
        echo "ABORT: at least one node has hosts.toml installed; baseline would be contaminated." >&2
        kubectl logs -l app=gantry-demo-hoststoml-check -n default --tail=5
        exit 1
    fi
    echo "  all nodes confirm: no hosts.toml under /etc/containerd/certs.d/${acr_login}/"
    kubectl delete ds gantry-demo-hoststoml-check -n default --wait=false
    rm -f "${toolbox}"
}

# Cached port-forward for repeated Prom queries.
PROM_PF_PID=""
_prom_pf_start() {
    if [[ -n "${PROM_PF_PID}" ]] && kill -0 "${PROM_PF_PID}" 2>/dev/null; then
        return
    fi
    kubectl -n "${PROM_NAMESPACE}" port-forward \
        "svc/${PROM_RELEASE}-kube-prometheus-prometheus" 9090:9090 \
        >/tmp/gantry-demo-prom-pf.log 2>&1 &
    PROM_PF_PID=$!
    # Some chart versions name the service slightly differently; fall
    # back to the discovered prometheus svc.
    sleep 2
    if ! kill -0 "${PROM_PF_PID}" 2>/dev/null; then
        local svc
        svc="$(kubectl -n "${PROM_NAMESPACE}" get svc \
            -l app.kubernetes.io/name=prometheus -o jsonpath='{.items[0].metadata.name}')"
        kubectl -n "${PROM_NAMESPACE}" port-forward "svc/${svc}" 9090:9090 \
            >/tmp/gantry-demo-prom-pf.log 2>&1 &
        PROM_PF_PID=$!
        sleep 2
    fi
    trap '_prom_pf_stop' EXIT
}
_prom_pf_stop() {
    if [[ -n "${PROM_PF_PID:-}" ]] && kill -0 "${PROM_PF_PID}" 2>/dev/null; then
        kill "${PROM_PF_PID}" 2>/dev/null || true
    fi
}

prom_query() {
    local q="$1"
    _prom_pf_start
    curl -fsSG --data-urlencode "query=${q}" \
        http://127.0.0.1:9090/api/v1/query
}

prom_query_scalar() {
    local q="$1"
    prom_query "${q}" | jq -r '
        if .data.result | length == 0 then "0"
        else (.data.result | map(.value[1] | tonumber) | add | tostring)
        end'
}

run_kql() {
    local kql="$1"
    az monitor log-analytics query \
        --workspace "$(az monitor log-analytics workspace show \
            -g "${RG_NAME}" -n "${LAW_NAME}" --query customerId -o tsv)" \
        --analytics-query "${kql}" \
        -o json
}

acr_metric() {
    local metric="$1" start_iso="$2" end_iso="$3"
    az monitor metrics list \
        --resource "${ACR_ID}" \
        --metric "${metric}" \
        --interval PT1M \
        --start-time "${start_iso}" \
        --end-time "${end_iso}" \
        -o json
}
