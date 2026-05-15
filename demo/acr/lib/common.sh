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

# wait_for_kql_ingest <start_iso> <end_iso> [max_wait_seconds] [poll_interval_seconds]
#
# Polls Log Analytics for ContainerRegistryRepositoryEvents in the given
# window. Returns as soon as the row count is non-zero AND stable across
# two consecutive polls (so we don't snapshot mid-ingest), or after
# max_wait_seconds elapses.
#
# Caller is expected to handle the "still zero at deadline" case (the
# subsequent KQL query will simply return no rows). For the warm-cache
# scenario, zero is the expected outcome — pass a shorter max_wait.
wait_for_kql_ingest() {
    local start_iso="$1" end_iso="$2"
    local max_wait="${3:-180}" interval="${4:-15}"
    local cust
    cust="$(az monitor log-analytics workspace show \
        -g "${RG_NAME}" -n "${LAW_NAME}" --query customerId -o tsv)"
    local q="ContainerRegistryRepositoryEvents
| where TimeGenerated between (datetime(\"${start_iso}\") .. datetime(\"${end_iso}\"))
| where Repository == \"${DEMO_REPO}\"
| count"

    # Stream status to stderr (unbuffered) so it shows up live even
    # when stdout is being piped/tee'd.
    echo "  window: ${start_iso} .. ${end_iso}, repo=${DEMO_REPO}, max_wait=${max_wait}s, interval=${interval}s" >&2
    local elapsed=0 prev=-1 cur=0 t0
    t0="$(date -u +%s)"
    while (( elapsed < max_wait )); do
        printf '  [+%4ds] querying Log Analytics...' "${elapsed}" >&2
        cur="$(az monitor log-analytics query \
            --workspace "${cust}" --analytics-query "${q}" -o tsv 2>/dev/null \
            | head -1 | tr -d '[:space:]')"
        cur="${cur:-0}"
        printf ' events=%s\n' "${cur}" >&2
        if (( cur > 0 && cur == prev )); then
            echo "  ingest stable at ${cur} rows — proceeding" >&2
            return 0
        fi
        prev="${cur}"
        sleep "${interval}"
        elapsed=$(( $(date -u +%s) - t0 ))
    done
    echo "  reached ${max_wait}s wait; current row count=${cur}, proceeding anyway" >&2
    return 0
}

acr_metric() {
    local metric="$1" start_iso="$2" end_iso="$3"
    # Azure Monitor requires (end - start) >= interval. Pad the window
    # by 60s on each side so PT1M is always satisfied even for brief
    # workload Jobs (the demo's baseline can finish in <1 min when the
    # cluster network is fast).
    local start_padded end_padded
    start_padded="$(date -u -d "${start_iso} -60 seconds" +%FT%TZ 2>/dev/null || echo "${start_iso}")"
    end_padded="$(date -u -d "${end_iso} +60 seconds"   +%FT%TZ 2>/dev/null || echo "${end_iso}")"
    az monitor metrics list \
        --resource "${ACR_ID}" \
        --metric "${metric}" \
        --interval PT1M \
        --start-time "${start_padded}" \
        --end-time "${end_padded}" \
        -o json
}

# safe_az_json <command...> — runs a command capturing stdout, returns
# its output if it's valid JSON, otherwise prints '{}'. Used to wrap
# best-effort Azure calls so a transient failure doesn't poison the
# downstream `jq -n --argjson` assembly.
safe_az_json() {
    local out
    if out="$("$@" 2>/dev/null)" && [[ -n "${out}" ]] && echo "${out}" | jq -e . >/dev/null 2>&1; then
        echo "${out}"
    else
        echo '{}'
    fi
}

# scrape_containerd_429s <since_iso> <until_iso> <out_summary_file> <out_raw_dir>
#
# Real-time ACR-throttling detector that does NOT rely on Azure Log
# Analytics ingest. Deploys a transient DaemonSet whose pods nsenter
# into PID 1 on each node and run `journalctl -u containerd` for the
# given window, greps for 429 / TOOMANYREQUESTS / "throttle", and
# writes:
#   - <out_summary_file>: JSON {nodes_checked, nodes_with_429,
#       total_429_lines, per_node: [{node, count}]}.
#   - <out_raw_dir>/<node>.log: raw matching journald lines per node.
#
# Designed to finish in ~10s. Uses the same source the Azure ingest
# pipeline drinks from (containerd's own HTTP-response logging),
# just read at the spigot instead of after a 5-min ingest detour.
scrape_containerd_429s() {
    local since_iso="$1" until_iso="$2" out_summary="$3" out_raw_dir="$4"
    mkdir -p "${out_raw_dir}"

    local ds_yaml
    ds_yaml="$(mktemp -t throttle-scraper-XXXXXX.yaml)"
    cat > "${ds_yaml}" <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: gantry-demo-throttle-scraper
  namespace: default
spec:
  selector: { matchLabels: { app: gantry-demo-throttle-scraper } }
  template:
    metadata:
      labels: { app: gantry-demo-throttle-scraper }
    spec:
      hostPID: true
      tolerations: [{ operator: Exists }]
      containers:
        - name: scrape
          image: mcr.microsoft.com/cbl-mariner/base/core:2.0
          command: ["/bin/sh","-c"]
          args:
            - |
              set -eu
              nsenter -t 1 -m -u -i -n -p -- \
                journalctl -u containerd \
                --since "${since_iso}" --until "${until_iso}" \
                --no-pager 2>/dev/null \
                | grep -iE '429|toomanyrequests|throttle' \
                > /tmp/hits.log || true
              wc -l < /tmp/hits.log > /tmp/count.txt
              sleep 600
          securityContext: { privileged: true, runAsUser: 0 }
EOF
    kubectl apply -f "${ds_yaml}" >&2
    kubectl rollout status ds/gantry-demo-throttle-scraper -n default --timeout=120s >&2 || true
    rm -f "${ds_yaml}"

    local total=0 nodes_checked=0 nodes_with_hits=0
    local per_node_json="[]"
    for pod in $(kubectl get pods -l app=gantry-demo-throttle-scraper -n default -o jsonpath='{.items[*].metadata.name}'); do
        local node count
        node="$(kubectl get pod "${pod}" -n default -o jsonpath='{.spec.nodeName}')"
        nodes_checked=$(( nodes_checked + 1 ))
        kubectl exec "${pod}" -n default -- cat /tmp/hits.log > "${out_raw_dir}/${node}.log" 2>/dev/null || true
        count="$(kubectl exec "${pod}" -n default -- cat /tmp/count.txt 2>/dev/null | tr -dc '0-9')"
        count="${count:-0}"
        if (( count > 0 )); then
            nodes_with_hits=$(( nodes_with_hits + 1 ))
        fi
        total=$(( total + count ))
        per_node_json="$(echo "${per_node_json}" | jq --arg n "${node}" --argjson c "${count}" '. + [{node:$n, count:$c}]')"
    done

    jq -n \
        --argjson nc "${nodes_checked}" \
        --argjson nh "${nodes_with_hits}" \
        --argjson t  "${total}" \
        --argjson pn "${per_node_json}" \
        --arg since  "${since_iso}" \
        --arg until  "${until_iso}" \
        '{
            window: { since: $since, until: $until },
            nodes_checked: $nc,
            nodes_with_429: $nh,
            total_429_lines: $t,
            per_node: $pn
        }' > "${out_summary}"

    kubectl delete ds gantry-demo-throttle-scraper -n default --wait=false >&2
    echo "  containerd 429 scan: ${total} hit lines across ${nodes_with_hits}/${nodes_checked} nodes" >&2
}

# scrape_pull_events <run_label> <out_file>
#
# Aggregates kubelet Pulling/Pulled events for the given workload Job's
# pods. Computes per-pod pull duration (Pulled - Pulling) and writes a
# single JSON artifact:
#   { pods: [{pod, node, pulling, pulled, duration_seconds}],
#     summary: { count, min, p50, p95, max, mean } }
#
# Instant; no Azure dependency.
scrape_pull_events() {
    local run_label="$1" out_file="$2"
    local rows="[]"
    for p in $(kubectl get pods -l "gantry.demo/run-label=${run_label}" -o name 2>/dev/null); do
        local pn=${p#pod/}
        local node pulling pulled
        node="$(kubectl get pod "${pn}" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
        pulling="$(kubectl get events --field-selector "involvedObject.name=${pn},reason=Pulling" \
            -o jsonpath='{.items[0].firstTimestamp}' 2>/dev/null)"
        pulled="$(kubectl get events --field-selector "involvedObject.name=${pn},reason=Pulled" \
            -o jsonpath='{.items[0].firstTimestamp}' 2>/dev/null)"
        if [[ -n "${pulling}" && -n "${pulled}" ]]; then
            local t1 t2 dur
            t1=$(date -u -d "${pulling}" +%s 2>/dev/null || echo 0)
            t2=$(date -u -d "${pulled}"  +%s 2>/dev/null || echo 0)
            dur=$(( t2 - t1 ))
            rows="$(echo "${rows}" | jq --arg pod "${pn}" --arg node "${node}" \
                --arg pulling "${pulling}" --arg pulled "${pulled}" --argjson dur "${dur}" \
                '. + [{pod:$pod, node:$node, pulling:$pulling, pulled:$pulled, duration_seconds:$dur}]')"
        fi
    done
    echo "${rows}" | jq '{
        pods: .,
        summary: ([.[].duration_seconds] | {
            count: length,
            min:   (min // 0),
            p50:   (sort | .[length/2|floor] // 0),
            p95:   (sort | .[((length-1)*0.95)|floor] // 0),
            max:   (max // 0),
            mean:  ((add // 0) / (length // 1))
        })
    }' > "${out_file}"
    echo "  pull events: $(jq -r '.summary | "count=\(.count) min=\(.min)s p50=\(.p50)s p95=\(.p95)s max=\(.max)s"' "${out_file}")" >&2
}
