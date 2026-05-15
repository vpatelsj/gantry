#!/usr/bin/env bash
# 62-cached-rerun.sh — Phase 6b warm-cache run.
#
# Steps:
#   1. Single-node manual cleanup pre-flight: pick one node, exec the
#      cleanup commands manually, assert content-store is empty.
#   2. Apply the fleet-wide cleanup Job (parallelism=NODE_COUNT,
#      podAntiAffinity, privileged, hostPath of containerd sock + data).
#   3. Wait for cleanup completion (Job will fail loud if any node's
#      content store still holds the digest).
#   4. Re-apply the workload Job with the SAME RUN_ID — containerd has
#      no local content, so containerd MUST route through gantry, which
#      hits its own warm cache.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./lib/common.sh
load_state

if [[ ! -f .run-id-with-gantry || ! -f .last-digest-with-gantry ]]; then
    echo "Phase 6 must run first (need .run-id-with-gantry and .last-digest-with-gantry)" >&2
    exit 1
fi
RUN_ID="$(cat .run-id-with-gantry)"
MANIFEST_DIGEST="$(cat .last-digest-with-gantry)"
IMAGE_REF="${ACR_LOGIN_SERVER}/${DEMO_REPO}:${RUN_ID}"

# 1. Pre-flight on a single node (manual cleanup + assertions).
echo "==> Pre-flight: cleanup on one node, then assert content store empty"
PREFLIGHT_NODE="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
echo "  node: ${PREFLIGHT_NODE}"
PREFLIGHT_FILE="$(mktemp -t cleanup-preflight-XXXXXX.yaml)"
cat > "${PREFLIGHT_FILE}" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: gantry-demo-cleanup-preflight
  namespace: default
spec:
  restartPolicy: Never
  hostPID: true
  nodeName: ${PREFLIGHT_NODE}
  containers:
    - name: ctr
      image: mcr.microsoft.com/cbl-mariner/base/core:2.0
      command: ["/bin/sh","-c"]
      args:
        - |
          set -eu
          CTR="nsenter -t 1 -m -u -i -n -p -- ctr -a /run/containerd/containerd.sock -n k8s.io"
          \$CTR images rm ${IMAGE_REF} || true
          for lease in \$(\$CTR leases ls 2>/dev/null | tail -n +2 | awk '{print \$1}'); do
              if \$CTR leases list-resources "\$lease" 2>/dev/null | grep -q "${MANIFEST_DIGEST}"; then
                  \$CTR leases delete "\$lease" || true
              fi
          done
          \$CTR content prune references || true
          if \$CTR content ls | grep -q "${MANIFEST_DIGEST}"; then
              echo "FAIL: ${MANIFEST_DIGEST} still in content store on ${PREFLIGHT_NODE}" >&2
              exit 1
          fi
          if \$CTR images ls | grep -q "${IMAGE_REF}"; then
              echo "FAIL: image ref still listed on ${PREFLIGHT_NODE}" >&2
              exit 1
          fi
          echo "OK: ${PREFLIGHT_NODE} cleared"
      securityContext: { privileged: true, runAsUser: 0 }
      volumeMounts:
        - { name: sock, mountPath: /run/containerd/containerd.sock }
        - { name: data, mountPath: /var/lib/containerd }
  volumes:
    - name: sock
      hostPath: { path: /run/containerd/containerd.sock, type: Socket }
    - name: data
      hostPath: { path: /var/lib/containerd, type: Directory }
EOF
kubectl delete pod gantry-demo-cleanup-preflight -n default --ignore-not-found
kubectl apply -f "${PREFLIGHT_FILE}"
kubectl wait --for=jsonpath='{.status.phase}=Succeeded' \
    pod/gantry-demo-cleanup-preflight -n default --timeout=5m
kubectl logs pod/gantry-demo-cleanup-preflight -n default
kubectl delete pod gantry-demo-cleanup-preflight -n default --wait=false
rm -f "${PREFLIGHT_FILE}"

# 2. Render + apply the fleet-wide cleanup Job.
echo "==> Fleet-wide cleanup Job"
RENDERED="$(mktemp -t cleanup-XXXXXX.yaml)"
IMAGE_REF="${IMAGE_REF}" \
    MANIFEST_DIGEST="${MANIFEST_DIGEST}" \
    NODE_COUNT="${NODE_COUNT}" \
    envsubst '${IMAGE_REF} ${MANIFEST_DIGEST} ${NODE_COUNT}' \
    < manifests/cleanup-containerd-cache-job.yaml.tmpl > "${RENDERED}"
kubectl delete job gantry-demo-cleanup -n default --ignore-not-found
kubectl apply -f "${RENDERED}"
kubectl wait --for=condition=complete job/gantry-demo-cleanup -n default --timeout=10m

# 3. Snapshot Prom counters BEFORE the warm rerun.
echo "==> Snapshotting Prometheus counters (before warm rerun)"
PROM_BEFORE="${ARTIFACTS_DIR}/cached-${RUN_ID}-prom-before.json"
{
    echo '{'
    printf '"origin_pull_total": %s,\n' "$(prom_query_scalar 'sum(p2p_origin_pull_total)')"
    printf '"peer_fetch_total": %s,\n' "$(prom_query_scalar 'sum(p2p_peer_fetch_total)')"
    printf '"cache_hit_total": %s\n' "$(prom_query_scalar 'sum(p2p_cache_hit_total)')"
    echo '}'
} > "${PROM_BEFORE}"

# 4. Re-apply the workload Job with same RUN_ID under run-label=warm.
kubectl delete job gantry-demo-workload-cold -n default --ignore-not-found

START_ISO="$(date -u +%FT%TZ)"
echo "${START_ISO}" > .cached-start

apply_workload_job "${IMAGE_REF}" warm >/dev/null
wait_workload_job warm 30m

END_ISO="$(date -u +%FT%TZ)"
echo "${END_ISO}" > .cached-end

echo
echo "Cached rerun complete."
echo "  RUN_ID=${RUN_ID}  start=${START_ISO}  end=${END_ISO}"
echo "  Next: ./63-record-cached.sh"
