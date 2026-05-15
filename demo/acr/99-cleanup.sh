#!/usr/bin/env bash
# 99-cleanup.sh — print expected month-to-date cost for the RG, then
# require a typed "DELETE" to remove the resource group.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh
# shellcheck disable=SC1091
source ./.provision-state

echo "==> Month-to-date cost for ${RG_NAME}"
START_OF_MONTH="$(date -u +%Y-%m-01)"
TODAY="$(date -u +%Y-%m-%d)"
# Best-effort: az consumption may not be available in every tenant.
az consumption usage list \
    --start-date "${START_OF_MONTH}" \
    --end-date "${TODAY}" \
    --query "[?contains(instanceId, '/resourceGroups/${RG_NAME}/')] | [].{date:usageStart, meter:meterName, cost:pretaxCost}" \
    -o table 2>/dev/null \
    || echo "  (az consumption usage list not available; skip cost preview)"

echo
read -r -p "Type DELETE (uppercase) to confirm 'az group delete --name ${RG_NAME} --yes --no-wait': " CONFIRM

if [[ "${CONFIRM}" != "DELETE" ]]; then
    echo "Aborted (no resources removed)."
    exit 1
fi

echo "==> az group delete --name ${RG_NAME} --yes --no-wait"
az group delete --name "${RG_NAME}" --yes --no-wait

echo
echo "Group deletion submitted. Removing local state files."
rm -f .provision-state .run-id .run-id-* .last-digest-* .last-layers-*.txt \
      .baseline-start .baseline-end .with-gantry-start .with-gantry-end \
      .cached-start .cached-end
echo "Done."
