#!/usr/bin/env bash
# 10b-set-budget-alert.sh — RG-scoped daily budget alert so an
# overnight-forgotten cluster pages someone instead of silently
# burning $90 by morning.

set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./env.sh

if [[ ! -f .provision-state ]]; then
    echo ".provision-state missing — run 10-provision.sh first" >&2
    exit 1
fi
# shellcheck disable=SC1091
source ./.provision-state

echo "==> Action group: gantry-demo-budget-ag"
az monitor action-group create \
    --resource-group "${RG_NAME}" \
    --name gantry-demo-budget-ag \
    --short-name gd-budget \
    --action email gantry-demo-email "${BUDGET_ALERT_EMAIL}" \
    -o none || true

AG_ID="$(az monitor action-group show \
    -g "${RG_NAME}" -n gantry-demo-budget-ag --query id -o tsv)"

# Budget needs a stable date range (consumption budgets must start on
# the first of a month). Use the current month start.
START_DATE="$(date -u +%Y-%m-01)"
# Three months out is safely after the demo session is over.
END_DATE="$(date -u -d "${START_DATE} +3 month" +%Y-%m-%d 2>/dev/null \
    || date -u -v+3m -j -f %Y-%m-%d "${START_DATE}" +%Y-%m-%d)"

# az consumption budget create-with-rg requires the *monthly* equivalent
# of the daily cap; multiply by 30 for a soft monthly bound and let the
# alert thresholds fire well before that.
MONTHLY_CAP=$(( DAILY_BUDGET_USD * 30 ))

echo "==> Budget: gantry-demo-daily (\$${DAILY_BUDGET_USD}/day soft cap → \$${MONTHLY_CAP}/mo)"
az consumption budget create-with-rg \
    --resource-group "${RG_NAME}" \
    --budget-name gantry-demo-daily \
    --amount "${MONTHLY_CAP}" \
    --category Cost \
    --time-grain Monthly \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --notifications "{
        \"actual_GreaterThan_50_Percent\": {
            \"enabled\": true,
            \"operator\": \"GreaterThan\",
            \"threshold\": 50,
            \"contactEmails\": [\"${BUDGET_ALERT_EMAIL}\"],
            \"contactGroups\": [\"${AG_ID}\"],
            \"thresholdType\": \"Actual\"
        },
        \"actual_GreaterThan_90_Percent\": {
            \"enabled\": true,
            \"operator\": \"GreaterThan\",
            \"threshold\": 90,
            \"contactEmails\": [\"${BUDGET_ALERT_EMAIL}\"],
            \"contactGroups\": [\"${AG_ID}\"],
            \"thresholdType\": \"Actual\"
        }
    }" \
    -o none || echo "  (budget may already exist; continuing)"

echo
echo "Budget alert configured. Check ${BUDGET_ALERT_EMAIL} for a confirmation."
