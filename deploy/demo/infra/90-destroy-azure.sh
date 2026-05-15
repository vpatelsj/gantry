#!/usr/bin/env bash
# Destroy the greenfield Azure resource group for the demo.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_demo_env "${1:-}"

require_cmd az
require_env AZ_RESOURCE_GROUP
select_subscription

if [[ "${CONFIRM_DESTROY}" != "yes" ]]; then
    die "set CONFIRM_DESTROY=yes in the environment or env file before deleting ${AZ_RESOURCE_GROUP}"
fi

log "Deleting resource group ${AZ_RESOURCE_GROUP}; Azure will continue deletion in the background"
az group delete --name "${AZ_RESOURCE_GROUP}" --yes --no-wait
