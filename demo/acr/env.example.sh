# Source this file (`source env.sh`) before running any phase script.
# Copy to `env.sh` and edit the values that are placeholders.
#
# Every variable below is consumed by one or more of the 0X-XX.sh scripts
# in this directory.

# ---------- Azure ----------
# Resolved by `00-prereqs.sh`'s `az account show` if left blank, but
# pinning it explicitly avoids "wrong subscription" mistakes if the
# operator has multiple.
export SUBSCRIPTION_ID="${SUBSCRIPTION_ID:-}"
export LOCATION="${LOCATION:-eastus}"

# All resources land in this resource group. `99-cleanup.sh` deletes
# the whole RG to guarantee no leftover spend.
export RG_NAME="${RG_NAME:-gantry-demo-rg}"

# ACR name must be globally unique, 5-50 alphanumerics, lowercase.
export ACR_NAME="${ACR_NAME:-gantrydemo$(whoami)}"

# AKS cluster.
export AKS_NAME="${AKS_NAME:-gantry-demo-aks}"
export NODE_COUNT="${NODE_COUNT:-20}"
export NODE_VM_SIZE="${NODE_VM_SIZE:-Standard_D4s_v5}"

# Log Analytics workspace (used by ACR diagnostic settings + KQL).
export LAW_NAME="${LAW_NAME:-gantry-demo-law}"

# Daily $ budget alert on the resource group. Tripping this fires a
# notification to the configured action group (set up by 10b).
export DAILY_BUDGET_USD="${DAILY_BUDGET_USD:-50}"
# Email address(es) for the budget alert notification, comma separated.
export BUDGET_ALERT_EMAIL="${BUDGET_ALERT_EMAIL:-you@example.com}"

# ---------- Demo image ----------
# Image generator parameters (consumed by 20-push-demo-image.sh).
export DEMO_REPO="${DEMO_REPO:-demo}"
export DEMO_LAYER_COUNT="${DEMO_LAYER_COUNT:-30}"
export DEMO_LAYER_BYTES="${DEMO_LAYER_BYTES:-20971520}"  # ~20 MiB / layer

# RUN_ID is normally minted by 20-push-demo-image.sh from the epoch and
# written to .run-id; downstream scripts read it back. Override only
# when intentionally re-using a tag (Phase 6b uses .run-id-with-gantry).
export RUN_ID="${RUN_ID:-}"

# How many baseline workload iterations 40-baseline.sh runs back-to-back
# to drive ACR pull load high enough to risk Basic-SKU throttling.
# Implementation: 40-baseline.sh pre-builds all N images first (Phase A),
# then fires the workload Jobs back-to-back with no build pauses (Phase
# B), so the Phase-B burst rate is the actual ACR-side load. Each
# iteration produces ~640 ACR repository events. 10 iterations ≈ 6400
# events over ~2 min — usually enough to bite Basic-SKU ReadOps.
# Set to 1 for the original single-pass behaviour.
export BASELINE_HAMMER_ITERATIONS="${BASELINE_HAMMER_ITERATIONS:-10}"

# ---------- Gantry image ----------
# Tag pushed to <acr>.azurecr.io/gantry by 50-build-gantry.sh.
export GANTRY_IMAGE_TAG="${GANTRY_IMAGE_TAG:-demo}"

# ---------- Observability ----------
export PROM_NAMESPACE="${PROM_NAMESPACE:-monitoring}"
export PROM_RELEASE="${PROM_RELEASE:-kps}"

# Wait this many seconds after a workload Job completes before scraping
# Azure Monitor / Log Analytics. Documented ingest lag is 2–5 min; 5 min
# is the safer default.
export AZ_INGEST_LAG_SECONDS="${AZ_INGEST_LAG_SECONDS:-300}"
# Re-pull window for 61b-dashboard-replay.sh.
export AZ_INGEST_REPLAY_SECONDS="${AZ_INGEST_REPLAY_SECONDS:-600}"

# ---------- Internal ----------
# Where artifacts (KQL outputs, Prom deltas, pod logs) get written.
export ARTIFACTS_DIR="${ARTIFACTS_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/artifacts}"
mkdir -p "${ARTIFACTS_DIR}"
