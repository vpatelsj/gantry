# deploy/demo/Makefile
#
# Demo-only Makefile. NEVER include or invoke this from the root
# Makefile — per docs/acr-counting-proxy-demo.md §1.1, the demo lives
# entirely under deploy/demo/ and the root Makefile must remain
# untouched.
#
# Invoke from anywhere:
#   make -C deploy/demo <target>
# or:
#   cd deploy/demo && make <target>

PROXY_DIR := acr-origin-proxy
INFRA_DIR := infra
IMAGE_NAME ?= acr-origin-proxy
IMAGE_TAG  ?= dev
NAMESPACE  ?= gantry-demo
ENV_FILE   ?= $(CURDIR)/$(INFRA_DIR)/env.local

.PHONY: all
all: proxy-build

# ---- proxy: build / lint / test --------------------------------------------

.PHONY: proxy-build
proxy-build:
	cd $(PROXY_DIR) && go build ./...

.PHONY: proxy-vet
proxy-vet:
	cd $(PROXY_DIR) && go vet ./...

.PHONY: proxy-test
proxy-test:
	cd $(PROXY_DIR) && go test ./...

# ---- proxy: container image ------------------------------------------------

.PHONY: proxy-image
proxy-image:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) $(PROXY_DIR)

# ---- proxy: local run (Phase 0.5 manual gate, see plan §6) -----------------
#
# Set ACR_NAME, ACR_USERNAME, ACR_PASSWORD in the environment.
# Optional: AUTH_MODE=basic|bearer|auto (default auto).
.PHONY: proxy-run-local
proxy-run-local:
	cd $(PROXY_DIR) && \
		UPSTREAM_REGISTRY=https://$(ACR_NAME).azurecr.io \
		LISTEN_ADDR=127.0.0.1:5002 \
		go run .

# ---- harness ---------------------------------------------------------------

.PHONY: harness
harness:
	cd harness && go test -tags=demo ./...

.PHONY: harness-baseline
harness-baseline:
	set -a; \
	[ ! -f $(INFRA_DIR)/.state.env ] || . $(INFRA_DIR)/.state.env; \
	[ ! -f $(ENV_FILE) ] || . $(ENV_FILE); \
	set +a; \
	cd harness && DEMO_RUN_LIVE=1 go test -tags=demo -run TestPhaseBaseline -timeout=90m ./...

# ---- greenfield Azure infra ------------------------------------------------

.PHONY: infra-provision
infra-provision:
	$(INFRA_DIR)/10-provision-azure.sh $(ENV_FILE)

.PHONY: infra-images
infra-images:
	$(INFRA_DIR)/20-build-push-images.sh $(ENV_FILE)

.PHONY: infra-monitoring
infra-monitoring:
	$(INFRA_DIR)/30-install-monitoring.sh $(ENV_FILE)

.PHONY: infra-proxy
infra-proxy:
	$(INFRA_DIR)/40-deploy-proxy-spike.sh $(ENV_FILE)

.PHONY: infra-smoke
infra-smoke:
	$(INFRA_DIR)/50-smoke-proxy-auth.sh $(ENV_FILE)

.PHONY: infra-node-reachability
infra-node-reachability:
	$(INFRA_DIR)/60-check-node-reachability.sh $(ENV_FILE)

.PHONY: infra-hosts-baseline
infra-hosts-baseline:
	$(INFRA_DIR)/70-install-hosts-toml.sh $(ENV_FILE) baseline

.PHONY: infra-hosts-gantry
infra-hosts-gantry:
	$(INFRA_DIR)/70-install-hosts-toml.sh $(ENV_FILE) gantry

.PHONY: infra-destroy
infra-destroy:
	$(INFRA_DIR)/90-destroy-azure.sh $(ENV_FILE)
