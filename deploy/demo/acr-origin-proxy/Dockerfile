# syntax=docker/dockerfile:1.7
#
# Demo-only counting-proxy image (ACR demo plan, build-plan step 2).
# Not a production artifact; see docs/acr-counting-proxy-demo.md §1.1.
#
# Build context is this directory (deploy/demo/acr-origin-proxy/);
# the proxy is a standalone Go module with no imports from the parent
# repo, so the parent's source tree is not needed.

ARG GO_VERSION=1.26.3

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
COPY *.go ./

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -trimpath -ldflags="-s -w" \
            -o /out/acr-origin-proxy .

# Distroless static — smallest standard base for static Go binaries.
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="acr-origin-proxy"
LABEL org.opencontainers.image.description="DEMO-ONLY counting reverse proxy in front of ACR for the Gantry demo plan"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Proxy port and Prometheus/debug port.
EXPOSE 5002/tcp 9090/tcp

COPY --from=build /out/acr-origin-proxy /usr/local/bin/acr-origin-proxy

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/acr-origin-proxy"]
