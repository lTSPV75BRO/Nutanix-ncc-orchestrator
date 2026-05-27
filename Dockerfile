# NCC Orchestrator runner image for Kubernetes.
# Expects a pre-built Linux binary in dist/, for example:
# - dist/ncc-orchestrator-linux-amd64
# - dist/ncc-orchestrator-linux-arm64
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

# Buildx provides TARGETARCH (amd64/arm64). Default to amd64 for local builds.
ARG TARGETARCH=amd64
COPY dist/ncc-orchestrator-linux-${TARGETARCH} /usr/local/bin/ncc-orchestrator
RUN chmod 0755 /usr/local/bin/ncc-orchestrator

# Non-root by default. Kubernetes manifests can override runAsUser/runAsGroup.
USER 10001:10001
WORKDIR /workspace

# Config and output dirs are typically mounted in Kubernetes; CronJob overrides args.
ENTRYPOINT ["/usr/local/bin/ncc-orchestrator"]
CMD ["--config", "/config/config.yaml"]
