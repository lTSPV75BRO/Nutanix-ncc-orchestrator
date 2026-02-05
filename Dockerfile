# NCC Orchestrator runner image for Kubernetes
# Uses the pre-built Linux binary from dist/
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

# Use amd64 binary by default; override with build arg for arm64
ARG TARGETARCH
COPY dist/ncc-orchestrator-linux-amd64 /usr/local/bin/ncc-orchestrator
RUN chmod +x /usr/local/bin/ncc-orchestrator

# Config and output dir are mounted in Kubernetes; CronJob overrides args
ENTRYPOINT ["/usr/local/bin/ncc-orchestrator"]
CMD ["--config", "/config/config.yaml"]
