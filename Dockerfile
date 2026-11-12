# syntax=docker/dockerfile:1
#
# Production image of events-service.
#
# Multi-stage:
#   - builder: full Rust toolchain, builds the release `events_service` binary.
#   - runtime: distroless/cc (glibc, CA certificates, `nonroot` user) holding
#     only the binary: no toolchain, no sources, no shell.
#
# Base images are pinned by tag AND digest for reproducible builds. Refresh a
# digest with `docker buildx imagetools inspect <image:tag>`.

FROM rust:1.98-bookworm@sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922 AS builder

WORKDIR /app

# Cache dependency compilation: this layer is reused while Cargo.toml and
# Cargo.lock do not change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo '//! dependency cache placeholder' > src/lib.rs \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
RUN touch src/lib.rs src/main.rs \
    && cargo build --release --bin events_service \
    && strip target/release/events_service

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f

LABEL org.opencontainers.image.title="events-service" \
      org.opencontainers.image.description="Read side of the security-events Kinesis stream: consumer, aggregator and admin SSE" \
      org.opencontainers.image.source="https://github.com/cloud-native-security-app/events-service"

COPY --from=builder /app/target/release/events_service /usr/local/bin/events_service

USER nonroot

ENTRYPOINT ["/usr/local/bin/events_service"]
