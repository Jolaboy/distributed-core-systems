# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
FROM rust:1.85-slim AS builder

WORKDIR /app

# Copy the manifest first to leverage Docker layer caching for dependencies.
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

# ---- Runtime stage ---------------------------------------------------------
# Distroless keeps the final image tiny and rootless, complementing the
# service's sub-45MB memory footprint goal.
FROM gcr.io/distroless/cc-debian12 AS runtime

WORKDIR /app

COPY --from=builder /app/target/release/distributed-api-layer /usr/local/bin/distributed-api-layer

ENV BIND_ADDR=0.0.0.0:8080 \
    RUST_LOG=info

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/distributed-api-layer"]
