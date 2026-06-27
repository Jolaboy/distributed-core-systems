# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
FROM rust:1-slim AS builder

# protobuf-compiler: required by tonic-build (gRPC codegen)
# build-essential: C toolchain needed to compile lz4/zstd used by the Kafka client
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the manifest first to leverage Docker layer caching for dependencies.
COPY Cargo.toml Cargo.lock* build.rs ./
COPY proto ./proto
COPY src ./src

RUN cargo build --release

# ---- Runtime stage ---------------------------------------------------------
# Distroless keeps the final image tiny and rootless.
FROM gcr.io/distroless/cc-debian12 AS runtime

WORKDIR /app

COPY --from=builder /app/target/release/distributed-api-layer /usr/local/bin/distributed-api-layer

ENV BIND_ADDR=0.0.0.0:8080 \
    GRPC_ADDR=0.0.0.0:50051 \
    RUST_LOG=info

EXPOSE 8080 50051

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/distributed-api-layer"]
