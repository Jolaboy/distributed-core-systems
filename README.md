# High-Throughput Distributed API Layer (`distributed-api-layer`)

An asynchronous microservice pipeline compiled in native Rust. Utilizing the multi-threaded `Tokio` runtime alongside the `Axum` routing framework, this service processes intensive metrics ingestion under a low memory footprint ($< 45\text{MB}$ operational ceiling) with ultra-high request throughput.

## 📐 Architecture Topology

```mermaid
graph LR
    A[Concurrent Traffic Ingress] -->|JSON Payloads via TCP| B[Native Socket Listener]
    B -->|Async Spawning Engine| C[Tokio Thread Worker Pool]
    C -->|Zero-Allocation Router| D[Axum Ingestion Framework]
    D -->|Serde Zero-Copy Matrix| E[JSON Processing Handler]
    E -->|Status ACK Payload| F[Client Ingress Endpoint Return]

    style C fill:#1e1e2f,stroke:#f59e0b,stroke-width:2px
    style E fill:#0f172a,stroke:#3b82f6,stroke-width:2px
```

## 🛠️ System Stack & Core Dependencies

| Layer | Technology |
| --- | --- |
| Compilation language | Rust 1.85+ (edition 2024) |
| Asynchronous runtime | Tokio (`features = ["full"]`) |
| Routing middleware | Axum 0.8 |
| Serialization | Serde + `serde_json` |
| Observability | `tracing` + `tracing-subscriber` |

> **Note:** The project targets Rust edition 2024, which requires the **1.85+** stable toolchain.

## 🔌 HTTP Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/api/v1/telemetry` | Ingests a JSON telemetry frame and returns an ACK. |
| `GET` | `/healthz` | Liveness/readiness probe for orchestrators. |

## 📂 Repository Structure

```text
distributed-core-systems/
├── README.md
└── distributed-api-layer/
    ├── Cargo.toml                  # Dependencies + release build profile
    ├── Dockerfile                  # Multi-stage, distroless runtime image
    ├── .dockerignore
    ├── src/
    │   └── main.rs                 # Routing engine, handlers, graceful shutdown
    ├── infra/
    │   └── main.tf                 # Terraform IaC (EKS control plane mock)
    ├── k8s/
    │   └── deployment.yaml         # Kubernetes Deployment + Service
    └── .github/
        └── workflows/
            └── ci.yaml             # fmt, clippy, build & test pipeline
```

## 🚀 Local Compilation & Run

1. **Verify the toolchain** (Rust 1.85+):

   ```bash
   rustc --version
   cargo --version
   ```

2. **Build and run** from the service directory:

   ```bash
   cd distributed-api-layer
   cargo run
   ```

   By default the server binds to `0.0.0.0:8080`. Override the bind address or log level with environment variables:

   ```bash
   BIND_ADDR=127.0.0.1:8080 RUST_LOG=debug cargo run
   ```

3. **Optimized release build** (size + throughput tuned profile):

   ```bash
   cargo build --release
   ```

## 🔬 Endpoint Testing

Send a verification frame with `curl`:

```bash
curl -X POST http://127.0.0.1:8080/api/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{"event_id":"tx_9921","metric_signature":"v8_stable","data_points":[1.05, 99.4, 40.2]}'
```

Expected response:

```json
{"status":"ACK_RECEIVED_SUCCESS","processed_elements":3}
```

Health check:

```bash
curl http://127.0.0.1:8080/healthz
# {"status":"ok"}
```

## 🐳 Container Build

A multi-stage `Dockerfile` produces a minimal, rootless [distroless](https://github.com/GoogleContainerTools/distroless) image that complements the sub-45MB footprint goal:

```bash
cd distributed-api-layer
docker build -t jolaboy/distributed-api-layer:latest .
docker run --rm -p 8080:8080 jolaboy/distributed-api-layer:latest
```

## ☸️ Infrastructure-as-Code & Kubernetes

Mock configuration files demonstrate the deployment topology:

- **Terraform** ([`infra/main.tf`](distributed-api-layer/infra/main.tf)) — provisions a managed Kubernetes (EKS) control plane with parameterized region, cluster name, and subnets.

  ```bash
  cd distributed-api-layer/infra
  terraform init
  terraform plan
  ```

- **Kubernetes** ([`k8s/deployment.yaml`](distributed-api-layer/k8s/deployment.yaml)) — a `Deployment` (3 replicas) plus a `ClusterIP` `Service`. The pod enforces a `45Mi` memory limit and wires liveness/readiness probes to `/healthz`.

  ```bash
  kubectl apply -f distributed-api-layer/k8s/deployment.yaml
  ```

## 🔁 Continuous Integration

[`.github/workflows/ci.yaml`](distributed-api-layer/.github/workflows/ci.yaml) runs formatting checks, Clippy lints (warnings treated as errors), a release build, and the test suite on every push and pull request to `main`.

