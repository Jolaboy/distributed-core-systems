use std::net::SocketAddr;

use axum::{
    Json, Router,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::signal;
use tracing::{info, instrument};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Incoming telemetry frame ingested from concurrent traffic streams.
#[derive(Debug, Deserialize)]
struct IngestionPayload {
    event_id: String,
    metric_signature: String,
    data_points: Vec<f64>,
}

/// Acknowledgement returned to the client after a frame is processed.
#[derive(Debug, Serialize)]
struct SystemResponse {
    status: String,
    processed_elements: usize,
}

/// Lightweight payload served to orchestrator liveness/readiness probes.
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[instrument(skip(payload), fields(event_id = %payload.event_id))]
async fn ingest_pipeline_handler(Json(payload): Json<IngestionPayload>) -> Json<SystemResponse> {
    // Non-blocking asynchronous ingestion processing.
    let element_count = payload.data_points.len();

    info!(
        signature = %payload.metric_signature,
        processed_elements = element_count,
        "telemetry frame ingested"
    );

    Json(SystemResponse {
        status: "ACK_RECEIVED_SUCCESS".to_string(),
        processed_elements: element_count,
    })
}

/// Health endpoint consumed by Kubernetes liveness/readiness probes.
async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

fn build_router() -> Router {
    Router::new()
        .route("/api/v1/telemetry", post(ingest_pipeline_handler))
        .route("/healthz", get(health_handler))
}

#[tokio::main]
async fn main() {
    // Structured logging driven by the RUST_LOG environment variable.
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app = build_router();

    // Bind address is configurable so the same binary runs locally and in a
    // container. Defaults to 0.0.0.0:8080 so it is reachable inside a pod.
    let address: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()
        .expect("BIND_ADDR must be a valid socket address");

    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("failed to bind listener");

    info!(%address, "DEV_ENGINE::CORE -> asynchronous high-throughput engine listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

/// Resolves when the process receives SIGINT (Ctrl+C) or SIGTERM, enabling
/// graceful connection draining during rolling Kubernetes deployments.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, draining connections");
}
