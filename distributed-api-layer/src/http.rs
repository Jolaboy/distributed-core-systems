use axum::body::Body;
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::cache::Cache;
use crate::events::EventBus;

/// Shared application state injected into every handler.
#[derive(Clone)]
pub struct AppState {
    pub cache: Cache,
    pub events: EventBus,
    pub dedupe_ttl_secs: u64,
}

/// Incoming telemetry frame ingested from concurrent traffic streams.
#[derive(Debug, Deserialize, Serialize)]
pub struct IngestionPayload {
    pub event_id: String,
    pub metric_signature: String,
    pub data_points: Vec<f64>,
}

#[derive(Debug, Serialize)]
struct SystemResponse {
    status: String,
    processed_elements: usize,
    duplicate: bool,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct StreamSummary {
    frames: usize,
    processed_elements: usize,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/telemetry", post(ingest))
        .route("/api/v1/stream", post(ingest_stream))
        .route("/healthz", get(health))
        .with_state(state)
}

#[instrument(skip_all, fields(event_id = %payload.event_id))]
async fn ingest(
    State(state): State<AppState>,
    Json(payload): Json<IngestionPayload>,
) -> Json<SystemResponse> {
    let element_count = payload.data_points.len();

    let is_new = state
        .cache
        .register_event(&payload.event_id, state.dedupe_ttl_secs)
        .await;
    state.cache.incr_processed(element_count as u64).await;

    if let Ok(bytes) = serde_json::to_vec(&payload) {
        state.events.publish(&payload.event_id, &bytes).await;
    }

    info!(
        signature = %payload.metric_signature,
        processed_elements = element_count,
        duplicate = !is_new,
        "telemetry frame ingested"
    );

    Json(SystemResponse {
        status: "ACK_RECEIVED_SUCCESS".to_string(),
        processed_elements: element_count,
        duplicate: !is_new,
    })
}

/// Memory-efficient NDJSON streaming ingestion. The request body is consumed as
/// a stream and parsed line-by-line, so peak memory stays bounded regardless of
/// the total payload size.
async fn ingest_stream(State(state): State<AppState>, body: Body) -> Json<StreamSummary> {
    let mut stream = body.into_data_stream();
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut frames = 0usize;
    let mut processed_elements = 0usize;

    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { break };
        buf.extend_from_slice(&chunk);

        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = buf.drain(..=pos).collect();
            let line = &line[..line.len().saturating_sub(1)];
            if line.is_empty() {
                continue;
            }
            if let Ok(frame) = serde_json::from_slice::<IngestionPayload>(line) {
                frames += 1;
                processed_elements += frame.data_points.len();
            }
        }
    }

    // Handle a trailing frame that is not newline-terminated.
    if !buf.is_empty() {
        if let Ok(frame) = serde_json::from_slice::<IngestionPayload>(&buf) {
            frames += 1;
            processed_elements += frame.data_points.len();
        }
    }

    state.cache.incr_processed(processed_elements as u64).await;
    info!(frames, processed_elements, "telemetry stream ingested");

    Json(StreamSummary {
        frames,
        processed_elements,
    })
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
