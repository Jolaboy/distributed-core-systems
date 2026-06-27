use futures::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::cache::Cache;
use crate::events::EventBus;

pub mod proto {
    tonic::include_proto!("telemetry.v1");
}

use proto::telemetry_service_server::{TelemetryService, TelemetryServiceServer};
use proto::{Ack, StreamSummary, TelemetryFrame};

/// gRPC front-end sharing the same caching tier and event bus as the REST API.
#[derive(Clone)]
pub struct TelemetryGrpc {
    cache: Cache,
    events: EventBus,
    dedupe_ttl_secs: u64,
}

impl TelemetryGrpc {
    async fn process(&self, frame: &TelemetryFrame) -> (u64, bool) {
        let n = frame.data_points.len() as u64;
        let is_new = self
            .cache
            .register_event(&frame.event_id, self.dedupe_ttl_secs)
            .await;
        self.cache.incr_processed(n).await;

        if let Ok(payload) = serde_json::to_vec(&serde_json::json!({
            "event_id": frame.event_id,
            "metric_signature": frame.metric_signature,
            "data_points": frame.data_points,
        })) {
            self.events.publish(&frame.event_id, &payload).await;
        }

        (n, !is_new)
    }
}

#[tonic::async_trait]
impl TelemetryService for TelemetryGrpc {
    async fn ingest(&self, request: Request<TelemetryFrame>) -> Result<Response<Ack>, Status> {
        let frame = request.into_inner();
        let (processed_elements, duplicate) = self.process(&frame).await;
        info!(
            event_id = %frame.event_id,
            processed_elements,
            duplicate,
            "grpc telemetry frame ingested"
        );

        Ok(Response::new(Ack {
            status: "ACK_RECEIVED_SUCCESS".to_string(),
            processed_elements,
            duplicate,
        }))
    }

    async fn ingest_stream(
        &self,
        request: Request<Streaming<TelemetryFrame>>,
    ) -> Result<Response<StreamSummary>, Status> {
        let mut stream = request.into_inner();
        let mut frames = 0u64;
        let mut processed_elements = 0u64;

        // Frames are processed one at a time as they arrive — constant memory
        // regardless of how many frames the client sends.
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            let (n, _dup) = self.process(&frame).await;
            frames += 1;
            processed_elements += n;
        }

        Ok(Response::new(StreamSummary {
            frames,
            processed_elements,
        }))
    }
}

pub fn service(
    cache: Cache,
    events: EventBus,
    dedupe_ttl_secs: u64,
) -> TelemetryServiceServer<TelemetryGrpc> {
    TelemetryServiceServer::new(TelemetryGrpc {
        cache,
        events,
        dedupe_ttl_secs,
    })
}
