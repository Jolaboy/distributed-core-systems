use std::collections::BTreeMap;
use std::sync::Arc;

use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::record::Record;
use tracing::{info, warn};

/// Apache Kafka event bus producer. Publishes every ingested frame to a topic
/// so downstream services (e.g. the Node.js consumer) can react. Degrades to a
/// no-op when Kafka is not configured or unreachable.
#[derive(Clone)]
pub struct EventBus {
    inner: Option<Arc<PartitionClient>>,
}

impl EventBus {
    pub async fn connect(bootstrap: &[String], topic: &str) -> Self {
        if bootstrap.is_empty() {
            info!("kafka disabled (KAFKA_BOOTSTRAP not set); event bus inactive");
            return Self { inner: None };
        }

        match ClientBuilder::new(bootstrap.to_vec()).build().await {
            Ok(client) => {
                match client
                    .partition_client(topic.to_string(), 0, UnknownTopicHandling::Retry)
                    .await
                {
                    Ok(pc) => {
                        info!(topic, "connected to kafka event bus");
                        Self {
                            inner: Some(Arc::new(pc)),
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "kafka partition client failed; event bus disabled");
                        Self { inner: None }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "kafka connect failed; event bus disabled");
                Self { inner: None }
            }
        }
    }

    /// Publishes a record keyed by `key`. Errors are logged, never propagated,
    /// so a broker hiccup cannot stall the ingestion hot path.
    pub async fn publish(&self, key: &str, payload: &[u8]) {
        let Some(pc) = &self.inner else {
            return;
        };

        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(payload.to_vec()),
            headers: BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };

        if let Err(e) = pc.produce(vec![record], Compression::NoCompression).await {
            warn!(error = %e, "failed to publish event to kafka");
        }
    }
}
