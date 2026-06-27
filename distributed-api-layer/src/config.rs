use std::env;

/// Runtime configuration sourced from environment variables. Every integration
/// is optional so the binary runs locally with zero external dependencies.
#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: String,
    pub grpc_addr: String,
    pub redis_url: Option<String>,
    pub redis_cluster: bool,
    pub kafka_bootstrap: Vec<String>,
    pub kafka_topic: String,
    /// Deduplication window (seconds) for event ids in the cache tier.
    pub dedupe_ttl_secs: u64,
}

impl Config {
    pub fn from_env() -> Self {
        let kafka_bootstrap = env::var("KAFKA_BOOTSTRAP")
            .ok()
            .map(|v| split_csv(&v))
            .unwrap_or_default();

        Self {
            bind_addr: env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string()),
            grpc_addr: env::var("GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string()),
            redis_url: env::var("REDIS_URL").ok().filter(|s| !s.is_empty()),
            redis_cluster: env::var("REDIS_CLUSTER")
                .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE"))
                .unwrap_or(false),
            kafka_topic: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "telemetry.frames".to_string()),
            kafka_bootstrap,
            dedupe_ttl_secs: env::var("DEDUPE_TTL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
        }
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}
