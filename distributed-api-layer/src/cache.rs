use redis::aio::{ConnectionLike, ConnectionManager};
use redis::cluster_async::ClusterConnection;
use tracing::{info, warn};

/// Redis-backed caching tier used for event deduplication and aggregate
/// counters. Supports a single node or a Redis Cluster, and silently degrades
/// to a no-op when Redis is not configured or unreachable.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)] // ConnectionManager is a cheap multiplexed handle; boxing would only complicate the command helpers.
pub enum Cache {
    Disabled,
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

impl Cache {
    pub async fn connect(url: Option<&str>, cluster: bool) -> Self {
        let Some(url) = url else {
            info!("redis disabled (REDIS_URL not set); caching tier inactive");
            return Cache::Disabled;
        };

        if cluster {
            let nodes: Vec<String> = url.split(',').map(|s| s.trim().to_string()).collect();
            match redis::cluster::ClusterClient::new(nodes) {
                Ok(client) => match client.get_async_connection().await {
                    Ok(conn) => {
                        info!("connected to redis cluster caching tier");
                        return Cache::Cluster(conn);
                    }
                    Err(e) => warn!(error = %e, "redis cluster connect failed; caching disabled"),
                },
                Err(e) => warn!(error = %e, "invalid redis cluster config; caching disabled"),
            }
            return Cache::Disabled;
        }

        match redis::Client::open(url) {
            Ok(client) => match ConnectionManager::new(client).await {
                Ok(conn) => {
                    info!("connected to redis caching tier");
                    Cache::Single(conn)
                }
                Err(e) => {
                    warn!(error = %e, "redis connect failed; caching disabled");
                    Cache::Disabled
                }
            },
            Err(e) => {
                warn!(error = %e, "invalid redis url; caching disabled");
                Cache::Disabled
            }
        }
    }

    /// Registers an event id, returning `true` when it is newly seen within the
    /// TTL window and `false` when it is a duplicate. Always `true` when the
    /// cache is disabled (fail-open).
    pub async fn register_event(&self, event_id: &str, ttl_secs: u64) -> bool {
        let key = format!("telemetry:seen:{event_id}");
        match self.clone() {
            Cache::Disabled => true,
            Cache::Single(mut c) => set_nx(&mut c, &key, ttl_secs).await,
            Cache::Cluster(mut c) => set_nx(&mut c, &key, ttl_secs).await,
        }
    }

    /// Increments the global processed-elements counter.
    pub async fn incr_processed(&self, n: u64) {
        let key = "telemetry:processed_total";
        match self.clone() {
            Cache::Disabled => {}
            Cache::Single(mut c) => incr_by(&mut c, key, n).await,
            Cache::Cluster(mut c) => incr_by(&mut c, key, n).await,
        }
    }
}

async fn set_nx<C: ConnectionLike + Send>(conn: &mut C, key: &str, ttl: u64) -> bool {
    // SET key 1 NX EX ttl -> bulk string "OK" on success, nil if it already exists.
    let res: redis::RedisResult<Option<String>> = redis::cmd("SET")
        .arg(key)
        .arg(1)
        .arg("NX")
        .arg("EX")
        .arg(ttl)
        .query_async(conn)
        .await;
    // Fail-open: treat connection errors as "new" so ingestion is never blocked.
    matches!(res, Ok(Some(_)) | Err(_))
}

async fn incr_by<C: ConnectionLike + Send>(conn: &mut C, key: &str, n: u64) {
    let _: redis::RedisResult<i64> = redis::cmd("INCRBY")
        .arg(key)
        .arg(n as i64)
        .query_async(conn)
        .await;
}
