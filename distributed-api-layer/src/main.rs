use std::net::SocketAddr;

use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

mod cache;
mod config;
mod events;
mod grpc;
mod http;

use cache::Cache;
use config::Config;
use events::EventBus;
use http::AppState;

#[tokio::main]
async fn main() {
    // Structured logging driven by the RUST_LOG environment variable.
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env();

    // Connect optional integrations. Each degrades gracefully when absent.
    let cache = Cache::connect(cfg.redis_url.as_deref(), cfg.redis_cluster).await;
    let events = EventBus::connect(&cfg.kafka_bootstrap, &cfg.kafka_topic).await;

    let state = AppState {
        cache: cache.clone(),
        events: events.clone(),
        dedupe_ttl_secs: cfg.dedupe_ttl_secs,
    };

    let http_addr: SocketAddr = cfg
        .bind_addr
        .parse()
        .expect("BIND_ADDR must be a valid socket address");
    let grpc_addr: SocketAddr = cfg
        .grpc_addr
        .parse()
        .expect("GRPC_ADDR must be a valid socket address");

    let listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .expect("failed to bind HTTP listener");

    info!(
        %http_addr,
        %grpc_addr,
        "DEV_ENGINE::CORE -> asynchronous high-throughput engine listening (REST + gRPC)"
    );

    let http_server =
        axum::serve(listener, http::router(state)).with_graceful_shutdown(shutdown_signal());

    let grpc_service = grpc::service(cache, events, cfg.dedupe_ttl_secs);
    let grpc_server = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    // Run both transports concurrently; the first to stop (e.g. on shutdown
    // signal) cancels the other.
    tokio::select! {
        result = http_server => {
            if let Err(e) = result {
                error!(error = %e, "HTTP server error");
            }
        }
        result = grpc_server => {
            if let Err(e) = result {
                error!(error = %e, "gRPC server error");
            }
        }
    }

    info!("engine stopped");
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
