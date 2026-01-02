// Health check endpoint
use axum::{Json, Router, extract::State as AxumState, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, Clone)]
pub struct HealthState {
    healthy: Arc<Mutex<bool>>,
    last_kafka_success: Arc<Mutex<Instant>>,
    last_redis_success: Arc<Mutex<Instant>>,
    last_export_success: Arc<Mutex<Instant>>,
    recent_errors: Arc<Mutex<u64>>,
    version: String,
    instance_id: String,
}

impl HealthState {
    pub fn new(version: String, instance_id: String) -> Self {
        let now = Instant::now();
        Self {
            healthy: Arc::new(Mutex::new(true)),
            last_kafka_success: Arc::new(Mutex::new(now)),
            last_redis_success: Arc::new(Mutex::new(now)),
            last_export_success: Arc::new(Mutex::new(now)),
            recent_errors: Arc::new(Mutex::new(0)),
            version,
            instance_id,
        }
    }

    pub async fn mark_kafka_success(&self) {
        *self.last_kafka_success.lock().await = Instant::now();
    }

    pub async fn mark_redis_success(&self) {
        *self.last_redis_success.lock().await = Instant::now();
    }

    pub async fn mark_export_success(&self) {
        *self.last_export_success.lock().await = Instant::now();
    }

    pub async fn record_error(&self) {
        let mut errors = self.recent_errors.lock().await;
        *errors += 1;
        if *errors > 100 {
            *self.healthy.lock().await = false;
        }
    }

    pub async fn reset_errors(&self) {
        *self.recent_errors.lock().await = 0;
        *self.healthy.lock().await = true;
    }

    pub async fn is_healthy(&self) -> bool {
        *self.healthy.lock().await
    }
}

async fn health_handler(AxumState(state): AxumState<HealthState>) -> Json<serde_json::Value> {
    let healthy = state.is_healthy().await;
    Json(serde_json::json!({
        "status": if healthy { "healthy" } else { "unhealthy" },
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "version": state.version,
        "instance_id": state.instance_id,
    }))
}

async fn ready_handler(AxumState(state): AxumState<HealthState>) -> axum::http::StatusCode {
    if state.is_healthy().await {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn live_handler() -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}

pub fn create_health_router(state: HealthState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/health/ready", get(ready_handler))
        .route("/health/live", get(live_handler))
        .with_state(state)
}

pub async fn start_health_server(
    socket_addr: SocketAddr,
    state: Arc<HealthState>,
) -> Result<(), std::io::Error> {
    let app = create_health_router((*state).clone());
    let listener = tokio::net::TcpListener::bind(socket_addr).await?;
    info!("Health check server listening on {}", socket_addr);
    axum::serve(listener, app).await
}
