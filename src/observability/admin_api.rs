use crate::sampling::force_sampling::{ForceRuleCreateRequest, ForceRuleEngine, ForceRuleResponse};
use crate::storage::TraceStore;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

#[derive(Clone)]
pub struct AdminApiState {
    pub force_rule_engine: Arc<ForceRuleEngine>,
    pub trace_store: Option<Arc<dyn TraceStore>>,
}

#[derive(Serialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
}

impl<T> ApiResponse<T> {
    fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn err(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

#[derive(Deserialize)]
pub struct ExtendRuleRequest {
    pub additional_secs: u64,
}

async fn list_rules(
    State(state): State<AdminApiState>,
) -> Result<Json<ApiResponse<Vec<ForceRuleResponse>>>, StatusCode> {
    match state.force_rule_engine.list_rules().await {
        Ok(rules) => {
            let responses: Vec<ForceRuleResponse> =
                rules.iter().map(ForceRuleResponse::from).collect();
            Ok(Json(ApiResponse::ok(responses)))
        }
        Err(e) => {
            error!("Failed to list force rules: {}", e);
            Ok(Json(ApiResponse::err(format!(
                "Failed to list rules: {}",
                e
            ))))
        }
    }
}

async fn get_rule(
    State(state): State<AdminApiState>,
    Path(rule_id): Path<String>,
) -> Result<Json<ApiResponse<ForceRuleResponse>>, StatusCode> {
    match state.force_rule_engine.get_rule(&rule_id).await {
        Ok(Some(rule)) => Ok(Json(ApiResponse::ok(ForceRuleResponse::from(&rule)))),
        Ok(None) => Ok(Json(ApiResponse::err(format!(
            "Rule not found: {}",
            rule_id
        )))),
        Err(e) => {
            error!("Failed to get force rule {}: {}", rule_id, e);
            Ok(Json(ApiResponse::err(format!("Failed to get rule: {}", e))))
        }
    }
}

async fn create_rule(
    State(state): State<AdminApiState>,
    Json(request): Json<ForceRuleCreateRequest>,
) -> Result<Json<ApiResponse<ForceRuleResponse>>, StatusCode> {
    info!(
        "Creating force rule: {} (duration: {}s)",
        request.description, request.duration_secs
    );

    match state.force_rule_engine.create_rule(request).await {
        Ok(rule) => {
            info!("Created force rule: {}", rule.id);
            Ok(Json(ApiResponse::ok(ForceRuleResponse::from(&rule))))
        }
        Err(e) => {
            error!("Failed to create force rule: {}", e);
            Ok(Json(ApiResponse::err(format!(
                "Failed to create rule: {}",
                e
            ))))
        }
    }
}

async fn delete_rule(
    State(state): State<AdminApiState>,
    Path(rule_id): Path<String>,
) -> Result<Json<ApiResponse<bool>>, StatusCode> {
    info!("Deleting force rule: {}", rule_id);

    match state.force_rule_engine.delete_rule(&rule_id).await {
        Ok(deleted) => {
            if deleted {
                info!("Deleted force rule: {}", rule_id);
            }
            Ok(Json(ApiResponse::ok(deleted)))
        }
        Err(e) => {
            error!("Failed to delete force rule {}: {}", rule_id, e);
            Ok(Json(ApiResponse::err(format!(
                "Failed to delete rule: {}",
                e
            ))))
        }
    }
}

async fn extend_rule(
    State(state): State<AdminApiState>,
    Path(rule_id): Path<String>,
    Json(request): Json<ExtendRuleRequest>,
) -> Result<Json<ApiResponse<ForceRuleResponse>>, StatusCode> {
    info!(
        "Extending force rule {} by {}s",
        rule_id, request.additional_secs
    );

    match state
        .force_rule_engine
        .extend_rule(&rule_id, request.additional_secs)
        .await
    {
        Ok(rule) => {
            info!("Extended force rule: {}", rule_id);
            Ok(Json(ApiResponse::ok(ForceRuleResponse::from(&rule))))
        }
        Err(e) => {
            error!("Failed to extend force rule {}: {}", rule_id, e);
            Ok(Json(ApiResponse::err(format!(
                "Failed to extend rule: {}",
                e
            ))))
        }
    }
}

async fn disable_rule(
    State(state): State<AdminApiState>,
    Path(rule_id): Path<String>,
) -> Result<Json<ApiResponse<ForceRuleResponse>>, StatusCode> {
    info!("Disabling force rule: {}", rule_id);

    match state.force_rule_engine.disable_rule(&rule_id).await {
        Ok(rule) => {
            info!("Disabled force rule: {}", rule_id);
            Ok(Json(ApiResponse::ok(ForceRuleResponse::from(&rule))))
        }
        Err(e) => {
            error!("Failed to disable force rule {}: {}", rule_id, e);
            Ok(Json(ApiResponse::err(format!(
                "Failed to disable rule: {}",
                e
            ))))
        }
    }
}

async fn enable_rule(
    State(state): State<AdminApiState>,
    Path(rule_id): Path<String>,
) -> Result<Json<ApiResponse<ForceRuleResponse>>, StatusCode> {
    info!("Enabling force rule: {}", rule_id);

    match state.force_rule_engine.enable_rule(&rule_id).await {
        Ok(rule) => {
            info!("Enabled force rule: {}", rule_id);
            Ok(Json(ApiResponse::ok(ForceRuleResponse::from(&rule))))
        }
        Err(e) => {
            error!("Failed to enable force rule {}: {}", rule_id, e);
            Ok(Json(ApiResponse::err(format!(
                "Failed to enable rule: {}",
                e
            ))))
        }
    }
}

async fn cleanup_expired(
    State(state): State<AdminApiState>,
) -> Result<Json<ApiResponse<usize>>, StatusCode> {
    match state.force_rule_engine.cleanup_expired_rules().await {
        Ok(count) => Ok(Json(ApiResponse::ok(count))),
        Err(e) => {
            error!("Failed to cleanup expired rules: {}", e);
            Ok(Json(ApiResponse::err(format!("Failed to cleanup: {}", e))))
        }
    }
}

#[derive(Deserialize)]
pub struct SqlQueryRequest {
    pub sql: String,
}

#[derive(Serialize)]
pub struct QueryResultRow {
    pub columns: Vec<(String, String)>,
}

#[derive(Serialize)]
pub struct QueryResult {
    pub rows: Vec<QueryResultRow>,
    pub row_count: usize,
}

#[cfg(feature = "iceberg-storage")]
async fn query_sql(
    State(state): State<AdminApiState>,
    Json(request): Json<SqlQueryRequest>,
) -> Result<Json<ApiResponse<QueryResult>>, StatusCode> {
    use crate::storage::iceberg::IcebergTraceStore;

    let store = match &state.trace_store {
        Some(s) => s,
        None => return Ok(Json(ApiResponse::err("No trace store configured"))),
    };

    let iceberg_store: Option<&IcebergTraceStore> = store
        .as_any()
        .downcast_ref::<IcebergTraceStore>();

    let iceberg_store = match iceberg_store {
        Some(s) => s,
        None => return Ok(Json(ApiResponse::err("SQL queries require Iceberg storage backend"))),
    };

    match iceberg_store.query_sql(&request.sql).await {
        Ok(batches) => {
            let mut rows = Vec::new();
            for batch in &batches {
                let schema = batch.schema();
                for row_idx in 0..batch.num_rows() {
                    let mut columns = Vec::new();
                    for (col_idx, field) in schema.fields().iter().enumerate() {
                        let col = batch.column(col_idx);
                        let value = format_arrow_value(col, row_idx);
                        columns.push((field.name().clone(), value));
                    }
                    rows.push(QueryResultRow { columns });
                }
            }
            let row_count = rows.len();
            Ok(Json(ApiResponse::ok(QueryResult { rows, row_count })))
        }
        Err(e) => {
            error!("SQL query failed: {}", e);
            Ok(Json(ApiResponse::err(format!("Query failed: {}", e))))
        }
    }
}

#[cfg(feature = "iceberg-storage")]
fn format_arrow_value(col: &dyn arrow_array::Array, row_idx: usize) -> String {
    use arrow_array::*;

    if col.is_null(row_idx) {
        return "null".to_string();
    }

    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return arr.value(row_idx).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return arr.value(row_idx).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return arr.value(row_idx).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return format!("{:.2}", arr.value(row_idx));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        return format!("{:.2}", arr.value(row_idx));
    }
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return arr.value(row_idx).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return arr.value(row_idx).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return arr.value(row_idx).to_string();
    }

    format!("<{}>", col.data_type())
}

#[cfg(feature = "iceberg-storage")]
async fn slowest_traces(
    State(state): State<AdminApiState>,
) -> Result<Json<ApiResponse<QueryResult>>, StatusCode> {
    query_sql(
        State(state),
        Json(SqlQueryRequest {
            sql: "SELECT trace_id, MAX(duration_ms) as max_duration_ms, COUNT(*) as span_count 
                  FROM spans GROUP BY trace_id ORDER BY max_duration_ms DESC LIMIT 20".to_string(),
        }),
    )
    .await
}

#[cfg(feature = "iceberg-storage")]
async fn error_traces(
    State(state): State<AdminApiState>,
) -> Result<Json<ApiResponse<QueryResult>>, StatusCode> {
    query_sql(
        State(state),
        Json(SqlQueryRequest {
            sql: "SELECT trace_id, service_name, operation_name, duration_ms 
                  FROM spans WHERE status_code = 2 ORDER BY timestamp_ms DESC LIMIT 20".to_string(),
        }),
    )
    .await
}

#[cfg(feature = "iceberg-storage")]
async fn service_stats(
    State(state): State<AdminApiState>,
) -> Result<Json<ApiResponse<QueryResult>>, StatusCode> {
    query_sql(
        State(state),
        Json(SqlQueryRequest {
            sql: "SELECT service_name, COUNT(*) as span_count, COUNT(DISTINCT trace_id) as trace_count,
                  AVG(duration_ms) as avg_duration_ms, MAX(duration_ms) as max_duration_ms,
                  SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) as error_count
                  FROM spans GROUP BY service_name ORDER BY span_count DESC".to_string(),
        }),
    )
    .await
}

async fn store_stats(
    State(state): State<AdminApiState>,
) -> Result<Json<ApiResponse<crate::storage::StoreStats>>, StatusCode> {
    let store = match &state.trace_store {
        Some(s) => s,
        None => return Ok(Json(ApiResponse::err("No trace store configured"))),
    };

    match store.stats().await {
        Ok(stats) => Ok(Json(ApiResponse::ok(stats))),
        Err(e) => {
            error!("Failed to get store stats: {}", e);
            Ok(Json(ApiResponse::err(format!("Failed to get stats: {}", e))))
        }
    }
}

pub fn create_admin_router(state: AdminApiState) -> Router {
    let router = Router::new()
        .route("/admin/force-rules", get(list_rules).post(create_rule))
        .route(
            "/admin/force-rules/:rule_id",
            get(get_rule).delete(delete_rule),
        )
        .route("/admin/force-rules/:rule_id/extend", put(extend_rule))
        .route("/admin/force-rules/:rule_id/disable", put(disable_rule))
        .route("/admin/force-rules/:rule_id/enable", put(enable_rule))
        .route("/admin/force-rules/cleanup", post(cleanup_expired))
        .route("/admin/store/stats", get(store_stats));

    #[cfg(feature = "iceberg-storage")]
    let router = router
        .route("/admin/query", post(query_sql))
        .route("/admin/query/slowest-traces", get(slowest_traces))
        .route("/admin/query/error-traces", get(error_traces))
        .route("/admin/query/service-stats", get(service_stats));

    router.with_state(state)
}
