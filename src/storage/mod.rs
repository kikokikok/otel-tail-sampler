use crate::sampling::policies::TraceSummary;
use crate::state::BufferedSpan;
use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use std::any::Any;
use std::time::Duration;

pub mod memory;

#[cfg(feature = "iceberg-storage")]
pub mod iceberg;

#[cfg(feature = "iceberg-storage")]
pub use iceberg::{IcebergConfig, IcebergTraceStore};

#[cfg(feature = "iceberg-storage")]
impl From<&crate::config::IcebergStorageConfig> for IcebergConfig {
    fn from(cfg: &crate::config::IcebergStorageConfig) -> Self {
        IcebergConfig {
            catalog_uri: cfg.catalog_uri.clone(),
            warehouse: cfg.warehouse.clone(),
            namespace: cfg.namespace.clone(),
            table_name: cfg.table_name.clone(),
            inactivity_threshold_ms: cfg.inactivity_threshold_ms,
            project_id: cfg.project_id.clone(),
            s3_endpoint: cfg.s3_endpoint.clone(),
            s3_access_key_id: cfg.s3_access_key_id.clone(),
            s3_secret_access_key: cfg.s3_secret_access_key.clone(),
            s3_region: cfg.s3_region.clone(),
            s3_path_style: cfg.s3_path_style,
        }
    }
}

#[async_trait]
pub trait TraceStore: Send + Sync {
    async fn ingest(&self, spans: &[BufferedSpan]) -> Result<IngestResult>;

    async fn get_ready_traces(&self, inactivity_threshold: Duration) -> Result<Vec<String>>;

    async fn get_trace_summary(&self, trace_id: &str) -> Result<Option<TraceSummary>>;

    async fn get_trace_spans(&self, trace_id: &str) -> Result<Option<Vec<BufferedSpan>>>;

    async fn remove_traces(&self, trace_ids: &[String]) -> Result<usize>;

    async fn mark_processing(&self, trace_ids: &[String]) -> Result<()>;

    async fn get_all_trace_ids(&self) -> Result<Vec<String>>;

    async fn stats(&self) -> Result<StoreStats>;

    fn storage_type(&self) -> StorageType;

    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Clone)]
pub struct IngestResult {
    pub added: usize,
    pub dropped: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreStats {
    pub span_count: usize,
    pub trace_count: usize,
    pub memory_bytes: usize,
    pub storage_type: StorageType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum StorageType {
    Memory,
    Iceberg,
}

impl std::fmt::Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageType::Memory => write!(f, "memory"),
            StorageType::Iceberg => write!(f, "iceberg"),
        }
    }
}
