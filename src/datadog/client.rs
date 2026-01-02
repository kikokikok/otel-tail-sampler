// Datadog API client
use anyhow::{Context, Result};
use reqwest::{Client, ClientBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_retry::Retry;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{debug, error};

#[derive(Debug, Clone)]
pub struct DatadogClientConfig {
    pub api_endpoint: String,
    pub api_key: Option<String>,
    pub application_key: Option<String>,
    pub batch_size: usize,
    pub batch_timeout: Duration,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_initial_delay: Duration,
    pub max_concurrent_requests: usize,
}

#[derive(Debug)]
pub struct DatadogClient {
    client: Client,
    config: DatadogClientConfig,
    concurrency_semaphore: Arc<Semaphore>,
}

impl DatadogClient {
    pub fn new(config: DatadogClientConfig) -> Result<Self> {
        let client = ClientBuilder::new()
            .timeout(config.request_timeout)
            .build()
            .context("Failed to build HTTP client")?;

        let concurrency_semaphore = Arc::new(Semaphore::new(config.max_concurrent_requests));

        Ok(Self {
            client,
            config,
            concurrency_semaphore,
        })
    }

    pub async fn export_spans(&self, spans: &[DatadogSpan]) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let _permit = self
            .concurrency_semaphore
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("Failed to acquire semaphore permit"))?;

        let retry_strategy =
            ExponentialBackoff::from_millis(self.config.retry_initial_delay.as_millis() as u64)
                .factor(2)
                .take(self.config.max_retries as usize);

        let payload = SpanPayload {
            data: spans.to_vec(),
        };

        Retry::spawn(retry_strategy, || async { self.send_spans(&payload).await }).await
    }

    async fn send_spans(&self, payload: &SpanPayload) -> Result<()> {
        let url = format!(
            "{}/api/v2/spans",
            self.config.api_endpoint.trim_end_matches('/')
        );

        let mut request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json");

        if let Some(ref api_key) = self.config.api_key {
            request = request.header("DD-API-KEY", api_key);
        }

        if let Some(ref app_key) = self.config.application_key {
            request = request.header("DD-APPLICATION-KEY", app_key);
        }

        let response = request
            .json(payload)
            .send()
            .await
            .context("HTTP request failed")?;

        match response.status() {
            StatusCode::ACCEPTED | StatusCode::OK => {
                debug!("Successfully exported {} spans", payload.data.len());
                Ok(())
            }
            StatusCode::UNAUTHORIZED => {
                error!("Datadog API unauthorized - check API key");
                Err(anyhow::anyhow!("Datadog API unauthorized"))
            }
            StatusCode::TOO_MANY_REQUESTS => Err(anyhow::anyhow!("Rate limited")),
            StatusCode::SERVICE_UNAVAILABLE => Err(anyhow::anyhow!("Service unavailable")),
            status => {
                let body = response.text().await.unwrap_or_default();
                error!("Datadog API error {}: {}", status, body);
                Err(anyhow::anyhow!("API error: {}", status))
            }
        }
    }

    pub async fn health_check(&self) -> Result<bool> {
        let url = format!(
            "{}/api/v1/health",
            self.config.api_endpoint.trim_end_matches('/')
        );

        let mut request = self.client.get(&url);

        if let Some(ref api_key) = self.config.api_key {
            request = request.header("DD-API-KEY", api_key);
        }

        let response = request
            .send()
            .await
            .context("Health check request failed")?;

        Ok(response.status() == StatusCode::OK)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DatadogSpan {
    pub id: String,
    pub trace_id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub resource: String,
    pub service: String,
    #[serde(rename = "type")]
    pub span_type: String,
    pub start_ns: u64,
    pub end_ns: u64,
    pub duration_ns: u64,
    pub error: u32,
    pub meta: DatadogMeta,
    pub metrics: DatadogMetrics,
    #[serde(default)]
    pub child_span_ids: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DatadogMeta {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub span_kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub service: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub trace_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub span_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error_message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error_stack: String,
    #[serde(flatten)]
    pub additional: std::collections::HashMap<String, String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DatadogMetrics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_rate: Option<f64>,
    #[serde(flatten)]
    pub additional: std::collections::HashMap<String, f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpanPayload {
    pub data: Vec<DatadogSpan>,
}

impl From<&crate::state::BufferedSpan> for DatadogSpan {
    fn from(span: &crate::state::BufferedSpan) -> Self {
        let duration_ns = (span.duration_ms as u64) * 1_000_000;
        let start_ns = (span.timestamp_ms as u64) * 1_000_000;
        let end_ns = start_ns + duration_ns;

        let meta = DatadogMeta {
            span_kind: format!("{:?}", span.span_kind),
            service: span.service_name.clone(),
            trace_id: span.trace_id.clone(),
            span_id: span.span_id.clone(),
            parent_id: span.parent_span_id.clone(),
            error_message: if span.status_code == 2 {
                "Error status".to_string()
            } else {
                String::new()
            },
            error_type: if span.status_code == 2 {
                "error".to_string()
            } else {
                String::new()
            },
            ..Default::default()
        };

        Self {
            id: span.span_id.clone(),
            trace_id: span.trace_id.clone(),
            parent_id: span.parent_span_id.clone(),
            name: span.operation_name.clone(),
            resource: span.operation_name.clone(),
            service: span.service_name.clone(),
            span_type: "custom".to_string(),
            start_ns,
            end_ns,
            duration_ns,
            error: span.status_code as u32,
            meta,
            metrics: DatadogMetrics::default(),
            child_span_ids: vec![],
        }
    }
}

impl From<&crate::sampling::span_compression::CompressedSpan> for DatadogSpan {
    fn from(compressed: &crate::sampling::span_compression::CompressedSpan) -> Self {
        let duration_ns = (compressed.total_duration_ms as u64) * 1_000_000;
        let start_ns = (compressed.timestamp_ms as u64) * 1_000_000;
        let end_ns = (compressed.last_timestamp_ms as u64) * 1_000_000;

        let mut meta = DatadogMeta {
            service: compressed.service_name.clone(),
            trace_id: compressed.trace_id.clone(),
            span_id: compressed.span_id.clone(),
            parent_id: compressed.parent_span_id.clone(),
            ..Default::default()
        };

        // Add compression metadata as tags
        let mut additional = compressed.attributes.clone();
        additional.insert("compression.type".to_string(), "aggregated".to_string());
        additional.insert(
            "compression.count".to_string(),
            compressed.span_count.to_string(),
        );
        additional.insert(
            "compression.total_duration_ms".to_string(),
            compressed.total_duration_ms.to_string(),
        );
        additional.insert(
            "compression.mean_duration_ms".to_string(),
            format!("{:.2}", compressed.mean_duration_ms),
        );
        additional.insert(
            "compression.min_duration_ms".to_string(),
            compressed.min_duration_ms.to_string(),
        );
        additional.insert(
            "compression.max_duration_ms".to_string(),
            compressed.max_duration_ms.to_string(),
        );
        additional.insert(
            "compression.group_signature".to_string(),
            compressed.group_signature.clone(),
        );
        additional.insert("span.type".to_string(), "analytics".to_string());

        // Add error info
        if compressed.error_count > 0 {
            meta.error_message = format!(
                "{} errors out of {} requests",
                compressed.error_count, compressed.span_count
            );
            meta.error_type = "partial_failure".to_string();
        }

        meta.additional = additional;

        Self {
            id: compressed.span_id.clone(),
            trace_id: compressed.trace_id.clone(),
            parent_id: compressed.parent_span_id.clone(),
            name: format!("{} (aggregated)", compressed.operation_name),
            resource: compressed.group_signature.clone(),
            service: compressed.service_name.clone(),
            span_type: "analytics".to_string(),
            start_ns,
            end_ns,
            duration_ns,
            error: if compressed.error_count > 0 { 1 } else { 0 },
            meta,
            metrics: DatadogMetrics {
                sampling_rate: Some(1.0),
                additional: {
                    let mut m = std::collections::HashMap::new();
                    m.insert(
                        "compression.count".to_string(),
                        compressed.span_count as f64,
                    );
                    m.insert(
                        "compression.total_ms".to_string(),
                        compressed.total_duration_ms as f64,
                    );
                    m.insert(
                        "compression.mean_ms".to_string(),
                        compressed.mean_duration_ms,
                    );
                    m.insert(
                        "compression.errors".to_string(),
                        compressed.error_count as f64,
                    );
                    m
                },
            },
            child_span_ids: compressed.original_span_ids.clone(),
        }
    }
}
