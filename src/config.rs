// Configuration management for tail sampling selector
use anyhow::{Context, Result};
use config::builder::DefaultState;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

fn default_true() -> bool {
    true
}
fn default_false() -> bool {
    false
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub app: AppConfig,
    pub kafka: KafkaConfig,
    pub redis: RedisConfig,
    pub datadog: DatadogConfig,
    pub sampling: SamplingConfig,
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub span_compression: SpanCompressionConfig,
    #[serde(default)]
    pub force_sampling: ForceSamplingConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub oidc: OidcConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    #[serde(default = "default_instance_id")]
    pub instance_id: String,
    #[serde(default = "default_max_buffer_spans")]
    pub max_buffer_spans: usize,
    #[serde(default = "default_max_buffer_traces")]
    pub max_buffer_traces: usize,
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "default_evaluation_workers")]
    pub evaluation_workers: usize,
    #[serde(default = "default_true")]
    pub graceful_shutdown: bool,
    #[serde(default = "default_inactivity_timeout_secs")]
    pub inactivity_timeout_secs: u64,
    #[serde(default = "default_max_trace_duration_secs")]
    pub max_trace_duration_secs: u64,
}

fn default_instance_id() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}
const fn default_max_buffer_spans() -> usize {
    1_000_000
}
const fn default_max_buffer_traces() -> usize {
    100_000
}
const fn default_shutdown_timeout() -> u64 {
    30
}
const fn default_evaluation_workers() -> usize {
    4
}
const fn default_inactivity_timeout_secs() -> u64 {
    60
}
const fn default_max_trace_duration_secs() -> u64 {
    300
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    #[serde(default = "default_brokers")]
    pub brokers: String,
    #[serde(default = "default_input_topic")]
    pub input_topic: String,
    #[serde(default = "default_consumer_group")]
    pub consumer_group: String,
    #[serde(default = "default_consumer_threads")]
    pub consumer_threads: usize,
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,
    #[serde(default = "default_false")]
    pub enable_auto_commit: bool,
    #[serde(default = "default_session_timeout")]
    pub session_timeout_ms: u32,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub sasl_username: Option<String>,
    #[serde(default)]
    pub sasl_password: Option<String>,
    #[serde(default)]
    pub ssl_ca_path: Option<PathBuf>,
    #[serde(default)]
    pub ssl_cert_path: Option<PathBuf>,
    #[serde(default)]
    pub ssl_key_path: Option<PathBuf>,
}

fn default_brokers() -> String {
    "localhost:9092".to_string()
}
fn default_input_topic() -> String {
    "otel-traces-raw".to_string()
}
fn default_consumer_group() -> String {
    "tail-sampling-selector".to_string()
}
const fn default_consumer_threads() -> usize {
    1
}
fn default_auto_offset_reset() -> String {
    "latest".to_string()
}
const fn default_session_timeout() -> u32 {
    30000
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedisConfig {
    #[serde(default = "default_redis_url")]
    pub url: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default = "default_redis_db")]
    pub db: usize,
    #[serde(default = "default_redis_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_redis_timeout")]
    pub connection_timeout_secs: u64,
    #[serde(default = "default_false")]
    pub cluster_enabled: bool,
    #[serde(default)]
    pub cluster_nodes: Option<Vec<String>>,
    #[serde(default = "default_trace_ttl")]
    pub trace_ttl_secs: u64,
}

fn default_redis_url() -> String {
    "redis://127.0.0.1/".to_string()
}
const fn default_redis_db() -> usize {
    0
}
const fn default_redis_pool_size() -> usize {
    10
}
const fn default_redis_timeout() -> u64 {
    5
}
const fn default_trace_ttl() -> u64 {
    3600
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatadogConfig {
    #[serde(default = "default_datadog_endpoint")]
    pub api_endpoint: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub application_key: Option<String>,
    #[serde(default = "default_true")]
    pub batch_enabled: bool,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_secs: u64,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_delay")]
    pub retry_initial_delay_ms: u64,
}

fn default_datadog_endpoint() -> String {
    "https://api.datadoghq.com".to_string()
}
const fn default_batch_size() -> usize {
    100
}
const fn default_batch_timeout() -> u64 {
    5
}
const fn default_request_timeout() -> u64 {
    30
}
const fn default_max_retries() -> u32 {
    3
}
const fn default_retry_delay() -> u64 {
    1000
}

#[derive(Debug, Deserialize, Clone)]
pub struct SamplingConfig {
    #[serde(default = "default_true")]
    pub always_sample_errors: bool,
    #[serde(default = "default_error_sample_rate")]
    pub error_sample_rate: f64,
    #[serde(default = "default_true")]
    pub sample_latency: bool,
    #[serde(default = "default_latency_threshold")]
    pub latency_threshold_ms: u64,
    #[serde(default = "default_latency_rate")]
    pub latency_sample_rate: f64,
}

const fn default_error_sample_rate() -> f64 {
    1.0
}
const fn default_latency_threshold() -> u64 {
    30000
}
const fn default_latency_rate() -> f64 {
    1.0
}

#[derive(Debug, Deserialize, Clone)]
pub struct ObservabilityConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_true")]
    pub json_logging: bool,
    #[serde(default = "default_true")]
    pub enable_prometheus: bool,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
    #[serde(default = "default_true")]
    pub enable_health_check: bool,
    #[serde(default = "default_health_port")]
    pub health_port: u16,
    #[serde(default = "default_health_addr")]
    pub health_addr: String,
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

fn default_log_level() -> String {
    "info".to_string()
}
const fn default_metrics_port() -> u16 {
    9090
}
fn default_metrics_addr() -> String {
    "0.0.0.0".to_string()
}
const fn default_health_port() -> u16 {
    8080
}
fn default_health_addr() -> String {
    "0.0.0.0".to_string()
}
fn default_service_name() -> String {
    "tail-sampling-selector".to_string()
}

/// Configuration for span compression/aggregation
#[derive(Debug, Deserialize, Clone)]
pub struct SpanCompressionConfig {
    /// Enable span compression
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Minimum number of similar spans to trigger compression
    #[serde(default = "default_min_compression_count")]
    pub min_compression_count: usize,
    /// Maximum time window (in seconds) to group spans together
    #[serde(default = "default_compression_window_secs")]
    pub compression_window_secs: u64,
    /// Maximum duration (in seconds) for a span to be eligible for compression
    /// Spans exceeding this are never compressed and shown individually
    #[serde(default = "default_max_span_duration_secs")]
    pub max_span_duration_secs: u64,
    /// Operations/types to compress (empty = all)
    #[serde(default)]
    pub compress_operations: Vec<String>,
    /// Operations/types to exclude from compression
    #[serde(default)]
    pub exclude_operations: Vec<String>,
    /// SQL statement patterns to compress (for db.operation matching)
    #[serde(default)]
    pub sql_patterns: Vec<SqlPatternConfig>,
}

const fn default_min_compression_count() -> usize {
    3
}
const fn default_compression_window_secs() -> u64 {
    60
}
const fn default_max_span_duration_secs() -> u64 {
    60
} // 1 minute - spans over this are not compressed

/// SQL statement pattern for grouping similar queries
#[derive(Debug, Deserialize, Clone)]
pub struct SqlPatternConfig {
    /// Pattern to match (e.g., "SELECT * FROM users WHERE id = ?" or regex)
    pub pattern: String,
    /// Whether this is a regex pattern
    #[serde(default = "default_false")]
    pub is_regex: bool,
    /// Group name for this pattern (displayed in compressed span)
    pub group_name: Option<String>,
}

impl Default for SpanCompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_compression_count: 3,
            compression_window_secs: 60,
            max_span_duration_secs: 60,
            compress_operations: Vec::new(),
            exclude_operations: Vec::new(),
            sql_patterns: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ForceSamplingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_force_sampling_poll_interval")]
    pub poll_interval_secs: u64,
    /// Enable Redis pub/sub for instant rule propagation across instances
    #[serde(default = "default_true")]
    pub use_pubsub: bool,
    /// Redis pub/sub channel name for rule update notifications
    #[serde(default = "default_pubsub_channel")]
    pub pubsub_channel: String,
}

const fn default_force_sampling_poll_interval() -> u64 {
    5
}

fn default_pubsub_channel() -> String {
    "tss:force_rules:updates".to_string()
}

impl Default for ForceSamplingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 5,
            use_pubsub: true,
            pubsub_channel: "tss:force_rules:updates".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    #[serde(default = "default_storage_type")]
    pub storage_type: String,
    #[serde(default)]
    pub iceberg: IcebergStorageConfig,
}

fn default_storage_type() -> String {
    "memory".to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_type: "memory".to_string(),
            iceberg: IcebergStorageConfig::default(),
        }
    }
}

impl StorageConfig {
    pub fn iceberg_config(&self) -> Option<&IcebergStorageConfig> {
        if self.storage_type == "iceberg" && !self.iceberg.catalog_uri.is_empty() {
            Some(&self.iceberg)
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct IcebergStorageConfig {
    #[serde(default)]
    pub catalog_uri: String,
    #[serde(default)]
    pub warehouse: String,
    #[serde(default = "default_iceberg_namespace")]
    pub namespace: String,
    #[serde(default = "default_iceberg_table")]
    pub table_name: String,
    #[serde(default = "default_iceberg_inactivity_ms")]
    pub inactivity_threshold_ms: i64,
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub s3_access_key_id: Option<String>,
    #[serde(default)]
    pub s3_secret_access_key: Option<String>,
    #[serde(default = "default_s3_region")]
    pub s3_region: String,
    #[serde(default = "default_true")]
    pub s3_path_style: bool,
    /// Project ID for Lakekeeper REST catalog (used in x-project-id header)
    #[serde(default)]
    pub project_id: Option<String>,
}

impl Default for IcebergStorageConfig {
    fn default() -> Self {
        Self {
            catalog_uri: String::new(),
            warehouse: String::new(),
            namespace: default_iceberg_namespace(),
            table_name: default_iceberg_table(),
            inactivity_threshold_ms: default_iceberg_inactivity_ms(),
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_region: default_s3_region(),
            s3_path_style: true,
            project_id: None,
        }
    }
}

fn default_iceberg_namespace() -> String {
    "traces".to_string()
}

fn default_iceberg_table() -> String {
    "spans".to_string()
}

const fn default_iceberg_inactivity_ms() -> i64 {
    60_000
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct OidcConfig {
    #[serde(default = "default_false")]
    pub enabled: bool,
    #[serde(default)]
    pub issuer_url: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub required_roles: Vec<String>,
    #[serde(default = "default_roles_claim")]
    pub roles_claim: String,
    #[serde(default = "default_jwks_cache_secs")]
    pub jwks_cache_secs: u64,
}

fn default_roles_claim() -> String {
    "roles".to_string()
}

const fn default_jwks_cache_secs() -> u64 {
    300
}

impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            issuer_url: None,
            client_id: None,
            audience: None,
            required_scopes: vec!["openid".to_string(), "profile".to_string()],
            required_roles: Vec::new(),
            roles_claim: "roles".to_string(),
            jwks_cache_secs: 300,
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let environment =
            std::env::var("TSS_ENVIRONMENT").unwrap_or_else(|_| "default".to_string());

        config::ConfigBuilder::<DefaultState>::default()
            .add_source(
                config::File::with_name("config/default")
                    .required(false)
                    .format(config::FileFormat::Yaml),
            )
            .add_source(
                config::File::with_name(&format!("config/{}", environment))
                    .required(false)
                    .format(config::FileFormat::Yaml),
            )
            .add_source(
                config::Environment::with_prefix("TSS")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()
            .context("Failed to load configuration")
    }

    pub fn health_socket_addr(&self) -> SocketAddr {
        format!(
            "{}:{}",
            self.observability.health_addr, self.observability.health_port
        )
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 8080)))
    }

    pub fn metrics_socket_addr(&self) -> SocketAddr {
        format!(
            "{}:{}",
            self.observability.metrics_addr, self.observability.metrics_port
        )
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 9090)))
    }

    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.app.shutdown_timeout_secs)
    }

    pub fn inactivity_timeout(&self) -> Duration {
        Duration::from_secs(self.app.inactivity_timeout_secs)
    }

    pub fn max_trace_duration(&self) -> Duration {
        Duration::from_secs(self.app.max_trace_duration_secs)
    }
}
