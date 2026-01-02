# Developer Extension Guide

This guide covers how to extend the Tail Sampling Selector with new sampling policies, exporters, and span processing features.

## Table of Contents

1. [Adding New Sampling Policies](#adding-new-sampling-policies)
2. [Adding New Exporters](#adding-new-exporters)
3. [Extending Span Compression](#extending-span-compression)
4. [Adding New Configuration](#adding-new-configuration)
5. [Testing Extensions](#testing-extensions)

---

## Adding New Sampling Policies

### Overview

Sampling policies implement the `SamplingPolicy` trait. The system already has built-in policies for errors, latency, and cardinality.

### Step 1: Define the Policy Struct

Add your policy struct in `src/sampling/policies.rs`:

```rust
/// Custom sampling policy based on trace attributes
#[derive(Debug, Clone)]
pub struct AttributeSamplingPolicy {
    /// Attribute key to check
    pub attribute_key: String,
    /// Expected value (exact match)
    pub expected_value: String,
    /// Sample rate when matched
    pub sample_rate: f64,
}
```

### Step 2: Implement the SamplingPolicy Trait

```rust
impl SamplingPolicy for AttributeSamplingPolicy {
    fn name(&self) -> &str {
        "attribute_sampling"
    }

    fn evaluate(&self, trace: &TraceSummary) -> PolicyEvaluationResult {
        // Check if trace has the attribute with expected value
        // This is simplified - you'd need to access span attributes
        let has_match = trace.operations.iter().any(|op| {
            op.contains(&self.attribute_key)
        });

        if has_match {
            let should_sample = fastrand::f64() < self.sample_rate;
            PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: if should_sample {
                    SamplingDecision::Keep
                } else {
                    SamplingDecision::Drop
                },
                reason: format!(
                    "Attribute '{}' matched, sampled at rate {:.2}",
                    self.attribute_key, self.sample_rate
                ),
                matched_conditions: vec![format!("attribute.{}={}", self.attribute_key, self.expected_value)],
            }
        } else {
            PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: SamplingDecision::Drop,
                reason: "Attribute not found".to_string(),
                matched_conditions: vec![],
            }
        }
    }
}
```

### Step 3: Register in Evaluator

Modify `src/sampling/evaluator.rs` to include your policy:

```rust
// In EvaluatorConfig
pub struct EvaluatorConfig {
    pub worker_count: usize,
    // ... existing fields
    pub attribute_policy: Option<AttributeSamplingPolicy>,
}

// In EvaluationWorker::evaluate
if let Some(ref policy) = self.config.attribute_policy {
    let result = policy.evaluate(&summary);
    results.push(result);
}
```

### Using Configurable Policies

Alternatively, add your policy as a `PolicyCondition` type:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PolicyCondition {
    // ... existing variants
    /// Custom attribute condition
    Attribute {
        attribute_key: String,
        expected_value: String,
        sample_rate: f64,
    },
}
```

Then add the evaluation logic in `evaluate_condition()`.

### Configuration Example

```yaml
sampling:
  policies:
    - name: "attribute_sampling"
      enabled: true
      rate: 0.5
      conditions:
        - type: "attribute"
          attribute_key: "user_type"
          expected_value: "premium"
          sample_rate: 0.5
```

---

## Adding New Exporters

### Overview

Exporters implement a trait to send sampled traces to external systems. The current Datadog exporter demonstrates the pattern.

### Step 1: Define the Exporter Trait

Add in `src/exporters/mod.rs` (create if needed):

```rust
use anyhow::Result;
use crate::state::BufferedSpan;

pub trait SpanExporter: Send + Sync {
    /// Export spans to the target system
    async fn export(&self, spans: &[BufferedSpan]) -> Result<()>;

    /// Check exporter health
    async fn health_check(&self) -> Result<bool>;

    /// Shutdown the exporter gracefully
    async fn shutdown(&self) -> Result<()>;
}
```

### Step 2: Create Your Exporter

```rust
// src/exporters/otel.rs
use super::*;

pub struct OpenTelemetryExporter {
    endpoint: String,
    client: reqwest::Client,
}

impl OpenTelemetryExporter {
    pub fn new(endpoint: String) -> Result<Self> {
        let client = reqwest::Client::new();
        Ok(Self { endpoint, client })
    }
}

#[async_trait::async_trait]
impl SpanExporter for OpenTelemetryExporter {
    async fn export(&self, spans: &[BufferedSpan]) -> Result<()> {
        // Convert spans to OTLP format
        let payload = self.convert_to_otlp(spans);

        let response = self
            .client
            .post(&format!("{}/v1/traces", self.endpoint))
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            anyhow::bail!("OTLP export failed: {}", response.status())
        }
    }

    async fn health_check(&self) -> Result<bool> {
        let response = self
            .client
            .get(&format!("{}/v1/health", self.endpoint))
            .send()
            .await?;
        Ok(response.status() == reqwest::StatusCode::OK)
    }

    async fn shutdown(&self) -> Result<()> {
        // Flush any pending spans
        Ok(())
    }

    fn convert_to_otlp(&self, spans: &[BufferedSpan]) -> otlp::TraceExportRequest {
        // Implementation depends on otlp-rust crate
        // ...
        unimplemented!()
    }
}
```

### Step 3: Add Configuration

Update `src/config.rs`:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct ExporterConfig {
    pub enabled: bool,
    pub exporter_type: String,  // "datadog", "otlp", "jaeger", etc.
    // Type-specific config would be in variant structs
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ExporterConfigs {
    Datadog(DatadogConfig),
    Otlp(OtlpExporterConfig),
    Jaeger(JaegerExporterConfig),
}
```

### Step 4: Integrate with Evaluator

Modify `src/sampling/evaluator.rs`:

```rust
pub struct Evaluator {
    config: EvaluatorConfig,
    trace_buffer: Arc<TraceBuffer>,
    exporters: Vec<Arc<dyn SpanExporter>>,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
}

impl Evaluator {
    pub async fn export_traces(&self, traces: Vec<(String, Vec<BufferedSpan>)>) -> Result<()> {
        let mut all_spans = Vec::new();
        for (_, spans) in traces {
            all_spans.extend(spans);
        }

        // Export to all configured exporters in parallel
        let export_futures = self.exporters.iter().map(|exporter| {
            exporter.export(&all_spans)
        });

        let results: Vec<Result<()>> = futures::future::join_all(export_futures).await;

        // Check for any errors
        for result in results {
            result?;
        }

        Ok(())
    }
}
```

### Step 5: Update Main Application

In `src/main.rs`:

```rust
fn create_exporter(config: &ExporterConfigs) -> Arc<dyn SpanExporter> {
    match config {
        ExporterConfigs::Datadog(cfg) => Arc::new(DatadogClient::new(cfg.clone())?),
        ExporterConfigs::Otlp(cfg) => Arc::new(OpenTelemetryExporter::new(cfg.endpoint.clone())?),
    }
}
```

---

## Extending Span Compression

### Overview

The span compression feature groups repeated similar operations (SQL queries, HTTP calls, etc.) into single aggregated spans with statistics. This section covers how to extend it with custom grouping strategies, patterns, and statistics.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Span Compression Pipeline                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   BufferedSpan[]                                                │
│        │                                                         │
│        ▼                                                         │
│   ┌──────────────┐                                              │
│   │ SpanGrouper  │  ← Extracts group key per span               │
│   │              │    (trace_id + service + normalized_stmt)    │
│   └──────┬───────┘                                              │
│          │                                                       │
│          ▼                                                       │
│   ┌──────────────────┐                                          │
│   │ group_spans()    │  ← Groups spans by key                   │
│   └──────┬───────────┘                                          │
│          │                                                       │
│          ▼                                                       │
│   ┌────────────────────────┐                                    │
│   │ filter_compressible()  │  ← Filters by min_count & window   │
│   └──────┬─────────────────┘                                    │
│          │                                                       │
│          ▼                                                       │
│   ┌──────────────────┐     ┌─────────────────────┐              │
│   │ compress_groups()│────▶│ CompressedSpan[]    │              │
│   └──────────────────┘     │ (with statistics)   │              │
│                            └─────────────────────┘              │
│                                      +                           │
│                            ┌─────────────────────┐              │
│                            │ Uncompressed spans  │              │
│                            │ (didn't meet criteria)│            │
│                            └─────────────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

### Key Types

```rust
/// Grouping key for similar spans
pub struct SpanGroupKey {
    pub trace_id: String,
    pub service_name: String,
    pub operation_type: String,        // e.g., "db.postgresql.select"
    pub normalized_statement: String,  // e.g., "SELECT * FROM USERS WHERE ID = ?"
    pub parent_span_id: Option<String>,
}

/// Aggregated span representing multiple similar operations
pub struct CompressedSpan {
    pub trace_id: String,
    pub span_id: String,              // Generated unique ID
    pub timestamp_ms: i64,            // First span timestamp
    pub last_timestamp_ms: i64,       // Last span timestamp
    pub total_duration_ms: i64,       // Sum of all durations
    pub span_count: usize,            // Number of original spans
    pub error_count: usize,           // Spans with errors
    pub mean_duration_ms: f64,
    pub min_duration_ms: i64,
    pub max_duration_ms: i64,
    pub operation_name: String,
    pub service_name: String,
    pub parent_span_id: Option<String>,
    pub attributes: HashMap<String, String>,
    pub group_signature: String,      // Normalized statement
    pub original_span_ids: Vec<String>,
}
```

### Configuration Reference

```yaml
span_compression:
  # Master switch
  enabled: true
  
  # Minimum spans to trigger compression
  # Lower = more aggressive compression, Higher = preserve more detail
  min_compression_count: 3
  
  # Time window for grouping (seconds)
  # Spans outside this window won't be grouped together
  compression_window_secs: 60
  
  # Never compress spans longer than this (seconds)
  # Ensures long-running operations are always visible individually
  max_span_duration_secs: 60
  
  # Whitelist: only compress these operations (empty = all)
  compress_operations:
    - "postgresql"
    - "mysql"
    - "redis"
  
  # Blacklist: never compress these operations
  exclude_operations:
    - "authentication.verify"
    - "payment.process"
    - "order.create"
  
  # Custom SQL patterns for specialized grouping
  sql_patterns:
    - pattern: "SELECT .* FROM users WHERE"
      is_regex: true
      group_name: "users.lookup"
    
    - pattern: "INSERT INTO audit_log"
      is_regex: false
      group_name: "audit.write"
    
    - pattern: "UPDATE inventory SET quantity"
      is_regex: false
      group_name: "inventory.update"
```

### Example: Real-World SQL Compression

**Input: E-commerce checkout trace (47 spans)**

```
Trace: checkout-12345
├── HTTP POST /api/v1/checkout                    (250ms)
├── postgresql.query: SELECT * FROM users WHERE id = 789
├── postgresql.query: SELECT * FROM users WHERE id = 790  
├── postgresql.query: SELECT * FROM users WHERE id = 791
│   ... (15 more user lookups)
├── postgresql.query: SELECT * FROM products WHERE id = 101
├── postgresql.query: SELECT * FROM products WHERE id = 102
│   ... (8 more product lookups)
├── postgresql.query: SELECT * FROM inventory WHERE product_id = 101
├── postgresql.query: SELECT * FROM inventory WHERE product_id = 102
│   ... (8 more inventory checks)
├── redis.get: cart:user:789
├── redis.get: cart:user:790
│   ... (5 more cache lookups)
├── payment.process                                (1200ms) ← excluded
└── order.create                                   (45ms)   ← excluded
```

**Output: Compressed trace (8 spans)**

```
Trace: checkout-12345
├── HTTP POST /api/v1/checkout                    (250ms)
├── postgresql.query (aggregated): SELECT * FROM USERS WHERE ID = ?
│   └── count: 18, total: 54ms, mean: 3ms, min: 1ms, max: 8ms
├── postgresql.query (aggregated): SELECT * FROM PRODUCTS WHERE ID = ?
│   └── count: 10, total: 25ms, mean: 2.5ms, min: 1ms, max: 5ms
├── postgresql.query (aggregated): SELECT * FROM INVENTORY WHERE PRODUCT_ID = ?
│   └── count: 10, total: 30ms, mean: 3ms, min: 2ms, max: 6ms
├── redis.get (aggregated): cart:user:*
│   └── count: 7, total: 7ms, mean: 1ms, min: 0ms, max: 2ms
├── payment.process                                (1200ms) ← preserved
└── order.create                                   (45ms)   ← preserved
```

**Result**: 47 spans → 8 spans (83% reduction)

### Adding Custom Grouping Keys

Modify `SpanGroupKey` in `src/sampling/span_compression.rs`:

```rust
/// Extended grouping key with custom fields
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ExtendedSpanGroupKey {
    pub trace_id: String,
    pub service_name: String,
    pub operation_type: String,
    pub normalized_statement: String,
    pub parent_span_id: Option<String>,
    pub environment: String,          // NEW: Group by environment
    pub deployment_version: String,   // NEW: Group by version
}
```

### Adding Custom Pattern Matchers

Create a new pattern matcher for specialized operations:

```rust
/// Custom matcher for gRPC operations
pub struct GrpcPatternMatcher {
    /// Package name (e.g., "com.example")
    package: String,
    /// Service name
    service: String,
}

impl GrpcPatternMatcher {
    pub fn matches(&self, span: &BufferedSpan) -> bool {
        // Check if operation matches gRPC pattern: /package.Service/Method
        span.operation_name.starts_with(&format!("/{}.{}/", self.package, self.service))
    }
    
    pub fn normalize(&self, span: &BufferedSpan) -> String {
        // Extract method and normalize: /com.example.UserService/GetUser → GetUser
        span.operation_name
            .split('/')
            .last()
            .unwrap_or(&span.operation_name)
            .to_string()
    }
}

/// Custom matcher for HTTP endpoints with path parameters
pub struct HttpPatternMatcher;

impl HttpPatternMatcher {
    pub fn normalize(path: &str) -> String {
        // /api/v1/users/123/orders/456 → /api/v1/users/:id/orders/:id
        let re = regex::Regex::new(r"/\d+").unwrap();
        re.replace_all(path, "/:id").to_string()
    }
}
```

### Adding Compression Statistics (Percentiles)

Extend `CompressedSpan` with percentile calculations:

```rust
pub struct CompressedSpan {
    // ... existing fields
    
    // NEW: Percentile statistics
    pub p50_duration_ms: f64,
    pub p95_duration_ms: f64,
    pub p99_duration_ms: f64,
    pub stddev_duration_ms: f64,
}

impl CompressedSpan {
    pub fn from_spans(
        trace_id: String,
        span_id: String,
        parent_span_id: Option<String>,
        service_name: String,
        group_signature: String,
        spans: &[BufferedSpan],
    ) -> Self {
        // ... existing code
        
        // Calculate percentiles
        let mut durations: Vec<i64> = spans.iter().map(|s| s.duration_ms).collect();
        durations.sort();
        
        let len = durations.len();
        let p50 = if len > 0 { durations[len / 2] } else { 0 };
        let p95 = if len > 0 { durations[(len * 95) / 100] } else { 0 };
        let p99 = if len > 0 { durations[(len * 99) / 100] } else { 0 };
        
        // Calculate standard deviation
        let mean = total_duration as f64 / len as f64;
        let variance = durations.iter()
            .map(|&d| (d as f64 - mean).powi(2))
            .sum::<f64>() / len as f64;
        let stddev = variance.sqrt();
        
        Self {
            // ... existing fields
            p50_duration_ms: p50 as f64,
            p95_duration_ms: p95 as f64,
            p99_duration_ms: p99 as f64,
            stddev_duration_ms: stddev,
        }
    }
}
```

### Extending Datadog Output

Add percentile metrics to the Datadog span conversion in `src/datadog/client.rs`:

```rust
impl From<&CompressedSpan> for DatadogSpan {
    fn from(compressed: &CompressedSpan) -> Self {
        // ... existing conversion code
        
        // Add percentile tags
        additional.insert("compression.p50_ms".to_string(), 
                          format!("{:.2}", compressed.p50_duration_ms));
        additional.insert("compression.p95_ms".to_string(), 
                          format!("{:.2}", compressed.p95_duration_ms));
        additional.insert("compression.p99_ms".to_string(), 
                          format!("{:.2}", compressed.p99_duration_ms));
        additional.insert("compression.stddev_ms".to_string(), 
                          format!("{:.2}", compressed.stddev_duration_ms));
        
        // ... rest of conversion
    }
}
```

### Testing Span Compression

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_sql_span(trace_id: &str, query: &str, duration_ms: i64) -> BufferedSpan {
        let mut attributes = HashMap::new();
        attributes.insert("db.system".to_string(), "postgresql".to_string());
        attributes.insert("db.statement".to_string(), query.to_string());
        
        BufferedSpan {
            trace_id: trace_id.to_string(),
            span_id: uuid::Uuid::new_v4().to_string(),
            parent_span_id: None,
            timestamp_ms: 1000,
            duration_ms,
            status_code: 0,
            span_kind: SpanKind::Client,
            service_name: "order-service".to_string(),
            operation_name: "postgresql.query".to_string(),
            attributes,
        }
    }
    
    #[test]
    fn test_sql_queries_grouped_correctly() {
        let config = Arc::new(SpanCompressionConfig {
            enabled: true,
            min_compression_count: 2,
            compression_window_secs: 60,
            max_span_duration_secs: 60,
            compress_operations: vec![],
            exclude_operations: vec![],
            sql_patterns: vec![],
        });
        
        let grouper = SpanGrouper::new(config);
        
        // Create similar SQL queries with different parameter values
        let spans = vec![
            create_sql_span("trace-1", "SELECT * FROM users WHERE id = 123", 5),
            create_sql_span("trace-1", "SELECT * FROM users WHERE id = 456", 3),
            create_sql_span("trace-1", "SELECT * FROM users WHERE id = 789", 4),
            create_sql_span("trace-1", "INSERT INTO logs VALUES ('test')", 2),
        ];
        
        let (compressed, uncompressed) = process_spans_for_compression(&spans, &grouper);
        
        // SELECT queries should be compressed (3 spans → 1)
        assert_eq!(compressed.len(), 1);
        assert_eq!(compressed[0].span_count, 3);
        assert_eq!(compressed[0].total_duration_ms, 12); // 5 + 3 + 4
        
        // INSERT query should remain uncompressed (only 1 span)
        assert_eq!(uncompressed.len(), 1);
    }
    
    #[test]
    fn test_long_spans_never_compressed() {
        let config = Arc::new(SpanCompressionConfig {
            enabled: true,
            min_compression_count: 2,
            compression_window_secs: 60,
            max_span_duration_secs: 5, // 5 second limit
            compress_operations: vec![],
            exclude_operations: vec![],
            sql_patterns: vec![],
        });
        
        let grouper = SpanGrouper::new(config);
        
        let spans = vec![
            create_sql_span("trace-1", "SELECT * FROM users", 3000),  // 3s - under limit
            create_sql_span("trace-1", "SELECT * FROM users", 6000),  // 6s - OVER limit
            create_sql_span("trace-1", "SELECT * FROM users", 4000),  // 4s - under limit
        ];
        
        let (compressed, uncompressed) = process_spans_for_compression(&spans, &grouper);
        
        // Only 2 spans should be compressed (the ones under 5s)
        assert_eq!(compressed.len(), 1);
        assert_eq!(compressed[0].span_count, 2);
        
        // The 6s span should remain uncompressed
        assert_eq!(uncompressed.len(), 1);
        assert_eq!(uncompressed[0].duration_ms, 6000);
    }
    
    #[test]
    fn test_excluded_operations_never_compressed() {
        let config = Arc::new(SpanCompressionConfig {
            enabled: true,
            min_compression_count: 2,
            compression_window_secs: 60,
            max_span_duration_secs: 60,
            compress_operations: vec![],
            exclude_operations: vec!["payment.process".to_string()],
            sql_patterns: vec![],
        });
        
        let grouper = SpanGrouper::new(config);
        
        // Even with 5 identical payment spans, they should NOT be compressed
        let mut spans = Vec::new();
        for _ in 0..5 {
            let mut span = create_sql_span("trace-1", "charge card", 100);
            span.operation_name = "payment.process".to_string();
            spans.push(span);
        }
        
        let (compressed, uncompressed) = process_spans_for_compression(&spans, &grouper);
        
        // Nothing should be compressed
        assert_eq!(compressed.len(), 0);
        assert_eq!(uncompressed.len(), 5);
    }
}
```

---

## Adding New Configuration

### Step 1: Define Config Struct

In `src/config.rs`:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct CustomFeatureConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub custom_field: String,
}

fn default_enabled() -> bool { false }
fn default_batch_size() -> usize { 100 }
```

### Step 2: Add to Main Config

```rust
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
    #[serde(default)]  // NEW
    pub custom_feature: CustomFeatureConfig,  // NEW
}
```

### Step 3: Use in Application

```rust
fn main() -> Result<()> {
    let config = Config::load()?;

    if config.custom_feature.enabled {
        // Initialize custom feature
        let feature = CustomFeature::new(config.custom_feature.clone());
        // ...
    }

    Ok(())
}
```

### YAML Configuration Example

```yaml
custom_feature:
  enabled: true
  batch_size: 200
  custom_field: "custom_value"
```

### Environment Variable Override

Environment variables are automatically prefixed with `TSS_`:

```bash
TSS_CUSTOM_FEATURE_ENABLED=true \
TSS_CUSTOM_FEATURE_BATCH_SIZE=500 \
TSS_CUSTOM_FEATURE_CUSTOM_FIELD=production \
./target/release/tail-sampling-selector
```

---

## Testing Extensions

### Unit Tests

Add tests in the same file as your implementation:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_my_policy() {
        let policy = MyPolicy { /* config */ };

        let trace = TraceSummary {
            trace_id: "test".to_string(),
            // ... other required fields
            has_error: true,
            max_duration_ms: 1000,
            span_count: 10,
            operations: vec!["test_op".to_string()],
            service_name: "test-service".to_string(),
            min_timestamp_ms: 0,
            max_timestamp_ms: 1000,
            total_spans: 10,
            error_count: 1,
            root_span_id: Some("root".to_string()),
        };

        let result = policy.evaluate(&trace);
        assert_eq!(result.decision, SamplingDecision::Keep);
        assert!(!result.reason.is_empty());
    }
}
```

### Integration Tests

Add integration tests in `src/tests/mod.rs`:

```rust
#[tokio::test]
async fn test_exporter_integration() {
    // Set up test infrastructure
    let test_server = mock::MockServer::start().await;

    // Configure exporter with mock server
    let exporter = MyExporter::new(test_server.url());

    // Create test spans
    let spans = create_test_spans(10);

    // Export
    let result = exporter.export(&spans).await;
    assert!(result.is_ok());

    // Verify mock server received requests
    test_server.assert().await;
}
```

### Property-Based Tests

Use `proptest` for property-based testing:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_policy_rejects_empty_operations(ops in prop::collection::vec("[a-z]+", 0..10)) {
        let trace = TraceSummary {
            trace_id: "test".to_string(),
            operations: ops,
            // ... other required fields
            has_error: false,
            max_duration_ms: 100,
            span_count: ops.len(),
            service_name: "test".to_string(),
            min_timestamp_ms: 0,
            max_timestamp_ms: 100,
            total_spans: ops.len(),
            error_count: 0,
            root_span_id: None,
        };

        let policy = MyPolicy::default();
        let result = policy.evaluate(&trace);

        // Property: decision should always be valid
        matches!(result.decision, SamplingDecision::Keep | SamplingDecision::Drop);
    }
}
```

---

## Best Practices

### 1. Follow Existing Patterns

- Use `Arc<T>` for shared state
- Implement `Clone` for config structs
- Use `anyhow::Result` for error handling
- Add `#[serde(default)]` for optional fields

### 2. Performance Considerations

- Batch operations when possible
- Use async/await with Tokio
- Avoid locks; prefer channels or atomics
- Profile before optimizing

### 3. Error Handling

- Never use `.unwrap()` in production code
- Provide context with `.context()`
- Log errors with appropriate level

### 4. Documentation

- Document public APIs with doc comments
- Provide configuration examples
- Add migration notes for breaking changes

### 5. Backwards Compatibility

- Use additive changes for new features
- Deprecate old APIs before removing
- Version configuration schema changes
