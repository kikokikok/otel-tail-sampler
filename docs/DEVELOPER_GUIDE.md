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

The span compression feature can be extended with custom grouping strategies, new pattern types, and additional statistics.

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

Create a new pattern matcher:

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
        // Check if operation matches gRPC pattern
        span.operation_name.starts_with(&format("/{}/{}", self.package, self.service))
    }
}
```

### Adding Compression Statistics

Extend `CompressedSpan`:

```rust
pub struct CompressedSpan {
    // ... existing fields
    pub p50_duration_ms: f64,    // NEW: 50th percentile
    pub p95_duration_ms: f64,    // NEW: 95th percentile
    pub p99_duration_ms: f64,    // NEW: 99th percentile
}
```

Calculate in `from_spans()`:

```rust
impl CompressedSpan {
    pub fn from_spans(...) -> Self {
        // ... existing code

        // Calculate percentiles
        let mut durations: Vec<i64> = spans.iter().map(|s| s.duration_ms).collect();
        durations.sort();
        let p50 = durations[durations.len() / 2];
        let p95 = durations[(durations.len() * 95) / 100];
        let p99 = durations[(durations.len() * 99) / 100];

        Self {
            // ... existing fields
            p50_duration_ms: p50 as f64,
            p95_duration_ms: p95 as f64,
            p99_duration_ms: p99 as f64,
        }
    }
}
```

### Custom Datadog Output Format

The compressed span conversion in `src/datadog/client.rs` can be extended:

```rust
impl From<&CompressedSpan> for DatadogSpan {
    fn from(compressed: &CompressedSpan) -> Self {
        // ... existing code

        // Add percentile metrics
        if let Some(ref mut metrics) = self.metrics.additional {
            metrics.insert("compression.p50_ms".to_string(), compressed.p50_duration_ms);
            metrics.insert("compression.p95_ms".to_string(), compressed.p95_duration_ms);
            metrics.insert("compression.p99_ms".to_string(), compressed.p99_duration_ms);
        }

        self
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
