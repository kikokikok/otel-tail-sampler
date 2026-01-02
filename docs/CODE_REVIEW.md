# Code Review Guide

## Architecture Overview

Tail Sampling Selector is a Rust-based distributed tracing processor that consumes traces from Kafka, evaluates sampling decisions, and exports to Datadog.

### System Architecture

```
┌─────────────────┐     ┌─────────────────────┐     ┌─────────────────┐
│ OpenTelemetry   │────▶│  Kafka              │────▶│  Tail Sampling  │
│ Agents          │     │  (otel-traces-raw)  │     │  Selector       │
└─────────────────┘     └─────────────────────┘     └────────┬────────┘
                                                            │
                              ┌─────────────────────────────┘
                              │
                              ▼
                       ┌──────────────┐     ┌─────────────────┐
                       │  Redis       │────▶│  Datadog        │
                       │  (TTL State) │     │  (Export)       │
                       └──────────────┘     └─────────────────┘
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| Config | `src/config.rs` | YAML/Env configuration management |
| Kafka Consumer | `src/kafka/consumer.rs` | Partition-aware trace ingestion |
| Evaluator | `src/sampling/evaluator.rs` | Parallel sampling decision workers |
| Policies | `src/sampling/policies.rs` | Sampling policy definitions |
| Span Compression | `src/sampling/span_compression.rs` | Aggregates similar spans |
| Datadog Client | `src/datadog/client.rs` | APIv2 span export |
| State | `src/state.rs` | In-memory trace buffer with Redis fallback |

---

## Code Patterns and Conventions

### 1. Error Handling

All errors use `anyhow::Result` for ergonomic error handling:

```rust
// GOOD: Use context for debugging
Config::load()
    .context("Failed to load configuration")?
    .try_deserialize()
    .context("Configuration validation failed")?
```

```rust
// BAD: Generic errors without context
fn load() -> Result<Config> {
    Config::builder().build()?.try_deserialize().ok()?;
    Err(anyhow!("error"))
}
```

**Rule**: Always attach context using `.context()` or `.with_context()`.

### 2. Async Runtime

Uses **Tokio** with a multi-worker pattern:

```rust
// Spawn evaluation workers
pub fn start(&self) -> Vec<JoinHandle<Result<()>>> {
    let mut handles = Vec::new();
    for worker_id in 0..self.config.worker_count {
        let handle = spawn(async move {
            let mut worker = EvaluationWorker::new(...);
            worker.run(shutdown_rx).await
        });
        handles.push(handle);
    }
    handles
}
```

**Rule**: Use `tokio::spawn` for concurrent work; use `Arc<T>` for shared state.

### 3. Configuration

Uses the `config` crate with environment variable overrides:

```rust
// Loading order (first wins):
// 1. config/default.yaml
// 2. config/{TSS_ENVIRONMENT}.yaml
// 3. Environment variables (TSS_* prefix)
config::ConfigBuilder::<DefaultState>::default()
    .add_source(config::File::with_name("config/default").required(false))
    .add_source(config::Environment::with_prefix("TSS"))
    .build()?
    .try_deserialize()
```

**Rule**: Use `#[serde(default)]` for optional fields; provide `default_*()` functions for complex defaults.

### 4. Metrics and Observability

Uses `tracing` for structured logging and Prometheus metrics:

```rust
// GOOD: Structured logging with spans
info!(
    "Worker {}: Found {} closed traces for evaluation",
    self.worker_id,
    closed_trace_count
);

debug!(
    "Worker {}: Evaluated {} traces in {:?}ms",
    self.worker_id,
    closed_trace_count,
    duration.as_millis()
);
```

**Rule**: Use `info!` for production events, `debug!` for verbose, `error!` for failures.

---

## Span Compression Feature

### Overview

The span compression feature groups similar operations (SQL queries, API calls) into aggregated analytics spans.

### Key Design Decisions

1. **Never compress spans >1 minute** - Preserves individual slow query visibility
2. **SQL pattern matching** - Configurable regex/literal patterns for grouping
3. **Statistical aggregation** - Tracks min/max/mean/count/error_rate
4. **Preserves errors** - Spans with errors are kept individually if significant

### Configuration

```yaml
span_compression:
  enabled: true
  min_compression_count: 3          # Minimum spans to compress
  compression_window_secs: 60       # Time window for grouping
  max_span_duration_secs: 60        # Never compress spans >1min
  compress_operations: []           # Empty = all operations
  exclude_operations: []            # Operations to skip
  sql_patterns:                     # Custom grouping rules
    - pattern: "SELECT.*FROM users"
      is_regex: true
      group_name: "user_selects"
```

### CompressedSpan Structure

```rust
pub struct CompressedSpan {
    pub trace_id: String,
    pub span_id: String,
    pub timestamp_ms: i64,
    pub last_timestamp_ms: i64,
    pub total_duration_ms: i64,
    pub span_count: usize,
    pub error_count: usize,
    pub mean_duration_ms: f64,
    pub min_duration_ms: i64,
    pub max_duration_ms: i64,
    pub operation_name: String,
    pub service_name: String,
    pub attributes: HashMap<String, String>,
    pub group_signature: String,
    pub original_span_ids: Vec<String>,
}
```

### Compression Pipeline

```
Spans → Group by (trace_id, service, operation, normalized_stmt)
     → Filter groups with count >= min_compression_count
     → Filter groups within time window
     → Create CompressedSpan with aggregated stats
     → Export to Datadog with span.type: analytics
```

---

## Trade-offs and Design Decisions

### 1. In-Memory Buffer + Redis Fallback

**Decision**: Keep most recent traces in memory; use Redis for cross-instance state.

**Rationale**: Kafka provides ordering guarantees within partitions. Single-consumer-per-partition pattern means most trace data stays local.

**Trade-off**: If pod restarts before evaluation, buffered traces are lost. Kafka consumer lag provides backpressure signal.

### 2. Evaluation Workers Polling Model

**Decision**: Workers poll for closed traces at fixed intervals.

**Rationale**: Simpler than event-driven; avoids lock contention on buffer.

**Trade-off**: Adds poll_interval latency (default: configurable). Workers wake up periodically even when idle.

### 3. No Async Streams for Kafka

**Decision**: Use rdkafka synchronous consumer with manual polling.

**Rationale**: Better control over backpressure; simpler error recovery.

**Trade-off**: Less idiomatic Rust async. Manual offset management required.

---

## Testing Patterns

### Unit Tests

Located in `src/**/mod.rs` as `#[cfg(test)]` modules:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_grouping() {
        let config = Arc::new(SpanCompressionConfig {
            enabled: true,
            min_compression_count: 2,
            // ...
        });
        let grouper = SpanGrouper::new(config);
        // assertions...
    }
}
```

**Rule**: Unit tests should be fast (<100ms), isolated, and test one behavior.

### Integration Tests

See `src/tests/mod.rs` for integration tests.

**Rule**: Integration tests should test component interactions and real middleware.

---

## Security Considerations

### 1. Secrets Management

Secrets passed via:
- Environment variables from Kubernetes secrets
- Mounted secret files

```yaml
env:
  - name: TSS_DATADOG_API_KEY
    valueFrom:
      secretKeyRef:
        name: datadog-secret
        key: api-key
        optional: false
```

**Rule**: Never commit secrets to version control; use secrets management.

### 2. Network Security

- TLS for Kafka (configure via SSL paths in config)
- TLS for Redis (connection string prefix `rediss://`)
- HTTPS for Datadog API

### 3. Container Security

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
```

**Rule**: Run containers as non-root with minimal capabilities.

---

## Performance Considerations

### 1. Batch Processing

Datadog exports are batched:

```rust
// Default: 100 spans per batch, 5 second timeout
datadog:
  batch_enabled: true
  batch_size: 100
  batch_timeout_secs: 5
```

**Rule**: Tune batch_size based on payload limits (default: 3MB max).

### 2. Memory Limits

Default resource limits:
```yaml
resources:
  requests:
    cpu: 250m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 1Gi
```

**Rule**: Monitor `buffer_traces_current` metric; adjust `max_buffer_spans`.

### 3. Evaluation Workers

Default: 4 workers processing in parallel.

```yaml
app:
  evaluationWorkers: 4
```

**Rule**: Scale workers with CPU cores; avoid over-subscription.

---

## Common Review Checklist

- [ ] Error context is descriptive and actionable
- [ ] Async code uses proper Tokio patterns
- [ ] Configuration has sensible defaults
- [ ] Metrics/logging provides observability
- [ ] No `.unwrap()`, `.expect()`, or `as any` type coercion
- [ ] Tests cover happy path and error cases
- [ ] Secrets handled via proper mechanisms
- [ ] Container security context configured
- [ ] Resource limits defined
- [ ] Health checks configured
