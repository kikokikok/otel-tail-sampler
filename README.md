# Tail Sampling Selector

A production-ready, Rust-based tail sampling selector for distributed tracing pipelines. Consumes traces from Kafka, evaluates sampling decisions, and exports to Datadog.

## Features

- **High Performance**: Built on Tokio async runtime for maximum throughput
- **Scalable**: Partition-aware Kafka consumers with parallel evaluation workers
- **Configurable**: YAML-based configuration with environment variable overrides
- **Observable**: Prometheus metrics, health checks, and structured logging
- **Production Ready**: Graceful shutdown, backpressure, and circuit breakers

## Architecture

### Standard Mode (In-Memory)

```
┌─────────────────┐     ┌─────────────────────┐     ┌─────────────────┐
│ OpenTelemetry   │────▶│  Kafka              │────▶│  Tail Sampling  │
│ Agents          │     │  (otel-traces-raw)  │     │  Selector       │
└─────────────────┘     └─────────────────────┘     └────────┬────────┘
                                                             │
                           ┌─────────────────────────────────┘
                           │
                           ▼
                    ┌──────────────┐     ┌─────────────────┐
                    │  Redis       │────▶│  Datadog        │
                    │  (TTL State) │     │  (Export)       │
                    └──────────────┘     └─────────────────┘
```

### AutoMQ Table Topic Mode (Iceberg Storage)

For massive-scale deployments with long-running traces, AutoMQ Table Topic provides native Kafka→Iceberg streaming:

```
┌─────────────────┐     ┌─────────────────────┐     ┌─────────────────┐
│ OpenTelemetry   │────▶│  AutoMQ Kafka       │────▶│  Apache Iceberg │
│ Agents          │     │  (Table Topic)      │     │  (via AutoMQ)   │
└─────────────────┘     └─────────────────────┘     └────────┬────────┘
                                                             │
                        ┌────────────────────────────────────┘
                        │ Query (read-only)
                        ▼
                 ┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
                 │ Tail Sampling   │────▶│  Redis       │────▶│  Datadog    │
                 │ Selector        │     │  (TTL State) │     │  (Export)   │
                 └─────────────────┘     └──────────────┘     └─────────────┘
```

**Key benefits of AutoMQ Table Topic mode:**
- **Zero-code Kafka→Iceberg**: AutoMQ streams data to Iceberg natively (no application writes)
- **Horizontal scalability**: Iceberg enables query parallelism across partitions
- **Long-running traces**: Hours-long traces without memory pressure
- **Historical analysis**: Query historical traces directly from Iceberg

## Quick Start

### Prerequisites

- Rust 1.75+
- Kafka 2.8+ (or AutoMQ for Table Topic mode)
- Redis 7.0+
- (Optional) Lakekeeper + MinIO for Iceberg storage

### Development

```bash
# Clone the repository
git clone https://github.com/your-org/tail-sampling-selector.git
cd tail-sampling-selector

# Build
cargo build --release

# Run with Docker Compose
docker-compose up -d

# Or run directly
cargo run --release
```

### Configuration

Create a configuration file in `config/default.yaml`:

```yaml
kafka:
  brokers: "localhost:9092"
  input_topic: "otel-traces-raw"
  consumer_group: "tail-sampling-selector"

redis:
  url: "redis://127.0.0.1/"
  trace_ttl_secs: 30

datadog:
  api_endpoint: "https://api.datadoghq.com"
  api_key: "your-api-key"

sampling:
  sample_errors: true
  error_sample_rate: 1.0
  sample_latency: true
  latency_threshold_ms: 5000
```

Override with environment variables:

```bash
TSS_KAFKA_BROKERS=kafka:29092 \
TSS_REDIS_URL=redis://redis:6379 \
TSS_DATADOG_API_KEY=your-key \
./target/release/tail-sampling-selector
```

### Iceberg Storage with AutoMQ Table Topic

For large-scale deployments with long-running traces, enable Iceberg storage:

```yaml
storage:
  storage_type: "iceberg"
  iceberg:
    catalog_uri: "http://lakekeeper:8181/catalog"
    warehouse: "s3://iceberg-warehouse/traces/"
    namespace: "default"
    table_name: "otel_traces"
    s3_endpoint: "http://minio:9000"
    s3_access_key_id: "admin"
    s3_secret_access_key: "password"
    s3_region: "us-east-1"
    s3_path_style: true
```

AutoMQ Table Topic configuration (in docker-compose or Kafka broker config):

```bash
# Cluster-level settings
automq.table.topic.catalog.type=rest
automq.table.topic.catalog.uri=http://lakekeeper:8181/catalog
automq.table.topic.catalog.warehouse=s3://iceberg-warehouse/traces/

# Topic-level settings (via kafka-configs or topic creation)
automq.table.topic.enable=true
automq.table.topic.namespace=default
automq.table.topic.commit.interval.ms=60000
```

Build with Iceberg support:

```bash
cargo build --release --features iceberg-storage
```

## Health Checks

- `GET /health` - Full health check with component status
- `GET /health/ready` - Readiness probe
- `GET /health/live` - Liveness probe
- `GET /metrics` - Prometheus metrics

## Metrics

| Metric | Description |
|--------|-------------|
| `traces_ingested_total` | Total traces ingested from Kafka |
| `spans_ingested_total` | Total spans ingested from Kafka |
| `traces_sampled_total` | Traces selected for export |
| `spans_exported_total` | Spans exported to Datadog |
| `buffer_traces_current` | Traces in memory buffer |
| `kafka_consumer_lag` | Kafka consumer lag |

## Sampling Policies

Configure sampling criteria in `config/default.yaml`:

```yaml
sampling:
  # Always sample error traces
  sample_errors: true
  error_sample_rate: 1.0

  # Sample slow traces
  sample_latency: true
  latency_threshold_ms: 5000
  latency_sample_rate: 0.1

  # Sample high-cardinality traces
  sample_cardinality: true
  max_span_count: 500
```

## Dynamic Force Sampling

Force sampling allows runtime override of sampling decisions for debugging purposes. Rules are stored in Redis with automatic expiration and evaluated before regular policies.

### Configuration

```yaml
force_sampling:
  enabled: true
  poll_interval_secs: 5
  use_pubsub: true
  pubsub_channel: "tss:force_rules:updates"
```

**Pub/Sub for Instant Propagation**: When `use_pubsub` is enabled, rule changes are instantly propagated to all instances via Redis pub/sub (typically <100ms). Without pub/sub, instances rely on polling every `poll_interval_secs`.

### Admin API

#### Create a Force Sampling Rule

```bash
curl -X POST http://localhost:8080/admin/force-rules \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Debug Acme Corp latency issues",
    "duration_secs": 3600,
    "priority": 100,
    "match": {
      "resource": [
        {"key": "service.namespace", "op": "eq", "value": "acme-corp"}
      ],
      "span": [
        {"key": "http.route", "op": "regex", "value": "/api/v2/.*"}
      ]
    },
    "action": "force_keep"
  }'
```

#### List All Rules

```bash
curl http://localhost:8080/admin/force-rules
```

#### Get a Specific Rule

```bash
curl http://localhost:8080/admin/force-rules/{rule_id}
```

#### Extend Rule TTL

```bash
curl -X PUT http://localhost:8080/admin/force-rules/{rule_id}/extend \
  -H "Content-Type: application/json" \
  -d '{"additional_secs": 3600}'
```

#### Disable/Enable a Rule

```bash
curl -X PUT http://localhost:8080/admin/force-rules/{rule_id}/disable
curl -X PUT http://localhost:8080/admin/force-rules/{rule_id}/enable
```

#### Delete a Rule

```bash
curl -X DELETE http://localhost:8080/admin/force-rules/{rule_id}
```

#### Cleanup Expired Rules

```bash
curl -X POST http://localhost:8080/admin/force-rules/cleanup
```

### Match Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Exact match | `{"key": "service.name", "op": "eq", "value": "api-gateway"}` |
| `neq` | Not equal | `{"key": "env", "op": "neq", "value": "production"}` |
| `contains` | Substring match | `{"key": "http.url", "op": "contains", "value": "/debug"}` |
| `starts_with` | Prefix match | `{"key": "span.name", "op": "starts_with", "value": "GET /"}` |
| `ends_with` | Suffix match | `{"key": "db.statement", "op": "ends_with", "value": "users"}` |
| `regex` | Regex match | `{"key": "http.route", "op": "regex", "value": "/api/v[0-9]+/.*"}` |
| `in` | In list | `{"key": "tenant.id", "op": "in", "values": ["tenant-1", "tenant-2"]}` |
| `exists` | Attribute exists | `{"key": "error.message", "op": "exists"}` |
| `gt`, `gte`, `lt`, `lte` | Numeric comparison | `{"key": "http.status_code", "op": "gte", "value": "500"}` |

### Force Sampling Metrics

| Metric | Description |
|--------|-------------|
| `tail_sampling_traces_force_sampled_total` | Traces force-sampled by rules |
| `tail_sampling_traces_force_dropped_total` | Traces force-dropped by rules |
| `tail_sampling_force_rule_matches_total` | Rule match count (by rule_id) |

## Deployment

### Docker

```bash
docker build -t tail-sampling-selector:latest .
docker run -p 8080:8080 -p 9090:9090 \
  -e TSS_KAFKA_BROKERS=kafka:29092 \
  -e TSS_REDIS_URL=redis://redis:6379 \
  tail-sampling-selector:latest
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tail-sampling-selector
spec:
  replicas: 3
  selector:
    matchLabels:
      app: tail-sampling-selector
  template:
    spec:
      containers:
      - name: tail-sampling-selector
        image: ghcr.io/your-org/tail-sampling-selector:latest
        ports:
        - containerPort: 8080
        - containerPort: 9090
        env:
        - name: TSS_KAFKA_BROKERS
          value: "kafka:29092"
        - name: TSS_REDIS_URL
          value: "redis://redis:6379"
```

## Performance

Expected throughput on modern hardware:
- 100K-500K spans/second
- <10ms p99 latency
- <1GB memory footprint

## Sizing Guidelines

### Memory Estimation

The primary memory consumer is the trace buffer. Each buffered span uses approximately **800-1200 bytes** depending on attribute count.

**Formula:**
```
Memory (MB) ≈ max_buffer_traces × avg_spans_per_trace × 1KB / 1024
```

**Examples:**
| Use Case | max_buffer_traces | avg_spans | Estimated Memory |
|----------|-------------------|-----------|------------------|
| Standard | 100,000 | 10 | ~1 GB |
| High volume | 500,000 | 10 | ~5 GB |
| Large traces | 10,000 | 1,000 | ~10 GB |
| Long-running | 50,000 | 500 | ~25 GB |

### Long-Running Traces (Multi-Hour)

For traces spanning hours (batch jobs, ETL pipelines, long transactions):

```yaml
app:
  # Increase from default 300s (5 min) to support hours-long traces
  max_trace_duration_secs: 14400  # 4 hours
  
  # Increase inactivity timeout - large gaps between spans are normal
  inactivity_timeout_secs: 300  # 5 minutes between spans
  
  # Reduce max traces if memory-constrained
  max_buffer_traces: 50000
  max_buffer_spans: 50000000  # 50M spans total
```

**Key Considerations:**

1. **Memory**: A trace with 20,000 spans uses ~20MB. With 100 such traces active, expect 2GB+ buffer usage.

2. **Redis TTL**: Set `trace_ttl_secs` higher than `max_trace_duration_secs`:
   ```yaml
   redis:
     trace_ttl_secs: 18000  # 5 hours > max_trace_duration
   ```

3. **Span Compression**: Enable for SQL-heavy traces to reduce memory:
   ```yaml
   span_compression:
     enabled: true
     min_compression_count: 3
   ```

4. **Force Completion**: Traces exceeding `max_trace_duration_secs` are force-evaluated and exported regardless of completion status.

### Worker Sizing

Based on simulation results with 10K spans/sec:

| Workers | Throughput | Utilization | Recommendation |
|---------|------------|-------------|----------------|
| 2 | 10K/sec | ~80% | Minimum viable |
| 4 | 10K/sec | ~40% | Recommended |
| 6 | 10K/sec | ~25% | High availability |
| 8 | 20K+/sec | ~20% | High volume |

**Configuration:**
```yaml
app:
  evaluation_workers: 4  # Adjust based on span rate
```

### Kubernetes Resource Recommendations

**Standard workload (10K spans/sec):**
```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "2000m"
```

**High volume (100K+ spans/sec):**
```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "2000m"
  limits:
    memory: "8Gi"
    cpu: "4000m"
```

**Long-running traces:**
```yaml
resources:
  requests:
    memory: "8Gi"
    cpu: "1000m"
  limits:
    memory: "16Gi"
    cpu: "2000m"
```

### Running the Demo

A complete demo environment is available:

```bash
# Start all services
docker compose up -d

# Run interactive demo
./scripts/demo.sh

# Generate test traces
cargo run --release --bin simple_producer

# Run load simulation
cargo run --release --bin load_simulation
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `cargo test --all-features`
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
