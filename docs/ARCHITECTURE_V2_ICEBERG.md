# Tail-Sampling Selector V2: Iceberg Architecture (Lakekeeper + Garage)

## Executive Summary

This document describes the next-generation architecture for the tail-sampling selector, replacing in-memory trace buffering with Iceberg-based storage via **Lakekeeper** (REST catalog) and **Garage** (S3-compatible storage). This approach reduces memory consumption by **50-250x** while enabling predicate pushdown for efficient sampling decisions.

**Target Environment**: 
- **Lakekeeper**: Apache Iceberg REST Catalog (Rust-based, running in k3s)
- **Garage**: S3-compatible object storage (running in k3s)
- **Production**: Databricks (same Iceberg format)

## Problem Statement

### Current Architecture Issues

```
Current Flow:
OTEL → Kafka → [Load ALL spans into memory] → Sampling Decision → Datadog

Memory Usage: ~25GB for 25M spans
Cost: High memory = high infrastructure cost
```

The current design loads every span into a `HashMap<trace_id, Vec<BufferedSpan>>` to answer 3 simple questions:
1. Does this trace have errors? (`status_code == 2`)
2. Is any span slow? (`duration_ms > threshold`)
3. How many spans?

**This is fundamentally inefficient** - we're storing ~800-1200 bytes per span when sampling decisions only need ~50 bytes of metadata.

## Solution: AutoMQ + Iceberg (Lakekeeper + Garage)

### New Architecture

```
┌──────────────┐    ┌─────────────────┐    ┌──────────────────┐
│ OTEL         │───▶│ AutoMQ          │───▶│ Iceberg Table    │
│ Collectors   │    │ (S3 WAL)        │    │ on Garage (S3)   │
└──────────────┘    └─────────────────┘    └────────┬─────────┘
                                                    │
                    ┌───────────────────────────────┘
                    │ Zero-ETL: AutoMQ Table Topic
                    │ auto-materializes to Iceberg
                    │
          ┌────────▼─────────┐    ┌──────────────────┐
          │ iceberg-rust     │───▶│ Lakekeeper       │
          │ + DataFusion     │    │ (REST Catalog)   │
          └────────┬─────────┘    └──────────────────┘
                   │
          ┌────────▼─────────┐
          │ Tail-Sampling    │  ← Memory: ~100MB vs 25GB
          │ Selector         │    Predicate pushdown!
          └────────┬─────────┘
                   │
   ┌───────────────┴───────────────┐
   │                               │
   ▼                               ▼
┌──────────────┐          ┌──────────────┐
│ Redis        │          │ Datadog      │
│ (Force Rules)│          │ (Export)     │
└──────────────┘          └──────────────┘
```

### Your K3s Infrastructure

```
┌─────────────────────────────────────────────────────────────┐
│                         K3s Cluster                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ Lakekeeper  │  │   Garage    │  │   Tail-Sampling     │ │
│  │ (Iceberg    │  │   (S3)      │  │   Selector          │ │
│  │  REST API)  │  │             │  │                     │ │
│  │ :8181       │  │ :3900       │  │ :8080               │ │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘ │
│         │                │                     │            │
│         └────────────────┼─────────────────────┘            │
│                          │                                  │
│                    ┌─────▼─────┐                           │
│                    │ PostgreSQL│  (Lakekeeper metadata)    │
│                    └───────────┘                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Production (Databricks):
┌─────────────────────────────────────────────────────────────┐
│  Same Iceberg table format → Unity Catalog + S3/ADLS       │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. AutoMQ (Replaces Kafka)
- **100% Kafka protocol compatible** - no client changes needed
- **S3-native storage** - brokers are stateless
- **Table Topics** - automatic Iceberg materialization (Zero-ETL)
- **Cost**: ~10x cheaper than traditional Kafka

#### 2. Iceberg Table Topic
- Spans are auto-materialized to Iceberg format on S3
- Columnar Parquet storage with min/max statistics
- Enables SQL queries with predicate pushdown

#### 3. DataFusion Query Engine
- Apache Arrow-based SQL engine
- Native Iceberg reader with predicate pushdown
- Reads only matching Parquet row groups

#### 4. Lightweight Sampling Selector
- Queries Iceberg for trace metadata only
- Memory footprint: ~100-500MB (vs 25GB)
- Lazy loading: full spans fetched only when exported

## Data Flow

### Step 1: Ingest (OTEL → AutoMQ)
```
OTEL Collector produces OTLP spans to AutoMQ topic: otel-traces-raw
```

### Step 2: Materialize (AutoMQ → Iceberg)
AutoMQ Table Topic automatically:
- Parses spans (Protobuf/Avro via Schema Registry)
- Writes to Iceberg table on S3
- Commits every `commit.interval.ms` (default: 60s)

### Step 3: Query (DataFusion → Iceberg)
```sql
-- Find traces ready for sampling evaluation
-- Only reads trace_id, status_code, duration_ms, timestamp columns
-- Parquet row group pruning based on timestamp

SELECT 
    trace_id,
    MAX(status_code = 2) as has_error,
    MAX(duration_ms) as max_duration,
    COUNT(*) as span_count,
    MIN(timestamp_ms) as first_seen,
    MAX(timestamp_ms) as last_seen
FROM otel_spans
WHERE timestamp_ms > NOW() - INTERVAL '5 minutes'
GROUP BY trace_id
HAVING last_seen < NOW() - INTERVAL '60 seconds'  -- Inactive traces
```

### Step 4: Sample Decision
```rust
// Sampling logic - same as before
if trace.has_error {
    return SampleDecision::Keep;
}
if trace.max_duration > latency_threshold_ms {
    return SampleDecision::Keep;
}
// ... other policies
```

### Step 5: Lazy Load & Export
```sql
-- Only for sampled traces (~1% of total)
-- Full span data fetched from Iceberg
SELECT * FROM otel_spans WHERE trace_id IN (sampled_trace_ids)
```

## Iceberg Schema Design

### Optimized for Sampling Queries

```sql
CREATE TABLE otel_spans (
    -- Primary identifiers (fixed-size binary for efficiency)
    trace_id        BINARY(16),
    span_id         BINARY(16),
    parent_span_id  BINARY(16),
    
    -- Sampling decision fields (hot path)
    status_code     INT,
    duration_ms     BIGINT,
    timestamp_ms    BIGINT,
    
    -- Metadata (warm path)
    service_name    STRING,
    operation_name  STRING,
    span_kind       INT,
    
    -- Full data (cold path - only read when exporting)
    attributes      MAP<STRING, STRING>,
    events          ARRAY<STRUCT<...>>,
    links           ARRAY<STRUCT<...>>,
    
    -- AutoMQ metadata
    _kafka_metadata STRUCT<partition: INT, offset: BIGINT, timestamp: BIGINT>
)
PARTITIONED BY (
    day(timestamp_ms),      -- Time-based partitioning
    bucket(trace_id, 16)    -- Distribute traces across files
)
```

### Column Ordering Strategy
- **Sampling columns first**: `trace_id`, `status_code`, `duration_ms`, `timestamp_ms`
- **Metadata second**: `service_name`, `operation_name`
- **Heavy payload last**: `attributes`, `events`, `links`

This ordering maximizes predicate pushdown effectiveness - Parquet readers can skip entire row groups without reading heavy columns.

## Memory Comparison

### Before (Current Architecture)
```
Component                   Memory
─────────────────────────────────────
TraceBuffer HashMap         ~20 GB
  - 100K traces
  - 250 spans/trace avg
  - 800 bytes/span
Metadata HashMap            ~2 GB
Redis connection pool       ~100 MB
Kafka consumer buffers      ~500 MB
─────────────────────────────────────
TOTAL                       ~23 GB
```

### After (Iceberg Architecture)
```
Component                   Memory
─────────────────────────────────────
DataFusion query cache      ~200 MB
Trace metadata cache        ~50 MB
  - Only sampled traces
Redis connection pool       ~100 MB
HTTP connection pool        ~50 MB
─────────────────────────────────────
TOTAL                       ~400 MB
```

**Reduction: 57x less memory**

## Configuration

### Lakekeeper Configuration (K3s)
```yaml
# Lakekeeper deployment environment
LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://lakekeeper:password@postgres:5432/lakekeeper
LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://lakekeeper:password@postgres:5432/lakekeeper
LAKEKEEPER__PG_ENCRYPTION_KEY: <your-32-byte-encryption-key>
LAKEKEEPER__BASE_URI: http://lakekeeper.observability.svc.cluster.local:8181

# S3 storage configuration for Garage
LAKEKEEPER__DEFAULT_WAREHOUSE_LOCATION: s3://iceberg-warehouse/
AWS_ENDPOINT_URL: http://garage.storage.svc.cluster.local:3900
AWS_ACCESS_KEY_ID: <garage-access-key>
AWS_SECRET_ACCESS_KEY: <garage-secret-key>
AWS_REGION: garage
AWS_S3_ALLOW_UNSAFE_RENAME: true
```

### AutoMQ Server Configuration
```properties
# S3 WAL mode (fully S3-native) - uses Garage
automq.wal.backend=s3

# Object storage (Garage)
s3.endpoint=http://garage.storage.svc.cluster.local:3900
s3.bucket=automq-data
s3.region=garage
s3.path.style.access=true
s3.access.key=<garage-access-key>
s3.secret.key=<garage-secret-key>

# Table Topic (Iceberg via Lakekeeper)
automq.table.topic.enable=true
automq.table.topic.catalog.type=rest
automq.table.topic.catalog.uri=http://lakekeeper.observability.svc.cluster.local:8181
automq.table.topic.catalog.warehouse=s3://iceberg-warehouse/
```

### Topic Configuration (otel-traces-raw)
```properties
# Enable Table Topic
automq.table.topic.enable=true
automq.table.topic.namespace=observability

# Schema handling
automq.table.topic.convert.value.type=by_schema_id
automq.table.topic.transform.value.type=flatten

# Commit interval (affects latency vs. S3 PUT costs)
automq.table.topic.commit.interval.ms=60000

# Partitioning for optimal query performance
automq.table.topic.partition.by=[day(timestamp_ms), bucket(trace_id, 16)]
```

### Tail-Sampling Selector Configuration
```yaml
# New iceberg-based storage
storage:
  type: iceberg
  catalog:
    type: rest
    uri: http://lakekeeper.observability.svc.cluster.local:8181
    # Warehouse path on Garage S3
    warehouse: s3://iceberg-warehouse/
  s3:
    endpoint: http://garage.storage.svc.cluster.local:3900
    region: garage
    path_style_access: true
    access_key_id: ${GARAGE_ACCESS_KEY}
    secret_access_key: ${GARAGE_SECRET_KEY}
  table:
    namespace: observability
    name: otel_spans

# Query settings
query:
  # How often to scan for ready traces
  scan_interval_secs: 10
  # Trace considered "complete" after this inactivity
  inactivity_threshold_secs: 60
  # Maximum trace age before force evaluation
  max_trace_age_secs: 3600
  # Batch size for sampling evaluation
  batch_size: 1000

# Sampling policies (unchanged)
sampling:
  sample_errors: true
  error_sample_rate: 1.0
  sample_latency: true
  latency_threshold_ms: 30000
```

### Databricks Production Configuration
```yaml
# Production: Use Unity Catalog instead of Lakekeeper
storage:
  type: iceberg
  catalog:
    type: unity
    workspace_url: https://your-workspace.databricks.com
    catalog_name: observability
  table:
    namespace: traces
    name: otel_spans
```

## Implementation Phases

### Phase 1: Infrastructure Setup (Week 1)
- [ ] Create docker-compose with AutoMQ + MinIO + Iceberg REST catalog
- [ ] Configure Table Topic for OTEL spans
- [ ] Verify Iceberg table creation and data flow

### Phase 2: Query Layer (Week 2)
- [ ] Add `datafusion` and `iceberg-rust` dependencies
- [ ] Implement `IcebergTraceStore` trait
- [ ] Build sampling query with predicate pushdown
- [ ] Add trace metadata caching

### Phase 3: Refactor Sampling Selector (Week 3)
- [ ] Replace `TraceBuffer` with `IcebergTraceStore`
- [ ] Implement lazy span loading for export
- [ ] Update evaluation workers for Iceberg queries
- [ ] Add metrics for query performance

### Phase 4: Testing & Optimization (Week 4)
- [ ] Load testing with realistic trace volumes
- [ ] Memory profiling comparison
- [ ] Query optimization (partition pruning, caching)
- [ ] Documentation and migration guide

## Dependencies to Add

```toml
[dependencies]
# Apache DataFusion for SQL queries
datafusion = "44"

# Iceberg Rust (Apache Iceberg implementation)
iceberg = "0.4"
iceberg-datafusion = "0.4"

# Object storage
object_store = { version = "0.11", features = ["aws"] }

# Arrow for columnar data
arrow = "54"
parquet = "54"
```

## Risks and Mitigations

### Risk 1: Query Latency
**Concern**: Iceberg queries might be slower than in-memory lookups.

**Mitigation**:
- Aggressive partition pruning on timestamp
- Metadata caching for hot traces
- Batch queries instead of per-trace queries
- Expected latency: <100ms for metadata queries

### Risk 2: S3 Costs
**Concern**: Many small S3 requests could be expensive.

**Mitigation**:
- AutoMQ batches writes to S3
- DataFusion uses columnar reads (minimal S3 GETs)
- Row group pruning reduces data scanned
- Expected: ~$0.01/million spans

### Risk 3: Data Freshness
**Concern**: Iceberg commit interval introduces latency.

**Mitigation**:
- Configure 60s commit interval (vs default 5min)
- Sampling evaluation happens after inactivity threshold anyway
- Acceptable for tail-sampling use case

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Memory usage | 25 GB | < 500 MB |
| Traces/second | 10K | 10K+ |
| Sampling latency p99 | 10ms | < 200ms |
| Infrastructure cost | $X/month | $X/10/month |

## References

- [AutoMQ Table Topic Documentation](https://www.automq.com/docs/automq/table-topic/overview)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [DataFusion Documentation](https://datafusion.apache.org/)
- [iceberg-rust](https://github.com/apache/iceberg-rust)
