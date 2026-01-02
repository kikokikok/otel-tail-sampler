# Tail-Sampling Selector V2: Iceberg Architecture (AutoMQ Table Topic)

## Executive Summary

This document describes the production architecture for the tail-sampling selector using **AutoMQ Table Topic** for native Kafka→Iceberg streaming. The application is **read-only** from Iceberg - AutoMQ handles all data ingestion automatically.

**Key insight**: AutoMQ Table Topic eliminates the need for application-level Iceberg writes. Data flows automatically from Kafka to Iceberg, and our app simply queries for sampling decisions.

**Target Environment**: 
- **AutoMQ**: Kafka-compatible broker with native Table Topic support
- **Lakekeeper**: Apache Iceberg REST Catalog (Rust-based)
- **MinIO/Garage/S3**: Object storage for Iceberg data files
- **Production**: Databricks Unity Catalog (same Iceberg format)

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

## Solution: AutoMQ Table Topic + Iceberg

### Architecture Overview

```
┌──────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│ OTEL         │───▶│ AutoMQ Kafka        │───▶│ Iceberg Table    │
│ Collectors   │    │ (Table Topic)       │    │ on S3/MinIO      │
└──────────────┘    └─────────────────────┘    └────────┬─────────┘
                                                        │
                    ┌───────────────────────────────────┘
                    │ READ-ONLY: App queries Iceberg
                    │ (AutoMQ handles all writes)
                    │
          ┌────────▼─────────┐    ┌──────────────────┐
          │ Tail-Sampling    │───▶│ Lakekeeper       │
          │ Selector         │    │ (REST Catalog)   │
          │ (iceberg-rust)   │    └──────────────────┘
          └────────┬─────────┘
                   │
   ┌───────────────┴───────────────┐
   │                               │
   ▼                               ▼
┌──────────────┐          ┌──────────────┐
│ Redis        │          │ Datadog      │
│ (Dedup/TTL)  │          │ (Export)     │
└──────────────┘          └──────────────┘
```

**Critical difference from V1**: The application does NOT write to Iceberg. AutoMQ Table Topic streams Kafka messages directly to Iceberg tables at the broker level.

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
(Standard Kafka producer - no changes needed)
```

### Step 2: Materialize (AutoMQ → Iceberg) - AUTOMATIC
AutoMQ Table Topic **automatically** (no application code):
- Receives Kafka messages from topic
- Parses spans based on configured schema
- Writes to Iceberg table on S3
- Commits every `commit.interval.ms` (default: 60s)

**This is the key innovation**: Zero application code for Kafka→Iceberg streaming.

### Step 3: Query (Tail-Sampling Selector → Iceberg) - READ-ONLY
```sql
-- Find traces ready for sampling evaluation
-- Application queries Iceberg via REST catalog
SELECT 
    trace_id,
    MAX(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) as has_error,
    MAX(duration_ms) as max_duration,
    COUNT(*) as span_count,
    MIN(timestamp_ms) as first_seen,
    MAX(timestamp_ms) as last_seen
FROM otel_spans
WHERE timestamp_ms > NOW() - INTERVAL '1 hour'
GROUP BY trace_id
HAVING last_seen < NOW() - INTERVAL '5 minutes'  -- Inactive traces
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

### AutoMQ Table Topic Configuration (docker-compose.yml)

**Cluster-level settings** (in kafka service command):
```bash
--override automq.table.topic.catalog.type=rest
--override automq.table.topic.catalog.uri=http://lakekeeper:8181/catalog
--override automq.table.topic.catalog.warehouse=s3://iceberg-warehouse/traces/
```

**Topic-level settings** (in kafka-init service):
```bash
kafka-topics.sh --create --topic otel-traces-raw \
  --partitions 3 --replication-factor 1 \
  --config automq.table.topic.enable=true \
  --config automq.table.topic.namespace=default \
  --config automq.table.topic.convert.value.type=raw \
  --config automq.table.topic.transform.value.type=none \
  --config automq.table.topic.commit.interval.ms=60000
```

### Tail-Sampling Selector Configuration (config/default.yaml)

```yaml
# Storage backend selection
storage:
  storage_type: "iceberg"  # or "memory" for traditional mode
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

# Long-running trace support
app:
  inactivity_timeout_secs: 300   # 5 minutes between spans
  max_trace_duration_secs: 3600  # 1 hour max trace duration

# Redis deduplication (TTL > max_trace_duration)
redis:
  trace_ttl_secs: 7200  # 2 hours
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

## Implementation Status

### Phase 1: Infrastructure Setup - COMPLETE
- [x] docker-compose with AutoMQ + MinIO + Lakekeeper
- [x] AutoMQ Table Topic configuration (cluster + topic level)
- [x] Iceberg table creation via Lakekeeper REST catalog

### Phase 2: Query Layer - COMPLETE
- [x] `iceberg-rust` and `datafusion` dependencies
- [x] `IcebergTraceStore` implementation (read-only)
- [x] Sampling query with predicate pushdown
- [x] Trace metadata caching

### Phase 3: Read-Only Refactor - COMPLETE
- [x] Removed write logic from `IcebergTraceStore` (AutoMQ handles writes)
- [x] Updated `ingest()` to log warning (no-op for Iceberg mode)
- [x] Long-running trace support (1hr max duration, 5min inactivity)
- [x] Fixed max_trace_duration detection bug

### Phase 4: Testing & Documentation - IN PROGRESS
- [x] Memory mode continues to work unchanged
- [ ] Load testing with realistic trace volumes
- [ ] Production deployment guide
- [ ] Monitoring and alerting setup

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
