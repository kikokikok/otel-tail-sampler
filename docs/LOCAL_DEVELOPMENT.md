# Local Development Setup

This guide covers setting up the full Iceberg-based stack locally for development.

## Storage Modes

The application supports two storage modes:

| Mode | Description | Use Case |
|------|-------------|----------|
| `memory` | In-memory trace buffer | Development, small scale |
| `iceberg` | Query Iceberg via Lakekeeper | Production, long-running traces |

**Important**: In Iceberg mode, the application is **read-only**. AutoMQ Table Topic handles all Kafka→Iceberg writes automatically.

## Stack Components

| Component | Image | Port | Purpose |
|-----------|-------|------|---------|
| MinIO | `minio/minio:latest` | 9000, 9001 | S3-compatible object storage |
| PostgreSQL | `postgres:16-alpine` | 5432 | Lakekeeper metadata |
| Lakekeeper | `quay.io/lakekeeper/catalog:v0.10.4` | 8181 | Iceberg REST Catalog |
| AutoMQ | `automqinc/automq:latest` | 9092, 9093 | Kafka with Table Topic (writes to Iceberg) |
| Redis | `redis:7.2-alpine` | 6379 | Deduplication and force sampling rules |
| Prometheus | `prom/prometheus:v2.48.0` | 9091 | Metrics collection |

## Quick Start (Recommended)

The automated docker-compose init scripts may have shell escaping issues. **The recommended approach is manual setup:**

### Step 1: Start Infrastructure

```bash
docker compose up -d minio postgres redis prometheus
docker compose up -d minio-init lakekeeper-migrate
sleep 5
docker compose up -d lakekeeper
```

Wait for Lakekeeper to be healthy:
```bash
until curl -sf http://localhost:8181/health > /dev/null 2>&1; do
  echo "Waiting for Lakekeeper..."
  sleep 2
done
echo "Lakekeeper is ready"
```

### Step 2: Create Lakekeeper Project

```bash
PROJECT_RESP=$(curl -s -X POST http://localhost:8181/management/v1/project \
  -H "Content-Type: application/json" \
  -d '{"project-name": "default"}')

PROJECT_ID=$(echo "$PROJECT_RESP" | grep -o '"project-id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "Project ID: $PROJECT_ID"

# Save for later use
export PROJECT_ID
```

### Step 3: Create Warehouse

```bash
curl -s -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  -H "x-project-id: $PROJECT_ID" \
  -d '{
    "warehouse-name": "traces",
    "storage-profile": {
      "type": "s3",
      "bucket": "iceberg-warehouse",
      "region": "us-east-1",
      "path-style-access": true,
      "endpoint": "http://minio:9000",
      "sts-enabled": false,
      "key-prefix": "warehouse"
    },
    "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "aws-access-key-id": "minioadmin",
      "aws-secret-access-key": "minioadmin123"
    }
  }'
```

### Step 4: Create Namespace

```bash
curl -s -X POST "http://localhost:8181/catalog/v1/traces/namespaces" \
  -H "Content-Type: application/json" \
  -H "x-project-id: $PROJECT_ID" \
  -d '{"namespace": ["default"]}'
```

### Step 5: Create Spans Table

```bash
curl -s -X POST "http://localhost:8181/catalog/v1/traces/namespaces/default/tables" \
  -H "Content-Type: application/json" \
  -H "x-project-id: $PROJECT_ID" \
  -d '{
    "name": "spans",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "trace_id", "type": "string", "required": true},
        {"id": 2, "name": "span_id", "type": "string", "required": true},
        {"id": 3, "name": "parent_span_id", "type": "string", "required": false},
        {"id": 4, "name": "timestamp_ms", "type": "long", "required": true},
        {"id": 5, "name": "duration_ms", "type": "long", "required": true},
        {"id": 6, "name": "status_code", "type": "int", "required": true},
        {"id": 7, "name": "span_kind", "type": "int", "required": false},
        {"id": 8, "name": "service_name", "type": "string", "required": true},
        {"id": 9, "name": "operation_name", "type": "string", "required": true}
      ]
    },
    "partition-spec": {"fields": []},
    "write-order": {"order-id": 0, "fields": []},
    "properties": {}
  }'
```

### Step 6: Run the Application

```bash
export TSS__STORAGE__STORAGE_TYPE=iceberg
export TSS__STORAGE__ICEBERG__CATALOG_URI="http://localhost:8181/catalog"
export TSS__STORAGE__ICEBERG__WAREHOUSE="traces"
export TSS__STORAGE__ICEBERG__NAMESPACE="default"
export TSS__STORAGE__ICEBERG__TABLE_NAME="spans"
export TSS__STORAGE__ICEBERG__INACTIVITY_THRESHOLD_MS=60000
export TSS__STORAGE__ICEBERG__PROJECT_ID="$PROJECT_ID"
export TSS__KAFKA__BROKERS="localhost:9092"
export TSS__REDIS__URL="redis://localhost:6379"

cargo run --release
```

### Step 7: Start AutoMQ with Table Topic

AutoMQ Table Topic automatically streams Kafka messages to Iceberg. The configuration is in docker-compose.yml:

```bash
docker compose up -d kafka kafka-init
```

The kafka-init service creates the topic with Table Topic enabled:
- `automq.table.topic.enable=true` - Enables Iceberg streaming
- `automq.table.topic.namespace=default` - Iceberg namespace
- `automq.table.topic.commit.interval.ms=60000` - 1 minute commits

**Data flow**: OTEL → AutoMQ (Kafka) → Iceberg (automatic) → App (queries only)

## Running Demo Traces

### Generate Test Traces

```bash
# Start the simple producer to generate test traces
cargo run --release --bin simple_producer
```

### Generate Long-Running Traces

```bash
# Simulate batch job traces (30-50 minutes)
cargo run --release --bin simple_producer -- --long-running --duration-minutes 30
```

## Verification Commands

### Check Container Status

```bash
docker compose ps
```

### Verify Lakekeeper Health

```bash
curl -s http://localhost:8181/health | jq .
```

### List Warehouses

```bash
curl -s -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/management/v1/warehouse" | jq '.warehouses[].name'
```

### List Namespaces

```bash
curl -s -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/catalog/v1/traces/namespaces" | jq .
```

### List Tables

```bash
curl -s -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/catalog/v1/traces/namespaces/default/tables" | jq .
```

### Check Catalog Config

```bash
curl -s -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/catalog/v1/config?warehouse=traces" | jq .
```

### Verify MinIO Buckets

```bash
docker exec minio mc ls local/
```

### Check Application Health

```bash
curl -s http://localhost:8080/health | jq .
curl -s http://localhost:8080/health/ready
curl -s http://localhost:8080/health/live
```

### View Store Stats (Iceberg mode)

```bash
curl -s http://localhost:8080/admin/store/stats | jq .
```

## Environment Variables Reference

### Storage Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `TSS__STORAGE__STORAGE_TYPE` | Storage backend: `memory` or `iceberg` | `iceberg` |
| `TSS__STORAGE__ICEBERG__CATALOG_URI` | Lakekeeper REST API base URL | `http://localhost:8181/catalog` |
| `TSS__STORAGE__ICEBERG__WAREHOUSE` | Warehouse name (not path) | `traces` |
| `TSS__STORAGE__ICEBERG__NAMESPACE` | Iceberg namespace | `default` |
| `TSS__STORAGE__ICEBERG__TABLE_NAME` | Table name | `spans` |
| `TSS__STORAGE__ICEBERG__PROJECT_ID` | Lakekeeper project UUID | `019b7c79-58a3-7fb3-89bc-81c272faed96` |
| `TSS__STORAGE__ICEBERG__INACTIVITY_THRESHOLD_MS` | Trace inactivity before evaluation | `60000` |

### Kafka Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `TSS__KAFKA__BROKERS` | Kafka bootstrap servers | `localhost:9092` |
| `TSS__KAFKA__INPUT_TOPIC` | Topic to consume traces from | `otel-traces-raw` |
| `TSS__KAFKA__CONSUMER_GROUP` | Consumer group ID | `tail-sampling-selector` |

### Redis Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `TSS__REDIS__URL` | Redis connection URL | `redis://localhost:6379` |
| `TSS__REDIS__TRACE_TTL_SECS` | Trace metadata TTL | `30` |

## Lakekeeper API Reference

### Project Management

```bash
# Create project
curl -X POST http://localhost:8181/management/v1/project \
  -H "Content-Type: application/json" \
  -d '{"project-name": "myproject"}'

# List projects
curl http://localhost:8181/management/v1/project
```

### Warehouse Management

All warehouse/catalog APIs require `x-project-id` header.

```bash
# Create warehouse
curl -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  -H "x-project-id: $PROJECT_ID" \
  -d '{"warehouse-name": "...", "storage-profile": {...}, "storage-credential": {...}}'

# List warehouses
curl -H "x-project-id: $PROJECT_ID" \
  http://localhost:8181/management/v1/warehouse

# Delete warehouse
curl -X DELETE -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/management/v1/warehouse/$WAREHOUSE_ID"
```

### Catalog Operations

```bash
# Get catalog config (required before table operations)
curl -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/catalog/v1/config?warehouse=traces"

# Create namespace
curl -X POST "http://localhost:8181/catalog/v1/traces/namespaces" \
  -H "Content-Type: application/json" \
  -H "x-project-id: $PROJECT_ID" \
  -d '{"namespace": ["myns"]}'

# Create table
curl -X POST "http://localhost:8181/catalog/v1/traces/namespaces/default/tables" \
  -H "Content-Type: application/json" \
  -H "x-project-id: $PROJECT_ID" \
  -d '{"name": "mytable", "schema": {...}}'

# Load table metadata
curl -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/catalog/v1/traces/namespaces/default/tables/spans"
```

## iceberg-rust Configuration

The Rust app uses `iceberg-rust` with REST catalog. Key configuration properties:

```rust
let props = HashMap::from([
    ("uri".to_string(), "http://localhost:8181/catalog".to_string()),
    ("warehouse".to_string(), "traces".to_string()),
    ("header.x-project-id".to_string(), project_id.to_string()),
]);

let catalog = RestCatalogBuilder::default()
    .load("lakekeeper", props)
    .await?;
```

The `header.x-project-id` property adds the required header to all REST requests.

## Troubleshooting

### Lakekeeper Init Fails

Check logs:
```bash
docker logs lakekeeper-init
```

Common issues:
- Shell escaping: Ensure `$` not `$$` in scripts
- Network: Container might not be able to reach `lakekeeper:8181`
- Timing: Lakekeeper might not be healthy yet

Fix: Run manual setup steps above.

### "Warehouse not found" Error

iceberg-rust requires exact warehouse name matching:
```bash
# Correct
warehouse: "traces"

# Incorrect
warehouse: "s3://iceberg-warehouse/warehouse/"
```

### "Project not found" / 401 Error

Lakekeeper requires `x-project-id` header for all operations:
```bash
# Missing header - will fail
curl http://localhost:8181/catalog/v1/config?warehouse=traces

# With header - works
curl -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/catalog/v1/config?warehouse=traces"
```

### Table Schema Mismatch

If the app expects different columns than what exists:
```bash
# Drop and recreate table
curl -X DELETE -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8181/catalog/v1/traces/namespaces/default/tables/spans"

# Then recreate with correct schema (see Step 5)
```

### S3/MinIO Connection Issues

Check MinIO is accessible:
```bash
curl -s http://localhost:9000/minio/health/live
```

Verify bucket exists:
```bash
docker exec minio mc ls local/iceberg-warehouse
```

### AutoMQ Not Starting

AutoMQ depends on `lakekeeper-init` completing:
```bash
docker compose logs lakekeeper-init
```

If init failed, fix it first, then:
```bash
docker compose rm -f lakekeeper-init
docker compose up -d lakekeeper-init
docker compose up -d automq
```

## Reset Everything

```bash
docker compose down -v
docker compose up -d
```

This removes all volumes and starts fresh.

## Scripts

### Full Reset and Setup

```bash
#!/bin/bash
set -e

echo "Stopping and removing all containers..."
docker compose down -v

echo "Starting infrastructure..."
docker compose up -d minio postgres
sleep 5

echo "Running init containers..."
docker compose up -d minio-init lakekeeper-migrate
sleep 10

echo "Starting Lakekeeper..."
docker compose up -d lakekeeper
sleep 15

echo "Waiting for Lakekeeper health..."
until curl -sf http://localhost:8181/health > /dev/null; do
  echo "Waiting..."
  sleep 2
done

echo "Running Lakekeeper init..."
docker compose up -d lakekeeper-init
sleep 5

echo "Starting remaining services..."
docker compose up -d redis prometheus

echo "Done! Check status with: docker compose ps"
```

### Get Project ID

```bash
#!/bin/bash
PROJECT_ID=$(curl -s http://localhost:8181/management/v1/project | \
  grep -o '"project-id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "export PROJECT_ID=$PROJECT_ID"
```

### Test Iceberg Connection

```bash
#!/bin/bash
PROJECT_ID=${PROJECT_ID:-$(curl -s http://localhost:8181/management/v1/project | grep -o '"project-id":"[^"]*"' | head -1 | cut -d'"' -f4)}

echo "Project: $PROJECT_ID"
echo ""
echo "Warehouses:"
curl -s -H "x-project-id: $PROJECT_ID" http://localhost:8181/management/v1/warehouse | jq '.warehouses[].name'
echo ""
echo "Namespaces in 'traces':"
curl -s -H "x-project-id: $PROJECT_ID" http://localhost:8181/catalog/v1/traces/namespaces | jq '.namespaces[][0]'
echo ""
echo "Tables in 'default':"
curl -s -H "x-project-id: $PROJECT_ID" http://localhost:8181/catalog/v1/traces/namespaces/default/tables | jq '.identifiers[].name'
```
