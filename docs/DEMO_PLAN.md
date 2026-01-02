# End-to-End Tail-Based Distributed Tracing Demo Plan

## Overview

This document outlines the complete plan to demonstrate the Tail-Based Distributed Tracing system end-to-end, from trace ingestion through sampling decisions to data export and query.

## System Architecture

```
┌──────────────────┐    ┌─────────────────┐    ┌──────────────────────┐
│ simple_producer  │───▶│ AutoMQ (Kafka)  │───▶│ Tail-Sampling        │
│ (Test Traces)    │    │ S3-backed       │    │ Selector             │
└──────────────────┘    └─────────────────┘    └──────────┬───────────┘
                                                          │
              ┌───────────────────────────────────────────┤
              │                                           │
              ▼                                           ▼
    ┌─────────────────┐                         ┌─────────────────┐
    │ Redis           │                         │ Iceberg Table   │
    │ - Force Rules   │                         │ on MinIO (S3)   │
    │ - Trace State   │                         │ via Lakekeeper  │
    └─────────────────┘                         └─────────────────┘
                                                          │
                                                          ▼
                                                ┌─────────────────┐
                                                │ DataFusion SQL  │
                                                │ Query Engine    │
                                                └─────────────────┘
```

## Current State (What's Working)

| Component | Status | Notes |
|-----------|--------|-------|
| MinIO (S3) | ✅ Running | Healthy inside Docker, port forwarding broken from host |
| Lakekeeper | ✅ Running | Warehouse "traces", namespace "default", table "spans" created |
| AutoMQ (Kafka) | ✅ Running | Marked unhealthy (slow healthcheck), but accepting messages |
| Redis | ✅ Running | Healthy |
| Prometheus | ✅ Running | Metrics collection |
| Dev Binary | ✅ Built | `--features iceberg-storage` enabled |
| simple_producer | ✅ Working | Sends test traces to Kafka (with 30s timeouts) |

## Current Blockers

### Blocker 1: Docker port forwarding broken
**Docker port forwarding to MinIO (port 9000) is broken on macOS**

- MinIO healthy inside Docker network
- Host cannot reach `localhost:9000`
- This breaks host-based testing of Iceberg writes

### Blocker 2: Binary architecture mismatch
**Existing Docker images contain macOS binaries, not Linux**

- `Dockerfile.local` copies host binary → macOS binary in Linux container
- Existing `tail-sampling-selector-tail-sampling-selector:latest` has macOS binary
- Error: `exec format error` when running

**Solutions**:
1. **Option A (Fast)**: Fix MinIO port forwarding and run app on host
2. **Option B (Slow)**: Build Linux binary inside Docker using multi-stage `Dockerfile` (~30+ min)
3. **Option C (Medium)**: Cross-compile using `cross` tool (~5-10 min first time)

---

## Demo Plan: Step-by-Step

### Phase 1: Fix Infrastructure

#### Option A: Fix MinIO Port Forwarding (Recommended)

The issue is macOS Docker Desktop port forwarding. Try these fixes:

**A.1: Restart Docker Desktop**
```bash
# On macOS, restart Docker Desktop from menu bar
# Or via CLI:
osascript -e 'quit app "Docker"' && sleep 5 && open -a Docker
```

**A.2: Verify MinIO accessible after restart**
```bash
curl -s http://localhost:9000/minio/health/live && echo "MinIO OK"
```

**A.3: If still broken, use Docker internal IP directly**
```bash
# Get MinIO container IP
MINIO_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' minio)
echo "MinIO IP: $MINIO_IP"
# Use this IP instead of localhost:9000
```

#### Option B: Build Linux Binary (if Option A fails)

**B.1: Use multi-stage Dockerfile (30+ minutes)**
```bash
docker build -t tail-sampling-selector:linux -f Dockerfile .
```

**B.2: Run with Linux image**
```bash
docker run -d --name tss --network tail-sampling-network \
  -p 8081:8080 \
  -e TSS__KAFKA__BROKERS=kafka:29092 \
  -e TSS__REDIS__URL=redis://redis:6379 \
  -e TSS__STORAGE__STORAGE_TYPE=iceberg \
  ... (other env vars) \
  tail-sampling-selector:linux
```

#### Option C: Host-based Testing (if MinIO port works)

If MinIO port forwarding is fixed:

```bash
# Run on host
TSS__STORAGE__STORAGE_TYPE=iceberg \
TSS__STORAGE__ICEBERG__CATALOG_URI=http://localhost:8181/catalog \
TSS__STORAGE__ICEBERG__WAREHOUSE=traces \
TSS__STORAGE__ICEBERG__NAMESPACE=default \
TSS__STORAGE__ICEBERG__TABLE_NAME=spans \
TSS__STORAGE__ICEBERG__PROJECT_ID=00000000-0000-0000-0000-000000000000 \
TSS__STORAGE__ICEBERG__S3_ENDPOINT=http://localhost:9000 \
TSS__STORAGE__ICEBERG__S3_ACCESS_KEY_ID=minioadmin \
TSS__STORAGE__ICEBERG__S3_SECRET_ACCESS_KEY=minioadmin123 \
TSS__KAFKA__BROKERS=127.0.0.1:9092 \
TSS__REDIS__URL=redis://localhost:6379 \
RUST_LOG=info,tail_sampling_selector=debug \
./target/debug/tail-sampling-selector
```

### Phase 1 Verification

Expected output (either option):
```
Starting Tail Sampling Selector v0.1.0
Initializing Iceberg storage backend
  Catalog URI: http://lakekeeper:8181/catalog
Connected to Iceberg catalog
DataFusion session initialized
Started 4 evaluation workers
Health/Admin API server listening on 0.0.0.0:8080
Consuming from Kafka topic: otel-traces-raw
```

---

### Phase 2: Test Trace Ingestion Pipeline

#### Step 2.1: Send test traces
```bash
# From host (Kafka port 9092 works)
./target/debug/simple_producer
```

Expected output:
```
=== Simple Trace Producer ===
Sending traces...
  [ERROR] Trace 0 -> partition 0, offset 0
  [OK] Trace 1 -> partition 0, offset 1
  ...
Done! Sent 10 traces (4 with errors)
```

#### Step 2.2: Verify traces consumed
```bash
docker logs tail-sampling-selector 2>&1 | grep -E "(ingested|ingest)"
```

Expected: Log entries showing spans ingested

#### Step 2.3: Wait for inactivity timeout (60s default)
The evaluator will process traces after 60s of inactivity.

```bash
# Watch for sampling decisions
docker logs -f tail-sampling-selector 2>&1 | grep -E "(sampled|evaluated|exported)"
```

---

### Phase 3: Verify Sampling Decisions

#### Step 3.1: Check Prometheus metrics
```bash
curl -s http://localhost:9090/metrics | grep -E "^tail_sampling"
```

Key metrics:
- `tail_sampling_traces_ingested_total` - Traces received
- `tail_sampling_traces_sampled_total` - Traces kept (errors, slow, etc.)
- `tail_sampling_traces_dropped_total` - Traces not sampled
- `tail_sampling_spans_exported_total` - Spans sent to Datadog/storage

#### Step 3.2: Verify error traces are sampled
The simple_producer sends traces where every 3rd trace has an error.
Error traces should always be sampled (config: `always_sample_errors: true`).

---

### Phase 4: Verify Iceberg Storage

#### Step 4.1: Check parquet files in MinIO
```bash
# Run mc client inside Docker network
docker run --rm --network tail-sampling-network --entrypoint sh minio/mc -c "
  mc alias set myminio http://minio:9000 minioadmin minioadmin123 &&
  mc ls -r myminio/iceberg-warehouse/
"
```

Expected: List of `.parquet` files and metadata files

#### Step 4.2: Check Iceberg table metadata
```bash
curl -s -H "x-project-id: 00000000-0000-0000-0000-000000000000" \
  http://localhost:8181/catalog/v1/traces/namespaces/default/tables/spans | jq .
```

---

### Phase 5: Test DataFusion SQL Queries

#### Step 5.1: Query via Admin API (if endpoint exists)
```bash
curl -X POST http://localhost:8080/admin/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT trace_id, status_code, duration_ms FROM spans LIMIT 10"}'
```

#### Step 5.2: Alternative - Add test query endpoint
If no query endpoint exists, we need to add one or use the test binary.

---

### Phase 6: Test Force Sampling Rules

#### Step 6.1: Create a force sampling rule
```bash
curl -X POST http://localhost:8080/admin/force-rules \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Force sample all demo-service traces",
    "duration_secs": 300,
    "priority": 100,
    "match": {
      "resource": [
        {"key": "service.name", "op": "eq", "value": "demo-service"}
      ]
    },
    "action": "force_keep"
  }'
```

#### Step 6.2: Verify rule created
```bash
curl http://localhost:8080/admin/force-rules | jq .
```

#### Step 6.3: Send more traces and verify force sampling
```bash
./target/debug/simple_producer

# Check metrics for force-sampled traces
curl -s http://localhost:9090/metrics | grep force_sampled
```

---

### Phase 7: Health & Observability Verification

#### Step 7.1: Health endpoints
```bash
# Liveness
curl http://localhost:8080/health/live

# Readiness
curl http://localhost:8080/health/ready

# Full health status
curl http://localhost:8080/health | jq .
```

#### Step 7.2: Prometheus targets
Open http://localhost:9091/targets to verify tail-sampling-selector is being scraped.

---

## Test Scenarios

### Scenario A: Error Trace Sampling
1. Send trace with `status_code = ERROR`
2. Verify trace is sampled (100% sample rate for errors)
3. Check `traces_sampled_total{reason="error"}` metric

### Scenario B: Slow Trace Sampling
1. Send trace with `duration_ms > latency_threshold_ms` (default 5000ms)
2. Verify trace is sampled based on latency policy
3. Check `traces_sampled_total{reason="latency"}` metric

### Scenario C: Normal Trace Dropping
1. Send trace with no errors, fast duration
2. Verify trace is NOT sampled (dropped)
3. Check `traces_dropped_total` metric

### Scenario D: Force Sampling Override
1. Create force rule for specific service
2. Send normal trace for that service
3. Verify trace is force-sampled despite no error/latency
4. Check `traces_force_sampled_total` metric

---

## Files Modified for Demo

| File | Change |
|------|--------|
| `Dockerfile.local` | Use debug binary instead of release |
| `src/bin/simple_producer.rs` | Increased timeouts for AutoMQ (30s) |
| `src/storage/iceberg.rs` | Added S3 credential fields |
| `src/storage/mod.rs` | Updated From impl for IcebergConfig |

---

## Known Issues & Workarounds

### Issue 1: AutoMQ healthcheck fails
**Symptom**: Kafka container marked "unhealthy"
**Cause**: CLI commands slow due to S3 backend
**Workaround**: Ignore healthcheck status, broker is functional
**Fix**: Increase healthcheck timeout or use TCP check

### Issue 2: MinIO port forwarding broken
**Symptom**: `curl localhost:9000` times out from host
**Cause**: macOS Docker Desktop networking issue (possibly other apps holding connections on port 9000 - Slack, Chrome detected)
**Workaround**: Run app inside Docker, use internal networking
**Alternative**: Restart Docker Desktop, or use Docker internal IP directly

### Issue 3: Slow initial Kafka producer
**Symptom**: First message times out
**Cause**: Metadata fetch slow with AutoMQ S3 backend
**Fix**: Increased `message.timeout.ms` to 30000 in simple_producer.rs

### Issue 4: Binary architecture mismatch (macOS → Linux)
**Symptom**: `exec format error` when running container
**Cause**: Dockerfile.local copies host binary (macOS/arm64) into Linux container
**Fix**: Use multi-stage Dockerfile to build Linux binary inside container
**Command**: `docker build -t tail-sampling-selector:linux -f Dockerfile .` (~30+ min)

---

## Success Criteria

- [ ] Application starts and connects to all dependencies
- [ ] Traces flow: Kafka → App → Iceberg storage
- [ ] Error traces are always sampled
- [ ] Normal traces are dropped (or sampled at configured rate)
- [ ] Force sampling rules work via Admin API
- [ ] Parquet files visible in MinIO
- [ ] DataFusion can query stored spans
- [ ] Prometheus metrics available
- [ ] Health endpoints respond correctly

---

## Commands Quick Reference

```bash
# Start everything (infrastructure only, app needs Linux binary)
docker compose up -d minio lakekeeper-postgres lakekeeper redis prometheus kafka

# Build Linux binary (one-time, ~30 min)
docker build -t tail-sampling-selector:linux -f Dockerfile .

# Run app with Linux binary
docker run -d --name tss --network tail-sampling-network \
  -p 8081:8080 -p 9095:9090 \
  -e TSS__KAFKA__BROKERS=kafka:29092 \
  -e TSS__REDIS__URL=redis://redis:6379 \
  -e TSS__STORAGE__STORAGE_TYPE=iceberg \
  -e TSS__STORAGE__ICEBERG__CATALOG_URI=http://lakekeeper:8181/catalog \
  -e TSS__STORAGE__ICEBERG__WAREHOUSE=traces \
  -e TSS__STORAGE__ICEBERG__NAMESPACE=default \
  -e TSS__STORAGE__ICEBERG__TABLE_NAME=spans \
  -e TSS__STORAGE__ICEBERG__PROJECT_ID=00000000-0000-0000-0000-000000000000 \
  -e TSS__STORAGE__ICEBERG__S3_ENDPOINT=http://minio:9000 \
  -e TSS__STORAGE__ICEBERG__S3_ACCESS_KEY_ID=minioadmin \
  -e TSS__STORAGE__ICEBERG__S3_SECRET_ACCESS_KEY=minioadmin123 \
  -e TSS__STORAGE__ICEBERG__S3_REGION=us-east-1 \
  -e TSS__STORAGE__ICEBERG__S3_PATH_STYLE=true \
  -e RUST_LOG=info,tail_sampling_selector=debug \
  tail-sampling-selector:linux

# View app logs
docker logs -f tss

# Send test traces (from host - Kafka port 9092 works)
./target/debug/simple_producer

# Check metrics (use port 9095 for containerized app)
curl -s http://localhost:9095/metrics | grep tail_sampling

# Check health
curl http://localhost:8081/health | jq .

# List force rules
curl http://localhost:8081/admin/force-rules | jq .

# Check MinIO files (from inside Docker network)
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin123 2>/dev/null
docker exec minio mc ls -r local/iceberg-warehouse/

# Stop and cleanup
docker rm -f tss
```

---

## Build Times Reference

| Build Type | Time | When to Use |
|------------|------|-------------|
| Debug (incremental) | ~10s | During development on host |
| Debug (clean) | ~2-3 min | After dependency changes |
| Docker Linux build | ~30+ min | Required for running in Docker |
| Release | ~30+ min | Final deployment only |

---

## Current Status

**Build in progress**: `docker build -t tail-sampling-selector:linux -f Dockerfile .`
- Log file: `/tmp/docker-build.log`
- Check progress: `tail -f /tmp/docker-build.log`

**Once build completes**, run the demo with:
```bash
docker run -d --name tss --network tail-sampling-network \
  -p 8081:8080 -p 9095:9090 \
  ... (see Commands Quick Reference above)
```
