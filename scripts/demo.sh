#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

HEALTH_URL="http://localhost:8080"
ADMIN_URL="http://localhost:8080/admin"
METRICS_URL="http://localhost:9090/metrics"
LAKEKEEPER_URL="http://localhost:8181"
MINIO_URL="http://localhost:9001"

print_header() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

wait_for_service() {
    local url=$1
    local name=$2
    local max_attempts=${3:-30}
    local attempt=0
    
    print_step "Waiting for $name to be ready..."
    while [ $attempt -lt $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            print_success "$name is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    print_error "$name failed to start after $max_attempts attempts"
    return 1
}

press_enter() {
    echo ""
    read -p "Press ENTER to continue..."
    echo ""
}

print_header "TAIL SAMPLING SELECTOR - ICEBERG ARCHITECTURE DEMO"

echo "This demo showcases the new Iceberg-based storage architecture:"
echo ""
echo "  Stack Components:"
echo "    • AutoMQ      - Kafka-compatible message queue with S3 storage"
echo "    • Lakekeeper  - Iceberg REST catalog (Rust-based)"
echo "    • MinIO       - S3-compatible object storage"
echo "    • Redis       - Force sampling rules + deduplication"
echo ""
echo "  Key Features:"
echo "    • ~57x memory reduction (spans in Iceberg, metadata in memory)"
echo "    • Lazy trace loading (only fetch spans when sampled)"
echo "    • Table Topics (zero-ETL from Kafka to Iceberg)"
echo "    • Dynamic force sampling rules via Admin API"
echo ""
echo "Prerequisites:"
echo "    • Docker & Docker Compose"
echo "    • curl, jq"
echo ""

press_enter

print_header "STEP 1: Start Iceberg Infrastructure"

print_step "Starting all services with docker compose..."
docker compose up -d

echo ""
print_info "Services starting (this may take 1-2 minutes)..."
echo ""

wait_for_service "http://localhost:9000/minio/health/live" "MinIO" 30
wait_for_service "$LAKEKEEPER_URL/health" "Lakekeeper" 60
wait_for_service "$HEALTH_URL/health/live" "Tail Sampling Selector" 60

print_success "All services started"
echo ""
docker compose ps

press_enter

print_header "STEP 2: Verify Iceberg Infrastructure"

print_step "Checking Lakekeeper catalog..."
NAMESPACES=$(curl -s "$LAKEKEEPER_URL/catalog/v1/observability/namespaces" 2>/dev/null || echo "{}")
echo "$NAMESPACES" | jq . 2>/dev/null || echo "$NAMESPACES"
echo ""

print_step "Checking MinIO buckets..."
echo "MinIO Console: $MINIO_URL (minioadmin/minioadmin123)"
curl -s "http://localhost:9000/minio/health/live" && print_success "MinIO healthy" || print_error "MinIO unhealthy"
echo ""

print_step "Checking AutoMQ topics..."
docker exec automq kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null || print_info "Topics will be created on first message"

press_enter

print_header "STEP 3: Health Checks"

print_step "Liveness probe..."
curl -s "$HEALTH_URL/health/live" | jq . 2>/dev/null || curl -s "$HEALTH_URL/health/live"
echo ""

print_step "Readiness probe..."
curl -s "$HEALTH_URL/health/ready" | jq . 2>/dev/null || curl -s "$HEALTH_URL/health/ready"
echo ""

print_step "Full health status..."
curl -s "$HEALTH_URL/health" | jq . 2>/dev/null || curl -s "$HEALTH_URL/health"

press_enter

print_header "STEP 4: Prometheus Metrics"

print_step "Key metrics from the selector..."
echo ""
curl -s "$METRICS_URL" 2>/dev/null | grep -E "^(traces_|spans_|buffer_|force_|iceberg_|storage_)" | head -30 || print_info "Metrics will appear after trace ingestion"

press_enter

print_header "STEP 5: Dynamic Force Sampling Rules"

print_step "Listing current rules (should be empty)..."
curl -s "$ADMIN_URL/force-rules" | jq . 2>/dev/null || curl -s "$ADMIN_URL/force-rules"
echo ""

print_step "Creating a force sampling rule..."
echo ""
echo "Rule: Force sample all traces from 'payment-service' for 1 hour"
echo ""

RULE_RESPONSE=$(curl -s -X POST "$ADMIN_URL/force-rules" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Debug payment service latency issues",
    "duration_secs": 3600,
    "priority": 100,
    "match": {
      "resource": [
        {"key": "service.name", "op": "eq", "value": "payment-service"}
      ]
    },
    "action": "force_keep"
  }')

echo "$RULE_RESPONSE" | jq . 2>/dev/null || echo "$RULE_RESPONSE"
RULE_ID=$(echo "$RULE_RESPONSE" | jq -r '.id' 2>/dev/null || echo "")

if [ -n "$RULE_ID" ] && [ "$RULE_ID" != "null" ]; then
    print_success "Rule created with ID: $RULE_ID"
else
    print_info "Rule creation response received"
fi

press_enter

print_step "Creating a rule with regex matching..."
echo ""
echo "Rule: Force sample all API v2 endpoints returning 5xx errors"
echo ""

curl -s -X POST "$ADMIN_URL/force-rules" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Debug API v2 server errors",
    "duration_secs": 1800,
    "priority": 90,
    "match": {
      "span": [
        {"key": "http.route", "op": "regex", "value": "/api/v2/.*"},
        {"key": "http.status_code", "op": "gte", "value": "500"}
      ]
    },
    "action": "force_keep"
  }' | jq . 2>/dev/null || echo "Rule created"

press_enter

print_step "Creating a force-drop rule..."
echo ""
echo "Rule: Drop all health check traces to reduce noise"
echo ""

curl -s -X POST "$ADMIN_URL/force-rules" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Drop health check noise",
    "duration_secs": 86400,
    "priority": 200,
    "match": {
      "span": [
        {"key": "http.route", "op": "in", "values": ["/health", "/health/live", "/health/ready", "/healthz"]}
      ]
    },
    "action": "force_drop"
  }' | jq . 2>/dev/null || echo "Rule created"

press_enter

print_step "Listing all active rules..."
curl -s "$ADMIN_URL/force-rules" | jq . 2>/dev/null || curl -s "$ADMIN_URL/force-rules"

press_enter

print_header "STEP 6: Rule Management"

if [ -n "$RULE_ID" ] && [ "$RULE_ID" != "null" ]; then
    print_step "Getting rule details..."
    curl -s "$ADMIN_URL/force-rules/$RULE_ID" | jq . 2>/dev/null || echo "Rule details"
    echo ""

    print_step "Extending rule TTL by 30 minutes..."
    curl -s -X PUT "$ADMIN_URL/force-rules/$RULE_ID/extend" \
      -H "Content-Type: application/json" \
      -d '{"additional_secs": 1800}' | jq . 2>/dev/null || echo "Extended"
    echo ""

    print_step "Disabling the rule..."
    curl -s -X PUT "$ADMIN_URL/force-rules/$RULE_ID/disable" | jq . 2>/dev/null || echo "Disabled"
    echo ""

    print_step "Re-enabling the rule..."
    curl -s -X PUT "$ADMIN_URL/force-rules/$RULE_ID/enable" | jq . 2>/dev/null || echo "Enabled"
else
    print_info "Skipping rule management (no valid rule ID)"
fi

press_enter

print_header "STEP 7: Iceberg Table Inspection"

print_step "Checking Iceberg tables in Lakekeeper..."
TABLES=$(curl -s "$LAKEKEEPER_URL/catalog/v1/observability/namespaces/traces/tables" 2>/dev/null || echo "{}")
echo "$TABLES" | jq . 2>/dev/null || echo "$TABLES"

print_info "Tables are created automatically when AutoMQ Table Topics write data"
echo ""
print_info "Query options:"
echo "  DataFusion (in-app): Used for trace queries in the selector"
echo "  Spark (optional):    docker compose --profile spark up -d"
echo "                       docker exec -it spark spark-sql"
echo "                       > SELECT * FROM lakekeeper.traces.otel_spans LIMIT 10;"

press_enter

print_header "STEP 8: Storage Stats"

print_step "Getting storage statistics..."
STATS=$(curl -s "$HEALTH_URL/admin/stats" 2>/dev/null || echo "{}")
echo "$STATS" | jq . 2>/dev/null || echo "$STATS"

print_info "With Iceberg storage:"
echo "  • Spans stored in S3 (MinIO) via Iceberg tables"
echo "  • Only metadata (TraceSummary) kept in memory"
echo "  • Full spans fetched on-demand when trace is sampled"
echo "  • ~57x memory reduction vs in-memory storage"

press_enter

print_header "STEP 9: Cleanup"

print_step "Cleaning up expired rules..."
curl -s -X POST "$ADMIN_URL/force-rules/cleanup" | jq . 2>/dev/null || echo "Cleanup done"
echo ""

print_step "Deleting demo rules..."
curl -s "$ADMIN_URL/force-rules" 2>/dev/null | jq -r '.[].id' 2>/dev/null | while read id; do
    if [ -n "$id" ]; then
        curl -s -X DELETE "$ADMIN_URL/force-rules/$id" > /dev/null
        echo "  Deleted rule: $id"
    fi
done

press_enter

print_header "DEMO COMPLETE"

echo "Services are still running. Access points:"
echo ""
echo "  Tail Sampling Selector:"
echo "    Health API:     $HEALTH_URL/health"
echo "    Admin API:      $ADMIN_URL/force-rules"
echo "    Metrics:        $METRICS_URL"
echo ""
echo "  Iceberg Infrastructure:"
echo "    Lakekeeper:     $LAKEKEEPER_URL"
echo "    MinIO Console:  $MINIO_URL (minioadmin/minioadmin123)"
echo ""
echo "  Observability:"
echo "    Prometheus:     http://localhost:9091"
echo ""
echo "Optional profiles:"
echo "    Spark (ad-hoc):   docker compose --profile spark up -d"
echo "    Datadog OTEL:     DD_API_KEY=xxx docker compose --profile datadog up -d"
echo ""
echo "To stop all services:"
echo "    docker compose down"
echo ""
echo "To stop and remove volumes:"
echo "    docker compose down -v"
echo ""
print_success "Thanks for trying the Tail Sampling Selector with Iceberg!"
