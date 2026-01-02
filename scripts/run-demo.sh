#!/bin/bash
# End-to-End Demo Script for Tail-Based Distributed Tracing
# Requires: docker build -t tail-sampling-selector:linux -f Dockerfile . (completed)

set -e

echo "=== Tail-Based Distributed Tracing Demo ==="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Linux image exists
if ! docker images | grep -q "tail-sampling-selector.*linux"; then
    echo -e "${RED}ERROR: tail-sampling-selector:linux image not found${NC}"
    echo "Build it first with: docker build -t tail-sampling-selector:linux -f Dockerfile ."
    exit 1
fi

echo -e "${GREEN}✓ Docker image found${NC}"

# Check infrastructure
echo ""
echo "=== Checking Infrastructure ==="

for svc in minio lakekeeper redis kafka; do
    if docker ps | grep -q "$svc"; then
        echo -e "${GREEN}✓ $svc is running${NC}"
    else
        echo -e "${RED}✗ $svc is not running${NC}"
        echo "Start infrastructure with: docker compose up -d"
        exit 1
    fi
done

# Stop any existing test container
echo ""
echo "=== Starting Application ==="
docker rm -f tss 2>/dev/null || true

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
  -e TSS__FORCE_SAMPLING__ENABLED=true \
  -e RUST_LOG=info,tail_sampling_selector=debug \
  tail-sampling-selector:linux

echo "Waiting for app to start..."
sleep 5

# Check if app is running
if docker ps | grep -q "tss"; then
    echo -e "${GREEN}✓ Application started${NC}"
else
    echo -e "${RED}✗ Application failed to start${NC}"
    echo "Logs:"
    docker logs tss
    exit 1
fi

# Check health
echo ""
echo "=== Health Check ==="
if curl -s http://localhost:8081/health/live | grep -q "ok\|healthy"; then
    echo -e "${GREEN}✓ Health endpoint responding${NC}"
else
    echo -e "${YELLOW}⚠ Health check pending...${NC}"
fi

# Show initial logs
echo ""
echo "=== Application Logs (first 20 lines) ==="
docker logs tss 2>&1 | head -20

echo ""
echo "=== Demo Ready ==="
echo ""
echo "Next steps:"
echo "1. Send test traces:  ./target/debug/simple_producer"
echo "2. View logs:         docker logs -f tss"
echo "3. Check metrics:     curl -s http://localhost:9095/metrics | grep tail_sampling"
echo "4. Check health:      curl http://localhost:8081/health | jq ."
echo "5. List force rules:  curl http://localhost:8081/admin/force-rules | jq ."
echo ""
echo "To stop: docker rm -f tss"
