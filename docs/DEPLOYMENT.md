# Deployment Guide

This guide covers deploying the Tail Sampling Selector to Kubernetes using Helm, with KEDA autoscaling and comprehensive monitoring.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Deployment](#quick-deployment)
3. [Helm Configuration](#helm-configuration)
4. [KEDA Autoscaling](#keda-autoscaling)
5. [Secret Management](#secret-management)
6. [Monitoring](#monitoring)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Tools

| Tool | Version | Purpose |
|------|---------|---------|
| kubectl | 1.20+ | Kubernetes CLI |
| helm | 3.8+ | Helm package manager |
| docker | 20.10+ | Container builder |

### Infrastructure Requirements

| Component | Version | Purpose |
|-----------|---------|---------|
| Kubernetes | 1.20+ | Container orchestration |
| Kafka | 2.8+ | Trace ingestion |
| Redis | 7.0+ | State caching |
| KEDA | 2.10+ | Autoscaling (optional) |

### Memory Requirements

| Component | CPU | Memory |
|-----------|-----|--------|
| Minimum per pod | 250m | 256Mi |
| Recommended | 500m | 512Mi |
| Maximum | 1000m | 1Gi |

---

## Quick Deployment

### 1. Build the Container

```bash
# Build the container image
docker build -t ghcr.io/your-org/tail-sampling-selector:latest .

# Push to your registry
docker push ghcr.io/your-org/tail-sampling-selector:latest
```

### 2. Install with Helm

```bash
# Add the Helm repository (if published)
helm repo add tail-sampling-selector https://your-org.github.io/tail-sampling-selector

# Or install from local chart
helm install tail-sampling-selector ./helm-charts/tail-sampling-selector \
  --namespace tail-sampling \
  --create-namespace \
  --set image.repository=ghcr.io/your-org/tail-sampling-selector \
  --set image.tag=latest \
  --set config.kafka.brokers=kafka:9092 \
  --set config.redis.url=redis://redis:6379 \
  --set config.datadog.apiKey=your-api-key
```

### 3. Verify Installation

```bash
# Check pod status
kubectl get pods -n tail-sampling

# View logs
kubectl logs -n tail-sampling -l app=tail-sampling-selector

# Check health
curl http://<service-ip>:8080/health
```

---

## Helm Configuration

### Minimal values.yaml

```yaml
# values.yaml
image:
  repository: ghcr.io/your-org/tail-sampling-selector
  tag: "v1.0.0"

replicaCount: 2

config:
  kafka:
    brokers: "kafka-0.kafka-headless:9092"
    inputTopic: "otel-traces-raw"
    consumerGroup: "tail-sampling-selector"

  redis:
    url: "redis://redis-master:6379"

  datadog:
    apiEndpoint: "https://api.datadoghq.com"
    apiKey: ""  # Set via secret

  sampling:
    alwaysSampleErrors: true
    errorSampleRate: 1.0

env:
  - name: TSS_DATADOG_API_KEY
    valueFrom:
      secretKeyRef:
        name: datadog-secret
        key: api-key
```

### Production values.yaml

```yaml
# production-values.yaml
image:
  repository: ghcr.io/your-org/tail-sampling-selector
  tag: "v1.2.0"
  pullPolicy: Always

replicaCount: 3

imagePullSecrets:
  - name: registry-secret

podSecurityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
  seccompProfile:
    type: RuntimeDefault

containerSecurityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop:
      - ALL

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 1000m
    memory: 1Gi

livenessProbe:
  httpGet:
    path: /health/live
    port: http
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health/ready
    port: http
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3

config:
  app:
    instanceId: ""
    maxBufferSpans: 1000000
    maxBufferTraces: 100000
    shutdownTimeoutSecs: 30
    evaluationWorkers: 4
    gracefulShutdown: true
    inactivityTimeoutSecs: 600
    maxTraceDurationSecs: 10800

  kafka:
    brokers: "kafka-0.kafka-headless:9092,kafka-1.kafka-headless:9092"
    inputTopic: "otel-traces-raw"
    consumerGroup: "tail-sampling-selector"
    autoOffsetReset: "latest"
    enableAutoCommit: true
    sessionTimeoutMs: 30000

  redis:
    url: "redis://redis-master:6379"
    password: ""
    poolSize: 10
    connectionTimeoutSecs: 5

  datadog:
    apiEndpoint: "https://api.datadoghq.com"
    apiKey: ""
    applicationKey: ""
    batchEnabled: true
    batchSize: 100
    batchTimeoutSecs: 5
    requestTimeoutSecs: 30
    maxRetries: 3
    retryInitialDelayMs: 1000

  sampling:
    alwaysSampleErrors: true
    errorSampleRate: 1.0
    sampleLatency: true
    latencyThresholdMs: 5000
    latencySampleRate: 0.1

  observability:
    logLevel: "info"
    jsonLogging: true
    enablePrometheus: true
    metricsPort: 9090
    metricsAddr: "0.0.0.0"
    enableHealthCheck: true
    healthPort: 8080
    healthAddr: "0.0.0.0"
    serviceName: "tail-sampling-selector"

  spanCompression:
    enabled: true
    minCompressionCount: 3
    compressionWindowSecs: 60
    maxSpanDurationSecs: 60
    compressOperations: []
    excludeOperations: []
    sqlPatterns: []

env:
  - name: TSS_DATADOG_API_KEY
    valueFrom:
      secretKeyRef:
        name: datadog-secret
        key: api-key
        optional: false

affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchLabels:
              app: tail-sampling-selector
          topologyKey: kubernetes.io/hostname

topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        app: tail-sampling-selector

keda:
  enabled: true
  scaledObjectName: ""
  pollingInterval: 15
  cooldownPeriod: 300
  minReplicaCount: 2
  maxReplicaCount: 10
  kafka:
    brokers: "kafka-0.kafka-headless:9092,kafka-1.kafka-headless:9092"
    consumerGroup: "keda-tail-sampling-scaler"
    topic: "otel-traces-raw"
    offsetReset: "latest"
    lagThreshold: "100"
    scope: "consumergroup"
```

### Installing with Production Values

```bash
helm upgrade --install tail-sampling-selector \
  ./helm-charts/tail-sampling-selector \
  --namespace tail-sampling \
  --create-namespace \
  --values production-values.yaml \
  --wait \
  --timeout 5m
```

---

## KEDA Autoscaling

### How KEDA Works with Tail Sampling Selector

KEDA monitors Kafka consumer lag and scales the deployment based on message backlog:

```
High Lag (> threshold) → Scale UP
Low Lag (< threshold) → Scale DOWN (after cooldown)
```

### KEDA Configuration

```yaml
keda:
  enabled: true
  pollingInterval: 15        # Check every 15 seconds
  cooldownPeriod: 300        # Wait 5 minutes before scaling down
  minReplicaCount: 1         # Minimum pods
  maxReplicaCount: 10        # Maximum pods
  kafka:
    brokers: "kafka:9092"
    consumerGroup: "keda-tail-sampling-scaler"
    topic: "otel-traces-raw"
    lagThreshold: "100"      # Scale up when lag > 100
    scope: "consumergroup"   # Per-consumer-group lag
```

### Tuning Autoscaling

| Parameter | Recommended | Description |
|-----------|-------------|-------------|
| `pollingInterval` | 15-30s | Balance responsiveness vs overhead |
| `cooldownPeriod` | 300-600s | Prevent flapping during low traffic |
| `lagThreshold` | 50-200 | Depends on your throughput |
| `minReplicaCount` | 1-2 | Ensure availability during scale-down |

### Verify KEDA Scaling

```bash
# Check ScaledObject status
kubectl get scaledobject -n tail-sampling

# View scaling events
kubectl describe scaledobject tail-sampling-selector -n tail-sampling

# Monitor replica changes
kubectl get pods -n tail-sampling -w
```

### Custom Scaling Metrics

For more advanced scaling, create a `KEDAScaler` with custom metrics:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: tail-sampling-selector
spec:
  scaleTargetRef:
    name: tail-sampling-selector
  pollingInterval: 15
  cooldownPeriod: 300
  minReplicaCount: 1
  maxReplicaCount: 10
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka:9092
        consumerGroup: keda-tail-sampling-scaler
        topic: otel-traces-raw
        lagThreshold: "100"
        offsetResetPolicy: latest
```

---

## Secret Management

### Creating Secrets

```bash
# Datadog API Key
kubectl create secret generic datadog-secret \
  --from-literal=api-key=YOUR_DATADOG_API_KEY \
  -n tail-sampling

# Kafka Credentials (if using SASL)
kubectl create secret generic kafka-credentials \
  --from-literal=username=your-username \
  --from-literal=password=your-password \
  -n tail-sampling

# TLS Certificates (if using SSL)
kubectl create secret generic kafka-certs \
  --from-file=ca.crt=kafka-ca.crt \
  --from-file=client.crt=client.crt \
  --from-file=client.key=client.key \
  -n tail-sampling
```

### Using Secrets in Values

```yaml
env:
  - name: TSS_DATADOG_API_KEY
    valueFrom:
      secretKeyRef:
        name: datadog-secret
        key: api-key
        optional: false

  - name: TSS_KAFKA_SASL_USERNAME
    valueFrom:
      secretKeyRef:
        name: kafka-credentials
        key: username

  - name: TSS_KAFKA_SASL_PASSWORD
    valueFrom:
      secretKeyRef:
        name: kafka-credentials
        key: password

secrets:
  - name: kafka-certs
    secretName: kafka-certs
    items:
      - key: ca.crt
        path: kafka-ca.crt
```

### External Secret Operator

For production, consider using an External Secrets Operator:

```yaml
# external-secret-datadog.yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: datadog-api-key
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: datadog-secret
    creationPolicy: Owner
  data:
    - secretKey: api-key
      remoteRef:
        key: secret/datadog
        property: api-key
```

---

## Monitoring

### Prometheus Metrics

The application exposes metrics on port 9090:

```yaml
service:
  type: ClusterIP
  port: 8080
  metricsPort: 9090
```

#### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `traces_ingested_total` | Total traces from Kafka | N/A |
| `spans_ingested_total` | Total spans from Kafka | N/A |
| `traces_sampled_total` | Traces exported | N/A |
| `spans_exported_total` | Spans exported to Datadog | N/A |
| `buffer_traces_current` | Traces in buffer | > 80% of limit |
| `kafka_consumer_lag` | Kafka consumer lag | > 1000 |
| `export_errors_total` | Export failures | > 0 |

### ServiceMonitor for Prometheus Operator

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: tail-sampling-selector
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: tail-sampling-selector
  endpoints:
    - port: metrics
      path: /metrics
      interval: 15s
```

### Grafana Dashboard

Create a dashboard with panels for:

1. **Throughput**: traces_ingested vs spans_exported
2. **Latency**: export latency percentiles
3. **Buffer**: buffer utilization percentage
4. **Kafka**: consumer lag over time
5. **Errors**: error rate by type

### Alert Rules

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: tail-sampling-selector-alerts
  namespace: monitoring
spec:
  groups:
    - name: tail-sampling-selector
      rules:
        - alert: TSSHighConsumerLag
          expr: kafka_consumer_lag > 1000
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "High Kafka consumer lag"
            description: "Consumer lag is {{ $value }}"

        - alert: TSSBufferFull
          expr: buffer_traces_current / buffer_traces_limit > 0.8
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Buffer utilization high"
            description: "Buffer is {{ $value | humanizePercentage }} full"

        - alert: TSSExportFailures
          expr: increase(export_errors_total[5m]) > 0
          for: 1m
          labels:
            severity: critical
          annotations:
            summary: "Export failures detected"
```

---

## Troubleshooting

### Pod Not Starting

```bash
# Check pod status
kubectl describe pod -n tail-sampling -l app=tail-sampling-selector

# View recent events
kubectl get events -n tail-sampling --sort-by='.lastTimestamp'

# Check container logs
kubectl logs -n tail-sampling -l app=tail-sampling-selector --previous
```

### Common Issues

#### 1. Connection Refused to Kafka

```
Error: Failed to connect to Kafka
```

**Solution**: Verify Kafka brokers in config:
```bash
kubectl exec -n tail-sampling <pod-name> -- nc -zv kafka 9092
```

#### 2. Redis Connection Timeout

```
Error: Redis connection timeout
```

**Solution**: Check Redis endpoint and credentials:
```yaml
config:
  redis:
    url: "redis://redis-master:6379"
    password: ""  # Verify secret is mounted
```

#### 3. Datadog API Unauthorized

```
Error: 401 Unauthorized
```

**Solution**: Verify API key:
```bash
kubectl get secret datadog-secret -n tail-sampling -o yaml
```

#### 4. Out of Memory

```
OOMKilled
```

**Solution**: Increase memory limits:
```yaml
resources:
  limits:
    memory: 2Gi  # Increase from 1Gi
```

#### 5. KEDA Not Scaling

```bash
# Check ScaledObject status
kubectl get scaledobject tail-sampling-selector -n tail-sampling -o yaml

# Check KEDA operator logs
kubectl logs -n keda -l app=keda-operator
```

### Debug Mode

Enable debug logging:

```yaml
config:
  observability:
    logLevel: "debug"
    jsonLogging: true
```

### Health Check Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Full health check |
| `GET /health/ready` | Readiness probe |
| `GET /health/live` | Liveness probe |
| `GET /metrics` | Prometheus metrics |

```bash
# Manual health check
kubectl exec -n tail-sampling <pod-name> -- curl localhost:8080/health
```

### Scaling Up for Debugging

```bash
# Scale to single replica for debugging
kubectl scale deployment tail-sampling-selector -n tail-sampling --replicas=1

# Enable debug logging temporarily
kubectl set env deployment tail-sampling-selector TSS_OBSERVABILITY_LOG_LEVEL=debug -n tail-sampling
```

---

## Upgrades

### Upgrading the Application

```bash
# Check current version
helm list -n tail-sampling

# Upgrade to new version
helm upgrade tail-sampling-selector \
  ./helm-charts/tail-sampling-selector \
  --namespace tail-sampling \
  --set image.tag=v1.2.1 \
  --wait
```

### Rolling Back

```bash
# List revisions
helm history tail-sampling-selector -n tail-sampling

# Rollback to previous revision
helm rollback tail-sampling-selector 1 -n tail-sampling
```

### Configuration Changes

```bash
# Update configuration
helm upgrade tail-sampling-selector \
  ./helm-charts/tail-sampling-selector \
  --namespace tail-sampling \
  --values production-values.yaml \
  --reuse-values \
  --set config.sampling.alwaysSampleErrors=true
```

---

## Security Hardening

### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: tail-sampling-selector
  namespace: tail-sampling
spec:
  podSelector:
    matchLabels:
      app: tail-sampling-selector
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: kube-system
      ports:
        - protocol: TCP
          port: 8080
        - protocol: TCP
          port: 9090
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: kafka-namespace
      ports:
        - protocol: TCP
          port: 9092
    - to:
        - namespaceSelector:
            matchLabels:
              name: redis-namespace
      ports:
        - protocol: TCP
          port: 6379
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8
      ports:
        - protocol: TCP
          port: 443
```

### Pod Security Standards

```yaml
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
  seccompProfile:
    type: RuntimeDefault
  sysctls:
    - name: net.core.somaxconn
      value: "1024"
```
