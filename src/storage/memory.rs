use super::{IngestResult, StorageType, StoreStats, TraceStore};
use crate::observability::metrics::TraceMetrics;
use crate::sampling::policies::TraceSummary;
use crate::state::{BufferedSpan, TraceStatus};
use anyhow::Result;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

#[derive(Debug)]
pub struct MemoryTraceStore {
    inner: Arc<RwLock<MemoryStoreInner>>,
    max_spans: usize,
    max_traces: usize,
    span_count: AtomicUsize,
    trace_count: AtomicUsize,
    inactivity_window: Duration,
    max_trace_duration: Duration,
}

#[derive(Debug)]
struct MemoryStoreInner {
    traces: HashMap<String, Vec<BufferedSpan>>,
    trace_metadata: HashMap<String, TraceMetadata>,
}

#[derive(Debug, Clone)]
struct TraceMetadata {
    first_span_time: i64,
    last_span_time: i64,
    last_activity: Instant,
    has_error: bool,
    error_count: usize,
    max_duration: i64,
    root_span_id: Option<String>,
    operations: HashSet<String>,
    status: TraceStatus,
    service_name: Option<String>,
}

impl Default for TraceMetadata {
    fn default() -> Self {
        Self {
            first_span_time: i64::MAX,
            last_span_time: 0,
            last_activity: Instant::now(),
            has_error: false,
            error_count: 0,
            max_duration: 0,
            root_span_id: None,
            operations: HashSet::new(),
            status: TraceStatus::Active,
            service_name: None,
        }
    }
}

impl MemoryTraceStore {
    pub fn new(
        max_spans: usize,
        max_traces: usize,
        inactivity_window: Duration,
        max_trace_duration: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemoryStoreInner {
                traces: HashMap::new(),
                trace_metadata: HashMap::new(),
            })),
            max_spans,
            max_traces,
            span_count: AtomicUsize::new(0),
            trace_count: AtomicUsize::new(0),
            inactivity_window,
            max_trace_duration,
        }
    }

    fn estimate_memory_usage(&self) -> usize {
        let inner = self.inner.read();
        let spans = inner.traces.values().map(|v| v.len()).sum::<usize>();
        let avg_span_size = std::mem::size_of::<BufferedSpan>()
            + std::mem::size_of::<String>() * 5
            + std::mem::size_of::<HashMap<String, String>>();

        spans * avg_span_size
    }
}

#[async_trait]
impl TraceStore for MemoryTraceStore {
    async fn ingest(&self, spans: &[BufferedSpan]) -> Result<IngestResult> {
        let mut inner = self.inner.write();
        let mut added_count = 0;
        let mut dropped_count = 0;
        let now = Instant::now();

        if self.span_count.load(Ordering::SeqCst) >= self.max_spans {
            error!(
                "Buffer full ({} spans), dropping spans",
                self.span_count.load(Ordering::SeqCst)
            );
            TraceMetrics::span_dropped();
            return Ok(IngestResult {
                added: 0,
                dropped: spans.len(),
            });
        }

        for span in spans {
            let new_span_count = self.span_count.load(Ordering::SeqCst) + 1;

            if new_span_count > self.max_spans {
                warn!(
                    "Span limit reached, dropping span for trace {}",
                    span.trace_id
                );
                dropped_count += 1;
                continue;
            }

            let trace_id = span.trace_id.clone();

            if inner.traces.len() >= self.max_traces && !inner.traces.contains_key(&trace_id) {
                warn!("Trace limit reached, dropping spans");
                dropped_count += 1;
                continue;
            }

            let trace_entry = inner.traces.entry(trace_id.clone()).or_default();
            trace_entry.push(span.clone());
            added_count += 1;
            self.span_count.fetch_add(1, Ordering::SeqCst);

            let metadata = inner.trace_metadata.entry(trace_id).or_default();

            if span.timestamp_ms < metadata.first_span_time {
                metadata.first_span_time = span.timestamp_ms;
            }
            if span.timestamp_ms > metadata.last_span_time {
                metadata.last_span_time = span.timestamp_ms;
            }
            metadata.last_activity = now;
            metadata.max_duration = metadata.max_duration.max(span.duration_ms);
            metadata.status = TraceStatus::Active;

            if span.status_code == 2 {
                metadata.has_error = true;
                metadata.error_count += 1;
            }

            metadata.operations.insert(span.operation_name.clone());

            if span.parent_span_id.is_none() {
                metadata.root_span_id = Some(span.span_id.clone());
            }

            if metadata.service_name.is_none() {
                metadata.service_name = Some(span.service_name.clone());
            }
        }

        self.trace_count.store(inner.traces.len(), Ordering::SeqCst);

        TraceMetrics::buffer_spans(self.span_count.load(Ordering::SeqCst));
        TraceMetrics::buffer_traces(inner.traces.len());

        debug!(
            "Ingested {} spans, total spans: {}, traces: {}",
            added_count,
            self.span_count.load(Ordering::SeqCst),
            inner.traces.len()
        );

        Ok(IngestResult {
            added: added_count,
            dropped: dropped_count,
        })
    }

    async fn get_ready_traces(&self, inactivity_threshold: Duration) -> Result<Vec<String>> {
        let mut inner = self.inner.write();
        let now = Instant::now();
        let mut ready_traces = Vec::new();

        let now_epoch_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        for (trace_id, metadata) in inner.trace_metadata.iter_mut() {
            if metadata.status != TraceStatus::Active {
                continue;
            }

            let time_since_last_activity = now.duration_since(metadata.last_activity);
            let total_trace_duration_ms = (now_epoch_ms - metadata.first_span_time).max(0) as u64;
            let total_trace_duration = Duration::from_millis(total_trace_duration_ms);

            if total_trace_duration > self.max_trace_duration {
                debug!(
                    "Trace {} exceeded max duration ({:?} > {:?}), forcing export",
                    trace_id, total_trace_duration, self.max_trace_duration
                );
                metadata.status = TraceStatus::ForceExport;
                ready_traces.push(trace_id.clone());
                continue;
            }

            let has_root_span = metadata.root_span_id.is_some();
            let effective_inactivity_threshold = if has_root_span {
                Duration::from_secs(30)
            } else {
                inactivity_threshold
            };

            if time_since_last_activity > effective_inactivity_threshold {
                metadata.status = TraceStatus::ReadyForExport;
                ready_traces.push(trace_id.clone());
            }
        }

        Ok(ready_traces)
    }

    async fn get_trace_summary(&self, trace_id: &str) -> Result<Option<TraceSummary>> {
        let inner = self.inner.read();

        let spans = match inner.traces.get(trace_id) {
            Some(s) => s,
            None => return Ok(None),
        };

        let metadata = match inner.trace_metadata.get(trace_id) {
            Some(m) => m,
            None => return Ok(None),
        };

        let root_span_id = metadata.root_span_id.clone().or_else(|| {
            spans
                .iter()
                .find(|s| s.parent_span_id.is_none())
                .map(|s| s.span_id.clone())
        });

        let service_name = metadata
            .service_name
            .clone()
            .or_else(|| spans.first().map(|s| s.service_name.clone()))
            .unwrap_or_default();

        Ok(Some(TraceSummary {
            trace_id: trace_id.to_string(),
            service_name,
            span_count: spans.len(),
            has_error: metadata.has_error,
            max_duration_ms: metadata.max_duration,
            min_timestamp_ms: metadata.first_span_time,
            max_timestamp_ms: metadata.last_span_time,
            operations: metadata.operations.iter().cloned().collect(),
            total_spans: spans.len(),
            error_count: metadata.error_count,
            root_span_id,
        }))
    }

    async fn get_trace_spans(&self, trace_id: &str) -> Result<Option<Vec<BufferedSpan>>> {
        let inner = self.inner.read();
        Ok(inner.traces.get(trace_id).cloned())
    }

    async fn remove_traces(&self, trace_ids: &[String]) -> Result<usize> {
        let mut inner = self.inner.write();
        let mut removed_spans = 0;

        for trace_id in trace_ids {
            if let Some(spans) = inner.traces.remove(trace_id) {
                removed_spans += spans.len();
                inner.trace_metadata.remove(trace_id);
            }
        }

        self.span_count.fetch_sub(removed_spans, Ordering::SeqCst);
        self.trace_count.store(inner.traces.len(), Ordering::SeqCst);

        TraceMetrics::buffer_spans(self.span_count.load(Ordering::SeqCst));
        TraceMetrics::buffer_traces(inner.traces.len());

        Ok(removed_spans)
    }

    async fn mark_processing(&self, trace_ids: &[String]) -> Result<()> {
        let mut inner = self.inner.write();
        for trace_id in trace_ids {
            if let Some(metadata) = inner.trace_metadata.get_mut(trace_id) {
                metadata.status = TraceStatus::Processing;
            }
        }
        Ok(())
    }

    async fn get_all_trace_ids(&self) -> Result<Vec<String>> {
        let inner = self.inner.read();
        Ok(inner.traces.keys().cloned().collect())
    }

    async fn stats(&self) -> Result<StoreStats> {
        Ok(StoreStats {
            span_count: self.span_count.load(Ordering::SeqCst),
            trace_count: self.trace_count.load(Ordering::SeqCst),
            memory_bytes: self.estimate_memory_usage(),
            storage_type: StorageType::Memory,
        })
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Memory
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SpanKind;

    fn create_test_span(trace_id: &str, span_id: &str, has_error: bool) -> BufferedSpan {
        BufferedSpan {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            timestamp_ms: 1000,
            duration_ms: 100,
            status_code: if has_error { 2 } else { 0 },
            span_kind: SpanKind::Server,
            service_name: "test-service".to_string(),
            operation_name: "test-op".to_string(),
            attributes: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_ingest_and_retrieve() {
        let store = MemoryTraceStore::new(
            1000,
            100,
            Duration::from_secs(60),
            Duration::from_secs(300),
        );

        let spans = vec![
            create_test_span("trace1", "span1", false),
            create_test_span("trace1", "span2", true),
            create_test_span("trace2", "span3", false),
        ];

        let result = store.ingest(&spans).await.unwrap();
        assert_eq!(result.added, 3);
        assert_eq!(result.dropped, 0);

        let summary = store.get_trace_summary("trace1").await.unwrap().unwrap();
        assert_eq!(summary.span_count, 2);
        assert!(summary.has_error);
        assert_eq!(summary.error_count, 1);

        let summary = store.get_trace_summary("trace2").await.unwrap().unwrap();
        assert_eq!(summary.span_count, 1);
        assert!(!summary.has_error);
    }

    #[tokio::test]
    async fn test_remove_traces() {
        let store = MemoryTraceStore::new(
            1000,
            100,
            Duration::from_secs(60),
            Duration::from_secs(300),
        );

        let spans = vec![
            create_test_span("trace1", "span1", false),
            create_test_span("trace2", "span2", false),
        ];

        store.ingest(&spans).await.unwrap();

        let removed = store
            .remove_traces(&["trace1".to_string()])
            .await
            .unwrap();
        assert_eq!(removed, 1);

        assert!(store.get_trace_summary("trace1").await.unwrap().is_none());
        assert!(store.get_trace_summary("trace2").await.unwrap().is_some());
    }
}
