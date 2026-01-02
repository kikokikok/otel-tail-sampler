// State management with bounded buffer and trace activity tracking
use crate::observability::metrics::TraceMetrics;
use crate::sampling::policies::TraceSummary;
use crate::sampling::span_compression::{CompressedSpan, SpanGrouper};
use anyhow::Result;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// A buffered span waiting for trace completion
#[derive(Debug, Clone)]
pub struct BufferedSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub timestamp_ms: i64,
    pub duration_ms: i64,
    pub status_code: i32,
    pub span_kind: SpanKind,
    pub service_name: String,
    pub operation_name: String,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    Unspecified = 0,
    Server = 1,
    Client = 2,
    Producer = 3,
    Consumer = 4,
}

impl From<i32> for SpanKind {
    fn from(val: i32) -> Self {
        match val {
            1 => SpanKind::Server,
            2 => SpanKind::Client,
            3 => SpanKind::Producer,
            4 => SpanKind::Consumer,
            _ => SpanKind::Unspecified,
        }
    }
}

/// Trace status for lifecycle management
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceStatus {
    /// Active trace receiving spans
    Active,
    /// No new spans received recently - potentially closed
    Inactive { since: Instant },
    /// Flagged for investigation (exceeded max duration)
    Investigation {
        since: Instant,
        reason: InvestigationReason,
    },
    /// Ready to be exported (inactive for threshold or force export)
    ReadyForExport,
    /// Force export due to max duration exceeded
    ForceExport,
    /// Processing started
    Processing,
    /// Successfully sampled and exported
    Exported,
    /// Dropped
    Dropped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvestigationReason {
    /// Trace exceeded max duration without closure
    MaxDurationExceeded,
    /// Too many spans (cardinality limit)
    HighCardinality,
    /// Manual override
    Manual,
}

/// Trace metadata for lifecycle tracking
#[derive(Debug, Clone)]
pub struct TraceMetadata {
    pub first_span_time: i64,
    pub last_span_time: i64,
    pub last_activity: Instant,
    pub has_error: bool,
    pub error_count: usize,
    pub max_duration: i64,
    pub root_span_id: Option<String>,
    pub operations: HashSet<String>,
    pub status: TraceStatus,
    pub span_count: usize,
}

impl Default for TraceMetadata {
    fn default() -> Self {
        Self {
            first_span_time: 0,
            last_span_time: 0,
            last_activity: Instant::now(),
            has_error: false,
            error_count: 0,
            max_duration: 0,
            root_span_id: None,
            operations: HashSet::new(),
            status: TraceStatus::Active,
            span_count: 0,
        }
    }
}

/// In-memory trace buffer with activity-based lifecycle
#[derive(Debug)]
pub struct TraceBuffer {
    inner: Arc<RwLock<TraceBufferInner>>,
    max_spans: usize,
    max_traces: usize,
    span_count: AtomicUsize,
    trace_count: AtomicUsize,
    inactivity_window: Duration,
    max_trace_duration: Duration,
    /// Optional span grouper for real-time compression
    span_grouper: Option<Arc<SpanGrouper>>,
    /// Compressed spans cache for SQL-heavy operations
    compressed_spans: Arc<RwLock<Vec<CompressedSpan>>>,
}

#[derive(Debug)]
struct TraceBufferInner {
    traces: HashMap<String, Vec<BufferedSpan>>,
    trace_metadata: HashMap<String, TraceMetadata>,
}

impl TraceBuffer {
    pub fn new(
        max_spans: usize,
        max_traces: usize,
        inactivity_window: Duration,
        max_trace_duration: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TraceBufferInner {
                traces: HashMap::new(),
                trace_metadata: HashMap::new(),
            })),
            max_spans,
            max_traces,
            span_count: AtomicUsize::new(0),
            trace_count: AtomicUsize::new(0),
            inactivity_window,
            max_trace_duration,
            span_grouper: None,
            compressed_spans: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a TraceBuffer with span compression enabled
    pub fn with_compression(
        max_spans: usize,
        max_traces: usize,
        inactivity_window: Duration,
        max_trace_duration: Duration,
        compression_config: Option<Arc<crate::config::SpanCompressionConfig>>,
    ) -> Self {
        let span_grouper = compression_config.map(|config| Arc::new(SpanGrouper::new(config)));

        Self {
            inner: Arc::new(RwLock::new(TraceBufferInner {
                traces: HashMap::new(),
                trace_metadata: HashMap::new(),
            })),
            max_spans,
            max_traces,
            span_count: AtomicUsize::new(0),
            trace_count: AtomicUsize::new(0),
            inactivity_window,
            max_trace_duration,
            span_grouper,
            compressed_spans: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get compressed spans accumulated so far
    pub fn get_compressed_spans(&self) -> Vec<CompressedSpan> {
        self.compressed_spans.read().clone()
    }

    /// Ingest a batch of spans into the buffer
    pub fn ingest(&self, spans: &[BufferedSpan]) -> (usize, usize) {
        let mut inner = self.inner.write();
        let mut added_count = 0;
        let mut dropped_count = 0;
        let now = Instant::now();

        // Check if compression is enabled
        let grouper = self.span_grouper.as_ref();

        if self.span_count.load(Ordering::SeqCst) >= self.max_spans {
            error!(
                "Buffer full ({} spans), dropping spans",
                self.span_count.load(Ordering::SeqCst)
            );
            TraceMetrics::span_dropped();
            return (0, spans.len());
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

            let _is_compressible = grouper.is_some_and(|g| g.get_group_key(span).is_some());

            if inner.traces.len() >= self.max_traces && !inner.traces.contains_key(&trace_id) {
                warn!("Trace limit reached, dropping spans");
                dropped_count += 1;
                continue;
            }

            let trace_entry = inner.traces.entry(trace_id.clone()).or_default();

            trace_entry.push(span.clone());
            added_count += 1;

            let metadata = inner
                .trace_metadata
                .entry(span.trace_id.clone())
                .or_default();

            metadata.first_span_time = metadata.first_span_time.min(span.timestamp_ms);
            metadata.last_span_time = metadata.last_span_time.max(span.timestamp_ms);
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
        }

        self.span_count.fetch_add(added_count, Ordering::SeqCst);
        self.trace_count.store(inner.traces.len(), Ordering::SeqCst);

        TraceMetrics::buffer_spans(self.span_count.load(Ordering::SeqCst));
        TraceMetrics::buffer_traces(inner.traces.len());

        debug!(
            "Ingested {} spans, total spans: {}, traces: {}",
            added_count,
            self.span_count.load(Ordering::SeqCst),
            inner.traces.len()
        );

        (added_count, dropped_count)
    }

    /// Get traces that appear closed (no activity for inactivity_window)
    pub fn get_closed_traces(&self) -> Vec<String> {
        let inner = self.inner.read();
        let now = Instant::now();
        let mut closed_traces = Vec::new();

        for (trace_id, metadata) in &inner.trace_metadata {
            if metadata.status == TraceStatus::Active
                && now.duration_since(metadata.last_activity) > self.inactivity_window
            {
                closed_traces.push(trace_id.clone());
            }
        }

        closed_traces
    }

    /// Phase 1: MARK - Evaluate all traces and mark those ready for export
    /// Returns counts of traces marked
    pub fn mark_phase(&self) -> (usize, usize) {
        let mut inner = self.inner.write();
        let now = Instant::now();
        let mut marked_ready = 0;
        let mut marked_force = 0;

        for metadata in inner.trace_metadata.values_mut() {
            if metadata.status != TraceStatus::Active {
                continue;
            }

            let time_since_last_activity = now.duration_since(metadata.last_activity);

            let has_root_span = metadata.root_span_id.is_some();
            let root_span_inactivity = if has_root_span {
                Duration::from_secs(30)
            } else {
                self.inactivity_window
            };

            if time_since_last_activity > root_span_inactivity {
                metadata.status = TraceStatus::ReadyForExport;
                marked_ready += 1;
            } else if time_since_last_activity > self.max_trace_duration {
                metadata.status = TraceStatus::ForceExport;
                marked_force += 1;
            }
        }

        (marked_ready, marked_force)
    }

    /// Get traces marked as ready for export
    pub fn get_ready_traces(&self) -> Vec<String> {
        let inner = self.inner.read();
        let mut ready_traces = Vec::new();

        for (trace_id, metadata) in &inner.trace_metadata {
            if metadata.status == TraceStatus::ReadyForExport
                || metadata.status == TraceStatus::ForceExport
            {
                ready_traces.push(trace_id.clone());
            }
        }

        ready_traces
    }

    /// Get large traces (>threshold spans) that can be evaluated in chunks
    /// This allows processing long-running traces incrementally instead of waiting
    /// for them to complete, reducing memory pressure
    pub fn get_large_traces_for_chunked_evaluation(
        &self,
        span_threshold: usize,
        max_chunks: usize,
    ) -> Vec<String> {
        let inner = self.inner.read();
        let mut large_traces = Vec::new();

        for (trace_id, spans) in &inner.traces {
            if spans.len() >= span_threshold {
                // Check if this trace hasn't been queued for chunked evaluation recently
                if let Some(metadata) = inner.trace_metadata.get(trace_id) {
                    if metadata.status == TraceStatus::Active {
                        large_traces.push(trace_id.clone());
                    }
                }
            }
        }

        // Limit to max_chunks per call to avoid overwhelming the system
        large_traces.into_iter().take(max_chunks).collect()
    }

    /// Get the chunk of spans for a trace (for chunked evaluation)
    /// Returns the first N spans and removes them from the trace
    pub fn take_trace_chunk(&self, trace_id: &str, chunk_size: usize) -> Option<Vec<BufferedSpan>> {
        let mut inner = self.inner.write();

        if let Some(spans) = inner.traces.get_mut(trace_id) {
            if spans.is_empty() {
                return None;
            }

            // Take the first chunk_size spans
            let chunk: Vec<BufferedSpan> = spans.drain(..chunk_size.min(spans.len())).collect();
            let remaining_count = spans.len();

            // Collect first/last timestamps before we drop the spans borrow
            let first_timestamp = spans.first().map(|s| s.timestamp_ms);
            let last_timestamp = spans.last().map(|s| s.timestamp_ms);

            // Update metadata
            if let Some(metadata) = inner.trace_metadata.get_mut(trace_id) {
                metadata.span_count = remaining_count;
                if let Some(ts) = first_timestamp {
                    metadata.first_span_time = ts;
                }
                if let Some(ts) = last_timestamp {
                    metadata.last_span_time = ts;
                }
            }

            // Update counts
            self.span_count.fetch_sub(chunk.len(), Ordering::SeqCst);

            Some(chunk)
        } else {
            None
        }
    }

    /// Get traces that need investigation (exceeded max duration)
    pub fn get_investigation_traces(&self) -> Vec<String> {
        let inner = self.inner.read();
        let now = Instant::now();
        let mut investigation_traces = Vec::new();

        for (trace_id, metadata) in &inner.trace_metadata {
            if metadata.status == TraceStatus::Active && metadata.first_span_time > 0 {
                // first_span_time is in milliseconds, convert to Duration
                let trace_start = metadata.first_span_time;
                let trace_start_instant = Instant::now()
                    - Duration::from_millis(
                        (now.duration_since(Instant::now()).as_millis() as i64 - trace_start)
                            as u64,
                    );
                let elapsed = now.duration_since(trace_start_instant);

                if elapsed > self.max_trace_duration {
                    investigation_traces.push(trace_id.clone());
                }
            }
        }

        investigation_traces
    }

    /// Mark traces for investigation
    pub fn mark_for_investigation(&self, trace_ids: &[String], reason: InvestigationReason) {
        let mut inner = self.inner.write();
        let now = Instant::now();

        for trace_id in trace_ids {
            if let Some(metadata) = inner.trace_metadata.get_mut(trace_id) {
                metadata.status = TraceStatus::Investigation {
                    since: now,
                    reason: reason.clone(),
                };
            }
        }
    }

    /// Get trace by ID
    pub fn get_trace(&self, trace_id: &str) -> Option<Vec<BufferedSpan>> {
        self.inner.read().traces.get(trace_id).cloned()
    }

    /// Get trace status
    pub fn get_trace_status(&self, trace_id: &str) -> Option<TraceStatus> {
        self.inner
            .read()
            .trace_metadata
            .get(trace_id)
            .map(|m| m.status.clone())
    }

    /// Mark trace as processing
    pub fn mark_processing(&self, trace_ids: &[String]) {
        let mut inner = self.inner.write();
        for trace_id in trace_ids {
            if let Some(metadata) = inner.trace_metadata.get_mut(trace_id) {
                metadata.status = TraceStatus::Processing;
            }
        }
    }

    /// Remove traces from buffer
    pub fn remove_traces(&self, trace_ids: &[String]) -> usize {
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

        removed_spans
    }

    /// Get trace summary for sampling evaluation
    pub fn get_trace_summary(&self, trace_id: &str) -> Option<TraceSummary> {
        let inner = self.inner.read();
        let spans = inner.traces.get(trace_id)?;
        let metadata = inner.trace_metadata.get(trace_id)?;

        let root_span_id = metadata.root_span_id.clone().or_else(|| {
            spans
                .iter()
                .find(|s| s.parent_span_id.is_none())
                .map(|s| s.span_id.clone())
        });

        Some(TraceSummary {
            trace_id: trace_id.to_string(),
            service_name: spans.first()?.service_name.clone(),
            span_count: spans.len(),
            has_error: metadata.has_error,
            max_duration_ms: metadata.max_duration,
            min_timestamp_ms: metadata.first_span_time,
            max_timestamp_ms: metadata.last_span_time,
            operations: metadata.operations.iter().cloned().collect(),
            total_spans: spans.len(),
            error_count: metadata.error_count,
            root_span_id,
        })
    }

    /// Get all trace IDs
    pub fn get_all_trace_ids(&self) -> Vec<String> {
        self.inner.read().traces.keys().cloned().collect()
    }

    /// Garbage collect stuck traces (Processing/Investigation for too long)
    /// Returns (removed_stuck_count, removed_expired_count)
    pub fn garbage_collect(&self, stuck_timeout: Duration, max_age: Duration) -> (usize, usize) {
        let mut inner = self.inner.write();
        let now = Instant::now();
        let mut stuck_traces = Vec::new();
        let mut expired_traces = Vec::new();

        for (trace_id, metadata) in &inner.trace_metadata {
            match &metadata.status {
                TraceStatus::Processing => {
                    if now.duration_since(metadata.last_activity) > stuck_timeout {
                        stuck_traces.push(trace_id.clone());
                    }
                }
                TraceStatus::Investigation { since, .. } => {
                    if now.duration_since(*since) > stuck_timeout {
                        stuck_traces.push(trace_id.clone());
                    }
                }
                TraceStatus::Active => {
                    if now.duration_since(metadata.last_activity) > max_age {
                        expired_traces.push(trace_id.clone());
                    }
                }
                _ => {}
            }
        }

        let mut removed_spans = 0;

        for trace_id in &stuck_traces {
            if let Some(spans) = inner.traces.remove(trace_id) {
                removed_spans += spans.len();
                inner.trace_metadata.remove(trace_id);
            }
        }

        for trace_id in &expired_traces {
            if let Some(spans) = inner.traces.remove(trace_id) {
                removed_spans += spans.len();
                inner.trace_metadata.remove(trace_id);
            }
        }

        if removed_spans > 0 {
            self.span_count.fetch_sub(removed_spans, Ordering::SeqCst);
            self.trace_count.store(inner.traces.len(), Ordering::SeqCst);
            TraceMetrics::buffer_spans(self.span_count.load(Ordering::SeqCst));
            TraceMetrics::buffer_traces(inner.traces.len());
        }

        (stuck_traces.len(), expired_traces.len())
    }

    /// Get current buffer stats
    pub fn stats(&self) -> BufferStats {
        let inner = self.inner.read();
        BufferStats {
            span_count: self.span_count.load(Ordering::SeqCst),
            trace_count: inner.traces.len(),
            memory_estimate_bytes: self.estimate_memory_usage(),
            active_traces: inner
                .trace_metadata
                .values()
                .filter(|m| m.status == TraceStatus::Active)
                .count(),
            inactive_traces: inner
                .trace_metadata
                .values()
                .filter(|m| matches!(m.status, TraceStatus::Inactive { .. }))
                .count(),
            investigation_traces: inner
                .trace_metadata
                .values()
                .filter(|m| matches!(m.status, TraceStatus::Investigation { .. }))
                .count(),
        }
    }

    fn estimate_memory_usage(&self) -> usize {
        let inner = self.inner.read();
        let spans = inner.traces.values().flatten().count();
        let avg_span_size = std::mem::size_of::<BufferedSpan>()
            + std::mem::size_of::<String>() * 5
            + std::mem::size_of::<HashMap<String, String>>();

        spans * avg_span_size
    }
}

/// Buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub span_count: usize,
    pub trace_count: usize,
    pub memory_estimate_bytes: usize,
    pub active_traces: usize,
    pub inactive_traces: usize,
    pub investigation_traces: usize,
}

/// Backpressure controller using token bucket
#[derive(Debug)]
pub struct BackpressureController {
    rate: f64,
    capacity: usize,
    tokens: f64,
    last_update: Instant,
}

impl BackpressureController {
    pub fn new(rate: f64, capacity: usize) -> Self {
        Self {
            rate,
            capacity,
            tokens: capacity as f64,
            last_update: Instant::now(),
        }
    }

    pub fn try_acquire(&mut self) -> Result<(), Duration> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update);

        let tokens_to_add = elapsed.as_secs_f64() * self.rate;
        self.tokens = (self.tokens + tokens_to_add).min(self.capacity as f64);
        self.last_update = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            let wait_time = (1.0 - self.tokens) / self.rate;
            Err(Duration::from_secs_f64(wait_time))
        }
    }

    pub async fn acquire(&mut self, timeout: Duration) -> Result<(), Duration> {
        match self.try_acquire() {
            Ok(()) => Ok(()),
            Err(wait_time) => {
                let sleep_time = std::cmp::min(wait_time, timeout);
                tokio::time::sleep(sleep_time).await;
                self.try_acquire()
            }
        }
    }

    pub fn available_tokens(&self) -> usize {
        self.tokens as usize
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
