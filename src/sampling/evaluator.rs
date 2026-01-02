// Parallel evaluation workers for trace sampling
use crate::config::SpanCompressionConfig;
use crate::datadog::client::{DatadogClient, DatadogSpan};
use crate::redis::pool::RedisPool;
use crate::sampling::force_sampling::{ForceAction, ForceRuleEngine};
use crate::sampling::policies::{
    DefaultSamplingPolicies, SamplingCombinationStrategy, SamplingDecision, TraceSummary,
    combine_policy_decisions,
};
use crate::sampling::span_compression::{SpanGrouper, process_spans_for_compression};
use crate::state::BufferedSpan;
use crate::storage::TraceStore;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc};
use tokio::task::{JoinHandle, spawn};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct EvaluatorConfig {
    pub worker_count: usize,
    pub batch_size: usize,
    pub combination_strategy: SamplingCombinationStrategy,
    pub poll_interval: Duration,
    pub inactivity_timeout: Duration,
    pub max_trace_duration: Duration,
    pub always_sample_errors: bool,
    pub latency_threshold_ms: u64,
    pub latency_sample_rate: f64,
    pub sample_latency: bool,
    pub max_span_count: usize,
    pub compression_config: Option<Arc<SpanCompressionConfig>>,
    pub trace_ttl_secs: u64,
}

pub struct Evaluator {
    config: EvaluatorConfig,
    trace_store: Arc<dyn TraceStore>,
    datadog_client: Arc<DatadogClient>,
    redis_pool: Arc<RwLock<RedisPool>>,
    force_rule_engine: Option<Arc<ForceRuleEngine>>,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
}

impl Evaluator {
    pub fn new(
        config: EvaluatorConfig,
        trace_store: Arc<dyn TraceStore>,
        datadog_client: Arc<DatadogClient>,
        redis_pool: Arc<RwLock<RedisPool>>,
        shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Self {
        Self {
            config,
            trace_store,
            datadog_client,
            redis_pool,
            force_rule_engine: None,
            shutdown_rx,
        }
    }

    pub fn with_force_rules(mut self, engine: Arc<ForceRuleEngine>) -> Self {
        self.force_rule_engine = Some(engine);
        self
    }

    pub fn start(&self) -> Vec<JoinHandle<Result<()>>> {
        let mut handles = Vec::new();

        for worker_id in 0..self.config.worker_count {
            let trace_store = self.trace_store.clone();
            let datadog_client = self.datadog_client.clone();
            let redis_pool = self.redis_pool.clone();
            let force_rule_engine = self.force_rule_engine.clone();
            let shutdown_rx = self.shutdown_rx.resubscribe();
            let config = self.config.clone();

            let handle = spawn(async move {
                let mut worker = EvaluationWorker::new(
                    worker_id,
                    config,
                    trace_store,
                    datadog_client,
                    redis_pool,
                    force_rule_engine,
                );
                worker.run(shutdown_rx).await
            });

            handles.push(handle);
        }

        info!(
            "Started {} evaluation workers (inactivity: {:?}, max_duration: {:?})",
            self.config.worker_count,
            self.config.inactivity_timeout,
            self.config.max_trace_duration
        );

        handles
    }
}

struct EvaluationWorker {
    worker_id: usize,
    config: EvaluatorConfig,
    trace_store: Arc<dyn TraceStore>,
    datadog_client: Arc<DatadogClient>,
    redis_pool: Arc<RwLock<RedisPool>>,
    force_rule_engine: Option<Arc<ForceRuleEngine>>,
    span_grouper: Option<Arc<SpanGrouper>>,
    sampling_policies: DefaultSamplingPolicies,
}

impl EvaluationWorker {
    fn new(
        worker_id: usize,
        config: EvaluatorConfig,
        trace_store: Arc<dyn TraceStore>,
        datadog_client: Arc<DatadogClient>,
        redis_pool: Arc<RwLock<RedisPool>>,
        force_rule_engine: Option<Arc<ForceRuleEngine>>,
    ) -> Self {
        let span_grouper = config
            .compression_config
            .as_ref()
            .map(|c| Arc::new(SpanGrouper::new(c.clone())));

        let sampling_policies = DefaultSamplingPolicies::new(
            1.0,
            config.latency_threshold_ms,
            config.latency_sample_rate,
            config.max_span_count,
        );

        Self {
            worker_id,
            config,
            trace_store,
            datadog_client,
            redis_pool,
            force_rule_engine,
            span_grouper,
            sampling_policies,
        }
    }

    async fn run(&mut self, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) -> Result<()> {
        info!("Worker {} started", self.worker_id);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Worker {} shutting down", self.worker_id);
                    break;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    if let Err(e) = self.evaluate().await {
                        error!("Worker {} evaluation error: {}", self.worker_id, e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn evaluate(&mut self) -> Result<()> {
        let start = Instant::now();

        // Get traces ready for export (marks and returns in one operation)
        let ready_trace_ids = self
            .trace_store
            .get_ready_traces(self.config.inactivity_timeout)
            .await?;
        let ready_count = ready_trace_ids.len();

        if ready_trace_ids.is_empty() {
            return Ok(());
        }

        info!(
            "Worker {}: Found {} traces ready for export",
            self.worker_id, ready_count
        );

        // Mark traces as processing to prevent double-processing
        self.trace_store.mark_processing(&ready_trace_ids).await?;

        let mut traces_to_export = Vec::new();
        let mut dropped_traces = Vec::new();
        let mut exported_trace_ids = Vec::new();
        let mut already_exported = Vec::new();

        for trace_id in ready_trace_ids {
            if self.is_trace_already_exported(&trace_id).await {
                debug!(
                    "Worker {}: Trace {} already exported (Redis), skipping",
                    self.worker_id, trace_id
                );
                already_exported.push(trace_id);
                continue;
            }

            let summary = match self.trace_store.get_trace_summary(&trace_id).await? {
                Some(s) => s,
                None => {
                    debug!(
                        "Worker {}: Trace {} has no summary, skipping",
                        self.worker_id, trace_id
                    );
                    continue;
                }
            };

            if self.config.always_sample_errors && summary.has_error {
                if let Some(spans) = self.trace_store.get_trace_spans(&trace_id).await? {
                    if let Some(engine) = &self.force_rule_engine {
                        if let Some(ForceAction::ForceDrop) = engine.evaluate_trace(&spans).await {
                            crate::observability::metrics::TraceMetrics::trace_force_dropped();
                            debug!(
                                "Worker {}: Trace {} force-dropped by rule (overriding error sampling)",
                                self.worker_id, trace_id
                            );
                            dropped_traces.push(trace_id);
                            continue;
                        }
                    }
                    exported_trace_ids.push(trace_id.clone());
                    traces_to_export.push((trace_id.clone(), spans));
                    debug!(
                        "Worker {}: Trace {} selected (contains errors)",
                        self.worker_id, trace_id
                    );
                }
                continue;
            }

            let decision = self.evaluate_trace(&summary);

            match decision {
                SamplingDecision::Keep => {
                    if let Some(spans) = self.trace_store.get_trace_spans(&trace_id).await? {
                        if let Some(engine) = &self.force_rule_engine {
                            if let Some(ForceAction::ForceDrop) = engine.evaluate_trace(&spans).await {
                                crate::observability::metrics::TraceMetrics::trace_force_dropped();
                                debug!(
                                    "Worker {}: Trace {} force-dropped by rule (overriding keep decision)",
                                    self.worker_id, trace_id
                                );
                                dropped_traces.push(trace_id);
                                continue;
                            }
                        }
                        exported_trace_ids.push(trace_id.clone());
                        traces_to_export.push((trace_id.clone(), spans));
                    }
                }
                SamplingDecision::Drop => {
                    dropped_traces.push(trace_id);
                }
                SamplingDecision::Pending => {}
            }
        }

        if let Some(ref engine) = self.force_rule_engine {
            let processed: std::collections::HashSet<_> = exported_trace_ids
                .iter()
                .chain(dropped_traces.iter())
                .chain(already_exported.iter())
                .collect();

            let unprocessed: Vec<_> = self
                .trace_store
                .get_all_trace_ids()
                .await?
                .into_iter()
                .filter(|id| !processed.contains(id))
                .collect();

            for trace_id in unprocessed {
                if let Some(spans) = self.trace_store.get_trace_spans(&trace_id).await? {
                    if let Some(ForceAction::ForceKeep) = engine.evaluate_trace(&spans).await {
                        crate::observability::metrics::TraceMetrics::trace_force_sampled();
                        exported_trace_ids.push(trace_id.clone());
                        traces_to_export.push((trace_id.clone(), spans));
                        debug!(
                            "Worker {}: Trace {} force-sampled by rule",
                            self.worker_id, trace_id
                        );
                    }
                }
            }
        }

        // Export all sampled traces
        if !traces_to_export.is_empty() {
            if let Err(e) = self.export_traces(traces_to_export).await {
                error!(
                    "Worker {}: Export failed: {}, traces will be retried",
                    self.worker_id, e
                );
            } else {
                for trace_id in &exported_trace_ids {
                    self.mark_trace_exported(trace_id).await;
                }
                let _ = self.trace_store.remove_traces(&exported_trace_ids).await;
                info!(
                    "Worker {}: Exported and removed {} traces from buffer",
                    self.worker_id,
                    exported_trace_ids.len()
                );
            }
        }

        // Clean up already-exported traces (duplicates from restart)
        if !already_exported.is_empty() {
            let _ = self.trace_store.remove_traces(&already_exported).await;
            info!(
                "Worker {}: Removed {} already-exported traces from buffer",
                self.worker_id,
                already_exported.len()
            );
        }

        // Clean up dropped traces
        if !dropped_traces.is_empty() {
            let _ = self.trace_store.remove_traces(&dropped_traces).await;
            info!(
                "Worker {}: Removed {} dropped traces from buffer",
                self.worker_id,
                dropped_traces.len()
            );
        }

        let duration = start.elapsed();
        debug!(
            "Worker {}: Processed {} traces in {:?}ms",
            self.worker_id,
            ready_count,
            duration.as_millis()
        );

        Ok(())
    }

    fn evaluate_trace(&self, summary: &TraceSummary) -> SamplingDecision {
        if !self.config.sample_latency {
            return SamplingDecision::Drop;
        }

        let results = self.sampling_policies.evaluate(summary);
        combine_policy_decisions(&results, &self.config.combination_strategy)
    }

    async fn is_trace_already_exported(&self, trace_id: &str) -> bool {
        let key = format!("exported:{}", trace_id);
        match self.redis_pool.read().await.get().await {
            Ok(mut conn) => match conn.get::<String>(key).await {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(e) => {
                    warn!(
                        "Worker {}: Redis get failed: {}, assuming not exported",
                        self.worker_id, e
                    );
                    false
                }
            },
            Err(e) => {
                warn!(
                    "Worker {}: Redis pool get failed: {}, assuming not exported",
                    self.worker_id, e
                );
                false
            }
        }
    }

    async fn mark_trace_exported(&self, trace_id: &str) {
        let key = format!("exported:{}", trace_id);
        let ttl = self.config.trace_ttl_secs;
        match self.redis_pool.read().await.get().await {
            Ok(mut conn) => {
                if let Err(e) = conn.set_ex(key, "1", ttl).await {
                    warn!(
                        "Worker {}: Failed to mark trace {} exported in Redis: {}",
                        self.worker_id, trace_id, e
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Worker {}: Redis pool get failed when marking exported: {}",
                    self.worker_id, e
                );
            }
        }
    }

    async fn export_traces(&self, traces: Vec<(String, Vec<BufferedSpan>)>) -> Result<()> {
        let mut all_spans = Vec::new();
        let mut compressed_count = 0usize;
        let mut original_span_count = 0usize;

        for (trace_id, spans) in traces {
            original_span_count += spans.len();

            if let Some(ref grouper) = self.span_grouper {
                let (compressed, uncompressed) = process_spans_for_compression(&spans, grouper);

                for cs in &compressed {
                    compressed_count += cs.span_count;
                    all_spans.push(DatadogSpan::from(cs));
                }

                for span in uncompressed {
                    all_spans.push(DatadogSpan::from(&span));
                }

                if !compressed.is_empty() {
                    debug!(
                        "Worker {}: Trace {} compressed {} spans into {} aggregated spans",
                        self.worker_id,
                        trace_id,
                        compressed_count,
                        compressed.len()
                    );
                }
            } else {
                for span in spans {
                    all_spans.push(DatadogSpan::from(&span));
                }
            }
        }

        if all_spans.is_empty() {
            return Ok(());
        }

        let batch_size = self.config.batch_size;
        for chunk in all_spans.chunks(batch_size) {
            let chunk = chunk.to_vec();
            if let Err(e) = self.datadog_client.export_spans(&chunk).await {
                error!("Failed to export batch: {}", e);
                return Err(e);
            }
        }

        info!(
            "Exported {} spans (original: {}, compressed: {})",
            all_spans.len(),
            original_span_count,
            compressed_count
        );
        Ok(())
    }
}

#[derive(Debug)]
pub enum EvaluationMessage {
    Trace(String),
    Batch(Vec<String>),
    Investigate(Vec<String>),
}

pub struct EvaluationDistributor {
    trace_store: Arc<dyn TraceStore>,
    work_tx: mpsc::Sender<EvaluationMessage>,
    worker_handles: Vec<JoinHandle<Result<()>>>,
}

impl EvaluationDistributor {
    pub fn new(
        trace_store: Arc<dyn TraceStore>,
        evaluator: &Evaluator,
    ) -> (Self, mpsc::Receiver<EvaluationMessage>) {
        let (work_tx, work_rx) = mpsc::channel(100);

        let worker_handles = evaluator.start();

        (
            Self {
                trace_store,
                work_tx,
                worker_handles,
            },
            work_rx,
        )
    }

    pub async fn submit_trace(&self, trace_id: String) {
        let _ = self.work_tx.send(EvaluationMessage::Trace(trace_id)).await;
    }

    pub async fn submit_batch(&self, trace_ids: Vec<String>) {
        let _ = self.work_tx.send(EvaluationMessage::Batch(trace_ids)).await;
    }

    pub async fn submit_investigation(&self, trace_ids: Vec<String>) {
        let _ = self
            .work_tx
            .send(EvaluationMessage::Investigate(trace_ids))
            .await;
    }

    pub async fn trigger_evaluation(&self) {
        if let Ok(trace_ids) = self.trace_store.get_all_trace_ids().await {
            if !trace_ids.is_empty() {
                let _ = self.work_tx.send(EvaluationMessage::Batch(trace_ids)).await;
            }
        }
    }

    pub async fn wait(self) {
        for handle in self.worker_handles {
            let _ = handle.await;
        }
    }
}
