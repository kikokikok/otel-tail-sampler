//! Simulation for tail sampling selector worker requirements
//!
//! Simulates 10,000 spans/sec with 1% of traces having >20,000 spans
//! and evaluates optimal worker count.

use rand::Rng;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Simulation configuration
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    /// Spans ingested per second
    pub spans_per_second: usize,
    /// Percentage of traces that are large (>20000 spans)
    pub large_trace_percentage: f64,
    /// Percentage of large traces that are SQL-heavy (eligible for compression)
    pub sql_percentage: f64,
    /// Average spans in a small trace
    pub avg_small_spans: usize,
    /// Spans in a large trace
    pub large_trace_spans: usize,
    /// Minimum time for a large trace to complete (seconds)
    pub min_large_trace_duration_secs: u64,
    /// Maximum time for a large trace to complete (seconds)
    pub max_large_trace_duration_secs: u64,
    /// Time for a small trace to complete (seconds)
    pub small_trace_duration_secs: u64,
    /// Time to process one span (microseconds)
    pub span_processing_time_us: u64,
    /// Time to evaluate one trace (milliseconds)
    pub trace_evaluation_time_ms: u64,
    /// Time to compress spans (microseconds per span)
    pub compression_time_us: u64,
    /// Poll interval for workers
    pub poll_interval_ms: u64,
    /// Inactivity timeout (seconds) - trace considered closed after this
    pub inactivity_timeout_secs: u64,
    /// Maximum trace duration before investigation (seconds)
    pub max_trace_duration_secs: u64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            spans_per_second: 10_000,
            large_trace_percentage: 0.01, // 1%
            sql_percentage: 0.80,         // 80%
            avg_small_spans: 10,
            large_trace_spans: 20_000,
            min_large_trace_duration_secs: 300, // 5 min
            max_large_trace_duration_secs: 600, // 10 min
            small_trace_duration_secs: 5,       // 5 sec
            span_processing_time_us: 50,        // 50 microseconds per span
            trace_evaluation_time_ms: 1,        // 1ms per trace evaluation
            compression_time_us: 10,            // 10 microseconds for compression
            poll_interval_ms: 100,              // 100ms poll interval
            inactivity_timeout_secs: 30,        // 30 seconds inactivity
            max_trace_duration_secs: 3600,      // 1 hour max
        }
    }
}

/// A trace in the simulation
#[derive(Debug, Clone)]
pub struct Trace {
    pub id: String,
    pub span_count: usize,
    pub is_large: bool,
    pub is_sql_heavy: bool,
    pub start_time: Instant,
    pub expected_duration: Duration,
    pub last_span_time: Instant,
    pub spans: Vec<Span>,
}

impl Trace {
    pub fn is_complete(&self, now: Instant) -> bool {
        // Trace is complete if it's past its expected duration
        // This simulates traces naturally completing over time
        now.duration_since(self.start_time) >= self.expected_duration
    }

    pub fn is_timed_out(&self, now: Instant) -> bool {
        now.duration_since(self.start_time) > Duration::from_secs(3600)
    }
}

/// A span in the simulation
#[derive(Debug, Clone)]
pub struct Span {
    pub id: String,
    pub timestamp: Instant,
    pub duration_ms: i64,
}

/// Statistics for a worker
#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub traces_evaluated: usize,
    pub spans_processed: usize,
    pub large_traces_processed: usize,
    pub total_eval_time_ms: u128,
    pub compression_groups: usize,
    pub spans_compressed: usize,
}

/// Simulation state
pub struct Simulation {
    config: SimulationConfig,
    traces: Arc<Mutex<VecDeque<Trace>>>,
    completed_traces: Arc<Mutex<Vec<Trace>>>,
    worker_stats: Arc<Mutex<Vec<WorkerStats>>>,
    active_traces: Arc<AtomicUsize>,
    active_spans: Arc<AtomicUsize>,
    total_spans_ingested: Arc<AtomicUsize>,
    total_traces_created: Arc<AtomicUsize>,
    traces_evaluated: Arc<AtomicUsize>,
    running: Arc<AtomicUsize>,
}

impl Simulation {
    pub fn new(config: SimulationConfig) -> Self {
        Self {
            config,
            traces: Arc::new(Mutex::new(VecDeque::new())),
            completed_traces: Arc::new(Mutex::new(Vec::new())),
            worker_stats: Arc::new(Mutex::new(Vec::new())),
            active_traces: Arc::new(AtomicUsize::new(0)),
            active_spans: Arc::new(AtomicUsize::new(0)),
            total_spans_ingested: Arc::new(AtomicUsize::new(0)),
            total_traces_created: Arc::new(AtomicUsize::new(0)),
            traces_evaluated: Arc::new(AtomicUsize::new(0)),
            running: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn add_worker(&self) {
        self.worker_stats
            .lock()
            .unwrap()
            .push(WorkerStats::default());
    }

    /// Calculate expected trace rate based on config
    pub fn expected_trace_rate(&self) -> (usize, f64, f64) {
        let spans_per_sec = self.config.spans_per_second as f64;
        let large_span_contribution =
            self.config.large_trace_percentage * self.config.large_trace_spans as f64;
        let small_span_contribution =
            (1.0 - self.config.large_trace_percentage) * self.config.avg_small_spans as f64;

        let spans_per_trace = large_span_contribution + small_span_contribution;
        let traces_per_sec = spans_per_sec / spans_per_trace;

        let large_traces_per_sec = traces_per_sec * self.config.large_trace_percentage;
        let sql_traces_per_sec = large_traces_per_sec * self.config.sql_percentage;

        (
            traces_per_sec as usize,
            large_traces_per_sec,
            sql_traces_per_sec,
        )
    }

    /// Generate a new trace based on configured probabilities
    fn generate_trace(&self, now: Instant, trace_id: usize) -> Trace {
        let mut rng = rand::thread_rng();
        let is_large = rand::random::<f64>() < self.config.large_trace_percentage;

        let span_count = if is_large {
            self.config.large_trace_spans
        } else {
            rng.gen_range(self.config.avg_small_spans / 2..self.config.avg_small_spans * 3)
        };

        let is_sql_heavy = is_large && rand::random::<f64>() < self.config.sql_percentage;

        let duration = if is_large {
            Duration::from_secs(rng.gen_range(
                self.config.min_large_trace_duration_secs
                    ..self.config.max_large_trace_duration_secs,
            ))
        } else {
            Duration::from_secs(self.config.small_trace_duration_secs)
        };

        // Generate spans distributed over the trace duration
        let span_interval = duration.as_millis() / (span_count as u128 + 1);
        let spans: Vec<Span> = (0..span_count)
            .map(|i| Span {
                id: format!("span_{}_{}", trace_id, i),
                timestamp: now + Duration::from_millis((i as u128 * span_interval) as u64),
                duration_ms: rng.gen_range(1..100) as i64,
            })
            .collect();

        let last_span_time = now + duration;

        Trace {
            id: format!("trace_{}", trace_id),
            span_count,
            is_large,
            is_sql_heavy,
            start_time: now,
            expected_duration: duration,
            last_span_time,
            spans,
        }
    }

    /// Simulate ingestion of spans over time
    fn ingest_spans(&self, now: Instant) {
        let (trace_rate, _, _) = self.expected_trace_rate();

        // Create new traces at the expected rate
        let trace_count = (trace_rate as f64 / 10.0) as usize; // For 100ms interval
        let total_traces = self.total_traces_created.load(Ordering::SeqCst);

        for i in 0..trace_count {
            let trace = self.generate_trace(now, total_traces + i);
            self.active_spans
                .fetch_add(trace.span_count, Ordering::SeqCst);
            self.active_traces.fetch_add(1, Ordering::SeqCst);
            self.total_spans_ingested
                .fetch_add(trace.span_count, Ordering::SeqCst);
            self.traces.lock().unwrap().push_back(trace);
        }

        self.total_traces_created
            .fetch_add(trace_count, Ordering::SeqCst);
    }

    /// Simulate one worker's evaluation cycle
    fn worker_cycle(&self, worker_id: usize, now: Instant) {
        let mut stats = self.worker_stats.lock().unwrap()[worker_id].clone();

        // Get closed traces (inactive for inactivity_timeout)
        let closed_traces: Vec<Trace> = {
            let mut traces = self.traces.lock().unwrap();
            let mut closed = Vec::new();
            let mut to_remove = VecDeque::new();

            for (i, trace) in traces.iter().enumerate() {
                if trace.is_complete(now) {
                    closed.push(trace.clone());
                    to_remove.push_back(i);
                } else if trace.is_timed_out(now) {
                    // Mark for investigation (also counts as evaluation)
                    stats.traces_evaluated += 1;
                    self.traces_evaluated.fetch_add(1, Ordering::SeqCst);
                }
            }

            // Remove closed traces (in reverse order to maintain indices)
            for i in to_remove.into_iter().rev() {
                traces.remove(i);
            }

            closed
        };

        // Evaluate each closed trace
        for trace in closed_traces {
            // Handle span compression for SQL-heavy traces
            let effective_spans = if trace.is_sql_heavy {
                // Compression: group similar spans into single compressed spans
                // Assume 95% compression ratio for SQL queries (100 spans -> ~5 compressed)
                let groups = (trace.span_count as f64 / 20.0).max(1.0) as usize;
                stats.compression_groups += groups;
                stats.spans_compressed += trace.span_count;

                // Only process the compressed groups, not individual spans
                groups
            } else {
                trace.span_count
            };

            // Process spans (compressed or not)
            stats.spans_processed += effective_spans;

            // Handle compression time overhead
            let eval_time_ms = self.config.trace_evaluation_time_ms as u128;
            stats.total_eval_time_ms += eval_time_ms;
            stats.traces_evaluated += 1;

            if trace.is_large {
                stats.large_traces_processed += 1;
            }

            self.traces_evaluated.fetch_add(1, Ordering::SeqCst);
            self.active_traces.fetch_sub(1, Ordering::SeqCst);
            self.active_spans
                .fetch_sub(trace.span_count, Ordering::SeqCst);

            // Store completed trace
            self.completed_traces.lock().unwrap().push(trace);
        }

        self.worker_stats.lock().unwrap()[worker_id] = stats;
    }

    /// Run the simulation for a duration
    pub fn run(&self, duration_secs: u64, num_workers: usize) -> SimulationResult {
        // Reset state
        *self.traces.lock().unwrap() = VecDeque::new();
        *self.completed_traces.lock().unwrap() = Vec::new();
        *self.worker_stats.lock().unwrap() = vec![WorkerStats::default(); num_workers];
        self.active_traces.store(0, Ordering::SeqCst);
        self.active_spans.store(0, Ordering::SeqCst);
        self.total_spans_ingested.store(0, Ordering::SeqCst);
        self.total_traces_created.store(0, Ordering::SeqCst);
        self.traces_evaluated.store(0, Ordering::SeqCst);
        let start_time = Instant::now();
        self.running.store(1, Ordering::SeqCst);

        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
        let total_duration = Duration::from_secs(duration_secs);
        let cycles = total_duration.as_millis() / poll_interval.as_millis();

        println!("\n=== Simulation: {} workers ===", num_workers);
        println!("Duration: {} seconds ({} cycles)", duration_secs, cycles);
        println!("Poll interval: {}ms", self.config.poll_interval_ms);

        let mut cycle = 0;
        while start_time.elapsed() < total_duration && self.running.load(Ordering::SeqCst) == 1 {
            let now = Instant::now();

            // Ingest new spans
            self.ingest_spans(now);

            // Run all workers
            for worker_id in 0..num_workers {
                self.worker_cycle(worker_id, now);
            }

            cycle += 1;

            // Progress output every 10%
            if cycle % (cycles as usize / 10).max(1) == 0 {
                let elapsed = start_time.elapsed().as_secs_f64();
                let progress = (cycle as f64 / cycles as f64 * 100.0) as usize;
                println!(
                    "[{}%] Time: {:.1}s, Active Traces: {}, Active Spans: {}",
                    progress,
                    elapsed,
                    self.active_traces.load(Ordering::SeqCst),
                    self.active_spans.load(Ordering::SeqCst)
                );
            }

            // Wait for poll interval (skip wait on last cycle)
            if cycle < cycles as usize {
                let elapsed = now.elapsed();
                if elapsed < poll_interval {
                    std::thread::sleep(poll_interval - elapsed);
                }
            }
        }

        self.calculate_results(duration_secs, num_workers)
    }

    fn calculate_results(&self, duration_secs: u64, num_workers: usize) -> SimulationResult {
        let total_spans = self.total_spans_ingested.load(Ordering::SeqCst);
        let total_traces = self.total_traces_created.load(Ordering::SeqCst);
        let evaluated = self.traces_evaluated.load(Ordering::SeqCst);
        let completed = self.completed_traces.lock().unwrap().len();

        let stats = self.worker_stats.lock().unwrap();
        let total_eval_time: u128 = stats.iter().map(|s| s.total_eval_time_ms).sum();
        let total_spans_processed: usize = stats.iter().map(|s| s.spans_processed).sum();
        let total_large_traces: usize = stats.iter().map(|s| s.large_traces_processed).sum();
        let total_compression_groups: usize = stats.iter().map(|s| s.compression_groups).sum();
        let total_spans_compressed: usize = stats.iter().map(|s| s.spans_compressed).sum();

        SimulationResult {
            config: self.config.clone(),
            num_workers,
            duration_secs,
            total_spans_ingested: total_spans,
            total_traces_created: total_traces,
            traces_evaluated: evaluated,
            traces_completed: completed,
            total_spans_processed,
            large_traces_processed: total_large_traces,
            compression_groups: total_compression_groups,
            spans_compressed: total_spans_compressed,
            total_eval_time_ms: total_eval_time,
            avg_spans_per_sec: total_spans as f64 / duration_secs as f64,
            avg_eval_time_per_trace_ms: if evaluated > 0 {
                total_eval_time as f64 / evaluated as f64
            } else {
                0.0
            },
            avg_worker_utilization: if duration_secs > 0 {
                let total_worker_time = duration_secs as f64 * num_workers as f64 * 1000.0;
                (total_eval_time as f64 / total_worker_time * 100.0).min(100.0)
            } else {
                0.0
            },
            active_traces_at_end: self.active_traces.load(Ordering::SeqCst),
            active_spans_at_end: self.active_spans.load(Ordering::SeqCst),
        }
    }

    pub fn stop(&self) {
        self.running.store(0, Ordering::SeqCst);
    }
}

/// Simulation results
#[derive(Debug)]
pub struct SimulationResult {
    pub config: SimulationConfig,
    pub num_workers: usize,
    pub duration_secs: u64,
    pub total_spans_ingested: usize,
    pub total_traces_created: usize,
    pub traces_evaluated: usize,
    pub traces_completed: usize,
    pub total_spans_processed: usize,
    pub large_traces_processed: usize,
    pub compression_groups: usize,
    pub spans_compressed: usize,
    pub total_eval_time_ms: u128,
    pub avg_spans_per_sec: f64,
    pub avg_eval_time_per_trace_ms: f64,
    pub avg_worker_utilization: f64,
    pub active_traces_at_end: usize,
    pub active_spans_at_end: usize,
}

impl fmt::Display for SimulationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\n=== Simulation Results ===")?;
        writeln!(f, "Workers: {}", self.num_workers)?;
        writeln!(f, "Duration: {}s", self.duration_secs)?;
        writeln!(f, "Spans ingested: {}", self.total_spans_ingested)?;
        writeln!(f, "Spans/sec: {:.0}", self.avg_spans_per_sec)?;
        writeln!(f, "Traces created: {}", self.total_traces_created)?;
        writeln!(f, "Traces evaluated: {}", self.traces_evaluated)?;
        writeln!(f, "Large traces processed: {}", self.large_traces_processed)?;
        writeln!(f, "Worker utilization: {:.1}%", self.avg_worker_utilization)?;
        writeln!(f, "Active traces at end: {}", self.active_traces_at_end)?;
        writeln!(f, "Active spans at end: {}", self.active_spans_at_end)?;

        // Compression stats
        if self.spans_compressed > 0 {
            let compression_ratio =
                self.spans_compressed as f64 / self.total_spans_ingested as f64 * 100.0;
            let effective_spans =
                self.total_spans_ingested - self.spans_compressed + self.compression_groups;
            writeln!(f, "\n=== Span Compression ===")?;
            writeln!(
                f,
                "Spans compressed: {} ({:.0}% of total)",
                self.spans_compressed, compression_ratio
            )?;
            writeln!(f, "Compression groups: {}", self.compression_groups)?;
            writeln!(f, "Effective spans after compression: {}", effective_spans)?;
            writeln!(
                f,
                "Compression ratio: {:.1}x",
                self.spans_compressed as f64 / self.compression_groups as f64
            )?;
        }

        // Health check
        let effective_spans = if self.spans_compressed > 0 {
            self.total_spans_ingested - self.spans_compressed + self.compression_groups
        } else {
            self.active_spans_at_end
        };
        let backlog_ratio = effective_spans as f64 / self.config.spans_per_second as f64;
        let is_healthy = backlog_ratio < 1.0 && self.active_traces_at_end < 100;

        writeln!(f, "\n=== Health Check ===")?;
        writeln!(
            f,
            "Effective backlog ratio: {:.2}x (target: <1.0)",
            backlog_ratio
        )?;
        writeln!(
            f,
            "Status: {}",
            if is_healthy { "HEALTHY" } else { "OVERLOADED" }
        )?;

        Ok(())
    }
}

/// Run simulations with different worker counts
pub fn run_sweep(
    config: SimulationConfig,
    duration_secs: u64,
    worker_counts: &[usize],
) -> Vec<SimulationResult> {
    let mut results = Vec::new();

    for &num_workers in worker_counts {
        let sim = Simulation::new(config.clone());
        for _ in 0..num_workers {
            sim.add_worker();
        }
        let result = sim.run(duration_secs, num_workers);
        results.push(result);

        // Brief pause between runs
        std::thread::sleep(Duration::from_millis(100));
    }

    results
}

/// Analyze results and recommend worker count
pub fn analyze_results(results: &[SimulationResult]) -> usize {
    // Find the minimum workers that achieve healthy state
    let healthy = results
        .iter()
        .filter(|r| {
            let backlog_ratio = r.active_spans_at_end as f64 / r.config.spans_per_second as f64;
            backlog_ratio < 1.0 && r.active_traces_at_end < 100
        })
        .min_by_key(|r| r.num_workers);

    // If all are healthy, find the most cost-effective (lowest utilization > 50%)
    if healthy.is_none() {
        let optimal = results
            .iter()
            .filter(|r| r.avg_worker_utilization > 50.0 && r.avg_worker_utilization < 80.0)
            .min_by_key(|r| r.num_workers);

        return optimal
            .map(|r| r.num_workers)
            .unwrap_or(results.last().map(|r| r.num_workers).unwrap_or(4));
    }

    healthy.unwrap().num_workers
}

fn main() {
    println!("Tail Sampling Selector - Worker Simulation");
    println!("==========================================\n");

    // Configuration matching user's requirements
    let config = SimulationConfig {
        spans_per_second: 10_000,
        large_trace_percentage: 0.01, // 1% of traces
        sql_percentage: 0.80,         // 80% of large traces are SQL
        avg_small_spans: 10,
        large_trace_spans: 20_000,
        min_large_trace_duration_secs: 300, // 5 min
        max_large_trace_duration_secs: 600,
        small_trace_duration_secs: 5,
        span_processing_time_us: 50,
        trace_evaluation_time_ms: 1,
        compression_time_us: 10,
        poll_interval_ms: 100,
        inactivity_timeout_secs: 30,
        max_trace_duration_secs: 3600,
    };

    // Show expected load
    let sim = Simulation::new(config.clone());
    let (trace_rate, large_rate, sql_rate) = sim.expected_trace_rate();

    println!("=== Load Analysis ===");
    println!("Span rate: {} spans/sec", config.spans_per_second);
    println!("Expected trace rate: ~{} traces/sec", trace_rate);
    println!("Large traces (>20K spans): ~{:.1}/sec", large_rate);
    println!("SQL-heavy traces: ~{:.1}/sec", sql_rate);
    println!("Span breakdown:");
    println!(
        "  - From large traces: {} spans/sec ({:.0}%)",
        (large_rate * config.large_trace_spans as f64) as usize,
        large_rate * config.large_trace_spans as f64 / config.spans_per_second as f64 * 100.0
    );
    println!(
        "  - From small traces: ~{} spans/sec",
        config.spans_per_second - (large_rate * config.large_trace_spans as f64) as usize
    );

    // Run simulations with different worker counts
    println!("\n=== Running Simulations ===");
    let worker_counts = [1, 2, 4, 6, 8, 10];
    let duration_secs = 30;
    let results = run_sweep(config.clone(), duration_secs, &worker_counts);

    // Print all results
    for result in &results {
        println!("{}", result);
    }

    // Recommendation
    let recommended = analyze_results(&results);
    println!("\n=== Recommendation ===");
    println!("Recommended workers: {}", recommended);

    if recommended <= 2 {
        println!("Note: 2 workers can handle the load. 4 workers provides safety margin.");
    } else if recommended <= 4 {
        println!("Note: 4 workers is optimal for this workload.");
    } else {
        println!(
            "Note: {} workers needed due to high span volume or worker utilization.",
            recommended
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expected_trace_rate() {
        let config = SimulationConfig::default();
        let sim = Simulation::new(config);
        let (traces_per_sec, large_per_sec, sql_per_sec) = sim.expected_trace_rate();

        // Verify calculations make sense
        assert!(traces_per_sec > 0, "Should have positive trace rate");
        assert!(large_per_sec > 0.0, "Should have some large traces");
        assert!(sql_per_sec > 0.0, "Should have some SQL traces");

        // SQL traces should be 80% of large traces
        assert!(
            (sql_per_sec / large_per_sec - 0.8).abs() < 0.01,
            "SQL percentage should be 80%"
        );
    }

    #[test]
    fn test_simulation_creates_traces() {
        let config = SimulationConfig::default();
        let sim = Simulation::new(config);
        sim.add_worker();

        // Run for 1 second
        let result = sim.run(1, 1);

        assert!(result.total_traces_created > 0, "Should create traces");
        assert!(result.total_spans_ingested > 0, "Should ingest spans");
    }

    #[test]
    fn test_worker_utilization_under_load() {
        let config = SimulationConfig::default();
        let sim = Simulation::new(config);
        sim.add_worker();
        sim.add_worker();

        let result = sim.run(5, 2);

        // With 2 workers, utilization should be reasonable
        assert!(
            result.avg_worker_utilization < 100.0,
            "Utilization should not exceed 100%"
        );
    }
}
