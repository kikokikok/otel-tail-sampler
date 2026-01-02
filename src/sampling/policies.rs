// Sampling policies for tail-based trace sampling
use serde::{Deserialize, Serialize};

/// Sampling decision for a trace
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SamplingDecision {
    /// Keep the trace
    Keep,
    /// Drop the trace
    Drop,
    /// Need more time to decide (trace not complete)
    Pending,
}

/// Configuration for a sampling policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingPolicyConfig {
    pub name: String,
    pub enabled: bool,
    pub rate: f64,
    pub conditions: Vec<PolicyCondition>,
}

/// Condition for a sampling policy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PolicyCondition {
    /// Error status condition
    Error {
        /// Include error traces
        include_errors: bool,
    },
    /// Latency condition
    Latency {
        /// Threshold in milliseconds
        threshold_ms: u64,
        /// Include traces exceeding threshold
        above_threshold: bool,
    },
    /// Duration condition
    Duration {
        /// Minimum duration in milliseconds
        min_ms: u64,
        /// Maximum duration in milliseconds
        max_ms: u64,
    },
    /// Service name condition
    Service {
        /// Services to match
        services: Vec<String>,
        /// Match type
        match_type: MatchType,
    },
    /// Operation name condition
    Operation {
        /// Operations to match
        operations: Vec<String>,
        /// Match type
        match_type: MatchType,
    },
    /// Cardinality condition
    Cardinality {
        /// Maximum span count
        max_span_count: usize,
        /// Force sample if exceeded
        force_sample: bool,
    },
    /// Boolean combination
    And { conditions: Vec<PolicyCondition> },
    /// Boolean negation
    Not { condition: Box<PolicyCondition> },
    /// Boolean OR
    Or { conditions: Vec<PolicyCondition> },
}

/// Match type for string conditions
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MatchType {
    /// Exact match
    Exact,
    /// Prefix match
    Prefix,
    /// Suffix match
    Suffix,
    /// Regex match
    Regex(String), // Store pattern string, compile lazily
}

impl MatchType {
    pub fn matches(&self, pattern: &str, value: &str) -> bool {
        match self {
            MatchType::Exact => value == pattern,
            MatchType::Prefix => value.starts_with(pattern),
            MatchType::Suffix => value.ends_with(pattern),
            MatchType::Regex(regex_pattern) => {
                regex::Regex::new(regex_pattern).is_ok_and(|re| re.is_match(value))
            }
        }
    }
}

impl std::fmt::Debug for MatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchType::Exact => write!(f, "Exact"),
            MatchType::Prefix => write!(f, "Prefix"),
            MatchType::Suffix => write!(f, "Suffix"),
            MatchType::Regex(pattern) => write!(f, "Regex({})", pattern),
        }
    }
}

/// Result of evaluating a policy
#[derive(Debug, Clone)]
pub struct PolicyEvaluationResult {
    pub policy_name: String,
    pub decision: SamplingDecision,
    pub reason: String,
    pub matched_conditions: Vec<String>,
}

/// Trait for sampling policy evaluators
pub trait SamplingPolicy: Send + Sync {
    /// Get the policy name
    fn name(&self) -> &str;

    /// Evaluate a trace against this policy
    fn evaluate(&self, trace: &TraceSummary) -> PolicyEvaluationResult;
}

/// Summary of a trace for sampling evaluation
#[derive(Debug, Clone)]
pub struct TraceSummary {
    pub trace_id: String,
    pub service_name: String,
    pub span_count: usize,
    pub has_error: bool,
    pub max_duration_ms: i64,
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub operations: Vec<String>,
    pub total_spans: usize,
    pub error_count: usize,
    pub root_span_id: Option<String>,
}

/// Default sampling policies
#[derive(Debug, Clone)]
pub struct DefaultSamplingPolicies {
    pub error_policy: ErrorSamplingPolicy,
    pub latency_policy: LatencySamplingPolicy,
    pub cardinality_policy: CardinalitySamplingPolicy,
}

impl DefaultSamplingPolicies {
    pub fn new(
        error_rate: f64,
        latency_threshold_ms: u64,
        latency_rate: f64,
        max_span_count: usize,
    ) -> Self {
        Self {
            error_policy: ErrorSamplingPolicy {
                sample_rate: error_rate,
            },
            latency_policy: LatencySamplingPolicy {
                threshold_ms: latency_threshold_ms,
                sample_rate: latency_rate,
            },
            cardinality_policy: CardinalitySamplingPolicy {
                max_span_count,
                force_sample: true,
            },
        }
    }

    /// Evaluate all policies against a trace
    pub fn evaluate(&self, trace: &TraceSummary) -> Vec<PolicyEvaluationResult> {
        vec![
            self.error_policy.evaluate(trace),
            self.latency_policy.evaluate(trace),
            self.cardinality_policy.evaluate(trace),
        ]
    }
}

/// Error sampling policy
#[derive(Debug, Clone)]
pub struct ErrorSamplingPolicy {
    pub sample_rate: f64,
}

impl SamplingPolicy for ErrorSamplingPolicy {
    fn name(&self) -> &str {
        "error_sampling"
    }

    fn evaluate(&self, trace: &TraceSummary) -> PolicyEvaluationResult {
        if trace.has_error {
            let should_sample = fastrand::f64() < self.sample_rate;
            PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: if should_sample {
                    SamplingDecision::Keep
                } else {
                    SamplingDecision::Drop
                },
                reason: format!(
                    "Error trace sampled at rate {:.2}: {} errors found",
                    self.sample_rate, trace.error_count
                ),
                matched_conditions: vec!["has_error".to_string()],
            }
        } else {
            PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: SamplingDecision::Drop,
                reason: "No errors found".to_string(),
                matched_conditions: vec![],
            }
        }
    }
}

/// Latency sampling policy
#[derive(Debug, Clone)]
pub struct LatencySamplingPolicy {
    pub threshold_ms: u64,
    pub sample_rate: f64,
}

impl SamplingPolicy for LatencySamplingPolicy {
    fn name(&self) -> &str {
        "latency_sampling"
    }

    fn evaluate(&self, trace: &TraceSummary) -> PolicyEvaluationResult {
        let exceeds_threshold = trace.max_duration_ms as u64 > self.threshold_ms;

        if exceeds_threshold {
            let should_sample = fastrand::f64() < self.sample_rate;
            PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: if should_sample {
                    SamplingDecision::Keep
                } else {
                    SamplingDecision::Drop
                },
                reason: format!(
                    "Latency {}ms > {}ms threshold, sampled at rate {:.2}",
                    trace.max_duration_ms, self.threshold_ms, self.sample_rate
                ),
                matched_conditions: vec![format!("latency_ms > {}", self.threshold_ms)],
            }
        } else {
            PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: SamplingDecision::Drop,
                reason: format!(
                    "Latency {}ms <= {}ms threshold",
                    trace.max_duration_ms, self.threshold_ms
                ),
                matched_conditions: vec![],
            }
        }
    }
}

/// Cardinality sampling policy
#[derive(Debug, Clone)]
pub struct CardinalitySamplingPolicy {
    pub max_span_count: usize,
    pub force_sample: bool,
}

impl SamplingPolicy for CardinalitySamplingPolicy {
    fn name(&self) -> &str {
        "cardinality_sampling"
    }

    fn evaluate(&self, trace: &TraceSummary) -> PolicyEvaluationResult {
        let exceeds_limit = trace.span_count > self.max_span_count;

        if exceeds_limit {
            PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: if self.force_sample {
                    SamplingDecision::Keep
                } else {
                    let should_sample = fastrand::f64() < 1.0; // Always sample when forced
                    SamplingDecision::from_bool(should_sample)
                },
                reason: format!(
                    "Span count {} > {} max, {}",
                    trace.span_count,
                    self.max_span_count,
                    if self.force_sample {
                        "forced sample"
                    } else {
                        "random sample"
                    }
                ),
                matched_conditions: vec![format!("span_count > {}", self.max_span_count)],
            }
        } else {
            PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: SamplingDecision::Drop,
                reason: format!(
                    "Span count {} <= {} max",
                    trace.span_count, self.max_span_count
                ),
                matched_conditions: vec![],
            }
        }
    }
}

/// Custom policy from configuration
#[derive(Debug, Clone)]
pub struct ConfigurablePolicy {
    pub config: SamplingPolicyConfig,
    compiled_conditions: Vec<CompiledCondition>,
}

impl ConfigurablePolicy {
    pub fn new(config: SamplingPolicyConfig) -> Self {
        let compiled_conditions = config.conditions.iter().map(compile_condition).collect();

        Self {
            config,
            compiled_conditions,
        }
    }
}

enum CompiledCondition {
    Error(CompiledErrorCondition),
    Latency(CompiledLatencyCondition),
    Duration(CompiledDurationCondition),
    Service(CompiledServiceCondition),
    Operation(CompiledOperationCondition),
    Cardinality(CompiledCardinalityCondition),
    And(Vec<CompiledCondition>),
    Or(Vec<CompiledCondition>),
    Not(Box<CompiledCondition>),
}

impl Clone for CompiledCondition {
    fn clone(&self) -> Self {
        match self {
            CompiledCondition::Error(c) => CompiledCondition::Error(CompiledErrorCondition {
                include_errors: c.include_errors,
            }),
            CompiledCondition::Latency(c) => CompiledCondition::Latency(CompiledLatencyCondition {
                threshold_ms: c.threshold_ms,
                above_threshold: c.above_threshold,
            }),
            CompiledCondition::Duration(c) => {
                CompiledCondition::Duration(CompiledDurationCondition {
                    min_ms: c.min_ms,
                    max_ms: c.max_ms,
                })
            }
            CompiledCondition::Service(c) => CompiledCondition::Service(CompiledServiceCondition {
                services: c.services.clone(),
                match_type: c.match_type.clone(),
            }),
            CompiledCondition::Operation(c) => {
                CompiledCondition::Operation(CompiledOperationCondition {
                    operations: c.operations.clone(),
                    match_type: c.match_type.clone(),
                })
            }
            CompiledCondition::Cardinality(c) => {
                CompiledCondition::Cardinality(CompiledCardinalityCondition {
                    max_span_count: c.max_span_count,
                    force_sample: c.force_sample,
                })
            }
            CompiledCondition::And(conditions) => CompiledCondition::And(conditions.clone()),
            CompiledCondition::Or(conditions) => CompiledCondition::Or(conditions.clone()),
            CompiledCondition::Not(condition) => CompiledCondition::Not(condition.clone()),
        }
    }
}

impl std::fmt::Debug for CompiledCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompiledCondition::Error(_) => write!(f, "Error(...)"),
            CompiledCondition::Latency(_) => write!(f, "Latency(...)"),
            CompiledCondition::Duration(_) => write!(f, "Duration(...)"),
            CompiledCondition::Service(_) => write!(f, "Service(...)"),
            CompiledCondition::Operation(_) => write!(f, "Operation(...)"),
            CompiledCondition::Cardinality(_) => write!(f, "Cardinality(...)"),
            CompiledCondition::And(conditions) => write!(f, "And({:?})", conditions),
            CompiledCondition::Or(conditions) => write!(f, "Or({:?})", conditions),
            CompiledCondition::Not(condition) => write!(f, "Not({:?})", condition),
        }
    }
}

struct CompiledErrorCondition {
    include_errors: bool,
}

struct CompiledLatencyCondition {
    threshold_ms: u64,
    above_threshold: bool,
}

struct CompiledDurationCondition {
    min_ms: u64,
    max_ms: u64,
}

struct CompiledServiceCondition {
    services: Vec<String>,
    match_type: MatchType,
}

struct CompiledOperationCondition {
    operations: Vec<String>,
    match_type: MatchType,
}

struct CompiledCardinalityCondition {
    max_span_count: usize,
    force_sample: bool,
}

fn compile_condition(condition: &PolicyCondition) -> CompiledCondition {
    match condition {
        PolicyCondition::Error { include_errors } => {
            CompiledCondition::Error(CompiledErrorCondition {
                include_errors: *include_errors,
            })
        }
        PolicyCondition::Latency {
            threshold_ms,
            above_threshold,
        } => CompiledCondition::Latency(CompiledLatencyCondition {
            threshold_ms: *threshold_ms,
            above_threshold: *above_threshold,
        }),
        PolicyCondition::Duration { min_ms, max_ms } => {
            CompiledCondition::Duration(CompiledDurationCondition {
                min_ms: *min_ms,
                max_ms: *max_ms,
            })
        }
        PolicyCondition::Service {
            services,
            match_type,
        } => CompiledCondition::Service(CompiledServiceCondition {
            services: services.clone(),
            match_type: match_type.clone(),
        }),
        PolicyCondition::Operation {
            operations,
            match_type,
        } => CompiledCondition::Operation(CompiledOperationCondition {
            operations: operations.clone(),
            match_type: match_type.clone(),
        }),
        PolicyCondition::Cardinality {
            max_span_count,
            force_sample,
        } => CompiledCondition::Cardinality(CompiledCardinalityCondition {
            max_span_count: *max_span_count,
            force_sample: *force_sample,
        }),
        PolicyCondition::And { conditions } => {
            CompiledCondition::And(conditions.iter().map(compile_condition).collect())
        }
        PolicyCondition::Or { conditions } => {
            CompiledCondition::Or(conditions.iter().map(compile_condition).collect())
        }
        PolicyCondition::Not { condition } => {
            CompiledCondition::Not(Box::new(compile_condition(condition)))
        }
    }
}

impl SamplingDecision {
    pub fn from_bool(value: bool) -> Self {
        if value {
            SamplingDecision::Keep
        } else {
            SamplingDecision::Drop
        }
    }
}

impl SamplingPolicy for ConfigurablePolicy {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn evaluate(&self, trace: &TraceSummary) -> PolicyEvaluationResult {
        if !self.config.enabled {
            return PolicyEvaluationResult {
                policy_name: self.name().to_string(),
                decision: SamplingDecision::Drop,
                reason: "Policy disabled".to_string(),
                matched_conditions: vec![],
            };
        }

        let matches = self
            .compiled_conditions
            .iter()
            .any(|c| evaluate_condition(c, trace));

        let should_sample = if matches {
            fastrand::f64() < self.config.rate
        } else {
            false
        };

        PolicyEvaluationResult {
            policy_name: self.name().to_string(),
            decision: SamplingDecision::from_bool(should_sample),
            reason: if matches {
                format!(
                    "Matched conditions, sampled at rate {:.2}",
                    self.config.rate
                )
            } else {
                "No conditions matched".to_string()
            },
            matched_conditions: if matches {
                vec!["custom_condition".to_string()]
            } else {
                vec![]
            },
        }
    }
}

fn evaluate_condition(condition: &CompiledCondition, trace: &TraceSummary) -> bool {
    match condition {
        CompiledCondition::Error(c) => trace.has_error == c.include_errors,
        CompiledCondition::Latency(c) => {
            let exceeds = trace.max_duration_ms as u64 > c.threshold_ms;
            exceeds == c.above_threshold
        }
        CompiledCondition::Duration(c) => {
            trace.max_duration_ms as u64 >= c.min_ms && trace.max_duration_ms as u64 <= c.max_ms
        }
        CompiledCondition::Service(c) => c
            .services
            .iter()
            .any(|s| c.match_type.matches(s, &trace.service_name)),
        CompiledCondition::Operation(c) => c.operations.iter().any(|op| {
            trace
                .operations
                .iter()
                .any(|trace_op| c.match_type.matches(op, trace_op))
        }),
        CompiledCondition::Cardinality(c) => trace.span_count > c.max_span_count,
        CompiledCondition::And(conditions) => {
            conditions.iter().all(|c| evaluate_condition(c, trace))
        }
        CompiledCondition::Or(conditions) => {
            conditions.iter().any(|c| evaluate_condition(c, trace))
        }
        CompiledCondition::Not(condition) => !evaluate_condition(condition, trace),
    }
}

/// Combine multiple policy results using a strategy
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "strategy")]
pub enum SamplingCombinationStrategy {
    #[default]
    AnyMatch,
    AllMatch,
    Consensus {
        threshold: f64,
    },
    Priority,
}

/// Combine policy results into a final decision
pub fn combine_policy_decisions(
    results: &[PolicyEvaluationResult],
    strategy: &SamplingCombinationStrategy,
) -> SamplingDecision {
    match strategy {
        SamplingCombinationStrategy::AnyMatch => {
            if results.iter().any(|r| r.decision == SamplingDecision::Keep) {
                SamplingDecision::Keep
            } else {
                SamplingDecision::Drop
            }
        }
        SamplingCombinationStrategy::AllMatch => {
            if results.iter().all(|r| r.decision == SamplingDecision::Keep) {
                SamplingDecision::Keep
            } else {
                SamplingDecision::Drop
            }
        }
        SamplingCombinationStrategy::Consensus { threshold } => {
            let keep_count = results
                .iter()
                .filter(|r| r.decision == SamplingDecision::Keep)
                .count();
            let total = results.len();
            if total > 0 && (keep_count as f64 / total as f64) >= *threshold {
                SamplingDecision::Keep
            } else {
                SamplingDecision::Drop
            }
        }
        SamplingCombinationStrategy::Priority => {
            for result in results {
                if result.decision == SamplingDecision::Keep {
                    return SamplingDecision::Keep;
                }
            }
            SamplingDecision::Drop
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_sampling_policy() {
        let policy = ErrorSamplingPolicy { sample_rate: 1.0 };

        let error_trace = TraceSummary {
            trace_id: "trace1".to_string(),
            service_name: "test-service".to_string(),
            span_count: 10,
            has_error: true,
            max_duration_ms: 100,
            min_timestamp_ms: 0,
            max_timestamp_ms: 100,
            operations: vec!["op1".to_string()],
            total_spans: 10,
            error_count: 1,
            root_span_id: Some("root1".to_string()),
        };

        let result = policy.evaluate(&error_trace);
        assert_eq!(result.decision, SamplingDecision::Keep);

        let clean_trace = TraceSummary {
            trace_id: "trace2".to_string(),
            service_name: "test-service".to_string(),
            span_count: 10,
            has_error: false,
            max_duration_ms: 100,
            min_timestamp_ms: 0,
            max_timestamp_ms: 100,
            operations: vec!["op1".to_string()],
            total_spans: 10,
            error_count: 0,
            root_span_id: Some("root2".to_string()),
        };

        let result = policy.evaluate(&clean_trace);
        assert_eq!(result.decision, SamplingDecision::Drop);
    }

    #[test]
    fn test_latency_sampling_policy() {
        let policy = LatencySamplingPolicy {
            threshold_ms: 100,
            sample_rate: 1.0,
        };

        let slow_trace = TraceSummary {
            trace_id: "trace1".to_string(),
            service_name: "test-service".to_string(),
            span_count: 10,
            has_error: false,
            max_duration_ms: 200,
            min_timestamp_ms: 0,
            max_timestamp_ms: 200,
            operations: vec!["op1".to_string()],
            total_spans: 10,
            error_count: 0,
            root_span_id: Some("root1".to_string()),
        };

        let result = policy.evaluate(&slow_trace);
        assert_eq!(result.decision, SamplingDecision::Keep);

        let fast_trace = TraceSummary {
            trace_id: "trace2".to_string(),
            service_name: "test-service".to_string(),
            span_count: 10,
            has_error: false,
            max_duration_ms: 50,
            min_timestamp_ms: 0,
            max_timestamp_ms: 50,
            operations: vec!["op1".to_string()],
            total_spans: 10,
            error_count: 0,
            root_span_id: Some("root2".to_string()),
        };

        let result = policy.evaluate(&fast_trace);
        assert_eq!(result.decision, SamplingDecision::Drop);
    }

    #[test]
    fn test_combination_strategies() {
        let results = vec![
            PolicyEvaluationResult {
                policy_name: "policy1".to_string(),
                decision: SamplingDecision::Keep,
                reason: "reason1".to_string(),
                matched_conditions: vec![],
            },
            PolicyEvaluationResult {
                policy_name: "policy2".to_string(),
                decision: SamplingDecision::Drop,
                reason: "reason2".to_string(),
                matched_conditions: vec![],
            },
        ];

        assert_eq!(
            SamplingDecision::Keep,
            combine_policy_decisions(&results, &SamplingCombinationStrategy::AnyMatch)
        );
        assert_eq!(
            SamplingDecision::Drop,
            combine_policy_decisions(&results, &SamplingCombinationStrategy::AllMatch)
        );
        assert_eq!(
            SamplingDecision::Keep,
            combine_policy_decisions(
                &results,
                &SamplingCombinationStrategy::Consensus { threshold: 0.3 }
            )
        );
        assert_eq!(
            SamplingDecision::Keep,
            combine_policy_decisions(&results, &SamplingCombinationStrategy::Priority)
        );
    }
}
