// Span compression/aggregation for similar operations
// Groups repeated SQL queries or API calls into analytics spans

use crate::config::SpanCompressionConfig;
use crate::state::BufferedSpan;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

/// A compressed/aggregated span representing multiple similar operations
#[derive(Debug, Clone)]
pub struct CompressedSpan {
    /// Original trace ID
    pub trace_id: String,
    /// Generated span ID for the compressed span
    pub span_id: String,
    /// First timestamp in the group
    pub timestamp_ms: i64,
    /// Last timestamp in the group
    pub last_timestamp_ms: i64,
    /// Total duration across all spans (sum of individual durations)
    pub total_duration_ms: i64,
    /// Number of spans compressed
    pub span_count: usize,
    /// Number of error spans
    pub error_count: usize,
    /// Mean duration per span
    pub mean_duration_ms: f64,
    /// Min duration observed
    pub min_duration_ms: i64,
    /// Max duration observed
    pub max_duration_ms: i64,
    /// The operation name (may be derived from group)
    pub operation_name: String,
    /// Service name
    pub service_name: String,
    /// Parent span ID if applicable
    pub parent_span_id: Option<String>,
    /// Attributes from the compressed spans
    pub attributes: HashMap<String, String>,
    /// The group signature used for matching
    pub group_signature: String,
    /// Individual span IDs that were compressed (for debugging)
    pub original_span_ids: Vec<String>,
}

impl CompressedSpan {
    /// Create a new compressed span from grouped spans
    pub fn from_spans(
        trace_id: String,
        span_id: String,
        parent_span_id: Option<String>,
        service_name: String,
        group_signature: String,
        spans: &[BufferedSpan],
    ) -> Self {
        let mut total_duration = 0i64;
        let mut min_duration = i64::MAX;
        let mut max_duration = 0i64;
        let mut error_count = 0;
        let mut first_timestamp = i64::MAX;
        let mut last_timestamp = i64::MIN;
        let mut original_span_ids = Vec::new();
        let mut merged_attributes = HashMap::new();

        // Collect attributes from first span and merge
        if let Some(first) = spans.first() {
            merged_attributes.extend(first.attributes.clone());
        }

        for span in spans {
            total_duration += span.duration_ms;
            min_duration = min_duration.min(span.duration_ms);
            max_duration = max_duration.max(span.duration_ms);
            first_timestamp = first_timestamp.min(span.timestamp_ms);
            last_timestamp = last_timestamp.max(span.timestamp_ms);

            if span.status_code == 2 {
                error_count += 1;
            }

            original_span_ids.push(span.span_id.clone());
        }

        let span_count = spans.len();
        let mean_duration_ms = if span_count > 0 {
            total_duration as f64 / span_count as f64
        } else {
            0.0
        };

        // Use operation name from first span
        let operation_name = spans
            .first()
            .map(|s| s.operation_name.clone())
            .unwrap_or_else(|| format!("compressed.{}", group_signature));

        Self {
            trace_id,
            span_id,
            parent_span_id,
            timestamp_ms: first_timestamp,
            last_timestamp_ms: last_timestamp,
            total_duration_ms: total_duration,
            span_count,
            error_count,
            mean_duration_ms,
            min_duration_ms: if min_duration == i64::MAX {
                0
            } else {
                min_duration
            },
            max_duration_ms: max_duration,
            operation_name,
            service_name,
            attributes: merged_attributes,
            group_signature,
            original_span_ids,
        }
    }

    /// Check if this compressed span should be kept (has errors or exceeds threshold)
    pub fn has_significant_content(&self) -> bool {
        self.error_count > 0 || self.max_duration_ms > 60_000 // 1 minute
    }
}

/// Grouping key for similar spans
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SpanGroupKey {
    /// Trace ID (spans are only grouped within the same trace)
    pub trace_id: String,
    /// Service name for grouping
    pub service_name: String,
    /// Operation type (e.g., "db.query", "http.request")
    pub operation_type: String,
    /// Normalized statement/query for SQL operations
    pub normalized_statement: String,
    /// Parent span ID for hierarchical grouping
    pub parent_span_id: Option<String>,
}

/// Span grouper for compressing similar spans
#[derive(Debug)]
pub struct SpanGrouper {
    config: Arc<SpanCompressionConfig>,
    /// Compiled SQL patterns
    sql_patterns: Vec<CompiledSqlPattern>,
    /// Set of operations to exclude
    excluded_operations: HashSet<String>,
}

#[derive(Debug, Clone)]
struct CompiledSqlPattern {
    pattern: String,
    regex: Option<Regex>,
    group_name: Option<String>,
}

impl SpanGrouper {
    pub fn new(config: Arc<SpanCompressionConfig>) -> Self {
        let sql_patterns = config
            .sql_patterns
            .iter()
            .map(|sp| CompiledSqlPattern {
                pattern: sp.pattern.clone(),
                regex: if sp.is_regex {
                    Regex::new(&sp.pattern).ok()
                } else {
                    None
                },
                group_name: sp.group_name.clone(),
            })
            .collect();

        let excluded_operations = config.exclude_operations.iter().cloned().collect();

        Self {
            config,
            sql_patterns,
            excluded_operations,
        }
    }

    /// Get the span group key for a span
    pub fn get_group_key(&self, span: &BufferedSpan) -> Option<SpanGroupKey> {
        // Check if operation is excluded
        if self.excluded_operations.contains(&span.operation_name) {
            return None;
        }

        // Check if operation should be compressed (if whitelist is non-empty)
        if !self.config.compress_operations.is_empty()
            && !self
                .config
                .compress_operations
                .iter()
                .any(|op| span.operation_name.contains(op) || span.service_name.contains(op))
        {
            return None;
        }

        // Check if span exceeds max duration (never compress long spans)
        let max_duration_ms = self.config.max_span_duration_secs * 1000;
        if span.duration_ms > max_duration_ms as i64 {
            debug!(
                "Skipping compression for span {} (duration {}ms > {}ms limit)",
                span.span_id, span.duration_ms, max_duration_ms
            );
            return None;
        }

        // Extract operation type from span kind or operation name
        let operation_type = self.extract_operation_type(span);

        // Normalize the statement/query
        let normalized_statement = self.normalize_statement(span);

        Some(SpanGroupKey {
            trace_id: span.trace_id.clone(),
            service_name: span.service_name.clone(),
            operation_type,
            normalized_statement,
            parent_span_id: span.parent_span_id.clone(),
        })
    }

    /// Extract operation type from span
    fn extract_operation_type(&self, span: &BufferedSpan) -> String {
        // Check for db.operation or db.system attributes
        if let Some(db_op) = span.attributes.get("db.operation") {
            return format!("db.query.{}", db_op);
        }

        if let Some(db_system) = span.attributes.get("db.system") {
            // Try to extract query type from statement
            let normalized = self.normalize_statement(span);
            let query_type = if normalized.starts_with("SELECT") {
                "select"
            } else if normalized.starts_with("INSERT") {
                "insert"
            } else if normalized.starts_with("UPDATE") {
                "update"
            } else if normalized.starts_with("DELETE") {
                "delete"
            } else {
                "query"
            };
            return format!("db.{}.{}", db_system, query_type);
        }

        // Fall back to operation name
        span.operation_name.clone()
    }

    /// Normalize SQL statement for grouping
    fn normalize_statement(&self, span: &BufferedSpan) -> String {
        // Try to match against configured SQL patterns first
        for sql_pattern in &self.sql_patterns {
            let matches = if let Some(ref regex) = sql_pattern.regex {
                regex.is_match(&span.operation_name)
                    || span.attributes.values().any(|v| regex.is_match(v))
            } else {
                span.operation_name.contains(&sql_pattern.pattern)
                    || span
                        .attributes
                        .values()
                        .any(|v| v.contains(&sql_pattern.pattern))
            };

            if matches {
                if let Some(ref group_name) = sql_pattern.group_name {
                    return group_name.clone();
                }
            }
        }

        // Generic normalization: remove literals, normalize whitespace
        let stmt = span.operation_name.clone();

        // Common SQL normalization patterns
        let number_pattern = Regex::new(r"\d+").unwrap();
        let string_pattern = Regex::new(r"'[^']*'").unwrap();
        let quote_pattern = Regex::new(r#""[^"]*""#).unwrap();

        let normalized = number_pattern.replace_all(&stmt, "?").to_string();
        let normalized = string_pattern.replace_all(&normalized, "?").to_string();
        let normalized = quote_pattern.replace_all(&normalized, "?").to_string();

        normalized
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase()
    }
}

/// Group spans by their group key
pub fn group_spans(
    spans: &[BufferedSpan],
    grouper: &SpanGrouper,
) -> HashMap<SpanGroupKey, Vec<BufferedSpan>> {
    let mut groups: HashMap<SpanGroupKey, Vec<BufferedSpan>> = HashMap::new();

    for span in spans {
        if let Some(key) = grouper.get_group_key(span) {
            groups.entry(key).or_default().push(span.clone());
        }
    }

    groups
}

/// Filter groups to only keep those meeting compression criteria
pub fn filter_compressible_groups(
    groups: HashMap<SpanGroupKey, Vec<BufferedSpan>>,
    min_compression_count: usize,
    compression_window_secs: u64,
) -> HashMap<SpanGroupKey, Vec<BufferedSpan>> {
    let mut filtered: HashMap<SpanGroupKey, Vec<BufferedSpan>> = HashMap::new();

    for (key, spans) in groups {
        if spans.len() >= min_compression_count {
            // Check time window
            let window_ms = compression_window_secs as i64 * 1000;
            let time_span = spans.iter().map(|s| s.timestamp_ms).max().unwrap_or(0)
                - spans.iter().map(|s| s.timestamp_ms).min().unwrap_or(0);

            if time_span <= window_ms {
                filtered.insert(key, spans);
            }
        }
    }

    filtered
}

/// Compress grouped spans into CompressedSpan instances
pub fn compress_groups(groups: HashMap<SpanGroupKey, Vec<BufferedSpan>>) -> Vec<CompressedSpan> {
    let mut compressed = Vec::new();

    for (key, spans) in groups {
        // Generate a new span ID for the compressed span
        let span_id = generate_compressed_span_id();

        // Create the compressed span
        let compressed_span = CompressedSpan::from_spans(
            key.trace_id.clone(),
            span_id,
            key.parent_span_id.clone(),
            key.service_name.clone(),
            key.normalized_statement.clone(),
            &spans,
        );

        compressed.push(compressed_span);
    }

    compressed
}

/// Generate a unique span ID for compressed spans
fn generate_compressed_span_id() -> String {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).unwrap_or_default();
    hex::encode_upper(bytes)
}

/// Process spans and return compressed versions along with uncompressed spans
pub fn process_spans_for_compression(
    spans: &[BufferedSpan],
    grouper: &SpanGrouper,
) -> (Vec<CompressedSpan>, Vec<BufferedSpan>) {
    let groups = group_spans(spans, grouper);
    let filtered = filter_compressible_groups(
        groups,
        grouper.config.min_compression_count,
        grouper.config.compression_window_secs,
    );
    let compressed = compress_groups(filtered);

    // Get span IDs that were compressed
    let compressed_span_ids: HashSet<String> = compressed
        .iter()
        .flat_map(|cs| cs.original_span_ids.clone())
        .collect();

    // Keep spans that weren't compressed
    let uncompressed: Vec<BufferedSpan> = spans
        .iter()
        .filter(|s| !compressed_span_ids.contains(&s.span_id))
        .cloned()
        .collect();

    (compressed, uncompressed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_span(
        trace_id: &str,
        span_id: &str,
        operation: &str,
        duration_ms: i64,
        timestamp_ms: i64,
    ) -> BufferedSpan {
        BufferedSpan {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            timestamp_ms,
            duration_ms,
            status_code: 0,
            span_kind: crate::state::SpanKind::Client,
            service_name: "test-service".to_string(),
            operation_name: operation.to_string(),
            attributes: HashMap::new(),
        }
    }

    #[test]
    fn test_span_grouping() {
        let config = Arc::new(SpanCompressionConfig {
            enabled: true,
            min_compression_count: 2,
            compression_window_secs: 60,
            max_span_duration_secs: 60,
            compress_operations: Vec::new(), // Empty = compress all operations
            exclude_operations: Vec::new(),
            sql_patterns: Vec::new(),
        });

        let grouper = SpanGrouper::new(config);

        let span1 = create_test_span(
            "trace1",
            "span1",
            "SELECT * FROM users WHERE id = ?",
            10,
            1000,
        );
        let span2 = create_test_span(
            "trace1",
            "span2",
            "SELECT * FROM users WHERE id = ?",
            15,
            1050,
        );
        let span3 = create_test_span("trace1", "span3", "INSERT INTO logs VALUES (?)", 20, 1100);

        let spans = vec![span1.clone(), span2.clone(), span3.clone()];
        let groups = group_spans(&spans, &grouper);

        // Should have 2 groups: one for SELECT, one for INSERT
        assert_eq!(groups.len(), 2);

        // First group should have 2 spans (SELECT queries)
        let select_key = groups
            .keys()
            .find(|k| k.normalized_statement.contains("SELECT"))
            .unwrap();
        assert_eq!(groups.get(select_key).unwrap().len(), 2);
    }

    #[test]
    fn test_compressed_span_stats() {
        let spans = vec![
            create_test_span("trace1", "span1", "SELECT * FROM users", 10, 1000),
            create_test_span("trace1", "span2", "SELECT * FROM users", 20, 1100),
            create_test_span("trace1", "span3", "SELECT * FROM users", 30, 1200),
        ];

        let compressed = CompressedSpan::from_spans(
            "trace1".to_string(),
            "compressed1".to_string(),
            None,
            "test-service".to_string(),
            "SELECT * FROM users".to_string(),
            &spans,
        );

        assert_eq!(compressed.span_count, 3);
        assert_eq!(compressed.total_duration_ms, 60);
        assert!((compressed.mean_duration_ms - 20.0).abs() < 0.01);
        assert_eq!(compressed.min_duration_ms, 10);
        assert_eq!(compressed.max_duration_ms, 30);
    }

    #[test]
    fn test_long_spans_not_compressed() {
        let config = Arc::new(SpanCompressionConfig {
            enabled: true,
            min_compression_count: 2,
            compression_window_secs: 60,
            max_span_duration_secs: 60, // 1 minute max
            compress_operations: Vec::new(),
            exclude_operations: Vec::new(),
            sql_patterns: Vec::new(),
        });

        let grouper = SpanGrouper::new(config);

        // Span longer than 60 seconds should not be grouped
        let long_span = create_test_span("trace1", "span1", "SELECT * FROM users", 70000, 1000);
        let key = grouper.get_group_key(&long_span);

        // Should return None because span exceeds max duration
        assert!(key.is_none());
    }
}
