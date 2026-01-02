use crate::config::{SpanCompressionConfig, SqlPatternConfig};
use crate::decoder::decode_otlp_payload;
use crate::sampling::policies::{
    ErrorSamplingPolicy, SamplingCombinationStrategy, SamplingDecision, SamplingPolicy,
    TraceSummary, combine_policy_decisions,
};
use crate::sampling::span_compression::{SpanGrouper, process_spans_for_compression};
use crate::state::{BackpressureController, BufferedSpan, SpanKind, TraceBuffer, TraceStatus};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod decoder_tests {
    use super::*;

    #[test]
    fn test_decode_empty_payload() {
        let result = decode_otlp_payload(&[]);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_decode_invalid_payload() {
        let result = decode_otlp_payload(b"invalid protobuf");
        assert!(result.unwrap().is_none());
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;

    #[tokio::test]
    async fn test_trace_buffer_ingest() {
        let buffer = TraceBuffer::new(
            1000,
            100,
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );

        let spans = vec![BufferedSpan {
            trace_id: "trace1".to_string(),
            span_id: "span1".to_string(),
            parent_span_id: None,
            timestamp_ms: 1000,
            duration_ms: 100,
            status_code: 0,
            span_kind: SpanKind::Server,
            service_name: "test-service".to_string(),
            operation_name: "test-operation".to_string(),
            attributes: HashMap::new(),
        }];

        let (added, dropped) = buffer.ingest(&spans);
        assert_eq!(added, 1);
        assert_eq!(dropped, 0);

        let stats = buffer.stats();
        assert_eq!(stats.span_count, 1);
        assert_eq!(stats.trace_count, 1);
    }

    #[tokio::test]
    async fn test_trace_buffer_complete_traces() {
        let buffer = TraceBuffer::new(
            1000,
            100,
            Duration::from_millis(1),
            Duration::from_secs(3600),
        );

        let spans = vec![BufferedSpan {
            trace_id: "trace1".to_string(),
            span_id: "span1".to_string(),
            parent_span_id: Some("root".to_string()),
            timestamp_ms: 1000,
            duration_ms: 100,
            status_code: 0,
            span_kind: SpanKind::Server,
            service_name: "test-service".to_string(),
            operation_name: "test-operation".to_string(),
            attributes: HashMap::new(),
        }];

        buffer.ingest(&spans);

        tokio::time::sleep(Duration::from_millis(10)).await;

        let (ready, force) = buffer.mark_phase();
        assert!(ready > 0 || force > 0);

        let ready_traces = buffer.get_ready_traces();
        assert_eq!(ready_traces.len(), 1);
        assert_eq!(ready_traces[0], "trace1");
    }

    #[test]
    fn test_backpressure_controller() {
        let mut controller = BackpressureController::new(10.0, 5);

        for _ in 0..5 {
            assert!(controller.try_acquire().is_ok());
        }

        assert!(controller.try_acquire().is_err());
    }
}

#[cfg(test)]
mod policy_tests {
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
    }

    #[test]
    fn test_combination_strategies() {
        let results = vec![
            crate::sampling::policies::PolicyEvaluationResult {
                policy_name: "policy1".to_string(),
                decision: SamplingDecision::Keep,
                reason: "reason1".to_string(),
                matched_conditions: vec![],
            },
            crate::sampling::policies::PolicyEvaluationResult {
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
    }
}

#[cfg(test)]
mod compression_integration_tests {
    use super::*;

    fn create_sql_span(
        trace_id: &str,
        span_id: &str,
        query: &str,
        duration_ms: i64,
    ) -> BufferedSpan {
        let mut attributes = HashMap::new();
        attributes.insert("db.statement".to_string(), query.to_string());
        attributes.insert("db.system".to_string(), "postgresql".to_string());

        BufferedSpan {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: Some("root".to_string()),
            timestamp_ms: 1000,
            duration_ms,
            status_code: 0,
            span_kind: SpanKind::Client,
            service_name: "test-service".to_string(),
            operation_name: "postgresql.query".to_string(),
            attributes,
        }
    }

    fn create_non_sql_span(trace_id: &str, span_id: &str, op_name: &str) -> BufferedSpan {
        BufferedSpan {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: Some("root".to_string()),
            timestamp_ms: 1000,
            duration_ms: 50,
            status_code: 0,
            span_kind: SpanKind::Server,
            service_name: "test-service".to_string(),
            operation_name: op_name.to_string(),
            attributes: HashMap::new(),
        }
    }

    #[test]
    fn test_sql_compression_groups_similar_queries() {
        let config = SpanCompressionConfig {
            enabled: true,
            min_compression_count: 2,
            compression_window_secs: 60,
            max_span_duration_secs: 60,
            compress_operations: vec![],
            exclude_operations: vec![],
            sql_patterns: vec![
                SqlPatternConfig {
                    pattern: "SELECT".to_string(),
                    is_regex: false,
                    group_name: Some("select_queries".to_string()),
                },
                SqlPatternConfig {
                    pattern: "INSERT".to_string(),
                    is_regex: false,
                    group_name: Some("insert_queries".to_string()),
                },
            ],
        };
        let grouper = SpanGrouper::new(Arc::new(config));

        let spans = vec![
            create_sql_span("trace1", "span1", "SELECT * FROM users WHERE id = 1", 10),
            create_sql_span("trace1", "span2", "SELECT * FROM users WHERE id = 2", 15),
            create_sql_span("trace1", "span3", "SELECT * FROM users WHERE id = 3", 20),
            create_sql_span("trace1", "span4", "INSERT INTO logs VALUES (...)", 5),
            create_sql_span("trace1", "span5", "INSERT INTO logs VALUES (...)", 8),
            create_non_sql_span("trace1", "span6", "http.request"),
        ];

        let (compressed, uncompressed) = process_spans_for_compression(&spans, &grouper);

        assert_eq!(compressed.len(), 2);
        assert_eq!(uncompressed.len(), 1);

        // group_signature is the normalized_statement which equals the group_name from SqlPatternConfig
        let select_group = compressed
            .iter()
            .find(|c| {
                c.operation_name.contains("SELECT")
                    || c.group_signature.contains("SELECT")
                    || c.group_signature.contains("select_queries")
            })
            .unwrap();
        assert_eq!(select_group.span_count, 3);
        assert_eq!(select_group.total_duration_ms, 45);

        let insert_group = compressed
            .iter()
            .find(|c| {
                c.operation_name.contains("INSERT")
                    || c.group_signature.contains("INSERT")
                    || c.group_signature.contains("insert_queries")
            })
            .unwrap();
        assert_eq!(insert_group.span_count, 2);
        assert_eq!(insert_group.total_duration_ms, 13);
    }

    #[test]
    fn test_compression_preserves_non_sql_spans() {
        let config = SpanCompressionConfig {
            enabled: true,
            min_compression_count: 2,
            compression_window_secs: 60,
            max_span_duration_secs: 60,
            compress_operations: vec![],
            exclude_operations: vec![],
            sql_patterns: vec![SqlPatternConfig {
                pattern: "SELECT".to_string(),
                is_regex: false,
                group_name: None,
            }],
        };
        let grouper = SpanGrouper::new(Arc::new(config));

        let spans = vec![
            create_non_sql_span("trace1", "span1", "http.request"),
            create_non_sql_span("trace1", "span2", "grpc.call"),
            create_non_sql_span("trace1", "span3", "redis.get"),
        ];

        let (compressed, uncompressed) = process_spans_for_compression(&spans, &grouper);

        assert_eq!(compressed.len(), 0);
        assert_eq!(uncompressed.len(), 3);
    }

    #[test]
    fn test_compression_requires_minimum_spans() {
        let config = SpanCompressionConfig {
            enabled: true,
            min_compression_count: 5,
            compression_window_secs: 60,
            max_span_duration_secs: 60,
            compress_operations: vec![],
            exclude_operations: vec![],
            sql_patterns: vec![SqlPatternConfig {
                pattern: "SELECT".to_string(),
                is_regex: false,
                group_name: None,
            }],
        };
        let grouper = SpanGrouper::new(Arc::new(config));

        let spans = vec![
            create_sql_span("trace1", "span1", "SELECT * FROM users", 10),
            create_sql_span("trace1", "span2", "SELECT * FROM users", 15),
        ];

        let (compressed, uncompressed) = process_spans_for_compression(&spans, &grouper);

        assert_eq!(compressed.len(), 0);
        assert_eq!(uncompressed.len(), 2);
    }
}

#[cfg(test)]
mod buffer_lifecycle_tests {
    use super::*;

    #[test]
    fn test_garbage_collection_removes_stuck_traces() {
        let buffer = TraceBuffer::new(
            1000,
            100,
            Duration::from_millis(1),
            Duration::from_millis(1),
        );

        let spans = vec![BufferedSpan {
            trace_id: "trace1".to_string(),
            span_id: "span1".to_string(),
            parent_span_id: None,
            timestamp_ms: 1000,
            duration_ms: 100,
            status_code: 0,
            span_kind: SpanKind::Server,
            service_name: "test-service".to_string(),
            operation_name: "test-op".to_string(),
            attributes: HashMap::new(),
        }];

        buffer.ingest(&spans);
        buffer.mark_processing(&["trace1".to_string()]);

        std::thread::sleep(Duration::from_millis(10));

        let (stuck, _expired) =
            buffer.garbage_collect(Duration::from_millis(1), Duration::from_secs(3600));

        assert_eq!(stuck, 1);
        assert!(buffer.get_trace("trace1").is_none());
    }

    #[test]
    fn test_trace_removal_updates_counts() {
        let buffer = TraceBuffer::new(
            1000,
            100,
            Duration::from_secs(60),
            Duration::from_secs(3600),
        );

        let spans: Vec<BufferedSpan> = (0..10)
            .map(|i| BufferedSpan {
                trace_id: "trace1".to_string(),
                span_id: format!("span{}", i),
                parent_span_id: if i == 0 {
                    None
                } else {
                    Some("span0".to_string())
                },
                timestamp_ms: 1000 + i as i64,
                duration_ms: 10,
                status_code: 0,
                span_kind: SpanKind::Server,
                service_name: "test-service".to_string(),
                operation_name: "test-op".to_string(),
                attributes: HashMap::new(),
            })
            .collect();

        buffer.ingest(&spans);

        let removed = buffer.remove_traces(&["trace1".to_string()]);
        assert_eq!(removed, 10);

        assert!(buffer.get_trace("trace1").is_none());
    }

    #[test]
    fn test_error_trace_detection() {
        let buffer = TraceBuffer::new(
            1000,
            100,
            Duration::from_secs(60),
            Duration::from_secs(3600),
        );

        let spans = vec![BufferedSpan {
            trace_id: "trace1".to_string(),
            span_id: "span1".to_string(),
            parent_span_id: None,
            timestamp_ms: 1000,
            duration_ms: 100,
            status_code: 2,
            span_kind: SpanKind::Server,
            service_name: "test-service".to_string(),
            operation_name: "failing-op".to_string(),
            attributes: HashMap::new(),
        }];

        buffer.ingest(&spans);

        let status = buffer.get_trace_status("trace1");
        assert!(matches!(status, Some(TraceStatus::Active)));

        let trace = buffer.get_trace("trace1").unwrap();
        let has_error = trace.iter().any(|s| s.status_code == 2);
        assert!(has_error);
    }
}
