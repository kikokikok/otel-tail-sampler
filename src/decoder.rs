// OTLP payload decoding utilities
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use polars::prelude::*;
use prost::Message as ProstMessage;
use std::collections::HashMap;

/// Decoded OTLP span data
#[derive(Debug, Clone)]
pub struct DecodedSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub timestamp_ms: i64,
    pub duration_ms: i64,
    pub status_code: i32,
    pub span_kind: i32,
    pub service_name: String,
    pub operation_name: String,
    pub attributes: HashMap<String, String>,
}

/// Decode OTLP payload into a DataFrame
pub fn decode_otlp_payload(
    payload: &[u8],
) -> Result<Option<DataFrame>, Box<dyn std::error::Error>> {
    let resource_spans = match ResourceSpans::decode(payload) {
        Ok(rs) => vec![rs],
        Err(e) => {
            tracing::warn!("Failed to decode ResourceSpans payload: {}", e);
            return Ok(None);
        }
    };

    let mut trace_ids = Vec::new();
    let mut span_ids = Vec::new();
    let mut parent_span_ids = Vec::new();
    let mut timestamps = Vec::new();
    let mut durations = Vec::new();
    let mut statuses = Vec::new();
    let mut span_kinds = Vec::new();
    let mut service_names = Vec::new();
    let mut operation_names = Vec::new();

    for rs in &resource_spans {
        let resource = match &rs.resource {
            Some(r) => r,
            None => continue,
        };

        let service_name = resource
            .attributes
            .iter()
            .find(|a| a.key == "service.name")
            .and_then(|a| {
                a.value.as_ref().and_then(|v| {
                    if let Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) = &v.value
                    {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
            })
            .unwrap_or_else(|| "unknown-service".to_string());

        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let trace_id = hex::encode_upper(&span.trace_id);
                let span_id = hex::encode_upper(&span.span_id);
                let parent_id = if !span.parent_span_id.is_empty() {
                    Some(hex::encode_upper(&span.parent_span_id))
                } else {
                    None
                };

                let status_code = span.status.as_ref().map_or(0, |s| s.code);
                let start_ms = span.start_time_unix_nano / 1_000_000;
                let duration_ms = (span.end_time_unix_nano - span.start_time_unix_nano) / 1_000_000;

                // Extract span name (operation)
                let operation_name = span.name.clone();

                trace_ids.push(trace_id);
                span_ids.push(span_id);
                parent_span_ids.push(parent_id.unwrap_or_default());
                timestamps.push(start_ms as i64);
                durations.push(duration_ms as i64);
                statuses.push(status_code);
                span_kinds.push(span.kind);
                service_names.push(service_name.clone());
                operation_names.push(operation_name);
            }
        }
    }

    if trace_ids.is_empty() {
        return Ok(None);
    }

    df!(
        "trace_id" => &trace_ids,
        "span_id" => &span_ids,
        "parent_span_id" => &parent_span_ids,
        "timestamp" => &timestamps,
        "duration_ms" => &durations,
        "status_code" => &statuses,
        "span_kind" => &span_kinds,
        "service_name" => &service_names,
        "operation_name" => &operation_names
    )
    .map(Some)
    .map_err(Into::into)
}

/// Convert DataFrame rows to DecodedSpan list
pub fn dataframe_to_spans(df: &DataFrame) -> Vec<DecodedSpan> {
    let trace_ids: Vec<String> = df
        .column("trace_id")
        .and_then(|c| c.str())
        .map(|c| c.into_no_null_iter().map(|s| s.to_string()).collect())
        .unwrap_or_default();

    let span_ids: Vec<String> = df
        .column("span_id")
        .and_then(|c| c.str())
        .map(|c| c.into_no_null_iter().map(|s| s.to_string()).collect())
        .unwrap_or_default();

    let parent_span_ids: Vec<String> = df
        .column("parent_span_id")
        .and_then(|c| c.str())
        .map(|c| c.into_no_null_iter().map(|s| s.to_string()).collect())
        .unwrap_or_default();

    let timestamps: Vec<i64> = df
        .column("timestamp")
        .and_then(|c| c.i64())
        .map(|c| c.into_iter().flatten().collect())
        .unwrap_or_default();

    let durations: Vec<i64> = df
        .column("duration_ms")
        .and_then(|c| c.i64())
        .map(|c| c.into_iter().flatten().collect())
        .unwrap_or_default();

    let statuses: Vec<i32> = df
        .column("status_code")
        .and_then(|c| c.i32())
        .map(|c| c.into_iter().flatten().collect())
        .unwrap_or_default();

    let kinds: Vec<i32> = df
        .column("span_kind")
        .and_then(|c| c.i32())
        .map(|c| c.into_iter().flatten().collect())
        .unwrap_or_default();

    let services: Vec<String> = df
        .column("service_name")
        .and_then(|c| c.str())
        .map(|c| c.into_no_null_iter().map(|s| s.to_string()).collect())
        .unwrap_or_default();

    let operations: Vec<String> = df
        .column("operation_name")
        .and_then(|c| c.str())
        .map(|c| c.into_no_null_iter().map(|s| s.to_string()).collect())
        .unwrap_or_default();

    trace_ids
        .into_iter()
        .zip(span_ids)
        .zip(parent_span_ids)
        .zip(timestamps)
        .zip(durations)
        .zip(statuses)
        .zip(kinds)
        .zip(services)
        .zip(operations)
        .map(
            |(
                (
                    ((((((trace_id, span_id), parent_id), timestamp), duration), status), kind),
                    service,
                ),
                operation,
            )| {
                DecodedSpan {
                    trace_id,
                    span_id,
                    parent_span_id: if parent_id.is_empty() {
                        None
                    } else {
                        Some(parent_id)
                    },
                    timestamp_ms: timestamp,
                    duration_ms: duration,
                    status_code: status,
                    span_kind: kind,
                    service_name: service,
                    operation_name: operation,
                    attributes: HashMap::new(),
                }
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
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
