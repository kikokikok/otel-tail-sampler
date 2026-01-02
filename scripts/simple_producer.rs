//! Simple producer for testing tail sampling selector
//! Sends OTLP trace payloads to Kafka

use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

static SENT: AtomicUsize = AtomicUsize::new(0);

fn generate_trace_id() -> [u8; 16] {
    rand::thread_rng().r#gen::<[u8; 16]>()
}

fn generate_span_id() -> [u8; 8] {
    rand::thread_rng().r#gen::<[u8; 8]>()
}

fn generate_trace_payload() -> Vec<u8> {
    let trace_id = generate_trace_id();
    let span_id = generate_span_id();
    let now = SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64;

    let spans: Vec<Span> = (0..5)
        .enumerate()
        .map(|(i, _)| {
            let parent_id = if i == 0 { vec![] } else { span_id.to_vec() };

            Span {
                trace_id: trace_id.to_vec(),
                span_id: if i == 0 {
                    span_id.to_vec()
                } else {
                    generate_span_id().to_vec()
                },
                parent_span_id: parent_id,
                name: format!("test-span-{}", i),
                start_time_unix_nano: now - (i as u64 * 1_000_000),
                end_time_unix_nano: now,
                ..Default::default()
            }
        })
        .collect();

    let scope_spans = ScopeSpans {
        scope: None,
        spans,
        schema_url: String::new(),
    };

    let resource_spans = ResourceSpans {
        resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "test-service".to_string(),
                        ),
                    ),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_spans: vec![scope_spans],
        schema_url: String::new(),
    };

    let mut payload = Vec::new();
    resource_spans.encode(&mut payload).unwrap();
    payload
}

#[tokio::main]
async fn main() {
    println!("Test Producer for Tail Sampling Selector");
    println!("====================================\n");

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let topic = "otel-traces-raw";
    let num_messages = 1000;
    let rate_limit = 50; // messages per second

    println!("Configuration:");
    println!("  Brokers: {}", brokers);
    println!("  Topic: {}", topic);
    println!("  Messages: {}", num_messages);
    println!("  Rate limit: {}/sec\n", rate_limit);

    // Create Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("acks", "all")
        .create()
        .expect("Failed to create producer");

    println!("Sending messages...\n");

    let start = std::time::Instant::now();
    let interval_ms = 1000 / rate_limit;

    for i in 0..num_messages {
        let payload = generate_trace_payload();
        let key = format!("test-key-{}", i);

        let record = FutureRecord::to(topic).key(&key).payload(&payload);

        let _ = producer.send(record, Duration::from_secs(5)).await;

        SENT.fetch_add(1, Ordering::Relaxed);

        if (i + 1) % 100 == 0 {
            println!("  Sent {} messages...", i + 1);
        }

        // Rate limiting
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }

    let duration = start.elapsed();
    let sent = SENT.load(Ordering::Relaxed);
    let throughput = sent as f64 / duration.as_secs_f64();

    println!("\nDone!");
    println!("  Total sent: {}", sent);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.1} msg/sec", throughput);

    // Flush remaining
    println!("\nFlushing...");
    let _ = producer.flush(Duration::from_secs(10));
    println!("Complete.");
}
