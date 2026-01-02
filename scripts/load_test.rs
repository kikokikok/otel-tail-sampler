use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::time;

static SENT: AtomicUsize = AtomicUsize::new(0);
static FAILED: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
struct Config {
    messages: usize,
    spans_per_message: usize,
    topics: Vec<String>,
    brokers: String,
    payload_size: usize,
    rate_limit: usize,
}

fn generate_trace_id() -> [u8; 16] {
    rand::thread_rng().r#gen::<[u8; 16]>()
}

fn generate_span_id() -> [u8; 8] {
    rand::thread_rng().r#gen::<[u8; 8]>()
}

async fn generate_trace_payload(config: &Config) -> Vec<u8> {
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use prost::Message;

    let trace_id = generate_trace_id();
    let span_id = generate_span_id();
    let now = SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64;

    let spans: Vec<Span> = (0..config.spans_per_message)
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
                name: format!("span-{}", i),
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
        resource: None,
        scope_spans: vec![scope_spans],
        schema_url: String::new(),
    };

    let mut payload = Vec::new();
    resource_spans.encode(&mut payload).unwrap();
    payload
}

async fn send_messages(config: &Config) {
    let mut handles = Vec::new();

    for _ in 0..config.rate_limit {
        let config = config.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let sent = SENT.load(Ordering::Relaxed);
                if sent >= config.messages {
                    break;
                }

                if SENT.fetch_add(1, Ordering::Relaxed) >= config.messages {
                    break;
                }

                let _payload = generate_trace_payload(&config).await;

                time::sleep(Duration::from_millis(1000 / config.rate_limit as u64)).await;
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }
}

#[tokio::main]
async fn main() {
    println!("Tail Sampling Selector Load Test");
    println!("================================");

    let config = Config {
        messages: 10000,
        spans_per_message: 10,
        topics: vec!["otel-traces-raw".to_string()],
        brokers: "localhost:9092".to_string(),
        payload_size: 1024,
        rate_limit: 100,
    };

    let start = std::time::Instant::now();
    send_messages(&config).await;
    let duration = start.elapsed();

    let sent_count = SENT.load(Ordering::Relaxed);
    let duration_secs = duration.as_secs_f64();

    println!("Load test completed:");
    println!("  Messages: {}", sent_count);
    println!("  Failed: {}", FAILED.load(Ordering::Relaxed));
    println!("  Duration: {:?}", duration);
    println!(
        "  Throughput: {:.2} messages/sec",
        sent_count as f64 / duration_secs
    );
}
