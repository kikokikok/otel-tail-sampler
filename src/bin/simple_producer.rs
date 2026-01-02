use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::{Duration, SystemTime, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const SERVICES: &[&str] = &[
    "api-gateway",
    "user-service",
    "order-service",
    "payment-service",
    "inventory-service",
    "notification-service",
    "analytics-service",
    "search-service",
];

const OPERATIONS: &[&str] = &[
    "HTTP GET /api/v1/users",
    "HTTP POST /api/v1/orders",
    "HTTP GET /api/v1/products",
    "grpc.UserService/GetUser",
    "grpc.OrderService/CreateOrder",
    "postgresql.query",
    "redis.get",
    "kafka.produce",
    "s3.GetObject",
    "http.client.request",
];

fn generate_trace_id() -> [u8; 16] {
    let mut id = [0u8; 16];
    getrandom::getrandom(&mut id).unwrap();
    id
}

fn generate_span_id() -> [u8; 8] {
    let mut id = [0u8; 8];
    getrandom::getrandom(&mut id).unwrap();
    id
}

fn generate_trace_payload(
    trace_idx: usize,
    with_error: bool,
    is_slow: bool,
    spans_per_trace: usize,
) -> Vec<u8> {
    let trace_id = generate_trace_id();
    let root_span_id = generate_span_id();
    let now = SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64;

    let service_name = SERVICES[trace_idx % SERVICES.len()];

    let base_duration_ns: u64 = if is_slow {
        ((5_000 + (trace_idx % 10) * 1_000) * 1_000_000) as u64
    } else {
        ((10 + (trace_idx % 490)) * 1_000_000) as u64
    };

    let spans: Vec<Span> = (0..spans_per_trace)
        .map(|i| {
            let (span_id, parent_id) = if i == 0 {
                (root_span_id.to_vec(), vec![])
            } else {
                (generate_span_id().to_vec(), root_span_id.to_vec())
            };

            let status = if with_error && i == 0 {
                Some(Status {
                    code: StatusCode::Error as i32,
                    message: format!("Simulated error in {}", service_name),
                })
            } else {
                Some(Status {
                    code: StatusCode::Ok as i32,
                    message: String::new(),
                })
            };

            let operation = OPERATIONS[(trace_idx + i) % OPERATIONS.len()];
            let span_duration = base_duration_ns / (spans_per_trace as u64);

            Span {
                trace_id: trace_id.to_vec(),
                span_id,
                parent_span_id: parent_id,
                name: operation.to_string(),
                start_time_unix_nano: now - base_duration_ns + (i as u64 * span_duration),
                end_time_unix_nano: now - base_duration_ns + ((i + 1) as u64 * span_duration),
                status,
                attributes: vec![
                    opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "http.method".to_string(),
                        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(
                                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                    if operation.contains("GET") { "GET" } else { "POST" }.to_string(),
                                ),
                            ),
                        }),
                    },
                    opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "span.kind".to_string(),
                        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(
                                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                    if i == 0 { "server" } else { "client" }.to_string(),
                                ),
                            ),
                        }),
                    },
                ],
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
            attributes: vec![
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "service.name".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                service_name.to_string(),
                            ),
                        ),
                    }),
                },
                opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "deployment.environment".to_string(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "demo".to_string(),
                            ),
                        ),
                    }),
                },
            ],
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

fn print_usage() {
    eprintln!("Usage: simple_producer [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --traces N       Number of traces to send (default: 10)");
    eprintln!("  --spans N        Spans per trace (default: 5)");
    eprintln!("  --batch N        Batch size for parallel sends (default: 10)");
    eprintln!("  --error-rate N   Error rate percentage (default: 10)");
    eprintln!("  --slow-rate N    Slow trace rate percentage (default: 5)");
    eprintln!("  --quiet          Suppress individual trace output");
    eprintln!();
    eprintln!("Environment:");
    eprintln!("  KAFKA_BROKERS    Kafka broker addresses (default: 127.0.0.1:9092)");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    
    let mut num_traces: usize = 10;
    let mut spans_per_trace: usize = 5;
    let mut batch_size: usize = 10;
    let mut error_rate: usize = 10;
    let mut slow_rate: usize = 5;
    let mut quiet = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--traces" => {
                num_traces = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(10);
                i += 2;
            }
            "--spans" => {
                spans_per_trace = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(5);
                i += 2;
            }
            "--batch" => {
                batch_size = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(10);
                i += 2;
            }
            "--error-rate" => {
                error_rate = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(10);
                i += 2;
            }
            "--slow-rate" => {
                slow_rate = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(5);
                i += 2;
            }
            "--quiet" | "-q" => {
                quiet = true;
                i += 1;
            }
            "--help" | "-h" => {
                print_usage();
                return;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                return;
            }
        }
    }

    println!("╔════════════════════════════════════════════════════════╗");
    println!("║       Tail-Sampling Demo Trace Producer                ║");
    println!("╚════════════════════════════════════════════════════════╝\n");

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let topic = "otel-traces-raw";

    println!("Configuration:");
    println!("  Kafka Brokers:    {}", brokers);
    println!("  Topic:            {}", topic);
    println!("  Traces to send:   {}", num_traces);
    println!("  Spans per trace:  {}", spans_per_trace);
    println!("  Batch size:       {}", batch_size);
    println!("  Error rate:       {}%", error_rate);
    println!("  Slow trace rate:  {}%", slow_rate);
    println!("  Services:         {} different services", SERVICES.len());
    println!();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("acks", "all")
        .set("message.timeout.ms", "30000")
        .set("request.timeout.ms", "30000")
        .set("metadata.max.age.ms", "60000")
        .set("socket.timeout.ms", "30000")
        .set("batch.size", "65536")
        .set("linger.ms", "5")
        .create()
        .expect("Failed to create producer");

    let start_time = Instant::now();
    let sent_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let slow_count = Arc::new(AtomicUsize::new(0));
    let total_spans = Arc::new(AtomicUsize::new(0));

    println!("Sending traces...\n");

    for batch_start in (0..num_traces).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_traces);
        let mut handles = Vec::new();

        for i in batch_start..batch_end {
            let with_error = (i * 100 / num_traces.max(1)) < error_rate || i % (100 / error_rate.max(1)) == 0;
            let is_slow = (i * 100 / num_traces.max(1)) < slow_rate;
            
            if with_error {
                error_count.fetch_add(1, Ordering::SeqCst);
            }
            if is_slow {
                slow_count.fetch_add(1, Ordering::SeqCst);
            }
            total_spans.fetch_add(spans_per_trace, Ordering::SeqCst);

            let payload = generate_trace_payload(i, with_error, is_slow, spans_per_trace);
            let key = format!("trace-{}", i);
            let producer = producer.clone();
            let topic = topic.to_string();
            let sent_count = Arc::clone(&sent_count);

            let handle = tokio::spawn(async move {
                let record = FutureRecord::to(&topic).key(&key).payload(&payload);
                match producer.send(record, Duration::from_secs(10)).await {
                    Ok(_) => {
                        sent_count.fetch_add(1, Ordering::SeqCst);
                        true
                    }
                    Err((e, _)) => {
                        eprintln!("  Failed to send trace {}: {:?}", i, e);
                        false
                    }
                }
            });
            handles.push((i, with_error, is_slow, handle));
        }

        for (i, with_error, is_slow, handle) in handles {
            if let Ok(true) = handle.await {
                if !quiet {
                    let status = if with_error {
                        "\x1b[31mERROR\x1b[0m"
                    } else if is_slow {
                        "\x1b[33mSLOW\x1b[0m"
                    } else {
                        "\x1b[32mOK\x1b[0m"
                    };
                    let service = SERVICES[i % SERVICES.len()];
                    println!("  [{}] Trace {:>4} | {} | {} spans", status, i, service, spans_per_trace);
                }
            }
        }

        if quiet && batch_end % 100 == 0 {
            let elapsed = start_time.elapsed();
            let rate = sent_count.load(Ordering::SeqCst) as f64 / elapsed.as_secs_f64();
            print!("\r  Progress: {}/{} traces ({:.0} traces/sec)", batch_end, num_traces, rate);
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }

    if quiet {
        println!();
    }

    println!("\nFlushing...");
    producer.flush(Duration::from_secs(30)).unwrap();

    let elapsed = start_time.elapsed();
    let sent = sent_count.load(Ordering::SeqCst);
    let errors = error_count.load(Ordering::SeqCst);
    let slow = slow_count.load(Ordering::SeqCst);
    let spans = total_spans.load(Ordering::SeqCst);

    println!("\n╔════════════════════════════════════════════════════════╗");
    println!("║                    Summary                             ║");
    println!("╠════════════════════════════════════════════════════════╣");
    println!("║  Traces sent:       {:>6}                             ║", sent);
    println!("║  Total spans:       {:>6}                             ║", spans);
    println!("║  Error traces:      {:>6} ({:>2}%)                       ║", errors, errors * 100 / sent.max(1));
    println!("║  Slow traces:       {:>6} ({:>2}%)                       ║", slow, slow * 100 / sent.max(1));
    println!("║  Duration:          {:>6.2}s                           ║", elapsed.as_secs_f64());
    println!("║  Throughput:        {:>6.0} traces/sec                 ║", sent as f64 / elapsed.as_secs_f64());
    println!("║  Span throughput:   {:>6.0} spans/sec                  ║", spans as f64 / elapsed.as_secs_f64());
    println!("╚════════════════════════════════════════════════════════╝");
    println!();
    println!("Next steps:");
    println!("  • Wait 60s for trace inactivity timeout");
    println!("  • Check metrics: curl http://localhost:9090/metrics | grep tail_sampling");
    println!("  • Query errors:  curl http://localhost:8080/admin/query/error-traces | jq .");
    println!("  • Query stats:   curl http://localhost:8080/admin/query/service-stats | jq .");
}
