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

fn generate_long_running_span(
    trace_id: &[u8; 16],
    parent_span_id: Option<&[u8]>,
    service_name: &str,
    operation: &str,
    span_index: usize,
    with_error: bool,
) -> Vec<u8> {
    let span_id = generate_span_id();
    let now = SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64;

    let duration_ns = ((100 + (span_index % 500)) * 1_000_000) as u64;

    let status = if with_error {
        Some(Status {
            code: StatusCode::Error as i32,
            message: format!("Batch job error at step {}", span_index),
        })
    } else {
        Some(Status {
            code: StatusCode::Ok as i32,
            message: String::new(),
        })
    };

    let span = Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: parent_span_id.map(|p| p.to_vec()).unwrap_or_default(),
        name: operation.to_string(),
        start_time_unix_nano: now - duration_ns,
        end_time_unix_nano: now,
        status,
        attributes: vec![
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "batch.step".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(
                            span_index as i64,
                        ),
                    ),
                }),
            },
            opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "batch.type".to_string(),
                value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "long_running".to_string(),
                        ),
                    ),
                }),
            },
        ],
        ..Default::default()
    };

    let scope_spans = ScopeSpans {
        scope: None,
        spans: vec![span],
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
                                "batch".to_string(),
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
    eprintln!("  --traces N          Number of traces to send (default: 10)");
    eprintln!("  --spans N           Spans per trace (default: 5)");
    eprintln!("  --batch N           Batch size for parallel sends (default: 10)");
    eprintln!("  --error-rate N      Error rate percentage (default: 10)");
    eprintln!("  --slow-rate N       Slow trace rate percentage (default: 5)");
    eprintln!("  --quiet             Suppress individual trace output");
    eprintln!("  --long-running      Enable long-running trace simulation mode");
    eprintln!("  --duration-minutes N  Duration in minutes for long-running traces (default: 30)");
    eprintln!("  --span-interval N   Seconds between spans in long-running mode (default: 30)");
    eprintln!();
    eprintln!("Environment:");
    eprintln!("  KAFKA_BROKERS       Kafka broker addresses (default: 127.0.0.1:9092)");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  # Send 100 quick traces");
    eprintln!("  simple_producer --traces 100 --spans 10");
    eprintln!();
    eprintln!("  # Simulate 3 long-running batch jobs (30 min each)");
    eprintln!("  simple_producer --long-running --traces 3 --duration-minutes 30");
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
    let mut long_running = false;
    let mut duration_minutes: u64 = 30;
    let mut span_interval_secs: u64 = 30;

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
            "--long-running" => {
                long_running = true;
                i += 1;
            }
            "--duration-minutes" => {
                duration_minutes = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(30);
                i += 2;
            }
            "--span-interval" => {
                span_interval_secs = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(30);
                i += 2;
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

    if long_running {
        run_long_running_mode(num_traces, duration_minutes, span_interval_secs, error_rate, quiet).await;
    } else {
        run_standard_mode(num_traces, spans_per_trace, batch_size, error_rate, slow_rate, quiet).await;
    }
}

const BATCH_OPERATIONS: &[&str] = &[
    "batch.etl.extract",
    "batch.etl.transform",
    "batch.etl.load",
    "batch.partition.process",
    "batch.checkpoint.save",
    "batch.validation.run",
    "batch.aggregation.compute",
    "batch.output.write",
];

async fn run_long_running_mode(
    num_traces: usize,
    duration_minutes: u64,
    span_interval_secs: u64,
    error_rate: usize,
    quiet: bool,
) {
    println!("╔════════════════════════════════════════════════════════╗");
    println!("║   Long-Running Trace Simulation (Batch Job Mode)       ║");
    println!("╚════════════════════════════════════════════════════════╝\n");

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let topic = "otel-traces-raw";

    let total_spans_per_trace = (duration_minutes * 60 / span_interval_secs) as usize;

    println!("Configuration:");
    println!("  Kafka Brokers:      {}", brokers);
    println!("  Topic:              {}", topic);
    println!("  Concurrent traces:  {}", num_traces);
    println!("  Duration per trace: {} minutes", duration_minutes);
    println!("  Span interval:      {} seconds", span_interval_secs);
    println!("  Spans per trace:    ~{}", total_spans_per_trace);
    println!("  Error rate:         {}%", error_rate);
    println!();
    println!("This will simulate {} batch job(s) running for {} minutes each.", num_traces, duration_minutes);
    println!("Press Ctrl+C to stop early.\n");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("acks", "all")
        .set("message.timeout.ms", "30000")
        .set("request.timeout.ms", "30000")
        .set("metadata.max.age.ms", "60000")
        .set("socket.timeout.ms", "30000")
        .create()
        .expect("Failed to create producer");

    let trace_ids: Vec<[u8; 16]> = (0..num_traces).map(|_| generate_trace_id()).collect();
    let root_span_ids: Vec<[u8; 8]> = (0..num_traces).map(|_| generate_span_id()).collect();

    let start_time = Instant::now();
    let total_duration = Duration::from_secs(duration_minutes * 60);
    let span_interval = Duration::from_secs(span_interval_secs);

    let sent_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let mut span_index = 0usize;

    println!("Sending spans...\n");

    while start_time.elapsed() < total_duration {
        for (trace_idx, (trace_id, root_span_id)) in trace_ids.iter().zip(root_span_ids.iter()).enumerate() {
            let with_error = span_index > 0 && (trace_idx * 100 / num_traces.max(1)) < error_rate && span_index == total_spans_per_trace - 1;
            
            if with_error {
                error_count.fetch_add(1, Ordering::SeqCst);
            }

            let service_name = format!("batch-job-{}", trace_idx);
            let operation = BATCH_OPERATIONS[span_index % BATCH_OPERATIONS.len()];
            let parent = if span_index == 0 { None } else { Some(root_span_id.as_slice()) };

            let payload = generate_long_running_span(
                trace_id,
                parent,
                &service_name,
                operation,
                span_index,
                with_error,
            );

            let trace_id_hex = hex::encode(trace_id);
            let key = format!("trace-{}", trace_id_hex);
            let record = FutureRecord::to(topic).key(&key).payload(&payload);

            match producer.send(record, Duration::from_secs(10)).await {
                Ok(_) => {
                    sent_count.fetch_add(1, Ordering::SeqCst);
                    if !quiet {
                        let elapsed = start_time.elapsed();
                        let remaining = total_duration.saturating_sub(elapsed);
                        let status = if with_error { "\x1b[31mERROR\x1b[0m" } else { "\x1b[32mOK\x1b[0m" };
                        println!(
                            "  [{}] Trace {} | Step {:>3} | {} | Remaining: {:>3}m {:>2}s",
                            status,
                            trace_idx,
                            span_index,
                            operation,
                            remaining.as_secs() / 60,
                            remaining.as_secs() % 60
                        );
                    }
                }
                Err((e, _)) => {
                    eprintln!("  Failed to send span: {:?}", e);
                }
            }
        }

        span_index += 1;

        if quiet {
            let elapsed = start_time.elapsed();
            let remaining = total_duration.saturating_sub(elapsed);
            print!(
                "\r  Progress: {} spans sent | Elapsed: {}m {}s | Remaining: {}m {}s     ",
                sent_count.load(Ordering::SeqCst),
                elapsed.as_secs() / 60,
                elapsed.as_secs() % 60,
                remaining.as_secs() / 60,
                remaining.as_secs() % 60
            );
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }

        tokio::time::sleep(span_interval).await;
    }

    if quiet {
        println!();
    }

    println!("\nFlushing...");
    producer.flush(Duration::from_secs(30)).unwrap();

    let elapsed = start_time.elapsed();
    let sent = sent_count.load(Ordering::SeqCst);
    let errors = error_count.load(Ordering::SeqCst);

    println!("\n╔════════════════════════════════════════════════════════╗");
    println!("║              Long-Running Simulation Complete          ║");
    println!("╠════════════════════════════════════════════════════════╣");
    println!("║  Traces simulated:  {:>6}                             ║", num_traces);
    println!("║  Total spans sent:  {:>6}                             ║", sent);
    println!("║  Error traces:      {:>6}                             ║", errors);
    println!("║  Duration:          {:>6.1} minutes                    ║", elapsed.as_secs_f64() / 60.0);
    println!("║  Spans per trace:   {:>6}                             ║", span_index);
    println!("╚════════════════════════════════════════════════════════╝");
    println!();
    println!("Next steps:");
    println!("  • Traces should be force-exported after max_trace_duration_secs");
    println!("  • Check metrics: curl http://localhost:9090/metrics | grep tail_sampling");
    println!("  • View buffer:   curl http://localhost:8080/admin/store/stats | jq .");
}

async fn run_standard_mode(
    num_traces: usize,
    spans_per_trace: usize,
    batch_size: usize,
    error_rate: usize,
    slow_rate: usize,
    quiet: bool,
) {
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
