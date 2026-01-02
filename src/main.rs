// Allow dead code for config fields and infrastructure that will be used later
#![allow(dead_code)]

use crate::config::Config;
use crate::decoder::{dataframe_to_spans, decode_otlp_payload};
use crate::kafka::consumer::{KafkaConsumerConfig, KafkaMessagePayload, PartitionAwareConsumer};
use crate::observability::admin_api::{AdminApiState, create_admin_router};
use crate::observability::health::HealthState;
use crate::observability::metrics::{TraceMetrics, initialize_all_metrics};
use crate::observability::oidc::{create_oidc_state, oidc_auth_middleware};
use crate::redis::pool::RedisPool;
use crate::sampling::evaluator::{Evaluator, EvaluatorConfig};
use crate::sampling::force_sampling::ForceRuleEngine;
use crate::shutdown::ShutdownCoordinator;
use crate::state::{BackpressureController, BufferedSpan, SpanKind};
use crate::storage::{TraceStore, memory::MemoryTraceStore};
use anyhow::{Context, Result};
use axum::middleware;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, broadcast};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

mod config;
mod datadog;
mod decoder;
mod kafka;
mod observability;
mod redis;
mod sampling;
mod shutdown;
mod state;
mod storage;

#[cfg(all(test, feature = "iceberg-storage"))]
mod tests;

const VERSION: &str = "0.1.0";

async fn create_trace_store(config: &Config) -> Result<Arc<dyn TraceStore>> {
    match config.storage.storage_type.as_str() {
        #[cfg(feature = "iceberg-storage")]
        "iceberg" => {
            let iceberg_cfg = config
                .storage
                .iceberg_config()
                .context("Iceberg storage config required when storage_type = 'iceberg'. Set TSS_STORAGE__ICEBERG__CATALOG_URI and TSS_STORAGE__ICEBERG__WAREHOUSE")?;
            
            info!("Initializing Iceberg storage backend");
            info!("  Catalog URI: {}", iceberg_cfg.catalog_uri);
            info!("  Namespace: {}", iceberg_cfg.namespace);
            info!("  Table: {}", iceberg_cfg.table_name);
            
            let iceberg_config = storage::IcebergConfig::from(iceberg_cfg);
            let store = storage::IcebergTraceStore::new(iceberg_config)
                .await
                .context("Failed to initialize Iceberg trace store")?;
            
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "iceberg-storage"))]
        "iceberg" => {
            anyhow::bail!(
                "Iceberg storage requested but 'iceberg-storage' feature not enabled. \
                 Rebuild with: cargo build --features iceberg-storage"
            );
        }
        "memory" | _ => {
            info!("Initializing in-memory storage backend");
            info!("  Max spans: {}", config.app.max_buffer_spans);
            info!("  Max traces: {}", config.app.max_buffer_traces);
            
            Ok(Arc::new(MemoryTraceStore::new(
                config.app.max_buffer_spans,
                config.app.max_buffer_traces,
                config.inactivity_timeout(),
                config.max_trace_duration(),
            )))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load().context("Failed to load configuration")?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&config.observability.log_level))
        .with_target(false)
        .init();

    info!("Starting Tail Sampling Selector v{}", VERSION);
    info!("Instance ID: {}", config.app.instance_id);

    let shutdown_coordinator = ShutdownCoordinator::new();
    shutdown_coordinator.install_handlers().await?;

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let health_state = HealthState::new(VERSION.to_string(), config.app.instance_id.clone());

    let _metrics_handle = initialize_all_metrics(config.metrics_socket_addr());

    let redis_pool = Arc::new(RwLock::new(
        RedisPool::new(
            &config.redis.url,
            config.redis.pool_size,
            Duration::from_secs(config.redis.connection_timeout_secs),
            Duration::from_secs(300),
        )
        .await
        .context("Failed to connect to Redis")?,
    ));

    let trace_store = create_trace_store(&config).await?;
    info!("Storage backend: {}", trace_store.storage_type());

    let datadog_client = Arc::new(crate::datadog::client::DatadogClient::new(
        crate::datadog::client::DatadogClientConfig {
            api_endpoint: config.datadog.api_endpoint.clone(),
            api_key: config.datadog.api_key.clone(),
            application_key: config.datadog.application_key.clone(),
            batch_size: config.datadog.batch_size,
            batch_timeout: Duration::from_secs(config.datadog.batch_timeout_secs),
            request_timeout: Duration::from_secs(config.datadog.request_timeout_secs),
            max_retries: config.datadog.max_retries,
            retry_initial_delay: Duration::from_millis(config.datadog.retry_initial_delay_ms),
            max_concurrent_requests: 10,
        },
    )?);

    let evaluator_config = EvaluatorConfig {
        worker_count: config.app.evaluation_workers,
        batch_size: 100,
        combination_strategy: crate::sampling::policies::SamplingCombinationStrategy::AnyMatch,
        poll_interval: Duration::from_secs(1),
        inactivity_timeout: config.inactivity_timeout(),
        max_trace_duration: config.max_trace_duration(),
        always_sample_errors: config.sampling.always_sample_errors,
        latency_threshold_ms: config.sampling.latency_threshold_ms,
        latency_sample_rate: config.sampling.latency_sample_rate,
        sample_latency: config.sampling.sample_latency,
        max_span_count: 500,
        compression_config: if config.span_compression.enabled {
            Some(Arc::new(config.span_compression.clone()))
        } else {
            None
        },
        trace_ttl_secs: config.redis.trace_ttl_secs,
    };

    let evaluator = if config.force_sampling.enabled {
        let force_rule_engine = Arc::new(ForceRuleEngine::new(
            redis_pool.clone(),
            config.force_sampling.poll_interval_secs,
        ));
        let _force_rule_refresh_handle = force_rule_engine.start_background_refresh().await;

        let health_state_clone = health_state.clone();
        let config_clone = config.clone();
        let force_rule_engine_clone = force_rule_engine.clone();
        let trace_store_clone = trace_store.clone();
        tokio::spawn(async move {
            let health_router =
                crate::observability::health::create_health_router(health_state_clone.clone());
            let admin_state = AdminApiState {
                force_rule_engine: force_rule_engine_clone,
                trace_store: Some(trace_store_clone),
            };

            let app = if let Some(oidc_state) = create_oidc_state(&config_clone.oidc) {
                info!("OIDC authentication enabled for Admin API");
                let admin_router = create_admin_router(admin_state)
                    .layer(middleware::from_fn_with_state(oidc_state, oidc_auth_middleware));
                health_router.merge(admin_router)
            } else {
                info!("OIDC authentication disabled for Admin API");
                let admin_router = create_admin_router(admin_state);
                health_router.merge(admin_router)
            };

            let listener = tokio::net::TcpListener::bind(config_clone.health_socket_addr())
                .await
                .expect("Failed to bind health server");
            info!(
                "Health/Admin API server listening on {}",
                config_clone.health_socket_addr()
            );
            if let Err(e) = axum::serve(listener, app).await {
                error!("Health/Admin server error: {}", e);
            }
        });

        Evaluator::new(
            evaluator_config,
            trace_store.clone(),
            datadog_client.clone(),
            redis_pool.clone(),
            shutdown_tx.subscribe(),
        )
        .with_force_rules(force_rule_engine)
    } else {
        let health_state_clone = health_state.clone();
        let config_clone = config.clone();
        tokio::spawn(async move {
            let health_router =
                crate::observability::health::create_health_router(health_state_clone.clone());

            let listener = tokio::net::TcpListener::bind(config_clone.health_socket_addr())
                .await
                .expect("Failed to bind health server");
            info!(
                "Health API server listening on {}",
                config_clone.health_socket_addr()
            );
            if let Err(e) = axum::serve(listener, health_router).await {
                error!("Health server error: {}", e);
            }
        });

        Evaluator::new(
            evaluator_config,
            trace_store.clone(),
            datadog_client.clone(),
            redis_pool.clone(),
            shutdown_tx.subscribe(),
        )
    };

    let _evaluator_handles = evaluator.start();

    let kafka_config = KafkaConsumerConfig {
        brokers: config.kafka.brokers.clone(),
        topic: config.kafka.input_topic.clone(),
        consumer_group: config.kafka.consumer_group.clone(),
        auto_offset_reset: config.kafka.auto_offset_reset.clone(),
        enable_auto_commit: config.kafka.enable_auto_commit,
        session_timeout_ms: config.kafka.session_timeout_ms,
        max_poll_records: 500,
        sasl_mechanism: config.kafka.sasl_mechanism.clone(),
        sasl_username: config.kafka.sasl_username.clone(),
        sasl_password: config.kafka.sasl_password.clone(),
        ssl_ca_path: config
            .kafka
            .ssl_ca_path
            .clone()
            .map(|p| p.to_string_lossy().into_owned()),
        ssl_cert_path: config
            .kafka
            .ssl_cert_path
            .clone()
            .map(|p| p.to_string_lossy().into_owned()),
        ssl_key_path: config
            .kafka
            .ssl_key_path
            .clone()
            .map(|p| p.to_string_lossy().into_owned()),
    };

    let backpressure = Arc::new(RwLock::new(BackpressureController::new(10000.0, 5000)));
    let ingest_store = trace_store.clone();

    let _health_state = Arc::new(health_state);

    let mut consumer = PartitionAwareConsumer::new(kafka_config, shutdown_tx.subscribe())
        .context("Failed to create Kafka consumer")?;
    consumer.subscribe()?;

    info!("Consuming from Kafka topic: {}", config.kafka.input_topic);
    info!("Sampling policy:");
    info!(
        "  - Always sample errors: {}",
        config.sampling.always_sample_errors
    );
    info!("  - Inactivity timeout: {:?}", config.inactivity_timeout());
    info!("  - Max trace duration: {:?}", config.max_trace_duration());

    consumer
        .consume_async(|msg: KafkaMessagePayload| {
            let store = ingest_store.clone();
            let bp = backpressure.clone();
            async move {
                if let Err(e) = bp.write().await.try_acquire() {
                    warn!("Backpressure engaged, sleeping for {:?}", e);
                    return Ok(());
                }

                let payload = msg.payload;

                match decode_otlp_payload(&payload) {
                    Ok(Some(df)) => {
                        let spans = dataframe_to_spans(&df);

                        if spans.is_empty() {
                            return Ok(());
                        }

                        let buffered_spans: Vec<BufferedSpan> = spans
                            .into_iter()
                            .map(|s| BufferedSpan {
                                trace_id: s.trace_id,
                                span_id: s.span_id,
                                parent_span_id: s.parent_span_id,
                                timestamp_ms: s.timestamp_ms,
                                duration_ms: s.duration_ms,
                                status_code: s.status_code,
                                span_kind: SpanKind::from(s.span_kind),
                                service_name: s.service_name,
                                operation_name: s.operation_name,
                                attributes: s.attributes,
                            })
                            .collect();

                        match store.ingest(&buffered_spans).await {
                            Ok(result) => {
                                if result.added > 0 {
                                    TraceMetrics::trace_ingested();
                                    TraceMetrics::span_ingested();
                                }

                                if result.dropped > 0 {
                                    for _ in 0..result.dropped {
                                        TraceMetrics::span_dropped();
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to ingest spans: {}", e);
                            }
                        }
                    }
                    Ok(None) => {
                        warn!("Empty OTLP payload received");
                    }
                    Err(e) => {
                        error!("Failed to decode OTLP payload: {}", e);
                    }
                }

                Ok(())
            }
        })
        .await?;

    info!("Shutdown complete");
    Ok(())
}
