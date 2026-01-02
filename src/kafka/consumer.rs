// Kafka consumer module
use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message as KafkaMessage;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub struct KafkaConsumerConfig {
    pub brokers: String,
    pub topic: String,
    pub consumer_group: String,
    pub auto_offset_reset: String,
    pub enable_auto_commit: bool,
    pub session_timeout_ms: u32,
    pub max_poll_records: usize,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_path: Option<String>,
    pub ssl_cert_path: Option<String>,
    pub ssl_key_path: Option<String>,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "otel-traces-raw".to_string(),
            consumer_group: "tail-sampling-selector".to_string(),
            auto_offset_reset: "latest".to_string(),
            enable_auto_commit: false,
            session_timeout_ms: 30000,
            max_poll_records: 500,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_path: None,
            ssl_cert_path: None,
            ssl_key_path: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaMessagePayload {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub payload: Vec<u8>,
    pub timestamp: Option<i64>,
}

pub struct PartitionAwareConsumer {
    consumer: StreamConsumer,
    config: KafkaConsumerConfig,
    shutdown_rx: broadcast::Receiver<()>,
}

impl PartitionAwareConsumer {
    pub fn new(config: KafkaConsumerConfig, shutdown_rx: broadcast::Receiver<()>) -> Result<Self> {
        let consumer = create_consumer(&config)?;
        Ok(Self {
            consumer,
            config,
            shutdown_rx,
        })
    }

    pub fn subscribe(&self) -> Result<()> {
        self.consumer
            .subscribe(&[&self.config.topic])
            .context("Failed to subscribe to topic")
    }

    pub async fn consume<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(KafkaMessagePayload) -> Result<()>,
    {
        info!("Starting Kafka consumer for topic: {}", self.config.topic);

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    info!("Shutdown signal received, stopping consumer");
                    break;
                }
                result = self.consumer.recv() => {
                    match result {
                        Ok(message) => {
                            let payload = message.payload().map(|p| p.to_vec());

                            if let Some(payload) = payload {
                                let timestamp = match message.timestamp() {
                                    rdkafka::Timestamp::CreateTime(ms) | rdkafka::Timestamp::LogAppendTime(ms) => Some(ms),
                                    rdkafka::Timestamp::NotAvailable => None,
                                };

                                let msg_payload = KafkaMessagePayload {
                                    topic: message.topic().to_string(),
                                    partition: message.partition(),
                                    offset: message.offset(),
                                    payload,
                                    timestamp,
                                };

                                if let Err(e) = callback(msg_payload) {
                                    error!("Error processing message: {}", e);
                                } else {
                                    if let Err(e) = self.consumer.commit_message(&message, CommitMode::Async) {
                                        error!("Failed to commit offset: {}", e);
                                    } else {
                                        debug!(
                                            "Committed offset {} for partition {}",
                                            message.offset(),
                                            message.partition()
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Kafka receive error: {}", e);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn consume_async<F, Fut>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(KafkaMessagePayload) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        info!("Starting Kafka consumer (async) for topic: {}", self.config.topic);

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    info!("Shutdown signal received, stopping consumer");
                    break;
                }
                result = self.consumer.recv() => {
                    match result {
                        Ok(message) => {
                            let payload = message.payload().map(|p| p.to_vec());

                            if let Some(payload) = payload {
                                let timestamp = match message.timestamp() {
                                    rdkafka::Timestamp::CreateTime(ms) | rdkafka::Timestamp::LogAppendTime(ms) => Some(ms),
                                    rdkafka::Timestamp::NotAvailable => None,
                                };

                                let msg_payload = KafkaMessagePayload {
                                    topic: message.topic().to_string(),
                                    partition: message.partition(),
                                    offset: message.offset(),
                                    payload,
                                    timestamp,
                                };

                                if let Err(e) = callback(msg_payload).await {
                                    error!("Error processing message: {}", e);
                                } else {
                                    if let Err(e) = self.consumer.commit_message(&message, CommitMode::Async) {
                                        error!("Failed to commit offset: {}", e);
                                    } else {
                                        debug!(
                                            "Committed offset {} for partition {}",
                                            message.offset(),
                                            message.partition()
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Kafka receive error: {}", e);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn create_consumer(config: &KafkaConsumerConfig) -> Result<StreamConsumer> {
    let mut client_config = ClientConfig::new();

    client_config
        .set("group.id", &config.consumer_group)
        .set("bootstrap.servers", &config.brokers)
        .set("auto.offset.reset", &config.auto_offset_reset)
        .set("broker.address.family", "v4")
        .set(
            "enable.auto.commit",
            if config.enable_auto_commit {
                "true"
            } else {
                "false"
            },
        )
        .set("session.timeout.ms", config.session_timeout_ms.to_string())
        .set("message.max.bytes", "10485760")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "100");

    if let Some(ref mechanism) = config.sasl_mechanism {
        client_config
            .set("security.protocol", "sasl_ssl")
            .set("sasl.mechanism", mechanism);

        if let Some(ref username) = config.sasl_username {
            client_config.set("sasl.username", username);
        }

        if let Some(ref password) = config.sasl_password {
            client_config.set("sasl.password", password);
        }
    }

    if let Some(ref ca_path) = config.ssl_ca_path {
        client_config.set("ssl.ca.location", ca_path);
    }

    let consumer: StreamConsumer = client_config
        .create()
        .context("Failed to create Kafka consumer")?;

    Ok(consumer)
}
