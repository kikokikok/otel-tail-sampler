use anyhow::{Context, Result};
use futures::StreamExt;
use redis::{Client as RedisClient, RedisResult, ToRedisArgs, aio::ConnectionManager};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

pub struct PooledConnection {
    connection: ConnectionManager,
    _permit: OwnedSemaphorePermit,
}

impl PooledConnection {
    pub fn connection(&self) -> &ConnectionManager {
        &self.connection
    }

    pub fn connection_mut(&mut self) -> &mut ConnectionManager {
        &mut self.connection
    }
}

pub struct RedisPool {
    client: RedisClient,
    semaphore: Arc<Semaphore>,
    connection_timeout: Duration,
    pool_size: usize,
}

impl RedisPool {
    pub async fn new(
        url: &str,
        pool_size: usize,
        connection_timeout: Duration,
        _max_lifetime: Duration,
    ) -> Result<Self> {
        let client = RedisClient::open(url)
            .with_context(|| format!("Failed to create Redis client for {}", url))?;

        let mut conn = timeout(connection_timeout, client.get_connection_manager())
            .await
            .context("Redis connection timeout")?
            .context("Failed to connect to Redis")?;

        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .context("Redis ping failed")?;
        if pong != "PONG" {
            return Err(anyhow::anyhow!("Redis ping failed: unexpected response"));
        }

        debug!("Successfully connected to Redis");

        Ok(Self {
            client,
            semaphore: Arc::new(Semaphore::new(pool_size)),
            connection_timeout,
            pool_size,
        })
    }

    pub async fn get(&self) -> Result<PooledConnection> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("Pool closed"))?;

        let connection = timeout(
            self.connection_timeout,
            self.client.get_connection_manager(),
        )
        .await
        .context("Redis connection timeout")?
        .context("Failed to get connection from pool")?;

        Ok(PooledConnection {
            connection,
            _permit: permit,
        })
    }

    pub async fn ping(&self) -> Result<bool> {
        let mut conn = self.get().await?;
        let result: RedisResult<String> =
            redis::cmd("PING").query_async(&mut conn.connection).await;

        match result {
            Ok(s) if s == "PONG" => Ok(true),
            Ok(_) => Ok(false),
            Err(e) => {
                error!("Redis ping failed: {}", e);
                Err(e.into())
            }
        }
    }

    pub fn available_connections(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    pub async fn health_check(&self) -> bool {
        self.ping().await.unwrap_or(false)
    }

    pub async fn publish(&self, channel: &str, message: &str) -> Result<i64> {
        let mut conn = self.get().await?;
        let result: RedisResult<i64> = redis::cmd("PUBLISH")
            .arg(channel)
            .arg(message)
            .query_async(conn.connection_mut())
            .await;
        result.context("Failed to publish message")
    }

    pub fn get_client(&self) -> &RedisClient {
        &self.client
    }
}

pub struct PubSubListener {
    client: RedisClient,
    channel: String,
}

impl PubSubListener {
    pub async fn new(client: &RedisClient, channel: &str) -> Result<Self> {
        Ok(Self {
            client: client.clone(),
            channel: channel.to_string(),
        })
    }

    pub async fn subscribe(
        &self,
        on_message: mpsc::Sender<String>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let client = self.client.clone();
        let channel = self.channel.clone();

        let handle = tokio::spawn(async move {
            loop {
                match Self::run_subscription(&client, &channel, &on_message).await {
                    Ok(()) => {
                        info!("PubSub subscription ended normally for channel {}", channel);
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "PubSub subscription error for channel {}: {}. Reconnecting in 1s...",
                            channel, e
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(handle)
    }

    async fn run_subscription(
        client: &RedisClient,
        channel: &str,
        on_message: &mpsc::Sender<String>,
    ) -> Result<()> {
        let conn = client
            .get_async_pubsub()
            .await
            .context("Failed to create PubSub connection")?;

        let mut pubsub = conn;
        pubsub
            .subscribe(channel)
            .await
            .context("Failed to subscribe to channel")?;

        info!("Subscribed to Redis channel: {}", channel);

        let mut stream = pubsub.on_message();

        while let Some(msg) = stream.next().await {
            let payload: String = msg.get_payload().unwrap_or_default();
            debug!("Received PubSub message on {}: {}", channel, payload);

            if on_message.send(payload).await.is_err() {
                debug!("Message receiver dropped, ending subscription");
                break;
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
pub trait RedisPoolExt {
    async fn add_trace_ttl(&mut self, trace_id: &str, timestamp_ms: i64) -> Result<()>;
    async fn remove_trace_ttl(&mut self, trace_ids: &[String]) -> Result<()>;
    async fn get_expired_trace_ids(&mut self, cutoff_ms: i64) -> Result<Vec<String>>;
    async fn get_active_trace_ids(&mut self, window_ms: i64) -> Result<Vec<String>>;
}

#[async_trait::async_trait]
impl RedisPoolExt for PooledConnection {
    async fn add_trace_ttl(&mut self, trace_id: &str, timestamp_ms: i64) -> Result<()> {
        let result: RedisResult<()> = redis::cmd("ZADD")
            .arg("trace_ttl")
            .arg(timestamp_ms)
            .arg(trace_id)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to add trace TTL")
    }

    async fn remove_trace_ttl(&mut self, trace_ids: &[String]) -> Result<()> {
        if trace_ids.is_empty() {
            return Ok(());
        }

        let result: RedisResult<()> = redis::cmd("ZREM")
            .arg("trace_ttl")
            .arg(trace_ids)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to remove trace TTL")
    }

    async fn get_expired_trace_ids(&mut self, cutoff_ms: i64) -> Result<Vec<String>> {
        let result: RedisResult<Vec<String>> = redis::cmd("ZRANGEBYSCORE")
            .arg("trace_ttl")
            .arg("-inf")
            .arg(cutoff_ms)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to get expired trace IDs")
    }

    async fn get_active_trace_ids(&mut self, window_ms: i64) -> Result<Vec<String>> {
        let now = chrono::Utc::now().timestamp_millis();
        let window_start = now - window_ms;

        let result: RedisResult<Vec<String>> = redis::cmd("ZRANGEBYSCORE")
            .arg("trace_ttl")
            .arg(window_start)
            .arg("+inf")
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to get active trace IDs")
    }
}

impl PooledConnection {
    pub async fn incr<K: ToRedisArgs>(&mut self, key: K) -> Result<i64> {
        let result: RedisResult<i64> = redis::cmd("INCR")
            .arg(key)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to increment counter")
    }

    pub async fn set_ex<K: ToRedisArgs, V: ToRedisArgs>(
        &mut self,
        key: K,
        value: V,
        seconds: u64,
    ) -> Result<()> {
        let result: RedisResult<()> = redis::cmd("SETEX")
            .arg(key)
            .arg(seconds)
            .arg(value)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to set key with expiration")
    }

    pub async fn get<K: ToRedisArgs>(&mut self, key: K) -> Result<Option<String>> {
        let result: RedisResult<Option<String>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to get value")
    }

    pub async fn del<K: ToRedisArgs>(&mut self, key: K) -> Result<i64> {
        let result: RedisResult<i64> = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to delete key")
    }

    pub async fn sadd<K: ToRedisArgs, V: ToRedisArgs>(&mut self, key: K, value: V) -> Result<i64> {
        let result: RedisResult<i64> = redis::cmd("SADD")
            .arg(key)
            .arg(value)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to add to set")
    }

    pub async fn srem<K: ToRedisArgs, V: ToRedisArgs>(&mut self, key: K, value: V) -> Result<i64> {
        let result: RedisResult<i64> = redis::cmd("SREM")
            .arg(key)
            .arg(value)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to remove from set")
    }

    pub async fn smembers<K: ToRedisArgs>(&mut self, key: K) -> Result<Vec<String>> {
        let result: RedisResult<Vec<String>> = redis::cmd("SMEMBERS")
            .arg(key)
            .query_async(&mut self.connection)
            .await;
        result.context("Failed to get set members")
    }
}
