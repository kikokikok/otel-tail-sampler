use crate::redis::pool::{PubSubListener, RedisPool};
use crate::state::BufferedSpan;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};

const REDIS_FORCE_RULES_KEY: &str = "tss:force_sample:rules";
const REDIS_FORCE_RULE_PREFIX: &str = "tss:force_sample:rule:";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatchOperator {
    Eq,
    Neq,
    Contains,
    StartsWith,
    EndsWith,
    Regex,
    In,
    Exists,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForceAction {
    ForceKeep,
    ForceDrop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeMatcher {
    pub key: String,
    pub op: MatchOperator,
    pub value: MatchValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MatchValue {
    String(String),
    Number(f64),
    List(Vec<String>),
    Bool(bool),
}

impl MatchValue {
    fn as_str(&self) -> Option<&str> {
        match self {
            MatchValue::String(s) => Some(s),
            _ => None,
        }
    }

    fn as_list(&self) -> Option<&[String]> {
        match self {
            MatchValue::List(l) => Some(l),
            _ => None,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            MatchValue::Number(n) => Some(*n),
            MatchValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchPredicate {
    #[serde(default)]
    pub resource: Vec<AttributeMatcher>,
    #[serde(default)]
    pub span: Vec<AttributeMatcher>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForceRule {
    pub id: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub expires_at: DateTime<Utc>,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(rename = "match")]
    pub predicate: MatchPredicate,
    #[serde(default = "default_action")]
    pub action: ForceAction,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub created_by: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_priority() -> i32 {
    100
}

fn default_action() -> ForceAction {
    ForceAction::ForceKeep
}

#[derive(Debug)]
struct CompiledMatcher {
    key: String,
    op: MatchOperator,
    value: MatchValue,
    regex: Option<Regex>,
}

impl CompiledMatcher {
    fn compile(matcher: &AttributeMatcher) -> Result<Self> {
        let regex = if matcher.op == MatchOperator::Regex {
            let pattern = matcher
                .value
                .as_str()
                .ok_or_else(|| anyhow!("Regex operator requires string value"))?;
            Some(Regex::new(pattern)?)
        } else {
            None
        };

        Ok(Self {
            key: matcher.key.clone(),
            op: matcher.op.clone(),
            value: matcher.value.clone(),
            regex,
        })
    }

    fn matches(&self, attributes: &HashMap<String, String>) -> bool {
        let actual_value = attributes.get(&self.key);

        match self.op {
            MatchOperator::Exists => actual_value.is_some(),
            _ => {
                let Some(actual) = actual_value else {
                    return false;
                };
                self.matches_value(actual)
            }
        }
    }

    fn matches_value(&self, actual: &str) -> bool {
        match &self.op {
            MatchOperator::Eq => self.value.as_str().map(|v| v == actual).unwrap_or(false),
            MatchOperator::Neq => self.value.as_str().map(|v| v != actual).unwrap_or(true),
            MatchOperator::Contains => self
                .value
                .as_str()
                .map(|v| actual.contains(v))
                .unwrap_or(false),
            MatchOperator::StartsWith => self
                .value
                .as_str()
                .map(|v| actual.starts_with(v))
                .unwrap_or(false),
            MatchOperator::EndsWith => self
                .value
                .as_str()
                .map(|v| actual.ends_with(v))
                .unwrap_or(false),
            MatchOperator::Regex => self
                .regex
                .as_ref()
                .map(|r| r.is_match(actual))
                .unwrap_or(false),
            MatchOperator::In => self
                .value
                .as_list()
                .map(|list| list.iter().any(|v| v == actual))
                .unwrap_or(false),
            MatchOperator::Exists => true,
            MatchOperator::Gt => {
                if let (Some(expected), Ok(actual_num)) =
                    (self.value.as_f64(), actual.parse::<f64>())
                {
                    actual_num > expected
                } else {
                    false
                }
            }
            MatchOperator::Gte => {
                if let (Some(expected), Ok(actual_num)) =
                    (self.value.as_f64(), actual.parse::<f64>())
                {
                    actual_num >= expected
                } else {
                    false
                }
            }
            MatchOperator::Lt => {
                if let (Some(expected), Ok(actual_num)) =
                    (self.value.as_f64(), actual.parse::<f64>())
                {
                    actual_num < expected
                } else {
                    false
                }
            }
            MatchOperator::Lte => {
                if let (Some(expected), Ok(actual_num)) =
                    (self.value.as_f64(), actual.parse::<f64>())
                {
                    actual_num <= expected
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Debug)]
struct CompiledRule {
    id: String,
    priority: i32,
    expires_at: DateTime<Utc>,
    resource_matchers: Vec<CompiledMatcher>,
    span_matchers: Vec<CompiledMatcher>,
    action: ForceAction,
}

impl CompiledRule {
    fn compile(rule: &ForceRule) -> Result<Self> {
        let resource_matchers = rule
            .predicate
            .resource
            .iter()
            .map(CompiledMatcher::compile)
            .collect::<Result<Vec<_>>>()?;

        let span_matchers = rule
            .predicate
            .span
            .iter()
            .map(CompiledMatcher::compile)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            id: rule.id.clone(),
            priority: rule.priority,
            expires_at: rule.expires_at,
            resource_matchers,
            span_matchers,
            action: rule.action.clone(),
        })
    }

    fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    fn matches_trace(&self, spans: &[BufferedSpan]) -> bool {
        if self.is_expired() {
            return false;
        }

        if spans.is_empty() {
            return false;
        }

        let resource_attrs = self.extract_resource_attributes(spans);
        if !self.resource_matchers.is_empty()
            && !self
                .resource_matchers
                .iter()
                .all(|m| m.matches(&resource_attrs))
        {
            return false;
        }

        if self.span_matchers.is_empty() {
            return true;
        }

        spans.iter().any(|span| {
            self.span_matchers
                .iter()
                .all(|m| m.matches(&span.attributes))
        })
    }

    fn extract_resource_attributes(&self, spans: &[BufferedSpan]) -> HashMap<String, String> {
        let mut attrs = HashMap::new();

        if let Some(first_span) = spans.first() {
            attrs.insert("service.name".to_string(), first_span.service_name.clone());

            for (k, v) in &first_span.attributes {
                if k.starts_with("service.")
                    || k.starts_with("deployment.")
                    || k.starts_with("tenant")
                    || k.starts_with("customer")
                    || k.starts_with("host.")
                    || k.starts_with("cloud.")
                    || k.starts_with("k8s.")
                {
                    attrs.insert(k.clone(), v.clone());
                }
            }
        }

        attrs
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForceRuleCreateRequest {
    pub id: Option<String>,
    pub description: String,
    pub duration_secs: u64,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(rename = "match")]
    pub predicate: MatchPredicate,
    #[serde(default = "default_action")]
    pub action: ForceAction,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForceRuleResponse {
    pub id: String,
    pub description: String,
    pub enabled: bool,
    pub expires_at: DateTime<Utc>,
    pub remaining_secs: i64,
    pub priority: i32,
    #[serde(rename = "match")]
    pub predicate: MatchPredicate,
    pub action: ForceAction,
    pub created_at: Option<DateTime<Utc>>,
    pub created_by: Option<String>,
}

impl From<&ForceRule> for ForceRuleResponse {
    fn from(rule: &ForceRule) -> Self {
        let remaining = (rule.expires_at - Utc::now()).num_seconds().max(0);
        Self {
            id: rule.id.clone(),
            description: rule.description.clone(),
            enabled: rule.enabled,
            expires_at: rule.expires_at,
            remaining_secs: remaining,
            priority: rule.priority,
            predicate: rule.predicate.clone(),
            action: rule.action.clone(),
            created_at: rule.created_at,
            created_by: rule.created_by.clone(),
        }
    }
}

pub struct ForceRuleEngine {
    redis_pool: Arc<RwLock<RedisPool>>,
    cached_rules: Arc<RwLock<Vec<CompiledRule>>>,
    poll_interval_secs: u64,
    pubsub_channel: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleUpdateNotification {
    Created { rule_id: String },
    Updated { rule_id: String },
    Deleted { rule_id: String },
    Refresh,
}

impl ForceRuleEngine {
    pub fn new(redis_pool: Arc<RwLock<RedisPool>>, poll_interval_secs: u64) -> Self {
        Self {
            redis_pool,
            cached_rules: Arc::new(RwLock::new(Vec::new())),
            poll_interval_secs,
            pubsub_channel: None,
        }
    }

    pub fn with_pubsub(mut self, channel: String) -> Self {
        self.pubsub_channel = Some(channel);
        self
    }

    pub async fn start_background_refresh(&self) -> tokio::task::JoinHandle<()> {
        let redis_pool = self.redis_pool.clone();
        let cached_rules = self.cached_rules.clone();
        let poll_interval = self.poll_interval_secs;

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::refresh_rules_from_redis(&redis_pool, &cached_rules).await {
                    error!("Failed to refresh force rules from Redis: {}", e);
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(poll_interval)).await;
            }
        })
    }

    pub async fn start_pubsub_listener(&self) -> Result<Option<tokio::task::JoinHandle<()>>> {
        let Some(channel) = &self.pubsub_channel else {
            debug!("PubSub not configured, skipping listener");
            return Ok(None);
        };

        let pool = self.redis_pool.read().await;
        let client = pool.get_client();
        let listener = PubSubListener::new(client, channel).await?;

        let (tx, mut rx) = mpsc::channel::<String>(100);

        let handle = listener.subscribe(tx).await?;

        let redis_pool = self.redis_pool.clone();
        let cached_rules = self.cached_rules.clone();
        let channel_name = channel.clone();

        tokio::spawn(async move {
            info!(
                "Starting PubSub message handler for channel: {}",
                channel_name
            );

            while let Some(message) = rx.recv().await {
                match serde_json::from_str::<RuleUpdateNotification>(&message) {
                    Ok(notification) => {
                        debug!("Received rule update notification: {:?}", notification);
                        if let Err(e) =
                            Self::refresh_rules_from_redis(&redis_pool, &cached_rules).await
                        {
                            error!("Failed to refresh rules after notification: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse PubSub message '{}': {}", message, e);
                        if let Err(e) =
                            Self::refresh_rules_from_redis(&redis_pool, &cached_rules).await
                        {
                            error!("Failed to refresh rules: {}", e);
                        }
                    }
                }
            }

            info!("PubSub message handler ended for channel: {}", channel_name);
        });

        Ok(Some(handle))
    }

    async fn publish_notification(&self, notification: RuleUpdateNotification) -> Result<()> {
        let Some(channel) = &self.pubsub_channel else {
            return Ok(());
        };

        let message = serde_json::to_string(&notification)?;
        let pool = self.redis_pool.read().await;
        let subscribers = pool.publish(channel, &message).await?;

        debug!(
            "Published {:?} to {} ({} subscribers)",
            notification, channel, subscribers
        );

        Ok(())
    }

    async fn refresh_rules_from_redis(
        redis_pool: &Arc<RwLock<RedisPool>>,
        cached_rules: &Arc<RwLock<Vec<CompiledRule>>>,
    ) -> Result<()> {
        let pool = redis_pool.read().await;
        let mut conn = pool.get().await?;

        let rule_ids: Vec<String> = conn.smembers(REDIS_FORCE_RULES_KEY.to_string()).await?;

        let mut rules = Vec::new();
        let now = Utc::now();

        for rule_id in rule_ids {
            let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);
            if let Some(rule_json) = conn.get::<String>(key.clone()).await? {
                match serde_json::from_str::<ForceRule>(&rule_json) {
                    Ok(rule) => {
                        if rule.enabled && rule.expires_at > now {
                            match CompiledRule::compile(&rule) {
                                Ok(compiled) => rules.push(compiled),
                                Err(e) => {
                                    warn!("Failed to compile force rule {}: {}", rule_id, e)
                                }
                            }
                        } else if rule.expires_at <= now {
                            debug!("Force rule {} expired, will be cleaned up", rule_id);
                        }
                    }
                    Err(e) => warn!("Failed to parse force rule {}: {}", rule_id, e),
                }
            }
        }

        rules.sort_by(|a, b| b.priority.cmp(&a.priority));

        let rule_count = rules.len();
        {
            let mut cache = cached_rules.write().await;
            *cache = rules;
        }

        debug!("Refreshed {} active force rules from Redis", rule_count);
        Ok(())
    }

    pub async fn evaluate_trace(&self, spans: &[BufferedSpan]) -> Option<ForceAction> {
        let rules = self.cached_rules.read().await;

        for rule in rules.iter() {
            if rule.is_expired() {
                continue;
            }

            if rule.matches_trace(spans) {
                debug!(
                    "Trace matched force rule '{}', action: {:?}",
                    rule.id, rule.action
                );
                return Some(rule.action.clone());
            }
        }

        None
    }

    pub async fn create_rule(&self, request: ForceRuleCreateRequest) -> Result<ForceRule> {
        let rule_id = request
            .id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(request.duration_secs as i64);

        let rule = ForceRule {
            id: rule_id.clone(),
            description: request.description,
            enabled: true,
            expires_at,
            priority: request.priority,
            predicate: request.predicate,
            action: request.action,
            created_at: Some(now),
            created_by: request.created_by,
        };

        CompiledRule::compile(&rule)?;

        let rule_json = serde_json::to_string(&rule)?;
        let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);

        let pool = self.redis_pool.read().await;
        let mut conn = pool.get().await?;

        conn.set_ex(key, rule_json, request.duration_secs + 60)
            .await?;
        conn.sadd(REDIS_FORCE_RULES_KEY.to_string(), rule_id.clone())
            .await?;

        info!(
            "Created force rule '{}' (expires in {}s): {}",
            rule_id, request.duration_secs, rule.description
        );

        Self::refresh_rules_from_redis(&self.redis_pool, &self.cached_rules).await?;

        if let Err(e) = self
            .publish_notification(RuleUpdateNotification::Created {
                rule_id: rule.id.clone(),
            })
            .await
        {
            warn!("Failed to publish rule creation notification: {}", e);
        }

        Ok(rule)
    }

    pub async fn get_rule(&self, rule_id: &str) -> Result<Option<ForceRule>> {
        let pool = self.redis_pool.read().await;
        let mut conn = pool.get().await?;

        let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);
        if let Some(rule_json) = conn.get::<String>(key).await? {
            let rule: ForceRule = serde_json::from_str(&rule_json)?;
            Ok(Some(rule))
        } else {
            Ok(None)
        }
    }

    pub async fn list_rules(&self) -> Result<Vec<ForceRule>> {
        let pool = self.redis_pool.read().await;
        let mut conn = pool.get().await?;

        let rule_ids: Vec<String> = conn.smembers(REDIS_FORCE_RULES_KEY.to_string()).await?;
        let mut rules = Vec::new();

        for rule_id in rule_ids {
            let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);
            if let Some(rule_json) = conn.get::<String>(key).await? {
                if let Ok(rule) = serde_json::from_str::<ForceRule>(&rule_json) {
                    rules.push(rule);
                }
            }
        }

        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        Ok(rules)
    }

    pub async fn delete_rule(&self, rule_id: &str) -> Result<bool> {
        let pool = self.redis_pool.read().await;
        let mut conn = pool.get().await?;

        let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);
        let deleted: i64 = conn.del(key).await?;
        conn.srem(REDIS_FORCE_RULES_KEY.to_string(), rule_id.to_string())
            .await?;

        if deleted > 0 {
            info!("Deleted force rule '{}'", rule_id);
            Self::refresh_rules_from_redis(&self.redis_pool, &self.cached_rules).await?;

            if let Err(e) = self
                .publish_notification(RuleUpdateNotification::Deleted {
                    rule_id: rule_id.to_string(),
                })
                .await
            {
                warn!("Failed to publish rule deletion notification: {}", e);
            }
        }

        Ok(deleted > 0)
    }

    pub async fn extend_rule(&self, rule_id: &str, additional_secs: u64) -> Result<ForceRule> {
        let mut rule = self
            .get_rule(rule_id)
            .await?
            .ok_or_else(|| anyhow!("Rule not found: {}", rule_id))?;

        let new_expires = if rule.expires_at < Utc::now() {
            Utc::now() + chrono::Duration::seconds(additional_secs as i64)
        } else {
            rule.expires_at + chrono::Duration::seconds(additional_secs as i64)
        };

        rule.expires_at = new_expires;

        let rule_json = serde_json::to_string(&rule)?;
        let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);

        let remaining_secs = (new_expires - Utc::now()).num_seconds().max(0) as u64;

        let pool = self.redis_pool.read().await;
        let mut conn = pool.get().await?;

        conn.set_ex(key, rule_json, remaining_secs + 60).await?;

        info!(
            "Extended force rule '{}' by {}s (new expiry: {})",
            rule_id, additional_secs, new_expires
        );

        Self::refresh_rules_from_redis(&self.redis_pool, &self.cached_rules).await?;

        if let Err(e) = self
            .publish_notification(RuleUpdateNotification::Updated {
                rule_id: rule_id.to_string(),
            })
            .await
        {
            warn!("Failed to publish rule extension notification: {}", e);
        }

        Ok(rule)
    }

    pub async fn disable_rule(&self, rule_id: &str) -> Result<ForceRule> {
        let mut rule = self
            .get_rule(rule_id)
            .await?
            .ok_or_else(|| anyhow!("Rule not found: {}", rule_id))?;

        rule.enabled = false;

        let rule_json = serde_json::to_string(&rule)?;
        let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);

        let remaining_secs = (rule.expires_at - Utc::now()).num_seconds().max(60) as u64;

        let pool = self.redis_pool.read().await;
        let mut conn = pool.get().await?;

        conn.set_ex(key, rule_json, remaining_secs).await?;

        info!("Disabled force rule '{}'", rule_id);

        Self::refresh_rules_from_redis(&self.redis_pool, &self.cached_rules).await?;

        if let Err(e) = self
            .publish_notification(RuleUpdateNotification::Updated {
                rule_id: rule_id.to_string(),
            })
            .await
        {
            warn!("Failed to publish rule disable notification: {}", e);
        }

        Ok(rule)
    }

    pub async fn enable_rule(&self, rule_id: &str) -> Result<ForceRule> {
        let mut rule = self
            .get_rule(rule_id)
            .await?
            .ok_or_else(|| anyhow!("Rule not found: {}", rule_id))?;

        if rule.expires_at <= Utc::now() {
            return Err(anyhow!("Cannot enable expired rule"));
        }

        rule.enabled = true;

        let rule_json = serde_json::to_string(&rule)?;
        let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);

        let remaining_secs = (rule.expires_at - Utc::now()).num_seconds().max(60) as u64;

        let pool = self.redis_pool.read().await;
        let mut conn = pool.get().await?;

        conn.set_ex(key, rule_json, remaining_secs).await?;

        info!("Enabled force rule '{}'", rule_id);

        Self::refresh_rules_from_redis(&self.redis_pool, &self.cached_rules).await?;

        if let Err(e) = self
            .publish_notification(RuleUpdateNotification::Updated {
                rule_id: rule_id.to_string(),
            })
            .await
        {
            warn!("Failed to publish rule enable notification: {}", e);
        }

        Ok(rule)
    }

    pub async fn cleanup_expired_rules(&self) -> Result<usize> {
        let pool = self.redis_pool.read().await;
        let mut conn = pool.get().await?;

        let rule_ids: Vec<String> = conn.smembers(REDIS_FORCE_RULES_KEY.to_string()).await?;
        let now = Utc::now();
        let mut cleaned = 0;

        for rule_id in rule_ids {
            let key = format!("{}{}", REDIS_FORCE_RULE_PREFIX, rule_id);
            let exists: bool = conn.get::<String>(key.clone()).await?.is_some();

            if !exists {
                conn.srem(REDIS_FORCE_RULES_KEY.to_string(), rule_id.clone())
                    .await?;
                cleaned += 1;
                continue;
            }

            if let Some(rule_json) = conn.get::<String>(key.clone()).await? {
                if let Ok(rule) = serde_json::from_str::<ForceRule>(&rule_json) {
                    if rule.expires_at <= now {
                        conn.del(key).await?;
                        conn.srem(REDIS_FORCE_RULES_KEY.to_string(), rule_id.clone())
                            .await?;
                        cleaned += 1;
                        debug!("Cleaned up expired force rule '{}'", rule_id);
                    }
                }
            }
        }

        if cleaned > 0 {
            info!("Cleaned up {} expired force rules", cleaned);
        }

        Ok(cleaned)
    }

    pub fn active_rule_count(&self) -> usize {
        futures::executor::block_on(async { self.cached_rules.read().await.len() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_span(service_name: &str, attributes: HashMap<String, String>) -> BufferedSpan {
        BufferedSpan {
            trace_id: "test-trace".to_string(),
            span_id: "test-span".to_string(),
            parent_span_id: None,
            timestamp_ms: 1000,
            duration_ms: 100,
            status_code: 0,
            span_kind: crate::state::SpanKind::Server,
            service_name: service_name.to_string(),
            operation_name: "test-op".to_string(),
            attributes,
        }
    }

    #[test]
    fn test_eq_matcher() {
        let matcher = CompiledMatcher::compile(&AttributeMatcher {
            key: "tenant.id".to_string(),
            op: MatchOperator::Eq,
            value: MatchValue::String("acme-corp".to_string()),
        })
        .unwrap();

        let mut attrs = HashMap::new();
        attrs.insert("tenant.id".to_string(), "acme-corp".to_string());
        assert!(matcher.matches(&attrs));

        attrs.insert("tenant.id".to_string(), "other-corp".to_string());
        assert!(!matcher.matches(&attrs));
    }

    #[test]
    fn test_contains_matcher() {
        let matcher = CompiledMatcher::compile(&AttributeMatcher {
            key: "http.url".to_string(),
            op: MatchOperator::Contains,
            value: MatchValue::String("/api/v2/".to_string()),
        })
        .unwrap();

        let mut attrs = HashMap::new();
        attrs.insert(
            "http.url".to_string(),
            "https://example.com/api/v2/users".to_string(),
        );
        assert!(matcher.matches(&attrs));

        attrs.insert(
            "http.url".to_string(),
            "https://example.com/api/v1/users".to_string(),
        );
        assert!(!matcher.matches(&attrs));
    }

    #[test]
    fn test_regex_matcher() {
        let matcher = CompiledMatcher::compile(&AttributeMatcher {
            key: "http.route".to_string(),
            op: MatchOperator::Regex,
            value: MatchValue::String(r"/users/\d+".to_string()),
        })
        .unwrap();

        let mut attrs = HashMap::new();
        attrs.insert("http.route".to_string(), "/users/123".to_string());
        assert!(matcher.matches(&attrs));

        attrs.insert("http.route".to_string(), "/users/abc".to_string());
        assert!(!matcher.matches(&attrs));
    }

    #[test]
    fn test_in_matcher() {
        let matcher = CompiledMatcher::compile(&AttributeMatcher {
            key: "rpc.method".to_string(),
            op: MatchOperator::In,
            value: MatchValue::List(vec![
                "ProcessPayment".to_string(),
                "CreateOrder".to_string(),
            ]),
        })
        .unwrap();

        let mut attrs = HashMap::new();
        attrs.insert("rpc.method".to_string(), "ProcessPayment".to_string());
        assert!(matcher.matches(&attrs));

        attrs.insert("rpc.method".to_string(), "GetUser".to_string());
        assert!(!matcher.matches(&attrs));
    }

    #[test]
    fn test_numeric_comparison() {
        let matcher = CompiledMatcher::compile(&AttributeMatcher {
            key: "http.status_code".to_string(),
            op: MatchOperator::Gte,
            value: MatchValue::Number(500.0),
        })
        .unwrap();

        let mut attrs = HashMap::new();
        attrs.insert("http.status_code".to_string(), "503".to_string());
        assert!(matcher.matches(&attrs));

        attrs.insert("http.status_code".to_string(), "200".to_string());
        assert!(!matcher.matches(&attrs));
    }

    #[test]
    fn test_compiled_rule_matches_trace() {
        let rule = ForceRule {
            id: "test-rule".to_string(),
            description: "Test".to_string(),
            enabled: true,
            expires_at: Utc::now() + chrono::Duration::hours(1),
            priority: 100,
            predicate: MatchPredicate {
                resource: vec![AttributeMatcher {
                    key: "service.name".to_string(),
                    op: MatchOperator::Eq,
                    value: MatchValue::String("payment-service".to_string()),
                }],
                span: vec![],
            },
            action: ForceAction::ForceKeep,
            created_at: Some(Utc::now()),
            created_by: None,
        };

        let compiled = CompiledRule::compile(&rule).unwrap();

        let spans = vec![create_test_span("payment-service", HashMap::new())];
        assert!(compiled.matches_trace(&spans));

        let spans = vec![create_test_span("user-service", HashMap::new())];
        assert!(!compiled.matches_trace(&spans));
    }

    #[test]
    fn test_expired_rule_does_not_match() {
        let rule = ForceRule {
            id: "test-rule".to_string(),
            description: "Test".to_string(),
            enabled: true,
            expires_at: Utc::now() - chrono::Duration::hours(1),
            priority: 100,
            predicate: MatchPredicate {
                resource: vec![],
                span: vec![],
            },
            action: ForceAction::ForceKeep,
            created_at: Some(Utc::now()),
            created_by: None,
        };

        let compiled = CompiledRule::compile(&rule).unwrap();
        let spans = vec![create_test_span("any-service", HashMap::new())];
        assert!(!compiled.matches_trace(&spans));
    }

    #[test]
    fn test_span_attribute_matching() {
        let rule = ForceRule {
            id: "test-rule".to_string(),
            description: "Test".to_string(),
            enabled: true,
            expires_at: Utc::now() + chrono::Duration::hours(1),
            priority: 100,
            predicate: MatchPredicate {
                resource: vec![],
                span: vec![AttributeMatcher {
                    key: "customer.id".to_string(),
                    op: MatchOperator::Eq,
                    value: MatchValue::String("cust-123".to_string()),
                }],
            },
            action: ForceAction::ForceKeep,
            created_at: Some(Utc::now()),
            created_by: None,
        };

        let compiled = CompiledRule::compile(&rule).unwrap();

        let mut attrs = HashMap::new();
        attrs.insert("customer.id".to_string(), "cust-123".to_string());
        let spans = vec![create_test_span("any-service", attrs)];
        assert!(compiled.matches_trace(&spans));

        let mut attrs = HashMap::new();
        attrs.insert("customer.id".to_string(), "cust-456".to_string());
        let spans = vec![create_test_span("any-service", attrs)];
        assert!(!compiled.matches_trace(&spans));
    }
}
