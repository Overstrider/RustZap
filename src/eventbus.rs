use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    sync::mpsc,
    time::{Duration as TokioDuration, sleep},
};
use uuid::Uuid;

use crate::config::{EventBusMode, KafkaConfig};
use crate::db::{MetadataDb, OutboxRecord};
use crate::models::CommonEvent;

#[cfg(feature = "external-integrations")]
use std::time::Duration as StdDuration;

#[cfg(feature = "external-integrations")]
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::FromClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord, Producer},
    types::RDKafkaErrorCode,
};

#[allow(clippy::too_many_arguments)]
pub fn new_event(
    event_type: &str,
    project_id: &str,
    company_id: &str,
    channel_id: Option<String>,
    conversation_id: Option<String>,
    message_id: Option<String>,
    conversation_seq: Option<i64>,
    payload: Value,
) -> CommonEvent {
    let now = OffsetDateTime::now_utc();
    let event_id = format!("evt_{}", Uuid::now_v7().simple());
    CommonEvent {
        trace_id: format!("trace_{}", Uuid::now_v7().simple()),
        correlation_id: format!("corr_{}", Uuid::now_v7().simple()),
        event_id,
        event_type: event_type.to_string(),
        project_id: project_id.to_string(),
        company_id: company_id.to_string(),
        channel_id,
        conversation_id,
        message_id,
        conversation_seq,
        causation_id: None,
        occurred_at: now,
        produced_at: now,
        payload,
    }
}

pub fn dirty_signal(
    project_id: &str,
    company_id: &str,
    channel_id: &str,
    conversation_id: &str,
    to_seq: i64,
    reason: &str,
    priority: i32,
) -> CommonEvent {
    new_event(
        "conversation.dirty",
        project_id,
        company_id,
        Some(channel_id.to_string()),
        Some(conversation_id.to_string()),
        None,
        Some(to_seq),
        json!({
            "to_seq": to_seq,
            "reason": reason,
            "priority": priority
        }),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBusHealth {
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum EventBusError {
    #[error("KAFKA_BROKERS missing")]
    MissingBrokers,
    #[error("event {event_id} contains raw media bytes in payload field {field}")]
    RawMediaBytes { event_id: String, field: String },
    #[error("event {event_id} encoded payload is {actual} bytes, above limit {limit}")]
    MessageTooLarge {
        event_id: String,
        actual: usize,
        limit: usize,
    },
    #[error("event codec failed: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("Kafka support is disabled; rebuild with --features external-integrations")]
    KafkaFeatureDisabled,
    #[error("Kafka missing required topics: {0}")]
    MissingTopics(String),
    #[error("Kafka topic create failed for {topic}: {code:?}")]
    KafkaTopicCreate {
        topic: String,
        code: RdkafkaErrorCodeView,
    },
    #[error("Kafka error: {0}")]
    Kafka(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdkafkaErrorCodeView {
    TopicAlreadyExists,
    Other(i32),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopicSpec {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub retention_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublishAck {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeadLetterEnvelope {
    pub event: CommonEvent,
    pub retry_count: u32,
    pub first_failed_at: String,
    pub last_failed_at: String,
    pub error_code: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventSourceOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl EventSourceOffset {
    pub fn unknown() -> Self {
        Self {
            topic: "unknown".to_string(),
            partition: -1,
            offset: -1,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryEnvelope {
    pub event: CommonEvent,
    pub retry_count: u32,
    pub source: EventSourceOffset,
    pub first_failed_at: String,
    pub last_failed_at: String,
    pub next_attempt_at: String,
    pub error_code: String,
}

impl RetryEnvelope {
    pub fn should_deadletter(&self, kafka: &KafkaConfig) -> bool {
        self.retry_count >= kafka.retry_max_attempts
    }

    pub fn into_deadletter(self) -> DeadLetterEnvelope {
        DeadLetterEnvelope {
            event: self.event,
            retry_count: self.retry_count,
            first_failed_at: self.first_failed_at,
            last_failed_at: self.last_failed_at,
            error_code: self.error_code,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerHandlerStatus {
    Processed,
    Duplicate,
    Retried,
    DeadLettered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerCommitDecision {
    Commit,
    DoNotCommit,
}

impl ConsumerCommitDecision {
    pub fn from_handler_result(
        result: Result<ConsumerHandlerStatus, EventBusError>,
    ) -> ConsumerCommitDecision {
        match result {
            Ok(
                ConsumerHandlerStatus::Processed
                | ConsumerHandlerStatus::Duplicate
                | ConsumerHandlerStatus::Retried
                | ConsumerHandlerStatus::DeadLettered,
            ) => ConsumerCommitDecision::Commit,
            Err(_) => ConsumerCommitDecision::DoNotCommit,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaConsumerMessage {
    pub event: CommonEvent,
    pub source: EventSourceOffset,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventBusRuntimeSnapshot {
    pub mode: String,
    pub kafka_required: bool,
    pub brokers_configured: bool,
    pub required_topics: Vec<String>,
    pub publish_attempts: u64,
    pub publish_successes: u64,
    pub publish_failures: u64,
    pub deadletter_attempts: u64,
    pub deadletter_successes: u64,
    pub last_topic: Option<String>,
    pub last_error: Option<String>,
    pub recent_deadletters: Vec<DeadLetterEnvelope>,
    pub durable_outbox: bool,
    pub outbox_enqueue_observed: u64,
}

#[derive(Debug, Default)]
struct EventBusRuntimeMetrics {
    publish_attempts: u64,
    publish_successes: u64,
    publish_failures: u64,
    deadletter_attempts: u64,
    deadletter_successes: u64,
    outbox_enqueue_observed: u64,
    last_topic: Option<String>,
    last_error: Option<String>,
    recent_deadletters: Vec<DeadLetterEnvelope>,
}

#[derive(Clone)]
pub struct EventBusHandle {
    mode: EventBusMode,
    kafka_config: KafkaConfig,
    tx: Option<mpsc::UnboundedSender<CommonEvent>>,
    kafka: Option<Arc<KafkaEventBus>>,
    metrics: Arc<Mutex<EventBusRuntimeMetrics>>,
    durable_outbox: bool,
}

impl EventBusHandle {
    pub fn local(mode: EventBusMode, kafka_config: KafkaConfig) -> Self {
        Self {
            mode,
            kafka_config,
            tx: None,
            kafka: None,
            metrics: Arc::new(Mutex::new(EventBusRuntimeMetrics::default())),
            durable_outbox: false,
        }
    }

    pub fn durable_outbox(mode: EventBusMode, kafka_config: KafkaConfig) -> Self {
        Self {
            mode,
            kafka_config,
            tx: None,
            kafka: None,
            metrics: Arc::new(Mutex::new(EventBusRuntimeMetrics::default())),
            durable_outbox: true,
        }
    }

    pub async fn from_config(
        mode: EventBusMode,
        kafka_config: KafkaConfig,
    ) -> Result<Self, EventBusError> {
        Self::from_config_with_outbox(mode, kafka_config, None).await
    }

    pub async fn from_config_with_outbox(
        mode: EventBusMode,
        kafka_config: KafkaConfig,
        outbox_db: Option<Arc<MetadataDb>>,
    ) -> Result<Self, EventBusError> {
        match mode {
            EventBusMode::Kafka => {
                let kafka = Arc::new(KafkaEventBus::connect(kafka_config.clone()).await?);
                kafka.ensure_topics().await?;
                let metrics = Arc::new(Mutex::new(EventBusRuntimeMetrics::default()));
                let durable_outbox = outbox_db.is_some();
                let tx = if let Some(outbox_db) = outbox_db {
                    spawn_outbox_publisher(kafka.clone(), outbox_db, metrics.clone());
                    None
                } else {
                    let (tx, rx) = mpsc::unbounded_channel();
                    spawn_kafka_publisher(kafka.clone(), rx, metrics.clone());
                    Some(tx)
                };
                Ok(Self {
                    mode,
                    kafka_config,
                    tx,
                    kafka: Some(kafka),
                    metrics,
                    durable_outbox,
                })
            }
            EventBusMode::InMemory | EventBusMode::Postgres => Ok(Self::local(mode, kafka_config)),
        }
    }

    pub fn publish_background(&self, event: CommonEvent) {
        if self.durable_outbox {
            let mut metrics = self
                .metrics
                .lock()
                .expect("event bus metrics lock poisoned");
            metrics.outbox_enqueue_observed += 1;
            return;
        }
        if let Some(tx) = self.tx.as_ref()
            && tx.send(event).is_err()
        {
            let mut metrics = self
                .metrics
                .lock()
                .expect("event bus metrics lock poisoned");
            metrics.publish_failures += 1;
            metrics.last_error = Some("event bus publisher task is not running".to_string());
        }
    }

    pub async fn health(&self) -> EventBusHealth {
        if let Some(kafka) = self.kafka.as_ref() {
            return kafka.health().await.unwrap_or_else(|err| EventBusHealth {
                ok: false,
                detail: err.to_string(),
            });
        }
        health_check(self.mode, &self.kafka_config)
    }

    pub fn snapshot(&self) -> EventBusRuntimeSnapshot {
        let metrics = self
            .metrics
            .lock()
            .expect("event bus metrics lock poisoned");
        EventBusRuntimeSnapshot {
            mode: format!("{:?}", self.mode),
            kafka_required: self.mode == EventBusMode::Kafka,
            brokers_configured: self
                .kafka_config
                .brokers
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()),
            required_topics: required_topic_specs(&self.kafka_config)
                .into_iter()
                .map(|spec| spec.name)
                .collect(),
            publish_attempts: metrics.publish_attempts,
            publish_successes: metrics.publish_successes,
            publish_failures: metrics.publish_failures,
            deadletter_attempts: metrics.deadletter_attempts,
            deadletter_successes: metrics.deadletter_successes,
            last_topic: metrics.last_topic.clone(),
            last_error: metrics.last_error.clone(),
            recent_deadletters: metrics.recent_deadletters.clone(),
            durable_outbox: self.durable_outbox,
            outbox_enqueue_observed: metrics.outbox_enqueue_observed,
        }
    }
}

fn spawn_kafka_publisher(
    kafka: Arc<KafkaEventBus>,
    mut rx: mpsc::UnboundedReceiver<CommonEvent>,
    metrics: Arc<Mutex<EventBusRuntimeMetrics>>,
) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let topic = topic_for_event(&kafka.config(), &event.event_type);
            {
                let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                metrics.publish_attempts += 1;
                metrics.last_topic = Some(topic.clone());
            }

            match kafka.publish(&event).await {
                Ok(_) => {
                    let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                    metrics.publish_successes += 1;
                    metrics.last_error = None;
                }
                Err(err) => {
                    let error = err.to_string();
                    {
                        let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                        metrics.publish_failures += 1;
                        metrics.last_error = Some(error.clone());
                    }
                    let envelope = deadletter_envelope(event, 1, &error);
                    {
                        let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                        metrics.deadletter_attempts += 1;
                        if metrics.recent_deadletters.len() >= 20 {
                            metrics.recent_deadletters.remove(0);
                        }
                        metrics.recent_deadletters.push(envelope.clone());
                    }
                    if kafka.publish_deadletter(&envelope).await.is_ok() {
                        let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                        metrics.deadletter_successes += 1;
                    }
                }
            }
        }
    });
}

fn spawn_outbox_publisher(
    kafka: Arc<KafkaEventBus>,
    db: Arc<MetadataDb>,
    metrics: Arc<Mutex<EventBusRuntimeMetrics>>,
) {
    tokio::spawn(async move {
        let worker_id = format!(
            "{}-outbox-{}",
            kafka.config().client_id,
            Uuid::now_v7().simple()
        );
        loop {
            let records = match db.claim_event_outbox_batch(&worker_id, 100, 30).await {
                Ok(records) => records,
                Err(err) => {
                    {
                        let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                        metrics.last_error = Some(err.to_string());
                    }
                    sleep(TokioDuration::from_millis(500)).await;
                    continue;
                }
            };
            if records.is_empty() {
                sleep(TokioDuration::from_millis(250)).await;
                continue;
            }
            for record in records {
                publish_outbox_record(&kafka, &db, &metrics, record).await;
            }
        }
    });
}

async fn publish_outbox_record(
    kafka: &KafkaEventBus,
    db: &MetadataDb,
    metrics: &Arc<Mutex<EventBusRuntimeMetrics>>,
    record: OutboxRecord,
) {
    {
        let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
        metrics.publish_attempts += 1;
        metrics.last_topic = Some(record.topic.clone());
    }
    let event = match serde_json::from_value::<CommonEvent>(record.payload_json.clone()) {
        Ok(event) => event,
        Err(err) => {
            if let Err(mark_err) = db
                .mark_event_outbox_failed(&record.event_id, &err.to_string(), 60)
                .await
            {
                tracing::error!(event_id = record.event_id, error = %mark_err, "failed to mark undecodable outbox event failed");
            }
            let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
            metrics.publish_failures += 1;
            metrics.last_error = Some(err.to_string());
            return;
        }
    };
    let payload = match encode_event_for_kafka(&kafka.config(), &event) {
        Ok(payload) => payload,
        Err(err) => {
            let error = err.to_string();
            if let Err(mark_err) = db
                .mark_event_outbox_deadlettered(&record.event_id, &error)
                .await
            {
                tracing::error!(event_id = record.event_id, error = %mark_err, "failed to mark invalid outbox event deadlettered");
            }
            let envelope = deadletter_envelope(event, record.attempt_count as u32 + 1, &error);
            if let Err(err) = db
                .insert_kafka_deadletter(&envelope, Some(&record.topic), &record.partition_key)
                .await
            {
                tracing::error!(event_id = envelope.event.event_id, error = %err, "failed to persist invalid outbox deadletter");
            }
            let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
            metrics.publish_failures += 1;
            metrics.deadletter_attempts += 1;
            metrics.last_error = Some(error);
            return;
        }
    };
    match kafka
        .publish_payload(&record.topic, &record.partition_key, &payload)
        .await
    {
        Ok(ack) => {
            if let Err(err) = db
                .mark_event_outbox_published(&record.event_id, ack.partition, ack.offset)
                .await
            {
                tracing::error!(event_id = record.event_id, error = %err, "failed to mark outbox event published");
            }
            let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
            metrics.publish_successes += 1;
            metrics.last_error = None;
        }
        Err(err) => {
            let error = err.to_string();
            let attempts = record.attempt_count.saturating_add(1);
            if attempts as u32 >= kafka.config().retry_max_attempts {
                let envelope = deadletter_envelope(event, attempts as u32, &error);
                if let Err(err) = db
                    .insert_kafka_deadletter(&envelope, Some(&record.topic), &record.partition_key)
                    .await
                {
                    tracing::error!(event_id = envelope.event.event_id, error = %err, "failed to persist outbox deadletter");
                }
                if kafka.publish_deadletter(&envelope).await.is_ok() {
                    let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                    metrics.deadletter_successes += 1;
                }
                if let Err(err) = db
                    .mark_event_outbox_deadlettered(&record.event_id, &error)
                    .await
                {
                    tracing::error!(event_id = record.event_id, error = %err, "failed to mark outbox event deadlettered");
                }
                let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                metrics.deadletter_attempts += 1;
                metrics.publish_failures += 1;
                metrics.last_error = Some(error);
            } else {
                let backoff = 5_i64.saturating_mul(2_i64.saturating_pow(attempts.min(6) as u32));
                if let Err(err) = db
                    .mark_event_outbox_failed(&record.event_id, &error, backoff)
                    .await
                {
                    tracing::error!(event_id = record.event_id, error = %err, "failed to mark outbox event failed");
                }
                let mut metrics = metrics.lock().expect("event bus metrics lock poisoned");
                metrics.publish_failures += 1;
                metrics.last_error = Some(error);
            }
        }
    }
}

pub fn health_check(mode: EventBusMode, kafka: &KafkaConfig) -> EventBusHealth {
    match mode {
        EventBusMode::InMemory => EventBusHealth {
            ok: true,
            detail: "in-memory event bus".to_string(),
        },
        EventBusMode::Postgres => EventBusHealth {
            ok: true,
            detail: "postgres event bus placeholder".to_string(),
        },
        EventBusMode::Kafka => {
            if kafka
                .brokers
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            {
                EventBusHealth {
                    ok: true,
                    detail: format!("kafka configured at {}", kafka.brokers.as_deref().unwrap()),
                }
            } else {
                EventBusHealth {
                    ok: false,
                    detail: "KAFKA_BROKERS missing".to_string(),
                }
            }
        }
    }
}

const EVENT_TOPIC_SUFFIXES: &[&str] = &[
    "raw.inbound",
    "outbound.send.requested",
    "message.persisted",
    "media.download.requested",
    "media.stored",
    "audio.transcription.requested",
    "audio.transcribed",
    "conversation.dirty",
    "conversation.event",
    "delivery.receipt",
    "channel.status",
    "contact.updated",
    "group.event",
    "websocket.event",
    "system.event",
];

pub fn topic_for_event(kafka: &KafkaConfig, event_type: &str) -> String {
    let suffix = match event_type {
        "whatsapp.raw.inbound" => "raw.inbound",
        "outbound.send.requested" => "outbound.send.requested",
        "message.queued" | "message.received" | "message.sent" | "message.failed"
        | "message.updated" | "message.persisted" | "message.reaction" => "message.persisted",
        "media.download.requested" => "media.download.requested",
        "media.stored" => "media.stored",
        "audio.transcription.requested" => "audio.transcription.requested",
        "audio.transcribed" | "transcript.completed" => "audio.transcribed",
        "conversation.dirty" => "conversation.dirty",
        "conversation.read" => "conversation.event",
        "message.receipt" => "delivery.receipt",
        "channel.connected" | "channel.disconnected" | "channel.qr" | "channel.logged_out" => {
            "channel.status"
        }
        "contact.updated" => "contact.updated",
        event if event.starts_with("group.") => "group.event",
        event if event.starts_with("websocket.") => "websocket.event",
        event
            if event.ends_with(".ignored")
                || event.starts_with("whatsapp_updates.")
                || event.starts_with("history_sync.")
                || event == "ignored_old_message" =>
        {
            "system.event"
        }
        _ => "raw.inbound",
    };
    format!("{}.{}", kafka.topic_prefix, suffix)
}

pub fn required_topic_specs(kafka: &KafkaConfig) -> Vec<TopicSpec> {
    let mut suffixes = BTreeSet::new();
    suffixes.extend(EVENT_TOPIC_SUFFIXES.iter().copied());
    suffixes.insert(kafka.retry_topic_suffix.as_str());
    suffixes.insert(kafka.deadletter_topic_suffix.as_str());

    suffixes
        .into_iter()
        .map(|suffix| {
            let is_deadletter = suffix == kafka.deadletter_topic_suffix;
            TopicSpec {
                name: format!("{}.{}", kafka.topic_prefix, suffix),
                partitions: kafka.default_partitions,
                replication_factor: kafka.default_replication_factor,
                retention_ms: retention_ms(if is_deadletter {
                    kafka.deadletter_retention_hours
                } else {
                    kafka.message_retention_hours
                }),
            }
        })
        .collect()
}

fn retention_ms(hours: i64) -> i64 {
    hours
        .saturating_mul(60)
        .saturating_mul(60)
        .saturating_mul(1000)
}

pub fn retry_topic(kafka: &KafkaConfig) -> String {
    format!("{}.{}", kafka.topic_prefix, kafka.retry_topic_suffix)
}

pub fn deadletter_topic(kafka: &KafkaConfig) -> String {
    format!("{}.{}", kafka.topic_prefix, kafka.deadletter_topic_suffix)
}

pub fn partition_key(event: &CommonEvent) -> String {
    if let Some(conversation_id) = event.conversation_id.as_deref() {
        format!(
            "{}:{}:{}",
            event.project_id, event.company_id, conversation_id
        )
    } else if let Some(channel_id) = event.channel_id.as_deref() {
        format!("{}:{}:{}", event.project_id, event.company_id, channel_id)
    } else {
        format!("{}:{}", event.project_id, event.company_id)
    }
}

pub fn event_has_raw_media_bytes(event: &CommonEvent) -> bool {
    fn contains_bytes(value: &Value) -> bool {
        match value {
            Value::Object(map) => map.iter().any(|(key, value)| {
                matches!(key.as_str(), "bytes" | "file_bytes" | "raw_media" | "body")
                    || contains_bytes(value)
            }),
            Value::Array(items) => items.iter().any(contains_bytes),
            _ => false,
        }
    }
    contains_bytes(&event.payload)
}

pub fn encode_event_for_kafka(
    kafka: &KafkaConfig,
    event: &CommonEvent,
) -> Result<Vec<u8>, EventBusError> {
    if let Some(field) = raw_media_field(&event.payload) {
        return Err(EventBusError::RawMediaBytes {
            event_id: event.event_id.clone(),
            field,
        });
    }
    let bytes = serde_json::to_vec(event)?;
    if bytes.len() > kafka.message_max_bytes {
        return Err(EventBusError::MessageTooLarge {
            event_id: event.event_id.clone(),
            actual: bytes.len(),
            limit: kafka.message_max_bytes,
        });
    }
    Ok(bytes)
}

pub fn encode_deadletter_for_kafka(
    kafka: &KafkaConfig,
    envelope: &DeadLetterEnvelope,
) -> Result<Vec<u8>, EventBusError> {
    if let Some(field) = raw_media_field(&envelope.event.payload) {
        return Err(EventBusError::RawMediaBytes {
            event_id: envelope.event.event_id.clone(),
            field,
        });
    }
    let bytes = serde_json::to_vec(envelope)?;
    if bytes.len() > kafka.message_max_bytes {
        return Err(EventBusError::MessageTooLarge {
            event_id: envelope.event.event_id.clone(),
            actual: bytes.len(),
            limit: kafka.message_max_bytes,
        });
    }
    Ok(bytes)
}

pub fn decode_event_from_kafka(bytes: &[u8]) -> Result<CommonEvent, EventBusError> {
    Ok(serde_json::from_slice(bytes)?)
}

fn raw_media_field(value: &Value) -> Option<String> {
    fn find(value: &Value, path: &str) -> Option<String> {
        match value {
            Value::Object(map) => {
                for (key, value) in map {
                    let child_path = if path.is_empty() {
                        key.to_string()
                    } else {
                        format!("{path}.{key}")
                    };
                    if matches!(key.as_str(), "bytes" | "file_bytes" | "raw_media" | "body") {
                        return Some(child_path);
                    }
                    if let Some(found) = find(value, &child_path) {
                        return Some(found);
                    }
                }
                None
            }
            Value::Array(items) => items
                .iter()
                .enumerate()
                .find_map(|(index, value)| find(value, &format!("{path}[{index}]"))),
            _ => None,
        }
    }
    find(value, "")
}

pub fn deadletter_envelope(
    event: CommonEvent,
    retry_count: u32,
    error_code: &str,
) -> DeadLetterEnvelope {
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .expect("RFC3339 format works");
    DeadLetterEnvelope {
        event,
        retry_count,
        first_failed_at: now.clone(),
        last_failed_at: now,
        error_code: error_code.to_string(),
    }
}

pub fn retry_envelope(
    event: CommonEvent,
    previous: Option<RetryEnvelope>,
    error_code: &str,
) -> RetryEnvelope {
    retry_envelope_for_source(event, previous, error_code, EventSourceOffset::unknown(), 0)
}

pub fn retry_envelope_for_source(
    event: CommonEvent,
    previous: Option<RetryEnvelope>,
    error_code: &str,
    source: EventSourceOffset,
    retry_backoff_seconds: i64,
) -> RetryEnvelope {
    let now = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .expect("RFC3339 format works");
    let next_attempt_at = (OffsetDateTime::now_utc()
        + TimeDuration::seconds(retry_backoff_seconds.max(0)))
    .format(&time::format_description::well_known::Rfc3339)
    .expect("RFC3339 format works");
    RetryEnvelope {
        event,
        retry_count: previous
            .as_ref()
            .map(|envelope| envelope.retry_count.saturating_add(1))
            .unwrap_or(1),
        source: previous
            .as_ref()
            .map(|envelope| envelope.source.clone())
            .unwrap_or(source),
        first_failed_at: previous
            .as_ref()
            .map(|envelope| envelope.first_failed_at.clone())
            .unwrap_or_else(|| now.clone()),
        last_failed_at: now,
        next_attempt_at,
        error_code: error_code.to_string(),
    }
}

#[cfg(feature = "external-integrations")]
pub struct KafkaEventBus {
    config: KafkaConfig,
    producer: FutureProducer,
    admin: AdminClient<DefaultClientContext>,
}

#[cfg(feature = "external-integrations")]
impl KafkaEventBus {
    pub async fn connect(config: KafkaConfig) -> Result<Self, EventBusError> {
        let brokers = config
            .brokers
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(EventBusError::MissingBrokers)?;
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", brokers)
            .set("client.id", &config.client_id)
            .set("message.timeout.ms", config.request_timeout_ms.to_string())
            .set("delivery.timeout.ms", config.request_timeout_ms.to_string())
            .set("compression.type", &config.compression_type)
            .set("linger.ms", config.producer_linger_ms.to_string())
            .set("batch.size", config.producer_batch_size_bytes.to_string())
            .set("message.max.bytes", config.message_max_bytes.to_string())
            .set("enable.idempotence", "true")
            .set("acks", "all");

        let producer = FutureProducer::from_config(&client_config)
            .map_err(|err| EventBusError::Kafka(err.to_string()))?;
        let admin = AdminClient::from_config(&client_config)
            .map_err(|err| EventBusError::Kafka(err.to_string()))?;
        Ok(Self {
            config,
            producer,
            admin,
        })
    }

    pub fn config(&self) -> KafkaConfig {
        self.config.clone()
    }

    pub async fn ensure_topics(&self) -> Result<(), EventBusError> {
        let existing = self.existing_topics()?;
        let required = required_topic_specs(&self.config);
        let missing: Vec<_> = required
            .into_iter()
            .filter(|spec| !existing.contains(&spec.name))
            .collect();

        if missing.is_empty() {
            return Ok(());
        }
        if !self.config.enable_auto_create_topics {
            let names = missing
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(EventBusError::MissingTopics(names));
        }

        let retention_values: Vec<String> = missing
            .iter()
            .map(|spec| spec.retention_ms.to_string())
            .collect();
        let max_message_bytes = self.config.message_max_bytes.to_string();
        let topics: Vec<_> = missing
            .iter()
            .zip(retention_values.iter())
            .map(|(spec, retention)| {
                NewTopic::new(
                    spec.name.as_str(),
                    spec.partitions,
                    TopicReplication::Fixed(spec.replication_factor),
                )
                .set("retention.ms", retention.as_str())
                .set("max.message.bytes", max_message_bytes.as_str())
            })
            .collect();
        let opts = AdminOptions::new()
            .request_timeout(Some(StdDuration::from_millis(
                self.config.request_timeout_ms,
            )))
            .operation_timeout(Some(StdDuration::from_millis(
                self.config.request_timeout_ms,
            )));
        let results = self
            .admin
            .create_topics(&topics, &opts)
            .await
            .map_err(|err| EventBusError::Kafka(err.to_string()))?;
        for result in results {
            match result {
                Ok(_) => {}
                Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
                    tracing::debug!(topic, "Kafka topic already exists")
                }
                Err((topic, code)) => {
                    return Err(EventBusError::KafkaTopicCreate {
                        topic,
                        code: RdkafkaErrorCodeView::Other(code as i32),
                    });
                }
            }
        }
        Ok(())
    }

    pub async fn health(&self) -> Result<EventBusHealth, EventBusError> {
        let existing = self.existing_topics()?;
        let missing: Vec<_> = required_topic_specs(&self.config)
            .into_iter()
            .filter(|spec| !existing.contains(&spec.name))
            .map(|spec| spec.name)
            .collect();
        if missing.is_empty() {
            Ok(EventBusHealth {
                ok: true,
                detail: format!(
                    "kafka ready at {}",
                    self.config.brokers.as_deref().unwrap_or_default()
                ),
            })
        } else {
            Ok(EventBusHealth {
                ok: false,
                detail: format!("Kafka missing required topics: {}", missing.join(", ")),
            })
        }
    }

    pub async fn publish(&self, event: &CommonEvent) -> Result<PublishAck, EventBusError> {
        let topic = topic_for_event(&self.config, &event.event_type);
        let key = partition_key(event);
        let payload = encode_event_for_kafka(&self.config, event)?;
        self.publish_payload(&topic, &key, &payload).await
    }

    pub async fn publish_deadletter(
        &self,
        envelope: &DeadLetterEnvelope,
    ) -> Result<PublishAck, EventBusError> {
        let topic = deadletter_topic(&self.config);
        let key = partition_key(&envelope.event);
        let payload = encode_deadletter_for_kafka(&self.config, envelope)?;
        self.publish_payload(&topic, &key, &payload).await
    }

    async fn publish_payload(
        &self,
        topic: &str,
        key: &str,
        payload: &[u8],
    ) -> Result<PublishAck, EventBusError> {
        let delivery = self
            .producer
            .send(
                FutureRecord::to(topic).key(key).payload(payload),
                StdDuration::from_millis(self.config.request_timeout_ms),
            )
            .await;
        match delivery {
            Ok(delivery) => Ok(PublishAck {
                topic: topic.to_string(),
                partition: delivery.partition,
                offset: delivery.offset,
                key: key.to_string(),
            }),
            Err((err, _message)) => Err(EventBusError::Kafka(err.to_string())),
        }
    }

    fn existing_topics(&self) -> Result<BTreeSet<String>, EventBusError> {
        let metadata = self
            .admin
            .inner()
            .fetch_metadata(
                None,
                StdDuration::from_millis(self.config.request_timeout_ms),
            )
            .map_err(|err| EventBusError::Kafka(err.to_string()))?;
        Ok(metadata
            .topics()
            .iter()
            .filter(|topic| topic.error().is_none())
            .map(|topic| topic.name().to_string())
            .collect())
    }
}

#[cfg(feature = "external-integrations")]
impl Drop for KafkaEventBus {
    fn drop(&mut self) {
        if let Err(err) = self
            .producer
            .flush(StdDuration::from_millis(self.config.request_timeout_ms))
        {
            tracing::warn!(error = %err, "failed to flush Kafka producer during shutdown");
        }
    }
}

#[cfg(feature = "external-integrations")]
pub async fn consume_one_message_with_inbox<F, Fut>(
    consumer: &StreamConsumer,
    db: &MetadataDb,
    consumer_group: &str,
    handler: F,
) -> Result<ConsumerCommitDecision, EventBusError>
where
    F: FnOnce(KafkaConsumerMessage) -> Fut,
    Fut: std::future::Future<Output = Result<ConsumerHandlerStatus, EventBusError>>,
{
    let message = consumer
        .recv()
        .await
        .map_err(|err| EventBusError::Kafka(err.to_string()))?;
    let source = EventSourceOffset {
        topic: message.topic().to_string(),
        partition: message.partition(),
        offset: message.offset(),
    };
    let payload = message
        .payload()
        .ok_or_else(|| EventBusError::Kafka("Kafka message payload missing".to_string()))?;
    let event = decode_event_from_kafka(payload)?;
    let already_processed = db
        .event_inbox_processed(
            consumer_group,
            &event,
            &source.topic,
            source.partition,
            source.offset,
        )
        .await
        .map_err(|err| EventBusError::Kafka(err.to_string()))?;
    let result = if already_processed {
        Ok(ConsumerHandlerStatus::Duplicate)
    } else {
        handler(KafkaConsumerMessage {
            event: event.clone(),
            source: source.clone(),
        })
        .await
    };
    let decision = ConsumerCommitDecision::from_handler_result(result);
    if decision == ConsumerCommitDecision::Commit {
        if !already_processed {
            db.mark_event_inbox_processed(
                consumer_group,
                &event,
                &source.topic,
                source.partition,
                source.offset,
            )
            .await
            .map_err(|err| EventBusError::Kafka(err.to_string()))?;
        }
        consumer
            .commit_message(&message, CommitMode::Sync)
            .map_err(|err| EventBusError::Kafka(err.to_string()))?;
    }
    Ok(decision)
}

#[cfg(not(feature = "external-integrations"))]
pub struct KafkaEventBus {
    config: KafkaConfig,
}

#[cfg(not(feature = "external-integrations"))]
impl KafkaEventBus {
    pub async fn connect(config: KafkaConfig) -> Result<Self, EventBusError> {
        let _ = config;
        Err(EventBusError::KafkaFeatureDisabled)
    }

    pub fn config(&self) -> KafkaConfig {
        self.config.clone()
    }

    pub async fn ensure_topics(&self) -> Result<(), EventBusError> {
        Err(EventBusError::KafkaFeatureDisabled)
    }

    pub async fn health(&self) -> Result<EventBusHealth, EventBusError> {
        Err(EventBusError::KafkaFeatureDisabled)
    }

    pub async fn publish(&self, _event: &CommonEvent) -> Result<PublishAck, EventBusError> {
        Err(EventBusError::KafkaFeatureDisabled)
    }

    pub async fn publish_deadletter(
        &self,
        _envelope: &DeadLetterEnvelope,
    ) -> Result<PublishAck, EventBusError> {
        Err(EventBusError::KafkaFeatureDisabled)
    }

    async fn publish_payload(
        &self,
        _topic: &str,
        _key: &str,
        _payload: &[u8],
    ) -> Result<PublishAck, EventBusError> {
        Err(EventBusError::KafkaFeatureDisabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_kafka_config() -> KafkaConfig {
        KafkaConfig {
            brokers: Some("localhost:9092".to_string()),
            client_id: "rustzap".to_string(),
            topic_prefix: "rustzap".to_string(),
            consumer_group: "rustzap-main".to_string(),
            retry_topic_suffix: "retry".to_string(),
            deadletter_topic_suffix: "deadletter".to_string(),
            message_max_bytes: 1024,
            enable_auto_create_topics: true,
            default_partitions: 24,
            default_replication_factor: 1,
            message_retention_hours: 168,
            deadletter_retention_hours: 720,
            compression_type: "none".to_string(),
            producer_linger_ms: 5,
            producer_batch_size_bytes: 65_536,
            consumer_max_poll_records: 500,
            retry_max_attempts: 8,
            request_timeout_ms: 5_000,
        }
    }

    #[test]
    fn event_schema_has_required_ids_and_no_media_bytes() {
        let event = dirty_signal("p", "c", "ch", "conv", 7, "new_message", 100);
        let json = serde_json::to_value(event).unwrap();
        assert!(json["event_id"].as_str().unwrap().starts_with("evt_"));
        assert!(json["trace_id"].as_str().unwrap().starts_with("trace_"));
        assert_eq!(json["payload"]["to_seq"], 7);
        assert!(json.get("bytes").is_none());
    }

    #[test]
    fn topic_mapping_and_partition_key_are_stable() {
        let kafka = test_kafka_config();
        let event = dirty_signal("p", "c", "ch", "conv", 7, "new_message", 100);
        assert_eq!(
            topic_for_event(&kafka, &event.event_type),
            "rustzap.conversation.dirty"
        );
        assert_eq!(
            topic_for_event(&kafka, "whatsapp.raw.inbound"),
            "rustzap.raw.inbound"
        );
        assert_eq!(
            topic_for_event(&kafka, "message.received"),
            "rustzap.message.persisted"
        );
        assert_eq!(partition_key(&event), "p:c:conv");
        assert_eq!(retry_topic(&kafka), "rustzap.retry");
        assert_eq!(deadletter_topic(&kafka), "rustzap.deadletter");
    }

    #[test]
    fn detects_raw_media_bytes_in_event_payloads() {
        let event = new_event(
            "media.stored",
            "p",
            "c",
            None,
            None,
            None,
            None,
            json!({
                "media_id": "m",
                "bytes": [1, 2, 3]
            }),
        );
        assert!(event_has_raw_media_bytes(&event));
    }

    #[test]
    fn kafka_readiness_requires_brokers() {
        let mut kafka = test_kafka_config();
        kafka.brokers = None;
        assert!(!health_check(EventBusMode::Kafka, &kafka).ok);
        kafka.brokers = Some("redpanda:9092".to_string());
        assert!(health_check(EventBusMode::Kafka, &kafka).ok);
    }

    #[test]
    fn required_topic_specs_cover_runtime_topics_and_retention() {
        let kafka = test_kafka_config();
        let specs = required_topic_specs(&kafka);
        let names: Vec<_> = specs.iter().map(|spec| spec.name.as_str()).collect();

        assert!(names.contains(&"rustzap.raw.inbound"));
        assert!(names.contains(&"rustzap.message.persisted"));
        assert!(names.contains(&"rustzap.media.download.requested"));
        assert!(names.contains(&"rustzap.audio.transcribed"));
        assert!(names.contains(&"rustzap.conversation.dirty"));
        assert!(names.contains(&"rustzap.retry"));
        assert!(names.contains(&"rustzap.deadletter"));

        let main_topic = specs
            .iter()
            .find(|spec| spec.name == "rustzap.raw.inbound")
            .unwrap();
        assert_eq!(main_topic.partitions, 24);
        assert_eq!(main_topic.replication_factor, 1);
        assert_eq!(main_topic.retention_ms, 168 * 60 * 60 * 1000);

        let deadletter = specs
            .iter()
            .find(|spec| spec.name == "rustzap.deadletter")
            .unwrap();
        assert_eq!(deadletter.retention_ms, 720 * 60 * 60 * 1000);
    }

    #[test]
    fn kafka_codec_round_trips_compact_events_and_rejects_media_bytes() {
        let kafka = test_kafka_config();
        let event = dirty_signal("p", "c", "ch", "conv", 7, "new_message", 100);
        let encoded = encode_event_for_kafka(&kafka, &event).unwrap();
        let decoded = decode_event_from_kafka(&encoded).unwrap();
        assert_eq!(decoded.event_id, event.event_id);
        assert_eq!(decoded.event_type, "conversation.dirty");

        let raw_media = new_event(
            "media.stored",
            "p",
            "c",
            None,
            None,
            None,
            None,
            json!({ "media_id": "m", "body": "base64-raw" }),
        );
        let err = encode_event_for_kafka(&kafka, &raw_media).unwrap_err();
        assert!(matches!(err, EventBusError::RawMediaBytes { .. }));
    }

    #[test]
    fn kafka_deadletter_codec_rejects_raw_media_bytes() {
        let kafka = test_kafka_config();
        let raw_media = new_event(
            "media.stored",
            "p",
            "c",
            None,
            None,
            None,
            None,
            json!({ "media_id": "m", "body": "base64-raw" }),
        );
        let envelope = deadletter_envelope(raw_media, 1, "blocked");

        let err = encode_deadletter_for_kafka(&kafka, &envelope).unwrap_err();

        assert!(matches!(err, EventBusError::RawMediaBytes { .. }));
    }

    #[test]
    fn kafka_codec_rejects_messages_over_configured_limit() {
        let mut kafka = test_kafka_config();
        kafka.message_max_bytes = 64;
        let event = dirty_signal("p", "c", "ch", "conv", 7, "new_message", 100);
        let err = encode_event_for_kafka(&kafka, &event).unwrap_err();
        assert!(matches!(err, EventBusError::MessageTooLarge { .. }));
    }

    #[test]
    fn retry_envelope_promotes_to_deadletter_after_max_attempts() {
        let kafka = test_kafka_config();
        let event = dirty_signal("p", "c", "ch", "conv", 7, "new_message", 100);
        let retry = retry_envelope(event.clone(), None, "handler_failed");
        assert_eq!(retry.retry_count, 1);
        assert!(!retry.should_deadletter(&kafka));

        let retry = retry_envelope(event.clone(), Some(retry), "handler_failed_again");
        assert_eq!(retry.retry_count, 2);

        let terminal = RetryEnvelope {
            event,
            retry_count: kafka.retry_max_attempts,
            source: retry.source.clone(),
            first_failed_at: retry.first_failed_at,
            last_failed_at: retry.last_failed_at,
            next_attempt_at: retry.next_attempt_at,
            error_code: "terminal".to_string(),
        };
        assert!(terminal.should_deadletter(&kafka));
        let deadletter = terminal.into_deadletter();
        assert_eq!(deadletter.retry_count, kafka.retry_max_attempts);
        assert_eq!(deadletter.error_code, "terminal");
    }

    #[tokio::test]
    async fn local_event_bus_handle_reports_ready_and_noops_publish() {
        let kafka = test_kafka_config();
        let handle = EventBusHandle::local(EventBusMode::InMemory, kafka);
        handle.publish_background(dirty_signal("p", "c", "ch", "conv", 1, "debug", 1));

        let health = handle.health().await;
        assert!(health.ok);
        assert_eq!(health.detail, "in-memory event bus");

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.mode, "InMemory");
        assert_eq!(snapshot.publish_attempts, 0);
        assert_eq!(snapshot.publish_failures, 0);
    }

    #[test]
    fn retry_envelope_preserves_source_metadata_and_calculates_backoff() {
        let kafka = test_kafka_config();
        let event = dirty_signal("p", "c", "ch", "conv", 7, "new_message", 100);
        let retry = retry_envelope_for_source(
            event,
            None,
            "handler_failed",
            EventSourceOffset {
                topic: "rustzap.raw.inbound".to_string(),
                partition: 2,
                offset: 41,
            },
            30,
        );

        assert_eq!(retry.retry_count, 1);
        assert_eq!(retry.source.topic, "rustzap.raw.inbound");
        assert_eq!(retry.source.partition, 2);
        assert_eq!(retry.source.offset, 41);
        assert!(retry.next_attempt_at >= retry.last_failed_at);
        assert!(!retry.should_deadletter(&kafka));
    }

    #[test]
    fn consumer_decision_commits_only_after_successful_handler_or_durable_failure_record() {
        assert_eq!(
            ConsumerCommitDecision::from_handler_result(Ok(ConsumerHandlerStatus::Processed)),
            ConsumerCommitDecision::Commit
        );
        assert_eq!(
            ConsumerCommitDecision::from_handler_result(Ok(ConsumerHandlerStatus::Duplicate)),
            ConsumerCommitDecision::Commit
        );
        assert_eq!(
            ConsumerCommitDecision::from_handler_result(Ok(ConsumerHandlerStatus::Retried)),
            ConsumerCommitDecision::Commit
        );
        assert_eq!(
            ConsumerCommitDecision::from_handler_result(Ok(ConsumerHandlerStatus::DeadLettered)),
            ConsumerCommitDecision::Commit
        );
        assert_eq!(
            ConsumerCommitDecision::from_handler_result(Err(EventBusError::Kafka(
                "broker down".to_string()
            ))),
            ConsumerCommitDecision::DoNotCommit
        );
    }

    #[test]
    fn durable_outbox_mode_never_directly_publishes_app_events() {
        let kafka = test_kafka_config();
        let handle = EventBusHandle::durable_outbox(EventBusMode::Kafka, kafka);
        handle.publish_background(dirty_signal("p", "c", "ch", "conv", 1, "debug", 1));

        let snapshot = handle.snapshot();
        assert!(snapshot.durable_outbox);
        assert_eq!(snapshot.publish_attempts, 0);
        assert_eq!(snapshot.outbox_enqueue_observed, 1);
    }
}
