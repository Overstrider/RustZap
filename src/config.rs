use std::{env, net::SocketAddr, path::PathBuf, str::FromStr};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub env: String,
    pub bind_addr: String,
    pub public_base_url: String,
    pub log_level: String,
    pub dev_mode: bool,
    pub dev_simulation_enabled: bool,
    pub admin_api_key: String,
    pub project_api_key: String,
    pub metadata_db: MetadataDbMode,
    pub database_url: Option<String>,
    pub database_max_connections: u32,
    pub database_min_connections: u32,
    pub wa_session_sqlite_dir: PathBuf,
    pub event_bus: EventBusMode,
    pub consumer_signal_mode: ConsumerSignalMode,
    pub storage_provider: StorageProvider,
    pub r2: R2Config,
    pub kafka: KafkaConfig,
    pub webhook: WebhookConfig,
    pub groq: GroqConfig,
    pub rate_limits: RateLimitConfig,
    pub media_local_temp_dir: PathBuf,
    pub media_local_temp_max_gb: u64,
    pub media_sniff_magic_bytes: bool,
    pub r2_presigned_url_ttl_seconds: u64,
    pub media_temp_retention_days: u32,
    pub media_quick_delete_threshold_mb: u64,
    pub media_reject_threshold_mb: u64,
    pub webhook_signing_header: String,
    pub log_redact_message_text: bool,
    pub log_redact_phone: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventBusMode {
    InMemory,
    Postgres,
    Kafka,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataDbMode {
    InMemory,
    Postgres,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerSignalMode {
    Polling,
    Websocket,
    Webhook,
    Kafka,
    WebsocketAndPolling,
    WebhookAndPolling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProvider {
    LocalFs,
    R2,
}

#[derive(Debug, Clone)]
pub struct R2Config {
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub bucket: String,
    pub public_url: Option<String>,
    pub region: String,
    pub base_prefix: String,
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: Option<String>,
    pub client_id: String,
    pub topic_prefix: String,
    pub consumer_group: String,
    pub retry_topic_suffix: String,
    pub deadletter_topic_suffix: String,
    pub message_max_bytes: usize,
    pub enable_auto_create_topics: bool,
    pub default_partitions: i32,
    pub default_replication_factor: i32,
    pub message_retention_hours: i64,
    pub deadletter_retention_hours: i64,
    pub compression_type: String,
    pub producer_linger_ms: u64,
    pub producer_batch_size_bytes: usize,
    pub consumer_max_poll_records: usize,
    pub retry_max_attempts: u32,
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub delivery_enabled: bool,
    pub timeout_seconds: u64,
    pub max_batch_size: usize,
    pub max_retries: u32,
    pub retry_base_seconds: u64,
    pub retry_max_seconds: u64,
    pub signing_header: String,
    pub timestamp_header: String,
    pub event_id_header: String,
}

#[derive(Debug, Clone)]
pub struct GroqConfig {
    pub api_key: Option<String>,
    pub stt_model: String,
    pub stt_language: String,
    pub stt_response_format: String,
    pub stt_timeout_seconds: u64,
    pub stt_max_audio_mb: u64,
    pub stt_enable_preprocessing: bool,
    pub ffmpeg_path: String,
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_minute_per_project: u32,
    pub send_message_per_minute_per_channel: u32,
    pub send_message_per_minute_per_conversation: u32,
    pub media_downloads_per_minute_per_channel: u32,
    pub stt_per_minute_per_project: u32,
    pub admin_requests_per_minute: u32,
    pub ws_subscriptions_per_connection: u32,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            env: env_or("RUSTZAP_ENV", "development"),
            bind_addr: env_or("RUSTZAP_BIND_ADDR", "0.0.0.0:8167"),
            public_base_url: env_or("RUSTZAP_PUBLIC_BASE_URL", "http://localhost:8167"),
            log_level: env_or("RUSTZAP_LOG_LEVEL", "info"),
            dev_mode: env_bool("RUSTZAP_DEV_MODE", true),
            dev_simulation_enabled: env_bool("DEV_SIMULATION_ENABLED", false),
            admin_api_key: env_or("RUSTZAP_ADMIN_API_KEY", "dev_admin_key"),
            project_api_key: env_or("RUSTZAP_PROJECT_API_KEY", "dev_project_key"),
            metadata_db: MetadataDbMode::from_env(),
            database_url: env::var("DATABASE_URL").ok(),
            database_max_connections: env_u32("DATABASE_MAX_CONNECTIONS", 50),
            database_min_connections: env_u32("DATABASE_MIN_CONNECTIONS", 5),
            wa_session_sqlite_dir: PathBuf::from(env_or(
                "WA_SESSION_SQLITE_DIR",
                "/data/rustzap/wa-sessions",
            )),
            event_bus: EventBusMode::from_env(),
            consumer_signal_mode: ConsumerSignalMode::from_env(),
            storage_provider: StorageProvider::from_env(),
            r2: R2Config::from_env(),
            kafka: KafkaConfig::from_env(),
            webhook: WebhookConfig::from_env(),
            groq: GroqConfig::from_env(),
            rate_limits: RateLimitConfig::from_env(),
            media_local_temp_dir: PathBuf::from(env_or(
                "MEDIA_LOCAL_TEMP_DIR",
                "/data/rustzap/tmp-media",
            )),
            media_local_temp_max_gb: env_u64("MEDIA_LOCAL_TEMP_MAX_GB", 20),
            media_sniff_magic_bytes: env_bool("MEDIA_SNIFF_MAGIC_BYTES", true),
            r2_presigned_url_ttl_seconds: env_u64("R2_PRESIGNED_URL_TTL_SECONDS", 900),
            media_temp_retention_days: env_u32("MEDIA_TEMP_RETENTION_DAYS", 30),
            media_quick_delete_threshold_mb: env_u64("MEDIA_QUICK_DELETE_THRESHOLD_MB", 25),
            media_reject_threshold_mb: env_u64("MEDIA_REJECT_THRESHOLD_MB", 100),
            webhook_signing_header: env_or("WEBHOOK_SIGNING_HEADER", "X-RustZap-Signature"),
            log_redact_message_text: env_bool("RUSTZAP_LOG_REDACT_MESSAGE_TEXT", true),
            log_redact_phone: env_bool("RUSTZAP_LOG_REDACT_PHONE", true),
        }
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        SocketAddr::from_str(&self.bind_addr)
    }

    pub fn public_object_url(&self, object_key: &str) -> Option<String> {
        self.r2.public_object_url(object_key)
    }

    pub fn dev_media_url(&self, media_id: &str) -> String {
        format!(
            "{}/dev-media/{}",
            self.public_base_url.trim_end_matches('/'),
            media_id
        )
    }
}

impl EventBusMode {
    fn from_env() -> Self {
        match env_or("EVENT_BUS", "in_memory").as_str() {
            "kafka" => Self::Kafka,
            "postgres" => Self::Postgres,
            _ => Self::InMemory,
        }
    }
}

impl MetadataDbMode {
    fn from_env() -> Self {
        match env_or("RUSTZAP_METADATA_DB", "in_memory").as_str() {
            "postgres" => Self::Postgres,
            _ => Self::InMemory,
        }
    }
}

impl ConsumerSignalMode {
    fn from_env() -> Self {
        match env_or("CONSUMER_SIGNAL_MODE", "polling").as_str() {
            "websocket" => Self::Websocket,
            "webhook" => Self::Webhook,
            "kafka" => Self::Kafka,
            "websocket_and_polling" => Self::WebsocketAndPolling,
            "webhook_and_polling" => Self::WebhookAndPolling,
            _ => Self::Polling,
        }
    }
}

impl StorageProvider {
    fn from_env() -> Self {
        match env_or("STORAGE_PROVIDER", "local_fs").as_str() {
            "r2" => Self::R2,
            _ => Self::LocalFs,
        }
    }
}

impl R2Config {
    fn from_env() -> Self {
        Self {
            endpoint: env_first(&["R2_DEV_ENDPOINT", "R2_ENDPOINT"]),
            access_key_id: env_first(&["R2_DEV_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID"]),
            secret_access_key: env_first(&["R2_DEV_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY"]),
            bucket: env_first(&["R2_DEV_BUCKET", "R2_BUCKET"])
                .unwrap_or_else(|| "devbucket".to_string()),
            public_url: env_first(&["R2_DEV_PUBLIC_URL", "R2_PUBLIC_URL"]),
            region: env_first(&["R2_DEV_REGION", "R2_REGION"])
                .unwrap_or_else(|| "auto".to_string()),
            base_prefix: env_first(&["R2_DEV_BASE_PREFIX", "R2_BASE_PREFIX"])
                .unwrap_or_else(|| "rustyzap".to_string()),
        }
    }

    pub fn public_object_url(&self, object_key: &str) -> Option<String> {
        self.public_url.as_ref().map(|base| {
            format!(
                "{}/{}",
                base.trim_end_matches('/'),
                object_key.trim_start_matches('/')
            )
        })
    }

    pub fn can_upload(&self) -> bool {
        self.endpoint
            .as_deref()
            .is_some_and(|value| !value.is_empty())
            && self
                .access_key_id
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            && self
                .secret_access_key
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            && !self.bucket.is_empty()
    }
}

impl KafkaConfig {
    fn from_env() -> Self {
        Self::from_lookup(|key| env::var(key).ok())
    }

    pub(crate) fn from_lookup<F>(mut lookup: F) -> Self
    where
        F: FnMut(&str) -> Option<String>,
    {
        Self {
            brokers: lookup("KAFKA_BROKERS").filter(|value| !value.trim().is_empty()),
            client_id: lookup_or(&mut lookup, "KAFKA_CLIENT_ID", "rustzap"),
            topic_prefix: lookup_or(&mut lookup, "KAFKA_TOPIC_PREFIX", "rustzap"),
            consumer_group: lookup_or(&mut lookup, "KAFKA_CONSUMER_GROUP", "rustzap-main"),
            retry_topic_suffix: lookup_or(&mut lookup, "KAFKA_RETRY_TOPIC_SUFFIX", "retry"),
            deadletter_topic_suffix: lookup_or(
                &mut lookup,
                "KAFKA_DEADLETTER_TOPIC_SUFFIX",
                "deadletter",
            ),
            message_max_bytes: lookup_usize(&mut lookup, "KAFKA_MESSAGE_MAX_BYTES", 1_048_576),
            enable_auto_create_topics: lookup_bool(
                &mut lookup,
                "KAFKA_ENABLE_AUTO_CREATE_TOPICS",
                true,
            ),
            default_partitions: lookup_i32(&mut lookup, "KAFKA_DEFAULT_PARTITIONS", 24),
            default_replication_factor: lookup_i32(
                &mut lookup,
                "KAFKA_DEFAULT_REPLICATION_FACTOR",
                1,
            ),
            message_retention_hours: lookup_i64(&mut lookup, "KAFKA_MESSAGE_RETENTION_HOURS", 168),
            deadletter_retention_hours: lookup_i64(
                &mut lookup,
                "KAFKA_DEADLETTER_RETENTION_HOURS",
                168,
            ),
            compression_type: lookup_or(&mut lookup, "KAFKA_COMPRESSION_TYPE", "none"),
            producer_linger_ms: lookup_u64(&mut lookup, "KAFKA_PRODUCER_LINGER_MS", 5),
            producer_batch_size_bytes: lookup_usize(
                &mut lookup,
                "KAFKA_PRODUCER_BATCH_SIZE_BYTES",
                65_536,
            ),
            consumer_max_poll_records: lookup_usize(
                &mut lookup,
                "KAFKA_CONSUMER_MAX_POLL_RECORDS",
                500,
            ),
            retry_max_attempts: lookup_u32(&mut lookup, "KAFKA_RETRY_MAX_ATTEMPTS", 8),
            request_timeout_ms: lookup_u64(&mut lookup, "KAFKA_REQUEST_TIMEOUT_MS", 5_000),
        }
    }
}

impl WebhookConfig {
    fn from_env() -> Self {
        Self {
            delivery_enabled: env_bool("WEBHOOK_DELIVERY_ENABLED", false),
            timeout_seconds: env_u64("WEBHOOK_TIMEOUT_SECONDS", 10),
            max_batch_size: env_usize("WEBHOOK_MAX_BATCH_SIZE", 100),
            max_retries: env_u32("WEBHOOK_MAX_RETRIES", 8),
            retry_base_seconds: env_u64("WEBHOOK_RETRY_BASE_SECONDS", 5),
            retry_max_seconds: env_u64("WEBHOOK_RETRY_MAX_SECONDS", 3600),
            signing_header: env_or("WEBHOOK_SIGNING_HEADER", "X-RustZap-Signature"),
            timestamp_header: env_or("WEBHOOK_TIMESTAMP_HEADER", "X-RustZap-Timestamp"),
            event_id_header: env_or("WEBHOOK_EVENT_ID_HEADER", "X-RustZap-Event-Id"),
        }
    }
}

impl GroqConfig {
    fn from_env() -> Self {
        Self {
            api_key: env::var("GROQ_API_KEY")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            stt_model: env_or("GROQ_STT_MODEL", "whisper-large-v3-turbo"),
            stt_language: env_or("GROQ_STT_LANGUAGE", "pt"),
            stt_response_format: env_or("GROQ_STT_RESPONSE_FORMAT", "verbose_json"),
            stt_timeout_seconds: env_u64("GROQ_STT_TIMEOUT_SECONDS", 60),
            stt_max_audio_mb: env_u64("GROQ_STT_MAX_AUDIO_MB", 25),
            stt_enable_preprocessing: env_bool("GROQ_STT_ENABLE_PREPROCESSING", true),
            ffmpeg_path: env_or("FFMPEG_PATH", "/usr/bin/ffmpeg"),
        }
    }
}

impl RateLimitConfig {
    fn from_env() -> Self {
        Self {
            requests_per_minute_per_project: env_u32(
                "RATE_LIMIT_REQUESTS_PER_MINUTE_PER_PROJECT",
                6000,
            ),
            send_message_per_minute_per_channel: env_u32(
                "RATE_LIMIT_SEND_MESSAGE_PER_MINUTE_PER_CHANNEL",
                120,
            ),
            send_message_per_minute_per_conversation: env_u32(
                "RATE_LIMIT_SEND_MESSAGE_PER_MINUTE_PER_CONVERSATION",
                20,
            ),
            media_downloads_per_minute_per_channel: env_u32(
                "RATE_LIMIT_MEDIA_DOWNLOADS_PER_MINUTE_PER_CHANNEL",
                300,
            ),
            stt_per_minute_per_project: env_u32("RATE_LIMIT_STT_PER_MINUTE_PER_PROJECT", 120),
            admin_requests_per_minute: env_u32("RATE_LIMIT_ADMIN_REQUESTS_PER_MINUTE", 300),
            ws_subscriptions_per_connection: env_u32(
                "RATE_LIMIT_WS_SUBSCRIPTIONS_PER_CONNECTION",
                1000,
            ),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_first(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| env::var(key).ok().filter(|value| !value.trim().is_empty()))
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn lookup_or<F>(lookup: &mut F, key: &str, default: &str) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn lookup_bool<F>(lookup: &mut F, key: &str, default: bool) -> bool
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn lookup_i32<F>(lookup: &mut F, key: &str, default: i32) -> i32
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn lookup_i64<F>(lookup: &mut F, key: &str, default: i64) -> i64
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn lookup_u32<F>(lookup: &mut F, key: &str, default: u32) -> u32
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn lookup_u64<F>(lookup: &mut F, key: &str, default: u64) -> u64
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn lookup_usize<F>(lookup: &mut F, key: &str, default: usize) -> usize
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn kafka_config_parses_full_env_surface() {
        let env = HashMap::from([
            ("KAFKA_BROKERS", "redpanda:9092"),
            ("KAFKA_CLIENT_ID", "rustzap-prod"),
            ("KAFKA_TOPIC_PREFIX", "rz"),
            ("KAFKA_CONSUMER_GROUP", "rz-main"),
            ("KAFKA_ENABLE_AUTO_CREATE_TOPICS", "false"),
            ("KAFKA_DEFAULT_PARTITIONS", "12"),
            ("KAFKA_DEFAULT_REPLICATION_FACTOR", "3"),
            ("KAFKA_MESSAGE_RETENTION_HOURS", "240"),
            ("KAFKA_DEADLETTER_RETENTION_HOURS", "1440"),
            ("KAFKA_COMPRESSION_TYPE", "lz4"),
            ("KAFKA_PRODUCER_LINGER_MS", "15"),
            ("KAFKA_PRODUCER_BATCH_SIZE_BYTES", "131072"),
            ("KAFKA_MESSAGE_MAX_BYTES", "2097152"),
            ("KAFKA_CONSUMER_MAX_POLL_RECORDS", "250"),
            ("KAFKA_RETRY_TOPIC_SUFFIX", "retry.v1"),
            ("KAFKA_DEADLETTER_TOPIC_SUFFIX", "dlq.v1"),
            ("KAFKA_RETRY_MAX_ATTEMPTS", "5"),
            ("KAFKA_REQUEST_TIMEOUT_MS", "7000"),
        ]);

        let config = KafkaConfig::from_lookup(|key| env.get(key).map(|value| value.to_string()));

        assert_eq!(config.brokers.as_deref(), Some("redpanda:9092"));
        assert_eq!(config.client_id, "rustzap-prod");
        assert_eq!(config.topic_prefix, "rz");
        assert_eq!(config.consumer_group, "rz-main");
        assert!(!config.enable_auto_create_topics);
        assert_eq!(config.default_partitions, 12);
        assert_eq!(config.default_replication_factor, 3);
        assert_eq!(config.message_retention_hours, 240);
        assert_eq!(config.deadletter_retention_hours, 1440);
        assert_eq!(config.compression_type, "lz4");
        assert_eq!(config.producer_linger_ms, 15);
        assert_eq!(config.producer_batch_size_bytes, 131_072);
        assert_eq!(config.message_max_bytes, 2_097_152);
        assert_eq!(config.consumer_max_poll_records, 250);
        assert_eq!(config.retry_topic_suffix, "retry.v1");
        assert_eq!(config.deadletter_topic_suffix, "dlq.v1");
        assert_eq!(config.retry_max_attempts, 5);
        assert_eq!(config.request_timeout_ms, 7_000);
    }

    #[test]
    fn kafka_config_defaults_are_production_safe_for_dev_redpanda() {
        let config = KafkaConfig::from_lookup(|_| None);

        assert_eq!(config.brokers, None);
        assert_eq!(config.client_id, "rustzap");
        assert_eq!(config.topic_prefix, "rustzap");
        assert_eq!(config.consumer_group, "rustzap-main");
        assert!(config.enable_auto_create_topics);
        assert_eq!(config.default_partitions, 24);
        assert_eq!(config.default_replication_factor, 1);
        assert_eq!(config.message_retention_hours, 168);
        assert_eq!(config.deadletter_retention_hours, 168);
        assert_eq!(config.compression_type, "none");
        assert_eq!(config.producer_linger_ms, 5);
        assert_eq!(config.producer_batch_size_bytes, 65_536);
        assert_eq!(config.message_max_bytes, 1_048_576);
        assert_eq!(config.consumer_max_poll_records, 500);
        assert_eq!(config.retry_max_attempts, 8);
        assert_eq!(config.request_timeout_ms, 5_000);
    }
}
