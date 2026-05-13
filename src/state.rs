use std::{
    collections::{HashMap, HashSet},
    env,
    io::Cursor,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration as StdDuration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

use crate::{
    config::{AppConfig, ConsumerSignalMode, EventBusMode, MetadataDbMode, StorageProvider},
    db::{KafkaDeadLetterRecord, MetadataDb, OutboxRecord},
    error::{ApiError, ApiResult},
    eventbus::{EventBusHandle, dirty_signal, new_event},
    media::{MediaDecision, MediaLimits, R2ObjectKeyInput, magic_matches_mime, r2_object_key},
    models::{
        DirtyAckRequest, DirtyConversationItem, MediaObject, Message, MessagesPage, QrState,
        SendMessageRequest, SimulateInboundMediaRequest, SimulateInboundTextRequest, Transcript,
        WebhookDeliveryAttempt,
    },
    rate_limit::RateLimiter,
    security::sha256_json,
    storage::MediaByteStore,
    transcription::{
        GroqSttClient, TranscriptLifecycle, groq_transcript_from_result, mock_transcript,
        transcript_with_lifecycle,
    },
    whatsapp::{
        ChannelRuntime, ContactProfile, GroupParticipantProfile, GroupProfile,
        InboundMediaDescriptor, OutboundMediaMessage, WhatsappEvent, WhatsappManager,
        is_updates_surface_jid, qr_expires_at, session_sqlite_path,
    },
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    inner: Arc<Mutex<StoreInner>>,
    whatsapp: WhatsappManager,
    events_tx: broadcast::Sender<crate::models::CommonEvent>,
    event_bus: EventBusHandle,
    rate_limiter: Arc<RateLimiter>,
    metrics: Arc<RuntimeMetrics>,
    media_bytes: MediaByteStore,
    dev_state_path: Option<PathBuf>,
    metadata_db: Option<Arc<MetadataDb>>,
    metadata_persist_tx: Option<mpsc::UnboundedSender<PersistedStore>>,
}

pub struct SendMessageOutcome {
    pub message: Message,
    pub should_dispatch: bool,
}

pub struct RateLimitScope<'a> {
    pub headers: &'a axum::http::HeaderMap,
    pub api_key_id: &'a str,
    pub project_id: Option<&'a str>,
    pub company_id: Option<&'a str>,
    pub family: &'a str,
    pub resource_id: Option<&'a str>,
    pub max_per_minute: u32,
}

struct InboundMediaRecordInput<'a> {
    runtime: &'a ChannelRuntime,
    message: &'a Message,
    conversation_id: &'a str,
    descriptor: &'a InboundMediaDescriptor,
    media_id: String,
    size_bytes: u64,
    sha256: String,
    storage_status: &'a str,
    object_key: Option<String>,
}

#[derive(Default)]
struct RuntimeMetrics {
    rate_limited_total: AtomicU64,
    webhook_delivery_attempts_total: AtomicU64,
    webhook_delivery_successes_total: AtomicU64,
    stt_requests_total: AtomicU64,
    websocket_subscriptions_total: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeMetricsSnapshot {
    pub rate_limited_total: u64,
    pub webhook_delivery_attempts_total: u64,
    pub webhook_delivery_successes_total: u64,
    pub stt_requests_total: u64,
    pub websocket_subscriptions_total: u64,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct RetentionSummary {
    pub messages_removed: usize,
    pub transcripts_removed: usize,
    pub transcripts_redacted: usize,
    pub media_removed: usize,
}

#[derive(Debug, Clone)]
pub struct OutboundMediaUpload {
    pub conversation_id: String,
    pub media_type: Option<String>,
    pub mime_type: Option<String>,
    pub filename: Option<String>,
    pub caption: Option<String>,
    pub bytes: Vec<u8>,
}

struct ContactRefreshContext {
    channel_id: Option<String>,
    conversation_id: Option<String>,
    alt_jid: Option<String>,
    push_name: Option<String>,
}

struct WhatsappMessageInput {
    project_id: String,
    company_id: String,
    channel_id: String,
    conversation_id: String,
    contact_id: Option<String>,
    contact_alt_jid: Option<String>,
    contact_name: Option<String>,
    direction: String,
    message_type: String,
    text: Option<String>,
    status: String,
    wa_message_id: Option<String>,
    sender_contact_id: Option<String>,
    sender_alt_jid: Option<String>,
    sender_name: Option<String>,
    created_at_wa: OffsetDateTime,
}

struct WebhookDeliveryJob {
    callback: Value,
    event: crate::models::CommonEvent,
    attempt: u32,
    first_failed_at: Option<OffsetDateTime>,
}

struct WebhookAttemptInput<'a> {
    callback_id: String,
    event: &'a crate::models::CommonEvent,
    attempt: u32,
    status: &'static str,
    http_status: Option<u16>,
    error_message: Option<String>,
    first_failed_at: Option<OffsetDateTime>,
    next_retry_at: Option<OffsetDateTime>,
}

#[derive(Clone, Serialize, Deserialize)]
struct RawWhatsappEventEnvelope {
    runtime: RawWhatsappRuntimeEnvelope,
    event: WhatsappEvent,
}

#[derive(Clone, Serialize, Deserialize)]
struct RawWhatsappRuntimeEnvelope {
    project_id: String,
    company_id: String,
    channel_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ContactRecord {
    pub(crate) id: String,
    pub(crate) canonical_jid: Option<String>,
    pub(crate) lid: Option<String>,
    pub(crate) project_id: String,
    pub(crate) company_id: String,
    #[serde(default)]
    pub(crate) channel_account_id: Option<String>,
    pub(crate) phone_e164: Option<String>,
    pub(crate) push_name: Option<String>,
    pub(crate) display_name: String,
    #[serde(default)]
    pub(crate) profile_picture_media_id: Option<String>,
    #[serde(default)]
    pub(crate) business_description: Option<String>,
    pub(crate) avatar_url: Option<String>,
    pub(crate) profile_picture_url: Option<String>,
    #[serde(with = "crate::models::serde_time_compat")]
    pub(crate) first_contact_at: OffsetDateTime,
    #[serde(with = "crate::models::serde_time_compat")]
    pub(crate) last_contact_at: OffsetDateTime,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct GroupMemberRecord {
    pub(crate) group_id: String,
    pub(crate) contact_id: String,
    #[serde(default)]
    pub(crate) wa_jid: Option<String>,
    #[serde(default)]
    pub(crate) phone_e164: Option<String>,
    pub(crate) display_name: String,
    pub(crate) role: String,
    pub(crate) is_admin: bool,
    #[serde(default = "default_now")]
    #[serde(with = "crate::models::serde_time_compat")]
    pub(crate) joined_at: OffsetDateTime,
    #[serde(default = "default_now")]
    #[serde(with = "crate::models::serde_time_compat")]
    pub(crate) updated_at: OffsetDateTime,
    #[serde(default = "default_now")]
    #[serde(with = "crate::models::serde_time_compat")]
    pub(crate) last_seen_at: OffsetDateTime,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct GroupRecord {
    #[serde(default)]
    pub(crate) wa_jid: Option<String>,
    pub(crate) subject: Option<String>,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) owner_jid: Option<String>,
    #[serde(default)]
    pub(crate) subject_owner_jid: Option<String>,
    #[serde(default)]
    pub(crate) profile_picture_media_id: Option<String>,
    pub(crate) avatar_url: Option<String>,
    pub(crate) profile_picture_url: Option<String>,
    #[serde(default)]
    #[serde(with = "crate::models::serde_time_compat::option")]
    pub(crate) created_at_wa: Option<OffsetDateTime>,
    #[serde(default)]
    pub(crate) members_count: Option<u32>,
    #[serde(default)]
    pub(crate) admins_count: Option<u32>,
}

#[derive(Default)]
struct StoreInner {
    projects: HashMap<String, Value>,
    companies: HashMap<(String, String), Value>,
    api_keys: HashMap<String, Value>,
    channels: HashMap<String, Value>,
    conversations: HashMap<String, crate::models::Conversation>,
    messages_by_conversation: HashMap<String, Vec<Message>>,
    messages_by_id: HashMap<String, Message>,
    idempotency: HashMap<(String, String, String), IdempotencyRecord>,
    dirty: HashMap<(String, String, String), DirtyRecord>,
    dirty_leases: HashMap<(String, String, String, String), DirtyLeaseRecord>,
    consumer_state: HashMap<(String, String, String, String), i64>,
    contacts: HashMap<String, ContactRecord>,
    groups: HashMap<String, GroupRecord>,
    group_members: HashMap<String, HashMap<String, GroupMemberRecord>>,
    group_profile_refreshes: HashMap<String, OffsetDateTime>,
    media: HashMap<String, MediaObject>,
    transcripts: HashMap<String, Transcript>,
    qr: HashMap<String, QrState>,
    events: Vec<crate::models::CommonEvent>,
    internal_processed_event_ids: HashSet<String>,
    callbacks: HashMap<String, Value>,
    webhook_delivery_attempts: Vec<WebhookDeliveryAttempt>,
    audit_logs: Vec<Value>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct PersistedStore {
    pub(crate) projects: HashMap<String, Value>,
    pub(crate) companies: Vec<((String, String), Value)>,
    pub(crate) api_keys: HashMap<String, Value>,
    pub(crate) channels: HashMap<String, Value>,
    pub(crate) conversations: HashMap<String, crate::models::Conversation>,
    pub(crate) messages_by_conversation: HashMap<String, Vec<Message>>,
    pub(crate) messages_by_id: HashMap<String, Message>,
    pub(crate) idempotency: Vec<((String, String, String), IdempotencyRecord)>,
    pub(crate) dirty: Vec<((String, String, String), DirtyRecord)>,
    pub(crate) dirty_leases: Vec<((String, String, String, String), DirtyLeaseRecord)>,
    pub(crate) consumer_state: Vec<((String, String, String, String), i64)>,
    pub(crate) contacts: HashMap<String, ContactRecord>,
    pub(crate) groups: HashMap<String, GroupRecord>,
    pub(crate) group_members: HashMap<String, HashMap<String, GroupMemberRecord>>,
    #[serde(with = "crate::models::serde_time_compat::map")]
    pub(crate) group_profile_refreshes: HashMap<String, OffsetDateTime>,
    pub(crate) media: HashMap<String, MediaObject>,
    pub(crate) transcripts: HashMap<String, Transcript>,
    pub(crate) qr: HashMap<String, QrState>,
    pub(crate) events: Vec<crate::models::CommonEvent>,
    pub(crate) callbacks: HashMap<String, Value>,
    #[serde(default)]
    pub(crate) webhook_delivery_attempts: Vec<WebhookDeliveryAttempt>,
    pub(crate) audit_logs: Vec<Value>,
}

impl PersistedStore {
    fn from_inner(inner: &StoreInner) -> Self {
        sanitize_updates_surfaces(Self {
            projects: inner.projects.clone(),
            companies: inner
                .companies
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            api_keys: inner.api_keys.clone(),
            channels: inner.channels.clone(),
            conversations: inner.conversations.clone(),
            messages_by_conversation: inner.messages_by_conversation.clone(),
            messages_by_id: inner.messages_by_id.clone(),
            idempotency: inner
                .idempotency
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            dirty: inner
                .dirty
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            dirty_leases: inner
                .dirty_leases
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            consumer_state: inner
                .consumer_state
                .iter()
                .map(|(key, value)| (key.clone(), *value))
                .collect(),
            contacts: inner.contacts.clone(),
            groups: inner.groups.clone(),
            group_members: inner.group_members.clone(),
            group_profile_refreshes: normalize_group_profile_refreshes(
                inner.group_profile_refreshes.clone(),
            ),
            media: inner.media.clone(),
            transcripts: inner.transcripts.clone(),
            qr: inner.qr.clone(),
            events: inner.events.clone(),
            callbacks: inner.callbacks.clone(),
            webhook_delivery_attempts: inner.webhook_delivery_attempts.clone(),
            audit_logs: inner.audit_logs.clone(),
        })
    }

    fn into_inner(self) -> StoreInner {
        let store = sanitize_updates_surfaces(self);
        StoreInner {
            projects: store.projects,
            companies: store.companies.into_iter().collect(),
            api_keys: store.api_keys,
            channels: store.channels,
            conversations: store.conversations,
            messages_by_conversation: store.messages_by_conversation,
            messages_by_id: store.messages_by_id,
            idempotency: store.idempotency.into_iter().collect(),
            dirty: store.dirty.into_iter().collect(),
            dirty_leases: store.dirty_leases.into_iter().collect(),
            consumer_state: store.consumer_state.into_iter().collect(),
            contacts: store.contacts,
            groups: store.groups,
            group_members: store.group_members,
            group_profile_refreshes: normalize_group_profile_refreshes(
                store.group_profile_refreshes,
            ),
            media: store.media,
            transcripts: store.transcripts,
            qr: store.qr,
            events: store.events,
            internal_processed_event_ids: HashSet::new(),
            callbacks: store.callbacks,
            webhook_delivery_attempts: store.webhook_delivery_attempts,
            audit_logs: store.audit_logs,
        }
    }
}

fn sanitize_updates_surfaces(mut store: PersistedStore) -> PersistedStore {
    let disallowed_conversations: HashSet<String> = store
        .conversations
        .values()
        .filter(|conversation| conversation_updates_surface(conversation))
        .map(|conversation| conversation.id.clone())
        .chain(
            store
                .messages_by_conversation
                .keys()
                .filter(|conversation_id| is_updates_surface_jid(conversation_id))
                .cloned(),
        )
        .collect();

    store
        .conversations
        .retain(|_, conversation| !conversation_updates_surface(conversation));
    store
        .contacts
        .retain(|_, contact| !contact_updates_surface(contact));
    store
        .groups
        .retain(|group_id, group| !group_updates_surface(group_id, group));
    store.group_members.retain(|group_id, members| {
        if disallowed_conversations.contains(group_id) || is_updates_surface_jid(group_id) {
            return false;
        }
        members.retain(|_, member| !group_member_updates_surface(member));
        !members.is_empty()
    });
    store
        .group_profile_refreshes
        .retain(|key, _| !key_updates_surface(key));

    let mut removed_message_ids = HashSet::new();
    for (message_id, message) in &store.messages_by_id {
        if disallowed_conversations.contains(&message.conversation_id)
            || message_updates_surface(message)
        {
            removed_message_ids.insert(message_id.clone());
        }
    }
    for messages in store.messages_by_conversation.values() {
        for message in messages {
            if disallowed_conversations.contains(&message.conversation_id)
                || message_updates_surface(message)
            {
                removed_message_ids.insert(message.id.clone());
            }
        }
    }

    store.messages_by_id.retain(|message_id, message| {
        !removed_message_ids.contains(message_id)
            && !disallowed_conversations.contains(&message.conversation_id)
            && !message_updates_surface(message)
    });
    store
        .messages_by_conversation
        .retain(|conversation_id, messages| {
            if disallowed_conversations.contains(conversation_id)
                || is_updates_surface_jid(conversation_id)
            {
                return false;
            }
            messages.retain(|message| {
                !removed_message_ids.contains(&message.id) && !message_updates_surface(message)
            });
            !messages.is_empty()
        });

    store.media.retain(|_, media| {
        !disallowed_conversations.contains(&media.conversation_id)
            && !is_updates_surface_jid(&media.conversation_id)
            && media
                .message_id
                .as_deref()
                .is_none_or(|message_id| !removed_message_ids.contains(message_id))
    });
    store
        .transcripts
        .retain(|message_id, _| !removed_message_ids.contains(message_id));
    store
        .idempotency
        .retain(|((_, _, conversation_id), record)| {
            !disallowed_conversations.contains(conversation_id)
                && !is_updates_surface_jid(conversation_id)
                && !removed_message_ids.contains(&record.message_id)
        });
    store
        .dirty
        .retain(|((_, _, conversation_id), _)| !disallowed_conversations.contains(conversation_id));
    store
        .dirty_leases
        .retain(|((_, _, _, conversation_id), _)| {
            !disallowed_conversations.contains(conversation_id)
        });
    store
        .consumer_state
        .retain(|((_, _, _, conversation_id), _)| {
            !disallowed_conversations.contains(conversation_id)
        });
    store.events.retain(|event| {
        event
            .conversation_id
            .as_deref()
            .is_none_or(|conversation_id| !is_updates_surface_jid(conversation_id))
            && event
                .message_id
                .as_deref()
                .is_none_or(|message_id| !removed_message_ids.contains(message_id))
    });
    ensure_media_conversation_placeholders(&mut store);
    store
}

fn ensure_media_conversation_placeholders(store: &mut PersistedStore) {
    let orphan_media: Vec<MediaObject> = store
        .media
        .values()
        .filter(|media| {
            !media.conversation_id.trim().is_empty()
                && !store.conversations.contains_key(&media.conversation_id)
        })
        .cloned()
        .collect();

    for media in orphan_media {
        if store.conversations.contains_key(&media.conversation_id) {
            continue;
        }
        let conversation_id = media.conversation_id.clone();
        let is_group = conversation_id.ends_with("@g.us");
        let channel_id =
            placeholder_channel_for_project_company(store, &media.project_id, &media.company_id);
        store.conversations.insert(
            conversation_id.clone(),
            crate::models::Conversation {
                id: conversation_id.clone(),
                project_id: media.project_id.clone(),
                company_id: media.company_id.clone(),
                channel_account_id: channel_id,
                conversation_type: if is_group { "group" } else { "direct" }.to_string(),
                contact_id: if is_group {
                    None
                } else {
                    Some(conversation_id.clone())
                },
                group_id: None,
                display_name: Some(if is_group {
                    group_subject_for_jid(&conversation_id)
                } else {
                    display_name_for_jid(&conversation_id)
                }),
                display_phone: if is_group {
                    None
                } else {
                    phone_from_jid(&conversation_id)
                },
                phone_number: if is_group {
                    None
                } else {
                    phone_number_from_jid(&conversation_id)
                },
                avatar_url: None,
                profile_picture_url: None,
                last_seq: 0,
                last_message_at: None,
                unread_count: 0,
                is_archived: false,
                is_muted: false,
                is_pinned: false,
                control_mode: "manual".to_string(),
                created_at: media.created_at,
                updated_at: media.updated_at,
            },
        );
    }
}

fn placeholder_channel_for_project_company(
    store: &PersistedStore,
    project_id: &str,
    company_id: &str,
) -> String {
    store
        .channels
        .iter()
        .find_map(|(id, channel)| {
            (channel_belongs_to_company(channel, project_id, company_id)
                && channel.get("status").and_then(Value::as_str) == Some("connected"))
            .then(|| id.clone())
        })
        .or_else(|| {
            store.channels.iter().find_map(|(id, channel)| {
                channel_belongs_to_company(channel, project_id, company_id).then(|| id.clone())
            })
        })
        .unwrap_or_else(|| "channel_dev".to_string())
}

fn channel_belongs_to_company(channel: &Value, project_id: &str, company_id: &str) -> bool {
    channel.get("project_id").and_then(Value::as_str) == Some(project_id)
        && channel.get("company_id").and_then(Value::as_str) == Some(company_id)
}

fn raw_whatsapp_event_must_stay_inline(event: &WhatsappEvent) -> bool {
    matches!(event, WhatsappEvent::PairingQrCode { .. })
}

fn conversation_updates_surface(conversation: &crate::models::Conversation) -> bool {
    is_updates_surface_jid(&conversation.id)
        || conversation
            .contact_id
            .as_deref()
            .is_some_and(is_updates_surface_jid)
        || conversation
            .group_id
            .as_deref()
            .is_some_and(is_updates_surface_jid)
}

fn contact_updates_surface(contact: &ContactRecord) -> bool {
    is_updates_surface_jid(&contact.id)
        || contact
            .canonical_jid
            .as_deref()
            .is_some_and(is_updates_surface_jid)
        || contact.lid.as_deref().is_some_and(is_updates_surface_jid)
}

fn group_updates_surface(group_id: &str, group: &GroupRecord) -> bool {
    is_updates_surface_jid(group_id) || group.wa_jid.as_deref().is_some_and(is_updates_surface_jid)
}

fn group_member_updates_surface(member: &GroupMemberRecord) -> bool {
    is_updates_surface_jid(&member.group_id)
        || is_updates_surface_jid(&member.contact_id)
        || member.wa_jid.as_deref().is_some_and(is_updates_surface_jid)
}

fn message_updates_surface(message: &Message) -> bool {
    is_updates_surface_jid(&message.conversation_id)
        || message
            .sender_contact_id
            .as_deref()
            .is_some_and(is_updates_surface_jid)
}

fn key_updates_surface(key: &str) -> bool {
    key.split('|').any(is_updates_surface_jid)
        || key.split('\0').any(is_updates_surface_jid)
        || is_updates_surface_jid(key)
}

fn normalize_group_profile_refreshes(
    refreshes: HashMap<String, OffsetDateTime>,
) -> HashMap<String, OffsetDateTime> {
    refreshes
        .into_iter()
        .map(|(key, value)| (key.replace('\0', "|"), value))
        .collect()
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct IdempotencyRecord {
    pub(crate) request_hash: String,
    pub(crate) message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DirtyRecord {
    pub(crate) conversation_id: String,
    pub(crate) max_seq: i64,
    pub(crate) reason: String,
    pub(crate) priority: i32,
    #[serde(with = "crate::models::serde_time_compat")]
    pub(crate) available_at: OffsetDateTime,
    pub(crate) lease_token: Option<String>,
    #[serde(with = "crate::models::serde_time_compat::option")]
    pub(crate) locked_until: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DirtyLeaseRecord {
    pub(crate) lease_token: String,
    #[serde(with = "crate::models::serde_time_compat")]
    pub(crate) locked_until: OffsetDateTime,
    pub(crate) max_seq: i64,
}

#[derive(Debug, Clone)]
struct CompanyPrivacyPolicy {
    message_retention_days: Option<u64>,
    media_temp_retention_days: Option<u64>,
    transcript_retention_days: Option<u64>,
    allow_transcript_storage: bool,
}

fn default_now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

fn company_privacy_policy(company: &Value) -> CompanyPrivacyPolicy {
    let privacy = company.get("privacy").unwrap_or(&Value::Null);
    CompanyPrivacyPolicy {
        message_retention_days: privacy
            .get("message_retention_days")
            .and_then(Value::as_u64)
            .or(Some(365)),
        media_temp_retention_days: privacy
            .get("media_temp_retention_days")
            .and_then(Value::as_u64)
            .or(Some(30)),
        transcript_retention_days: privacy
            .get("transcript_retention_days")
            .and_then(Value::as_u64)
            .or(Some(365)),
        allow_transcript_storage: privacy
            .get("allow_transcript_storage")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    }
}

fn retention_cutoff(now: OffsetDateTime, days: Option<u64>) -> Option<OffsetDateTime> {
    days.map(|days| now - Duration::days(i64::try_from(days).unwrap_or(i64::MAX)))
}

fn dev_state_path(config: &AppConfig) -> Option<PathBuf> {
    if cfg!(test) || config.metadata_db != MetadataDbMode::InMemory {
        return None;
    }
    env::var("RUSTZAP_DEV_STATE_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| Some(config.wa_session_sqlite_dir.join("dev-metadata.json")))
}

fn load_dev_state(path: &Path) -> Option<StoreInner> {
    let bytes = std::fs::read(path).ok()?;
    match serde_json::from_slice::<PersistedStore>(&bytes) {
        Ok(store) => {
            tracing::info!(path = %path.display(), "loaded dev metadata snapshot");
            Some(store.into_inner())
        }
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "failed to load dev metadata snapshot");
            None
        }
    }
}

fn persist_metadata_blocking(
    metadata_db: Arc<MetadataDb>,
    snapshot: PersistedStore,
) -> anyhow::Result<()> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| {
            handle.block_on(async move { metadata_db.persist_store(&snapshot).await })
        })
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async move { metadata_db.persist_store(&snapshot).await })
    }
}

fn client_ip(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .unwrap_or("unknown")
        .to_string()
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        let (events_tx, _) = broadcast::channel(1024);
        let event_bus = EventBusHandle::local(config.event_bus, config.kafka.clone());
        let media_bytes = MediaByteStore::from_config(&config);
        let dev_state_path = dev_state_path(&config);
        let inner = dev_state_path
            .as_deref()
            .and_then(load_dev_state)
            .unwrap_or_default();
        let state = Self {
            config: Arc::new(config),
            inner: Arc::new(Mutex::new(inner)),
            whatsapp: WhatsappManager::default(),
            events_tx,
            event_bus,
            rate_limiter: Arc::new(RateLimiter::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
            media_bytes,
            dev_state_path,
            metadata_db: None,
            metadata_persist_tx: None,
        };
        state.upgrade_legacy_callback_secrets();
        state
    }

    pub async fn from_config(config: AppConfig) -> anyhow::Result<Self> {
        if config.metadata_db != MetadataDbMode::Postgres {
            let event_bus =
                EventBusHandle::from_config(config.event_bus, config.kafka.clone()).await?;
            let media_bytes = MediaByteStore::from_config(&config);
            let (events_tx, _) = broadcast::channel(1024);
            let dev_state_path = dev_state_path(&config);
            let inner = dev_state_path
                .as_deref()
                .and_then(load_dev_state)
                .unwrap_or_default();
            let state = Self {
                config: Arc::new(config),
                inner: Arc::new(Mutex::new(inner)),
                whatsapp: WhatsappManager::default(),
                events_tx,
                event_bus,
                rate_limiter: Arc::new(RateLimiter::default()),
                metrics: Arc::new(RuntimeMetrics::default()),
                media_bytes,
                dev_state_path,
                metadata_db: None,
                metadata_persist_tx: None,
            };
            state.upgrade_legacy_callback_secrets();
            state.spawn_webhook_delivery_worker();
            state.spawn_internal_event_dispatcher();
            state.spawn_retention_worker();
            return Ok(state);
        }

        let metadata_db = Arc::new(MetadataDb::connect(&config).await?);
        metadata_db.migrate().await?;
        let media_bytes = MediaByteStore::from_config(&config);
        let event_bus = EventBusHandle::from_config_with_outbox(
            config.event_bus,
            config.kafka.clone(),
            Some(metadata_db.clone()),
        )
        .await?;
        let inner = metadata_db
            .load_snapshot::<PersistedStore>()
            .await?
            .map(PersistedStore::into_inner)
            .unwrap_or_default();
        let (events_tx, _) = broadcast::channel(1024);
        let (metadata_persist_tx, mut metadata_persist_rx) =
            mpsc::unbounded_channel::<PersistedStore>();
        let metadata_persist_db = metadata_db.clone();
        tokio::spawn(async move {
            while let Some(snapshot) = metadata_persist_rx.recv().await {
                if let Err(err) = metadata_persist_db.persist_store(&snapshot).await {
                    tracing::error!(error = %err, "failed to persist metadata to Postgres");
                }
            }
        });
        let state = Self {
            config: Arc::new(config),
            inner: Arc::new(Mutex::new(inner)),
            whatsapp: WhatsappManager::default(),
            events_tx,
            event_bus,
            rate_limiter: Arc::new(RateLimiter::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
            media_bytes,
            dev_state_path: None,
            metadata_db: Some(metadata_db),
            metadata_persist_tx: Some(metadata_persist_tx),
        };
        state.upgrade_legacy_callback_secrets();
        crate::workers::spawn_kafka_workers(state.clone());
        state.spawn_webhook_delivery_worker();
        state.spawn_internal_event_dispatcher();
        state.spawn_retention_worker();
        Ok(state)
    }

    fn spawn_internal_event_dispatcher(&self) {
        match self.config.event_bus {
            EventBusMode::Kafka => {}
            EventBusMode::InMemory => {
                let state = self.clone();
                let mut rx = state.events_tx.subscribe();
                tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(event) => state.process_internal_work_event(event).await,
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!(skipped, "internal event dispatcher lagged")
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                });
            }
            EventBusMode::Postgres => {
                self.spawn_postgres_event_drainer();
            }
        }
    }

    fn spawn_postgres_event_drainer(&self) {
        let Some(db) = self.metadata_db.clone() else {
            return;
        };
        let state = self.clone();
        tokio::spawn(async move {
            let worker_id = format!("postgres-dispatcher-{}", Uuid::now_v7().simple());
            loop {
                let records = match db.claim_event_outbox_batch(&worker_id, 50, 30).await {
                    Ok(records) => records,
                    Err(err) => {
                        tracing::warn!(error = %err, "postgres event drainer claim failed");
                        tokio::time::sleep(StdDuration::from_millis(500)).await;
                        continue;
                    }
                };
                if records.is_empty() {
                    tokio::time::sleep(StdDuration::from_millis(250)).await;
                    continue;
                }
                for record in records {
                    state.process_postgres_outbox_record(&db, record).await;
                }
            }
        });
    }

    async fn process_postgres_outbox_record(&self, db: &MetadataDb, record: OutboxRecord) {
        let event = match serde_json::from_value::<crate::models::CommonEvent>(
            record.payload_json.clone(),
        ) {
            Ok(event) => event,
            Err(err) => {
                if let Err(mark_err) = db
                    .mark_event_outbox_deadlettered(&record.event_id, &err.to_string())
                    .await
                {
                    tracing::error!(event_id = record.event_id, error = %mark_err, "failed to deadletter undecodable postgres event");
                }
                return;
            }
        };
        if !internal_work_event_type(&event.event_type) {
            if let Err(err) = db
                .mark_event_outbox_published(&record.event_id, -1, -1)
                .await
            {
                tracing::warn!(event_id = record.event_id, error = %err, "failed to mark ignored postgres event published");
            }
            return;
        }

        let result = self.handle_internal_work_event(&event).await;
        match result {
            Ok(()) => {
                if let Err(err) = db
                    .mark_event_outbox_published(&record.event_id, -1, -1)
                    .await
                {
                    tracing::warn!(event_id = record.event_id, error = %err, "failed to mark postgres event processed");
                }
            }
            Err(err) => {
                let error = err.to_string();
                let attempts = record.attempt_count.saturating_add(1);
                if attempts as u32 >= self.config.kafka.retry_max_attempts {
                    if let Err(mark_err) = db
                        .mark_event_outbox_deadlettered(&record.event_id, &error)
                        .await
                    {
                        tracing::error!(event_id = record.event_id, error = %mark_err, "failed to deadletter postgres event");
                    }
                } else {
                    let backoff =
                        5_i64.saturating_mul(2_i64.saturating_pow(attempts.min(6) as u32));
                    if let Err(mark_err) = db
                        .mark_event_outbox_failed(&record.event_id, &error, backoff)
                        .await
                    {
                        tracing::error!(event_id = record.event_id, error = %mark_err, "failed to mark postgres event failed");
                    }
                }
            }
        }
    }

    async fn process_internal_work_event(&self, event: crate::models::CommonEvent) {
        if !internal_work_event_type(&event.event_type) || !self.claim_internal_event(&event) {
            return;
        }
        if let Err(err) = self.handle_internal_work_event(&event).await {
            tracing::warn!(
                event_id = event.event_id,
                event_type = event.event_type,
                error = %err,
                "internal event dispatcher failed"
            );
        }
    }

    async fn handle_internal_work_event(
        &self,
        event: &crate::models::CommonEvent,
    ) -> ApiResult<()> {
        match event.event_type.as_str() {
            "outbound.send.requested" => {
                self.process_outbound_send_request(event).await.map(|_| ())
            }
            "audio.transcription.requested" => {
                self.process_transcription_request(event).await.map(|_| ())
            }
            _ => Ok(()),
        }
    }

    fn claim_internal_event(&self, event: &crate::models::CommonEvent) -> bool {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        inner
            .internal_processed_event_ids
            .insert(event.event_id.clone())
    }

    fn spawn_webhook_delivery_worker(&self) {
        if !self.config.webhook.delivery_enabled || !self.webhook_signal_enabled() {
            return;
        }
        let state = self.clone();
        let interval_seconds = state.config.webhook.retry_base_seconds.clamp(1, 60);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(StdDuration::from_secs(interval_seconds));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(err) = state.deliver_pending_webhooks_once().await {
                    tracing::warn!(error = %err, "webhook delivery cycle failed");
                }
            }
        });
    }

    fn spawn_retention_worker(&self) {
        let state = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(StdDuration::from_secs(60 * 60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let summary = state.apply_retention_once();
                if summary.messages_removed
                    + summary.transcripts_removed
                    + summary.transcripts_redacted
                    + summary.media_removed
                    > 0
                {
                    tracing::info!(
                        messages_removed = summary.messages_removed,
                        transcripts_removed = summary.transcripts_removed,
                        transcripts_redacted = summary.transcripts_redacted,
                        media_removed = summary.media_removed,
                        "retention worker applied policy"
                    );
                }
            }
        });
    }

    pub async fn metadata_ready(&self) -> anyhow::Result<()> {
        if let Some(metadata_db) = self.metadata_db.as_ref() {
            metadata_db.check_ready().await?;
        }
        Ok(())
    }

    pub async fn event_bus_ready(&self) -> crate::eventbus::EventBusHealth {
        self.event_bus.health().await
    }

    pub async fn storage_ready(&self) -> crate::models::ReadyCheck {
        match self
            .media_bytes
            .ready_check(self.config.r2_ready_write_check)
            .await
        {
            Ok(detail) => crate::models::ReadyCheck {
                name: "media_storage".to_string(),
                ok: true,
                detail,
            },
            Err(err) => crate::models::ReadyCheck {
                name: "media_storage".to_string(),
                ok: false,
                detail: err,
            },
        }
    }

    pub fn webhook_secret_ready(&self) -> crate::models::ReadyCheck {
        let required = self.config.webhook.delivery_enabled && self.webhook_signal_enabled();
        let ok = !required
            || (self.config.secret_master_key.is_some()
                && self.config.secret_master_key_error.is_none());
        crate::models::ReadyCheck {
            name: "webhook_secret_key".to_string(),
            ok,
            detail: if !required {
                "webhook delivery disabled".to_string()
            } else if let Some(error) = self.config.secret_master_key_error.as_ref() {
                error.clone()
            } else if self.config.secret_master_key.is_some() {
                "secret master key configured".to_string()
            } else {
                "RUSTZAP_SECRET_MASTER_KEY missing".to_string()
            },
        }
    }

    pub fn event_bus_snapshot(&self) -> crate::eventbus::EventBusRuntimeSnapshot {
        self.event_bus.snapshot()
    }

    pub fn runtime_metrics_snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            rate_limited_total: self.metrics.rate_limited_total.load(Ordering::Relaxed),
            webhook_delivery_attempts_total: self
                .metrics
                .webhook_delivery_attempts_total
                .load(Ordering::Relaxed),
            webhook_delivery_successes_total: self
                .metrics
                .webhook_delivery_successes_total
                .load(Ordering::Relaxed),
            stt_requests_total: self.metrics.stt_requests_total.load(Ordering::Relaxed),
            websocket_subscriptions_total: self
                .metrics
                .websocket_subscriptions_total
                .load(Ordering::Relaxed),
        }
    }

    fn media_bucket_name(&self) -> String {
        self.config.r2.bucket.clone()
    }

    #[cfg(feature = "external-integrations")]
    pub(crate) fn metadata_db_handle(&self) -> Option<Arc<MetadataDb>> {
        self.metadata_db.clone()
    }

    pub async fn kafka_deadletters(
        &self,
        limit: usize,
        offset: usize,
    ) -> ApiResult<Vec<KafkaDeadLetterRecord>> {
        let Some(metadata_db) = self.metadata_db.as_ref() else {
            return Ok(Vec::new());
        };
        metadata_db
            .list_kafka_deadletters(limit as i64, offset as i64)
            .await
            .map_err(|err| ApiError::Internal(err.to_string()))
    }

    pub async fn kafka_outbox_backlog(&self) -> ApiResult<Option<i64>> {
        let Some(metadata_db) = self.metadata_db.as_ref() else {
            return Ok(None);
        };
        metadata_db
            .outbox_backlog_count()
            .await
            .map(Some)
            .map_err(|err| ApiError::Internal(err.to_string()))
    }

    pub async fn replay_kafka_deadletter(
        &self,
        deadletter_id: &str,
        replayed_by: &str,
    ) -> ApiResult<String> {
        let Some(metadata_db) = self.metadata_db.as_ref() else {
            return Err(ApiError::NotSupported(
                "Kafka deadletter replay requires RUSTZAP_METADATA_DB=postgres".to_string(),
            ));
        };
        metadata_db
            .replay_kafka_deadletter(deadletter_id, replayed_by)
            .await
            .map_err(|err| ApiError::Internal(err.to_string()))?
            .ok_or_else(|| {
                ApiError::NotFound("deadletter not found or already replayed".to_string())
            })
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<crate::models::CommonEvent> {
        self.events_tx.subscribe()
    }

    pub fn enforce_rate_limit(&self, scope: RateLimitScope<'_>) -> ApiResult<()> {
        let mut keys = vec![
            format!("api_key:{}:family:{}", scope.api_key_id, scope.family),
            format!("ip:{}:family:{}", client_ip(scope.headers), scope.family),
        ];
        if let Some(project_id) = scope.project_id {
            keys.push(format!("project:{project_id}:family:{}", scope.family));
        }
        if let (Some(project_id), Some(company_id)) = (scope.project_id, scope.company_id) {
            keys.push(format!(
                "company:{project_id}/{company_id}:family:{}",
                scope.family
            ));
        }
        if let Some(resource_id) = scope.resource_id {
            keys.push(format!("resource:{}:{resource_id}", scope.family));
        }
        self.rate_limiter
            .check(&keys, scope.max_per_minute)
            .map_err(|decision| {
                self.metrics
                    .rate_limited_total
                    .fetch_add(1, Ordering::Relaxed);
                ApiError::RateLimited(format!(
                    "rate limit exceeded for {}; retry after {} seconds",
                    scope.family, decision.retry_after_seconds
                ))
            })
    }

    pub fn record_websocket_subscription_metric(&self) {
        self.metrics
            .websocket_subscriptions_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn is_whatsapp_channel_active(&self, channel_id: &str) -> bool {
        self.whatsapp.is_channel_connected(channel_id)
    }

    fn kafka_worker_mode_enabled(&self) -> bool {
        self.config.event_bus == EventBusMode::Kafka
            && self.config.metadata_db == MetadataDbMode::Postgres
            && self.metadata_db.is_some()
    }

    fn persist_locked(&self, inner: &StoreInner) {
        let snapshot = PersistedStore::from_inner(inner);
        if let Some(path) = self.dev_state_path.as_deref() {
            if let Some(parent) = path.parent()
                && let Err(err) = std::fs::create_dir_all(parent)
            {
                tracing::warn!(path = %path.display(), error = %err, "failed to create dev metadata directory");
                return;
            }
            let tmp_path = path.with_extension("json.tmp");
            match serde_json::to_vec_pretty(&snapshot) {
                Ok(bytes) => {
                    if let Err(err) = std::fs::write(&tmp_path, bytes) {
                        tracing::warn!(path = %tmp_path.display(), error = %err, "failed to write dev metadata snapshot");
                        return;
                    }
                    if let Err(err) = std::fs::rename(&tmp_path, path) {
                        tracing::warn!(path = %path.display(), error = %err, "failed to replace dev metadata snapshot");
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to serialize dev metadata snapshot");
                }
            }
        }
        if let Some(tx) = self.metadata_persist_tx.as_ref() {
            if let Err(err) = tx.send(snapshot) {
                tracing::error!(error = %err, "failed to queue metadata persistence");
            }
        } else if let Some(metadata_db) = self.metadata_db.as_ref()
            && let Err(err) = persist_metadata_blocking(metadata_db.clone(), snapshot)
        {
            tracing::error!(error = %err, "failed to persist metadata to Postgres");
        }
    }

    pub fn upsert_project(&self, id: String, name: String) -> Value {
        let now = OffsetDateTime::now_utc();
        let project = json!({
            "id": id,
            "name": name,
            "status": "active",
            "created_at": ts(now),
            "updated_at": ts(now)
        });
        {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            inner.projects.insert(id, project.clone());
            self.persist_locked(&inner);
        }
        project
    }

    pub fn upsert_company(&self, project_id: String, company_id: String, name: String) -> Value {
        let now = OffsetDateTime::now_utc();
        let company = json!({
            "id": company_id,
            "project_id": project_id,
            "name": name,
            "status": "active",
            "created_at": ts(now),
            "updated_at": ts(now),
            "privacy": {
                "message_retention_days": 365,
                "media_temp_retention_days": self.config.media_temp_retention_days,
                "media_permanent_retention_policy": "manual",
                "transcript_retention_days": 365,
                "allow_message_text_in_logs": false,
                "allow_transcript_storage": true
            }
        });
        {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            inner
                .companies
                .insert((project_id, company_id), company.clone());
            self.persist_locked(&inner);
        }
        company
    }

    pub fn store_api_key_metadata(
        &self,
        project_id: &str,
        company_id: Option<&str>,
        key_id: &str,
        key_hash: &str,
        scopes: &[String],
        name: &str,
    ) -> Value {
        let now = OffsetDateTime::now_utc();
        let metadata = json!({
            "id": key_id,
            "project_id": project_id,
            "company_id": company_id,
            "name": name,
            "key_hash": key_hash,
            "scopes": scopes,
            "status": "active",
            "created_at": ts(now),
            "revoked_at": null
        });
        let mut inner = self.inner.lock().expect("store lock poisoned");
        inner.api_keys.insert(key_id.to_string(), metadata.clone());
        self.persist_locked(&inner);
        metadata
    }

    pub fn audit_log(
        &self,
        project_id: &str,
        company_id: &str,
        action: &str,
        resource_type: &str,
        resource_id: Option<&str>,
        response: Value,
    ) -> Value {
        let now = OffsetDateTime::now_utc();
        let entry = json!({
            "id": format!("audit_{}", Uuid::now_v7().simple()),
            "project_id": project_id,
            "company_id": company_id,
            "actor_type": "api_key",
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "response_json": response,
            "created_at": ts(now)
        });
        let mut inner = self.inner.lock().expect("store lock poisoned");
        inner.audit_logs.push(entry.clone());
        self.persist_locked(&inner);
        entry
    }

    pub fn company(&self, project_id: &str, company_id: &str) -> Value {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .companies
            .get(&(project_id.to_string(), company_id.to_string()))
            .cloned()
            .unwrap_or_else(|| {
                json!({
                    "id": company_id,
                    "project_id": project_id,
                    "name": "Development Company",
                    "status": "active"
                })
            })
    }

    fn company_allows_transcript_storage(&self, project_id: &str, company_id: &str) -> bool {
        company_privacy_policy(&self.company(project_id, company_id)).allow_transcript_storage
    }

    pub fn create_channel(
        &self,
        project_id: &str,
        company_id: &str,
        id: Option<String>,
        label: Option<String>,
        phone_e164: Option<String>,
    ) -> ApiResult<Value> {
        let channel_id = id.unwrap_or_else(|| format!("channel_{}", Uuid::now_v7().simple()));
        let now = OffsetDateTime::now_utc();
        let mut inner = self.inner.lock().expect("store lock poisoned");
        if let Some(existing) = inner.channels.get_mut(&channel_id) {
            if !channel_belongs_to_company(existing, project_id, company_id) {
                return Err(ApiError::Conflict(format!(
                    "channel {channel_id} already exists for another company"
                )));
            }
            existing["project_id"] = json!(project_id);
            existing["company_id"] = json!(company_id);
            if let Some(label) = label {
                existing["label"] = json!(label);
            }
            if phone_e164.is_some() {
                existing["phone_e164"] = json!(phone_e164);
            }
            existing["updated_at"] = json!(ts(now));
            let updated = existing.clone();
            self.persist_locked(&inner);
            return Ok(updated);
        }
        let channel = json!({
            "id": channel_id,
            "project_id": project_id,
            "company_id": company_id,
            "provider": "whatsapp-rust",
            "phone_e164": phone_e164,
            "label": label.unwrap_or_else(|| "WhatsApp Dev".to_string()),
            "status": "disconnected",
            "connected_at": null,
            "created_at": ts(now),
            "updated_at": ts(now)
        });
        inner.channels.insert(channel_id, channel.clone());
        self.persist_locked(&inner);
        Ok(channel)
    }

    pub async fn connect_channel(
        &self,
        project_id: &str,
        company_id: &str,
        channel_id: &str,
    ) -> ApiResult<QrState> {
        self.channel_for_company(project_id, company_id, channel_id)?;
        let already_connected = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .channels
            .get(channel_id)
            .and_then(|channel| channel.get("status"))
            .and_then(|status| status.as_str())
            == Some("connected")
            && self.whatsapp.is_channel_connected(channel_id);
        if already_connected {
            return Ok(QrState {
                channel_id: channel_id.to_string(),
                status: "connected".to_string(),
                qr_code_text: None,
                expires_at: OffsetDateTime::now_utc(),
            });
        }
        let now = OffsetDateTime::now_utc();
        let cached_qr = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .qr
            .get(channel_id)
            .cloned();
        let active_qr = cached_qr
            .as_ref()
            .filter(|qr| qr.qr_code_text.is_some() && qr.expires_at > now + Duration::seconds(2))
            .cloned();
        if let Some(qr) = active_qr {
            return Ok(qr);
        }
        if cached_qr.as_ref().is_some_and(|qr| {
            qr.qr_code_text.is_some() && qr.expires_at <= now + Duration::seconds(2)
        }) {
            self.whatsapp.stop_channel(channel_id);
        }
        let qr = self.set_qr(channel_id, "connecting", None, OffsetDateTime::now_utc());
        self.set_channel_status(channel_id, "connecting", None);
        let runtime = ChannelRuntime {
            project_id: project_id.to_string(),
            company_id: company_id.to_string(),
            channel_id: channel_id.to_string(),
            session_path: session_sqlite_path(
                self.config.wa_session_sqlite_dir.as_path(),
                project_id,
                company_id,
                channel_id,
            ),
        };
        let state = self.clone();
        self.whatsapp
            .start_channel(runtime, move |runtime, event| {
                let state = state.clone();
                async move {
                    state.handle_whatsapp_event(runtime, event).await;
                }
            })
            .await
            .map_err(|err| ApiError::Internal(format!("failed to start WhatsApp: {err}")))?;
        Ok(qr)
    }

    pub fn disconnect_channel(&self, channel_id: &str) -> Value {
        self.whatsapp.stop_channel(channel_id);
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let channel = if let Some(channel) = inner.channels.get_mut(channel_id) {
            channel["status"] = json!("disconnected");
            channel["connected_at"] = Value::Null;
            channel["updated_at"] = json!(ts(OffsetDateTime::now_utc()));
            channel.clone()
        } else {
            json!({"id": channel_id, "status": "disconnected"})
        };
        self.persist_locked(&inner);
        channel
    }

    pub fn disconnect_channel_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        channel_id: &str,
    ) -> ApiResult<Value> {
        self.channel_for_company(project_id, company_id, channel_id)?;
        Ok(self.disconnect_channel(channel_id))
    }

    pub fn channel(&self, channel_id: &str) -> Value {
        self.reconcile_channel_runtime_status(channel_id);
        self.inner
            .lock()
            .expect("store lock poisoned")
            .channels
            .get(channel_id)
            .cloned()
            .unwrap_or_else(|| json!({"id": channel_id, "status": "disconnected"}))
    }

    pub fn channel_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        channel_id: &str,
    ) -> ApiResult<Value> {
        self.reconcile_channel_runtime_status(channel_id);
        self.inner
            .lock()
            .expect("store lock poisoned")
            .channels
            .get(channel_id)
            .filter(|channel| channel_belongs_to_company(channel, project_id, company_id))
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("channel {channel_id} not found")))
    }

    fn reconcile_channel_runtime_status(&self, channel_id: &str) {
        if self.whatsapp.is_channel_connected(channel_id) {
            return;
        }
        let disconnected = {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            let Some(channel) = inner.channels.get_mut(channel_id) else {
                return;
            };
            if channel.get("status").and_then(Value::as_str) != Some("connected") {
                return;
            }

            let now = OffsetDateTime::now_utc();
            let project_id = channel
                .get("project_id")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let company_id = channel
                .get("company_id")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            channel["status"] = json!("disconnected");
            channel["connected_at"] = Value::Null;
            channel["updated_at"] = json!(ts(now));
            inner.qr.insert(
                channel_id.to_string(),
                QrState {
                    channel_id: channel_id.to_string(),
                    status: "disconnected".to_string(),
                    qr_code_text: None,
                    expires_at: now,
                },
            );
            self.persist_locked(&inner);
            Some((project_id, company_id))
        };
        if let Some((project_id, company_id)) = disconnected {
            self.push_event(new_event(
                "channel.disconnected",
                &project_id,
                &company_id,
                Some(channel_id.to_string()),
                None,
                None,
                None,
                json!({}),
            ));
        }
    }

    pub fn set_channel_status(
        &self,
        channel_id: &str,
        status: &str,
        connected_at: Option<OffsetDateTime>,
    ) -> Value {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let now = OffsetDateTime::now_utc();
        let channel = inner
            .channels
            .entry(channel_id.to_string())
            .or_insert_with(|| {
                json!({
                    "id": channel_id,
                    "project_id": "unknown",
                    "company_id": "unknown",
                    "provider": "whatsapp-rust",
                    "label": "WhatsApp",
                    "status": "disconnected",
                    "connected_at": null,
                    "created_at": ts(now),
                    "updated_at": ts(now)
                })
            });
        channel["status"] = json!(status);
        channel["updated_at"] = json!(ts(now));
        if let Some(connected_at) = connected_at {
            channel["connected_at"] = json!(ts(connected_at));
        } else if status != "connected" {
            channel["connected_at"] = Value::Null;
        }
        let updated = channel.clone();
        self.persist_locked(&inner);
        updated
    }

    pub fn rotate_qr(&self, channel_id: &str, status: &str) -> QrState {
        self.set_qr(
            channel_id,
            status,
            Some(format!(
                "rustzap-dev-qr-{channel_id}-{}",
                Uuid::now_v7().simple()
            )),
            OffsetDateTime::now_utc() + Duration::seconds(20),
        )
    }

    pub fn set_qr(
        &self,
        channel_id: &str,
        status: &str,
        qr_code_text: Option<String>,
        expires_at: OffsetDateTime,
    ) -> QrState {
        let qr = QrState {
            channel_id: channel_id.to_string(),
            status: status.to_string(),
            qr_code_text,
            expires_at,
        };
        {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            inner.qr.insert(channel_id.to_string(), qr.clone());
            self.persist_locked(&inner);
        }
        qr
    }

    pub fn qr(&self, channel_id: &str) -> QrState {
        self.reconcile_channel_runtime_status(channel_id);
        let now = OffsetDateTime::now_utc();
        let mut inner = self.inner.lock().expect("store lock poisoned");
        if inner
            .channels
            .get(channel_id)
            .and_then(|channel| channel.get("status"))
            .and_then(|status| status.as_str())
            == Some("connected")
        {
            return QrState {
                channel_id: channel_id.to_string(),
                status: "connected".to_string(),
                qr_code_text: None,
                expires_at: OffsetDateTime::now_utc(),
            };
        }
        if let Some(qr) = inner.qr.get(channel_id).cloned() {
            if qr.qr_code_text.is_some() && qr.expires_at <= now {
                let expired = QrState {
                    channel_id: channel_id.to_string(),
                    status: "disconnected".to_string(),
                    qr_code_text: None,
                    expires_at: now,
                };
                inner.qr.insert(channel_id.to_string(), expired.clone());
                if let Some(channel) = inner.channels.get_mut(channel_id) {
                    if matches!(
                        channel.get("status").and_then(Value::as_str),
                        Some("connecting" | "waiting_qr")
                    ) {
                        channel["status"] = json!("disconnected");
                        channel["connected_at"] = Value::Null;
                        channel["updated_at"] = json!(ts(now));
                    }
                }
                self.persist_locked(&inner);
                return expired;
            }
            return qr;
        }
        QrState {
            channel_id: channel_id.to_string(),
            status: "disconnected".to_string(),
            qr_code_text: None,
            expires_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn qr_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        channel_id: &str,
    ) -> ApiResult<QrState> {
        self.channel_for_company(project_id, company_id, channel_id)?;
        Ok(self.qr(channel_id))
    }

    pub fn send_message(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
        idempotency_key: &str,
        request: SendMessageRequest,
    ) -> ApiResult<Message> {
        Ok(self
            .prepare_send_message(
                project_id,
                company_id,
                conversation_id,
                idempotency_key,
                request,
            )?
            .message)
    }

    pub fn prepare_send_message(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
        idempotency_key: &str,
        request: SendMessageRequest,
    ) -> ApiResult<SendMessageOutcome> {
        self.prepare_send_message_with_actor(
            project_id,
            company_id,
            conversation_id,
            idempotency_key,
            request,
            None,
        )
    }

    pub fn prepare_send_message_with_actor(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
        idempotency_key: &str,
        request: SendMessageRequest,
        sent_by_external_user_id: Option<String>,
    ) -> ApiResult<SendMessageOutcome> {
        let request_hash = sha256_json(&request)?;
        let idem_key = (
            project_id.to_string(),
            company_id.to_string(),
            idempotency_key.to_string(),
        );
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let conversation_id =
            resolve_conversation_id_inner(&inner, project_id, company_id, conversation_id);
        let conversation_id = conversation_id.as_str();

        if let Some(record) = inner.idempotency.get(&idem_key) {
            if record.request_hash == request_hash {
                let message = inner
                    .messages_by_id
                    .get(&record.message_id)
                    .cloned()
                    .ok_or_else(|| {
                        ApiError::Internal(format!(
                            "cached idempotency message {} missing",
                            record.message_id
                        ))
                    })?;
                return Ok(SendMessageOutcome {
                    message,
                    should_dispatch: false,
                });
            }
            return Err(ApiError::IdempotencyConflict(
                "same Idempotency-Key used with different request body".to_string(),
            ));
        }

        let media = if let Some(media_id) = request.media_id.as_deref() {
            let media = inner
                .media
                .get(media_id)
                .cloned()
                .ok_or_else(|| ApiError::NotFound(format!("media {media_id} not found")))?;
            if media.project_id != project_id || media.company_id != company_id {
                return Err(ApiError::NotFound(format!("media {media_id} not found")));
            }
            if media.conversation_id != conversation_id {
                return Err(ApiError::BadRequest(format!(
                    "media {media_id} belongs to conversation {}",
                    media.conversation_id
                )));
            }
            if media.storage_status == "rejected" {
                return Err(ApiError::PayloadTooLarge(format!(
                    "media {media_id} was rejected by the upload policy"
                )));
            }
            Some(media)
        } else {
            None
        };
        let message_type = media
            .as_ref()
            .map(|media| media.media_type.clone())
            .unwrap_or_else(|| request.message_type.clone());
        let message_text = request.text.clone().or(request.caption.clone());
        let quoted_message_id = request.quoted_message_id.clone();
        let channel_id = inner
            .conversations
            .get(conversation_id)
            .map(|conversation| conversation.channel_account_id.clone())
            .or_else(|| connected_channel_for_project_company(&inner, project_id, company_id));
        let mut message = record_message_inner(
            &mut inner,
            project_id,
            company_id,
            conversation_id,
            "outbound",
            &message_type,
            message_text,
            "queued",
            quoted_message_id,
            channel_id,
            None,
            None,
            None,
            sent_by_external_user_id,
            Some(&self.events_tx),
            Some(&self.event_bus),
        );
        if let Some(mut media) = media {
            if let Some(stored_media) = inner.media.get_mut(&media.id) {
                stored_media.message_id = Some(message.id.clone());
                stored_media.updated_at = OffsetDateTime::now_utc();
                media = stored_media.clone();
            }
            message = attach_media_to_message_inner(&mut inner, &message.id, &media)
                .unwrap_or_else(|| message.clone());
        }
        inner.idempotency.insert(
            idem_key,
            IdempotencyRecord {
                request_hash,
                message_id: message.id.clone(),
            },
        );
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "outbound.send.requested",
                project_id,
                company_id,
                Some(message.channel_account_id.clone()),
                Some(conversation_id.to_string()),
                Some(message.id.clone()),
                Some(message.conversation_seq),
                json!({
                    "message_id": message.id.clone(),
                    "media_id": message.media_id.clone(),
                    "message_type": message.message_type.clone()
                }),
            ),
        );
        self.persist_locked(&inner);
        Ok(SendMessageOutcome {
            message,
            should_dispatch: true,
        })
    }

    pub fn receive_inbound_text(
        &self,
        project_id: &str,
        company_id: &str,
        request: SimulateInboundTextRequest,
    ) -> Message {
        let conversation_id = request.conversation_id.unwrap_or_else(|| {
            request
                .from_phone_e164
                .as_deref()
                .map(phone_to_jid)
                .unwrap_or_else(|| "conv_dev".to_string())
        });
        let sender_contact_id = request
            .from_phone_e164
            .as_deref()
            .map(phone_to_jid)
            .unwrap_or_else(|| conversation_id.clone());
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let message = record_message_inner(
            &mut inner,
            project_id,
            company_id,
            &conversation_id,
            "inbound",
            "text",
            Some(request.text),
            "received",
            None,
            request.channel_id,
            Some(sender_contact_id),
            request.sender_name,
            request.profile_picture_url,
            None,
            Some(&self.events_tx),
            Some(&self.event_bus),
        );
        self.persist_locked(&inner);
        message
    }

    pub fn receive_inbound_media(
        &self,
        project_id: &str,
        company_id: &str,
        request: SimulateInboundMediaRequest,
    ) -> (Message, MediaObject, Option<Transcript>) {
        self.try_receive_inbound_media(project_id, company_id, request)
            .expect("dev inbound media should be stored")
    }

    pub fn try_receive_inbound_media(
        &self,
        project_id: &str,
        company_id: &str,
        request: SimulateInboundMediaRequest,
    ) -> ApiResult<(Message, MediaObject, Option<Transcript>)> {
        let conversation_id = request
            .conversation_id
            .clone()
            .unwrap_or_else(|| "conv_dev".to_string());
        let media_type = request.media_type.unwrap_or_else(|| "image".to_string());
        let size_bytes = request.size_bytes.unwrap_or(512_000);
        let mime_type = request
            .mime_type
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let channel_id = request
            .channel_id
            .clone()
            .unwrap_or_else(|| "channel_dev".to_string());
        let sender_contact_id = request
            .from_phone_e164
            .as_deref()
            .map(phone_to_jid)
            .unwrap_or_else(|| conversation_id.clone());

        let message = {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            let message = record_message_inner(
                &mut inner,
                project_id,
                company_id,
                &conversation_id,
                "inbound",
                &media_type,
                request.caption,
                "received",
                None,
                Some(channel_id.clone()),
                Some(sender_contact_id),
                request.sender_name,
                request.profile_picture_url,
                None,
                Some(&self.events_tx),
                Some(&self.event_bus),
            );
            self.persist_locked(&inner);
            message
        };

        let limits = MediaLimits {
            quick_delete_threshold_mb: self.config.media_quick_delete_threshold_mb,
            reject_threshold_mb: self.config.media_reject_threshold_mb,
        };
        let decision = limits.classify(size_bytes);
        let status = match decision {
            MediaDecision::Temp => "temp",
            MediaDecision::Quarantine => "quarantine",
            MediaDecision::Rejected => "rejected",
        };
        let now = OffsetDateTime::now_utc();
        let media_id = format!("media_{}", Uuid::now_v7().simple());
        let ext = request
            .filename
            .as_deref()
            .and_then(|name| name.rsplit_once('.').map(|(_, ext)| ext))
            .unwrap_or("bin");
        let object_key = if decision == MediaDecision::Rejected {
            None
        } else {
            Some(r2_object_key(R2ObjectKeyInput {
                base_prefix: &self.config.r2.base_prefix,
                class: status,
                project_id,
                company_id,
                channel_id: &channel_id,
                conversation_id: Some(&conversation_id),
                entity_type: None,
                entity_id: None,
                date: now.date(),
                media_id: &media_id,
                ext,
            }))
        };
        let public_url = object_key
            .as_deref()
            .and_then(|object_key| self.config.public_object_url(object_key));
        let dev_preview_url = (self.config.dev_mode && media_type == "image")
            .then(|| self.config.dev_media_url(&media_id));
        let thumbnail_url = if media_type == "image" {
            if self.config.r2.can_upload() {
                public_url.clone().or(dev_preview_url)
            } else {
                dev_preview_url.or(public_url.clone())
            }
        } else {
            None
        };
        let sha256 = hex::encode(Sha256::digest(format!("{media_id}:{size_bytes}")));
        let media = MediaObject {
            id: media_id.clone(),
            project_id: project_id.to_string(),
            company_id: company_id.to_string(),
            conversation_id: conversation_id.clone(),
            message_id: Some(message.id.clone()),
            media_type,
            mime_type,
            original_filename: request.filename,
            size_bytes,
            sha256,
            storage_status: status.to_string(),
            bucket: Some(self.media_bucket_name()),
            object_key,
            permanent_object_key: None,
            public_url: public_url.clone(),
            thumbnail_url: thumbnail_url.clone(),
            width: None,
            height: None,
            duration_seconds: None,
            expires_at: Some(now + Duration::days(self.config.media_temp_retention_days.into())),
            saved_at: None,
            created_at: now,
            updated_at: now,
        };
        if media.storage_status != "rejected"
            && let Some(object_key) = media.object_key.as_deref()
            && let Err(err) = self.media_bytes.put_blocking(
                object_key,
                &media.mime_type,
                dev_media_bytes(&media, &message),
            )
        {
            let _ = self.media_bytes.delete_blocking(object_key);
            let mut inner = self.inner.lock().expect("store lock poisoned");
            push_event_inner(
                &mut inner,
                Some(&self.events_tx),
                Some(&self.event_bus),
                new_event(
                    "media.storage_failed",
                    project_id,
                    company_id,
                    Some(channel_id.clone()),
                    Some(conversation_id.clone()),
                    Some(message.id.clone()),
                    Some(message.conversation_seq),
                    json!({
                        "message_id": message.id.clone(),
                        "media_id": media.id.clone(),
                        "code": "byte_store_put_failed",
                        "error": err
                    }),
                ),
            );
            self.persist_locked(&inner);
            return Err(ApiError::ProviderError(err));
        }

        let mut inner = self.inner.lock().expect("store lock poisoned");
        inner.media.insert(media_id.clone(), media.clone());
        let message = attach_media_to_message_inner(&mut inner, &message.id, &media)
            .unwrap_or_else(|| message.clone());

        let transcript = if media.media_type == "audio" && media.storage_status != "rejected" {
            let transcript = transcript_with_lifecycle(
                project_id,
                company_id,
                &message.id,
                Some(media_id.clone()),
                TranscriptLifecycle::Pending,
                None,
            );
            inner
                .transcripts
                .insert(message.id.clone(), transcript.clone());
            push_event_inner(
                &mut inner,
                Some(&self.events_tx),
                Some(&self.event_bus),
                new_event(
                    "audio.transcription.requested",
                    project_id,
                    company_id,
                    Some(channel_id.clone()),
                    Some(conversation_id.clone()),
                    Some(message.id.clone()),
                    Some(message.conversation_seq),
                    json!({
                        "message_id": message.id.clone(),
                        "media_id": media_id.clone(),
                        "transcript_id": transcript.id.clone()
                    }),
                ),
            );
            Some(transcript)
        } else {
            None
        };

        self.persist_locked(&inner);
        drop(inner);
        Ok((message, media, transcript))
    }

    pub async fn dispatch_prepared_message(
        &self,
        outcome: &SendMessageOutcome,
        request_text: Option<&str>,
    ) -> ApiResult<Message> {
        if !outcome.should_dispatch {
            return Ok(outcome.message.clone());
        }
        let channel_id = self
            .channel_for_conversation(&outcome.message.conversation_id)
            .unwrap_or_else(|| outcome.message.channel_account_id.clone());
        if self.channel_status(&channel_id).as_deref() != Some("connected") {
            return self.update_outbound_after_dispatch(
                &outcome.message.id,
                None,
                "failed",
                Some(format!("WhatsApp channel {channel_id} is not connected")),
            );
        }
        if !self.whatsapp.is_channel_connected(&channel_id) {
            self.set_channel_status(&channel_id, "disconnected", None);
            self.set_qr(&channel_id, "disconnected", None, OffsetDateTime::now_utc());
            return self.update_outbound_after_dispatch(
                &outcome.message.id,
                None,
                "failed",
                Some(format!("WhatsApp channel {channel_id} is not connected")),
            );
        }
        let send_result = if let Some(media_id) = outcome.message.media_id.as_deref() {
            let (media, bytes) = match self.media_blob(media_id).await {
                Ok(Some(blob)) => blob,
                Ok(None) => {
                    return self.update_outbound_after_dispatch(
                        &outcome.message.id,
                        None,
                        "failed",
                        Some(format!("media {media_id} has no upload bytes available")),
                    );
                }
                Err(err) => {
                    return self.update_outbound_after_dispatch(
                        &outcome.message.id,
                        None,
                        "failed",
                        Some(err.to_string()),
                    );
                }
            };
            tokio::time::timeout(
                StdDuration::from_secs(30),
                self.whatsapp.send_media(
                    &channel_id,
                    &outcome.message.conversation_id,
                    OutboundMediaMessage {
                        media_type: media.media_type,
                        mime_type: media.mime_type,
                        filename: media.original_filename,
                        caption: outcome.message.text.clone(),
                        bytes,
                        ptt: outcome.message.message_type == "audio",
                    },
                ),
            )
            .await
        } else if let Some(text) = request_text {
            tokio::time::timeout(
                StdDuration::from_secs(15),
                self.whatsapp
                    .send_text(&channel_id, &outcome.message.conversation_id, text),
            )
            .await
        } else {
            return Ok(outcome.message.clone());
        };
        match send_result {
            Ok(Ok(wa_message_id)) => self.update_outbound_after_dispatch(
                &outcome.message.id,
                Some(wa_message_id),
                "sent_to_whatsapp",
                None,
            ),
            Ok(Err(err)) => {
                let error_message = err.to_string();
                tracing::warn!(
                    channel_id,
                    conversation_id = outcome.message.conversation_id,
                    error = %error_message,
                    "WhatsApp send failed"
                );
                if error_message.contains("is not connected") {
                    self.set_channel_status(&channel_id, "disconnected", None);
                    self.set_qr(&channel_id, "disconnected", None, OffsetDateTime::now_utc());
                }
                self.update_outbound_after_dispatch(
                    &outcome.message.id,
                    None,
                    "failed",
                    Some(error_message),
                )
            }
            Err(_) => {
                tracing::warn!(
                    channel_id,
                    conversation_id = outcome.message.conversation_id,
                    "WhatsApp send timed out"
                );
                self.update_outbound_after_dispatch(
                    &outcome.message.id,
                    None,
                    "failed",
                    Some("WhatsApp send timed out".to_string()),
                )
            }
        }
    }

    pub async fn handle_whatsapp_event(&self, runtime: ChannelRuntime, event: WhatsappEvent) {
        if self.kafka_worker_mode_enabled() && !raw_whatsapp_event_must_stay_inline(&event) {
            match self.enqueue_whatsapp_raw_event(runtime.clone(), event.clone()) {
                Ok(_) => return,
                Err(err) => {
                    tracing::error!(error = %err, "failed to enqueue raw WhatsApp event; processing inline");
                }
            }
        }

        self.process_whatsapp_event(runtime, event).await;
    }

    fn enqueue_whatsapp_raw_event(
        &self,
        runtime: ChannelRuntime,
        event: WhatsappEvent,
    ) -> ApiResult<crate::models::CommonEvent> {
        if raw_whatsapp_event_must_stay_inline(&event) {
            return Err(ApiError::BadRequest(
                "sensitive WhatsApp control event cannot be enqueued to Kafka".to_string(),
            ));
        }
        let payload = serde_json::to_value(RawWhatsappEventEnvelope {
            runtime: RawWhatsappRuntimeEnvelope {
                project_id: runtime.project_id.clone(),
                company_id: runtime.company_id.clone(),
                channel_id: runtime.channel_id.clone(),
            },
            event,
        })
        .map_err(|err| ApiError::Internal(format!("failed to encode WhatsApp event: {err}")))?;
        let raw_event = new_event(
            "whatsapp.raw.inbound",
            &runtime.project_id,
            &runtime.company_id,
            Some(runtime.channel_id),
            None,
            None,
            None,
            payload,
        );
        self.push_event(raw_event.clone());
        Ok(raw_event)
    }

    pub async fn process_whatsapp_raw_event(
        &self,
        event: &crate::models::CommonEvent,
    ) -> ApiResult<()> {
        let envelope: RawWhatsappEventEnvelope = serde_json::from_value(event.payload.clone())
            .map_err(|err| ApiError::BadRequest(format!("invalid WhatsApp raw event: {err}")))?;
        self.process_whatsapp_event(
            ChannelRuntime {
                project_id: envelope.runtime.project_id,
                company_id: envelope.runtime.company_id,
                channel_id: envelope.runtime.channel_id,
                session_path: PathBuf::new(),
            },
            envelope.event,
        )
        .await;
        Ok(())
    }

    async fn process_whatsapp_event(&self, runtime: ChannelRuntime, event: WhatsappEvent) {
        match event {
            WhatsappEvent::PairingQrCode { code, timeout } => {
                self.set_channel_status(&runtime.channel_id, "waiting_qr", None);
                self.set_qr(
                    &runtime.channel_id,
                    "waiting_qr",
                    Some(code),
                    qr_expires_at(timeout),
                );
                self.push_event(new_event(
                    "channel.qr",
                    &runtime.project_id,
                    &runtime.company_id,
                    Some(runtime.channel_id),
                    None,
                    None,
                    None,
                    json!({ "status": "waiting_qr" }),
                ));
            }
            WhatsappEvent::Connected => {
                let connected_at = OffsetDateTime::now_utc();
                self.set_channel_status(&runtime.channel_id, "connected", Some(connected_at));
                self.push_event(new_event(
                    "channel.connected",
                    &runtime.project_id,
                    &runtime.company_id,
                    Some(runtime.channel_id),
                    None,
                    None,
                    None,
                    json!({ "connected_at": ts(connected_at) }),
                ));
            }
            WhatsappEvent::Disconnected => {
                self.set_channel_status(&runtime.channel_id, "disconnected", None);
                self.push_event(new_event(
                    "channel.disconnected",
                    &runtime.project_id,
                    &runtime.company_id,
                    Some(runtime.channel_id),
                    None,
                    None,
                    None,
                    json!({}),
                ));
            }
            WhatsappEvent::LoggedOut { reason } => {
                self.set_channel_status(&runtime.channel_id, "logged_out", None);
                self.push_event(new_event(
                    "channel.logged_out",
                    &runtime.project_id,
                    &runtime.company_id,
                    Some(runtime.channel_id),
                    None,
                    None,
                    None,
                    json!({ "reason": reason }),
                ));
            }
            WhatsappEvent::HistorySyncIgnored => {
                self.push_event(new_event(
                    "history_sync.ignored",
                    &runtime.project_id,
                    &runtime.company_id,
                    Some(runtime.channel_id),
                    None,
                    None,
                    None,
                    json!({ "reason": "skip_history_sync" }),
                ));
            }
            WhatsappEvent::Message {
                wa_message_id,
                chat_jid,
                sender_jid,
                sender_alt_jid,
                recipient_jid,
                recipient_alt_jid,
                push_name,
                text,
                message_type,
                media,
                created_at_wa,
                is_from_me,
            } => {
                if is_updates_event_surface(
                    &chat_jid,
                    &sender_jid,
                    sender_alt_jid.as_deref(),
                    recipient_jid.as_deref(),
                    recipient_alt_jid.as_deref(),
                ) {
                    self.push_event(new_event(
                        "whatsapp_updates.ignored",
                        &runtime.project_id,
                        &runtime.company_id,
                        Some(runtime.channel_id),
                        None,
                        None,
                        None,
                        json!({ "reason": "updates_surface" }),
                    ));
                    return;
                }
                if message_type == "system" && text.as_deref().unwrap_or_default().is_empty() {
                    self.push_event(new_event(
                        "message.system_ignored",
                        &runtime.project_id,
                        &runtime.company_id,
                        Some(runtime.channel_id),
                        Some(chat_jid),
                        None,
                        None,
                        json!({ "wa_message_id": wa_message_id }),
                    ));
                    return;
                }
                if let Some(connected_at) = self.channel_connected_at(&runtime.channel_id)
                    && created_at_wa < connected_at
                {
                    self.push_event(new_event(
                        "ignored_old_message",
                        &runtime.project_id,
                        &runtime.company_id,
                        Some(runtime.channel_id),
                        Some(chat_jid),
                        None,
                        None,
                        json!({
                            "wa_message_id": wa_message_id,
                            "created_at_wa": ts(created_at_wa),
                            "connected_at": ts(connected_at)
                        }),
                    ));
                    return;
                }
                let is_group = chat_jid.ends_with("@g.us");
                let conversation_id = chat_jid.clone();
                let direct_contact_alt_jid = if is_from_me {
                    recipient_alt_jid.clone().or_else(|| {
                        recipient_jid
                            .clone()
                            .filter(|jid| phone_from_jid(jid).is_some())
                    })
                } else {
                    sender_alt_jid.clone()
                };
                let contact_id = if is_group {
                    None
                } else {
                    Some(chat_jid.clone())
                };
                let contact_alt_jid = if is_group {
                    None
                } else {
                    direct_contact_alt_jid.clone()
                };
                let contact_name = if !is_group && !is_from_me {
                    push_name.clone()
                } else {
                    None
                };
                let sender_contact_id = if is_from_me {
                    None
                } else {
                    Some(sender_jid.clone())
                };
                let sender_name = if is_from_me { None } else { push_name.clone() };
                let contact_id_for_enrichment = if is_group {
                    (!is_from_me).then(|| sender_jid.clone())
                } else {
                    Some(chat_jid.clone())
                };
                let contact_alt_for_enrichment = if is_group {
                    (!is_from_me).then(|| sender_alt_jid.clone()).flatten()
                } else {
                    direct_contact_alt_jid
                };
                let push_name_for_enrichment = if is_from_me { None } else { push_name.clone() };
                let message = self.record_whatsapp_message(WhatsappMessageInput {
                    project_id: runtime.project_id.clone(),
                    company_id: runtime.company_id.clone(),
                    channel_id: runtime.channel_id.clone(),
                    conversation_id: chat_jid.clone(),
                    contact_id,
                    contact_alt_jid,
                    contact_name,
                    direction: if is_from_me { "outbound" } else { "inbound" }.to_string(),
                    message_type,
                    text,
                    status: if is_from_me {
                        "sent_to_whatsapp"
                    } else {
                        "received"
                    }
                    .to_string(),
                    wa_message_id: Some(wa_message_id),
                    sender_contact_id,
                    sender_alt_jid,
                    sender_name,
                    created_at_wa,
                });
                if let Some(media) = media {
                    let descriptor = *media;
                    self.push_event(new_event(
                        "media.download.requested",
                        &runtime.project_id,
                        &runtime.company_id,
                        Some(runtime.channel_id.clone()),
                        Some(chat_jid.clone()),
                        Some(message.id.clone()),
                        Some(message.conversation_seq),
                        json!({
                            "message_id": message.id.clone(),
                            "media": descriptor.clone()
                        }),
                    ));
                    if let Err(err) = self
                        .download_and_store_inbound_media(&runtime, &message, &chat_jid, descriptor)
                        .await
                    {
                        self.push_event(new_event(
                            "media.download.failed",
                            &runtime.project_id,
                            &runtime.company_id,
                            Some(runtime.channel_id.clone()),
                            Some(chat_jid.clone()),
                            Some(message.id.clone()),
                            Some(message.conversation_seq),
                            json!({
                                "message_id": message.id.clone(),
                                "error": err.to_string()
                            }),
                        ));
                    }
                }
                if let Some(contact_id_for_enrichment) = contact_id_for_enrichment
                    && !contact_id_for_enrichment.trim().is_empty()
                {
                    self.spawn_contact_enrichment(
                        &runtime,
                        conversation_id,
                        contact_id_for_enrichment,
                        contact_alt_for_enrichment,
                        push_name_for_enrichment,
                    );
                }
                if is_group {
                    self.spawn_group_enrichment(&runtime, chat_jid.clone(), false);
                }
                tracing::debug!(message_id = message.id, "WhatsApp message recorded");
            }
            WhatsappEvent::Receipt {
                wa_message_ids,
                receipt_type,
                chat_jid,
                created_at_wa,
            } => {
                if is_updates_surface_jid(&chat_jid) {
                    self.push_event(new_event(
                        "whatsapp_updates.ignored",
                        &runtime.project_id,
                        &runtime.company_id,
                        Some(runtime.channel_id),
                        None,
                        None,
                        None,
                        json!({ "reason": "updates_surface" }),
                    ));
                    return;
                }
                for wa_message_id in wa_message_ids {
                    let _ = self.update_receipt_by_wa_id_with_context(
                        &wa_message_id,
                        &receipt_type,
                        Some(created_at_wa),
                    );
                }
            }
            WhatsappEvent::GroupUpdate {
                group_jid,
                subject,
                created_at_wa,
            } => {
                if is_updates_surface_jid(&group_jid) {
                    self.push_event(new_event(
                        "whatsapp_updates.ignored",
                        &runtime.project_id,
                        &runtime.company_id,
                        Some(runtime.channel_id),
                        None,
                        None,
                        None,
                        json!({ "reason": "updates_surface" }),
                    ));
                    return;
                }
                if let Some(subject) = subject.as_deref() {
                    self.apply_group_subject(
                        &runtime.project_id,
                        &runtime.company_id,
                        &runtime.channel_id,
                        &group_jid,
                        subject,
                        created_at_wa,
                    );
                }
                self.spawn_group_enrichment(&runtime, group_jid, true);
            }
            WhatsappEvent::Diagnostic {
                event_type,
                payload,
            } => {
                if event_type == "channel.connect_failure" {
                    let reason = payload
                        .get("reason")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let status = if reason.contains("LoggedOut") {
                        "logged_out"
                    } else {
                        "disconnected"
                    };
                    self.set_channel_status(&runtime.channel_id, status, None);
                    self.set_qr(&runtime.channel_id, status, None, OffsetDateTime::now_utc());
                }
                self.push_event(new_event(
                    &event_type,
                    &runtime.project_id,
                    &runtime.company_id,
                    Some(runtime.channel_id),
                    None,
                    None,
                    None,
                    payload,
                ));
            }
        }
    }

    pub fn list_messages(
        &self,
        conversation_id: &str,
        after_seq: Option<i64>,
        before_seq: Option<i64>,
        limit: usize,
    ) -> MessagesPage {
        let inner = self.inner.lock().expect("store lock poisoned");
        let mut messages: Vec<_> = inner
            .messages_by_conversation
            .get(conversation_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|message| after_seq.is_none_or(|seq| message.conversation_seq > seq))
            .filter(|message| before_seq.is_none_or(|seq| message.conversation_seq < seq))
            .collect();
        messages.sort_by_key(|message| message.conversation_seq);
        for message in &mut messages {
            self.normalize_message_media_urls(message);
        }
        let has_more = messages.len() > limit;
        messages.truncate(limit);
        let from_seq = messages.first().map(|message| message.conversation_seq);
        let to_seq = messages.last().map(|message| message.conversation_seq);
        MessagesPage {
            conversation_id: conversation_id.to_string(),
            from_seq,
            to_seq,
            has_more,
            messages,
        }
    }

    fn normalize_message_media_urls(&self, message: &mut Message) {
        if self.config.storage_provider != StorageProvider::LocalFs || !self.config.dev_mode {
            return;
        }
        let Some(media_id) = message.media_id.as_deref() else {
            return;
        };
        let url = self.config.dev_media_url(media_id);
        message.media_url = Some(url.clone());
        if message.message_type == "image" {
            message.thumbnail_url = Some(url);
        }
    }

    pub fn list_messages_for_conversation(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
        after_seq: Option<i64>,
        before_seq: Option<i64>,
        limit: usize,
    ) -> ApiResult<MessagesPage> {
        let conversation = self.conversation(project_id, company_id, conversation_id)?;
        let public_id = public_conversation_id(&conversation);
        let mut page = self.list_messages(&conversation.id, after_seq, before_seq, limit);
        page.conversation_id = public_id.clone();
        for message in &mut page.messages {
            message.conversation_id = public_id.clone();
        }
        Ok(page)
    }

    fn record_whatsapp_message(&self, input: WhatsappMessageInput) -> Message {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        if let Some(existing) = input.wa_message_id.as_ref().and_then(|wa_message_id| {
            inner
                .messages_by_id
                .values()
                .find(|message| {
                    message.channel_account_id == input.channel_id
                        && message.wa_message_id.as_deref() == Some(wa_message_id.as_str())
                        && message.direction == input.direction
                })
                .cloned()
        }) {
            return existing;
        }

        let now = OffsetDateTime::now_utc();
        let is_group = input.conversation_id.ends_with("@g.us");
        let contact_id = input.contact_id.clone().or_else(|| {
            if is_group && input.direction == "inbound" {
                input.sender_contact_id.clone()
            } else if !is_group {
                Some(input.conversation_id.clone())
            } else {
                None
            }
        });
        let contact = contact_id.as_ref().map(|contact_id| {
            upsert_contact_with_alt_inner(
                &mut inner,
                ContactUpsertWithAltInput {
                    contact: ContactUpsertInput {
                        project_id: &input.project_id,
                        company_id: &input.company_id,
                        channel_id: Some(&input.channel_id),
                        contact_id,
                        push_name: input.contact_name.as_deref(),
                        profile_picture_url: None,
                        now,
                    },
                    alt_jid: input.contact_alt_jid.as_deref(),
                },
            )
        });
        let sender_contact = if is_group && input.direction == "inbound" {
            input.sender_contact_id.as_ref().map(|sender_contact_id| {
                upsert_contact_with_alt_inner(
                    &mut inner,
                    ContactUpsertWithAltInput {
                        contact: ContactUpsertInput {
                            project_id: &input.project_id,
                            company_id: &input.company_id,
                            channel_id: Some(&input.channel_id),
                            contact_id: sender_contact_id,
                            push_name: input.sender_name.as_deref(),
                            profile_picture_url: None,
                            now,
                        },
                        alt_jid: input.sender_alt_jid.as_deref(),
                    },
                )
            })
        } else {
            None
        };
        if is_group && let Some(sender_contact_id) = input.sender_contact_id.as_deref() {
            let display_name = sender_contact
                .as_ref()
                .or(contact.as_ref())
                .map(|contact| contact.display_name.clone())
                .or_else(|| input.sender_name.clone())
                .unwrap_or_else(|| display_name_for_jid(sender_contact_id));
            upsert_group_member_inner(
                &mut inner,
                &input.conversation_id,
                sender_contact_id,
                &display_name,
                now,
            );
        }
        let group_record = if is_group {
            inner.groups.get(&input.conversation_id).cloned()
        } else {
            None
        };
        let display_name = if is_group {
            Some(
                group_record
                    .as_ref()
                    .and_then(|group| group.subject.clone())
                    .unwrap_or_else(|| group_subject_for_jid(&input.conversation_id)),
            )
        } else {
            contact.as_ref().map(|contact| contact.display_name.clone())
        };
        let display_phone = if is_group {
            None
        } else {
            contact
                .as_ref()
                .and_then(|contact| contact.phone_e164.clone())
        };
        let avatar_url = if is_group {
            group_record
                .as_ref()
                .and_then(|group| group.avatar_url.clone())
        } else {
            contact
                .as_ref()
                .and_then(|contact| contact.avatar_url.clone())
        };
        let profile_picture_url = if is_group {
            group_record
                .as_ref()
                .and_then(|group| group.profile_picture_url.clone())
        } else {
            contact
                .as_ref()
                .and_then(|contact| contact.profile_picture_url.clone())
        };
        let conversation = inner
            .conversations
            .entry(input.conversation_id.clone())
            .or_insert_with(|| crate::models::Conversation {
                id: input.conversation_id.clone(),
                project_id: input.project_id.clone(),
                company_id: input.company_id.clone(),
                channel_account_id: input.channel_id.clone(),
                conversation_type: if is_group {
                    "group".to_string()
                } else {
                    "direct".to_string()
                },
                contact_id: if is_group { None } else { contact_id.clone() },
                group_id: is_group.then(|| input.conversation_id.clone()),
                display_name: display_name.clone(),
                display_phone: display_phone.clone(),
                phone_number: display_phone.as_deref().and_then(phone_number_from_e164),
                avatar_url: avatar_url.clone(),
                profile_picture_url: profile_picture_url.clone(),
                last_seq: 0,
                last_message_at: None,
                unread_count: 0,
                is_archived: false,
                is_muted: false,
                is_pinned: false,
                control_mode: "manual".to_string(),
                created_at: now,
                updated_at: now,
            });
        if is_group {
            apply_group_record_to_conversation(
                conversation,
                &input.conversation_id,
                group_record.as_ref(),
            );
        } else {
            if display_name.is_some() {
                conversation.display_name = display_name;
            }
            if display_phone.is_some() {
                conversation.phone_number =
                    display_phone.as_deref().and_then(phone_number_from_e164);
                conversation.display_phone = display_phone;
            }
            if avatar_url.is_some() {
                conversation.avatar_url = avatar_url;
            }
            if profile_picture_url.is_some() {
                conversation.profile_picture_url = profile_picture_url;
            }
        }
        conversation.last_seq += 1;
        conversation.last_message_at = Some(input.created_at_wa);
        conversation.updated_at = now;
        if input.direction == "inbound" {
            conversation.unread_count += 1;
        }
        let conversation_seq = conversation.last_seq;
        let message = Message {
            id: format!("msg_{}", Uuid::now_v7().simple()),
            project_id: input.project_id.clone(),
            company_id: input.company_id.clone(),
            conversation_id: input.conversation_id.clone(),
            channel_account_id: input.channel_id.clone(),
            conversation_seq,
            wa_message_id: input.wa_message_id.clone(),
            direction: input.direction.clone(),
            sender_contact_id: input.sender_contact_id,
            sender_display_name: if input.direction == "inbound" {
                sender_contact
                    .as_ref()
                    .or(contact.as_ref())
                    .map(|contact| contact.display_name.clone())
                    .or(input.sender_name.clone())
            } else {
                None
            },
            message_type: input.message_type,
            text: input.text,
            media_id: None,
            media_url: None,
            thumbnail_url: None,
            mime_type: None,
            file_name: None,
            quoted_message_id: None,
            status: input.status,
            error_message: None,
            is_starred: false,
            is_pinned: false,
            reaction: None,
            sent_by_source: (input.direction == "outbound").then(|| "whatsapp-rust".to_string()),
            sent_by_external_user_id: (input.direction == "outbound")
                .then_some(input.sender_name)
                .flatten(),
            created_at_wa: input.created_at_wa,
            created_at: now,
            updated_at: now,
        };
        inner
            .messages_by_conversation
            .entry(input.conversation_id.clone())
            .or_default()
            .push(message.clone());
        inner
            .messages_by_id
            .insert(message.id.clone(), message.clone());
        mark_dirty_inner(
            &mut inner,
            &input.project_id,
            &input.company_id,
            &input.conversation_id,
            conversation_seq,
            &input.channel_id,
            Some(&self.events_tx),
            Some(&self.event_bus),
        );
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                if input.direction == "inbound" {
                    "message.received"
                } else {
                    "message.sent"
                },
                &input.project_id,
                &input.company_id,
                Some(input.channel_id),
                Some(input.conversation_id),
                Some(message.id.clone()),
                Some(conversation_seq),
                json!({
                    "wa_message_id": message.wa_message_id,
                    "status": message.status.clone(),
                    "delivery_state": message.delivery_state()
                }),
            ),
        );
        self.persist_locked(&inner);
        message
    }

    fn channel_for_conversation(&self, conversation_id: &str) -> Option<String> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .conversations
            .get(conversation_id)
            .map(|conversation| conversation.channel_account_id.clone())
    }

    fn channel_status(&self, channel_id: &str) -> Option<String> {
        self.reconcile_channel_runtime_status(channel_id);
        self.inner
            .lock()
            .expect("store lock poisoned")
            .channels
            .get(channel_id)
            .and_then(|channel| channel.get("status"))
            .and_then(Value::as_str)
            .map(str::to_string)
    }

    fn contact_refresh_context(
        &self,
        project_id: &str,
        company_id: &str,
        contact_id: &str,
    ) -> ApiResult<ContactRefreshContext> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let resolved_contact_id =
            resolve_contact_id_inner(&inner, project_id, company_id, contact_id);
        let contact = inner
            .contacts
            .get(&resolved_contact_id)
            .filter(|contact| contact.project_id == project_id && contact.company_id == company_id)
            .ok_or_else(|| {
                ApiError::NotFound(format!("contact {resolved_contact_id} not found"))
            })?;
        let aliases = contact_aliases_for_record(contact);
        let conversation = inner.conversations.values().find(|conversation| {
            conversation.project_id == project_id
                && conversation.company_id == company_id
                && conversation.conversation_type == "direct"
                && (aliases.contains(&conversation.id)
                    || conversation
                        .contact_id
                        .as_ref()
                        .is_some_and(|contact_id| aliases.contains(contact_id)))
        });
        let channel_id = contact
            .channel_account_id
            .clone()
            .or_else(|| conversation.map(|conversation| conversation.channel_account_id.clone()))
            .or_else(|| connected_channel_for_project_company(&inner, project_id, company_id));
        Ok(ContactRefreshContext {
            channel_id,
            conversation_id: conversation.map(|conversation| conversation.id.clone()),
            alt_jid: contact
                .canonical_jid
                .clone()
                .or_else(|| contact.lid.clone())
                .filter(|jid| jid != &resolved_contact_id),
            push_name: contact.push_name.clone(),
        })
    }

    fn group_refresh_channel(
        &self,
        project_id: &str,
        company_id: &str,
        group_id: &str,
    ) -> ApiResult<Option<String>> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let conversation = inner
            .conversations
            .get(group_id)
            .filter(|conversation| {
                conversation.project_id == project_id
                    && conversation.company_id == company_id
                    && conversation.conversation_type == "group"
            })
            .ok_or_else(|| ApiError::NotFound(format!("group {group_id} not found")))?;
        Ok(Some(conversation.channel_account_id.clone())
            .or_else(|| connected_channel_for_project_company(&inner, project_id, company_id)))
    }

    fn spawn_contact_enrichment(
        &self,
        runtime: &ChannelRuntime,
        conversation_id: String,
        contact_id: String,
        alt_jid: Option<String>,
        push_name: Option<String>,
    ) {
        let state = self.clone();
        let project_id = runtime.project_id.clone();
        let company_id = runtime.company_id.clone();
        let channel_id = runtime.channel_id.clone();
        tokio::spawn(async move {
            match state
                .whatsapp
                .contact_profile(&channel_id, &contact_id)
                .await
            {
                Ok(profile) => {
                    let profile = merge_contact_profile_alt(profile, alt_jid.as_deref());
                    state.apply_contact_profile(ContactProfileApplyInput {
                        project_id: &project_id,
                        company_id: &company_id,
                        channel_id: &channel_id,
                        conversation_id: &conversation_id,
                        contact_id: &contact_id,
                        push_name: push_name.as_deref(),
                        profile,
                    });
                }
                Err(err) => {
                    if let Some(profile) = contact_profile_from_alt(&contact_id, alt_jid.as_deref())
                    {
                        state.apply_contact_profile(ContactProfileApplyInput {
                            project_id: &project_id,
                            company_id: &company_id,
                            channel_id: &channel_id,
                            conversation_id: &conversation_id,
                            contact_id: &contact_id,
                            push_name: push_name.as_deref(),
                            profile,
                        });
                    } else {
                        tracing::debug!(
                            %channel_id,
                            %contact_id,
                            error = %err,
                            "contact enrichment failed"
                        );
                    }
                }
            }
        });
    }

    fn spawn_group_enrichment(&self, runtime: &ChannelRuntime, group_id: String, force: bool) {
        if !group_id.ends_with("@g.us")
            || !self.reserve_group_profile_refresh(runtime, &group_id, force)
        {
            return;
        }

        let state = self.clone();
        let project_id = runtime.project_id.clone();
        let company_id = runtime.company_id.clone();
        let channel_id = runtime.channel_id.clone();
        tokio::spawn(async move {
            match state.whatsapp.group_profile(&channel_id, &group_id).await {
                Ok(profile) => {
                    state.apply_group_profile(
                        &project_id,
                        &company_id,
                        &channel_id,
                        &group_id,
                        profile,
                    );
                }
                Err(err) => {
                    tracing::debug!(
                        %channel_id,
                        %group_id,
                        error = %err,
                        "group enrichment failed"
                    );
                }
            }
        });
    }

    fn reserve_group_profile_refresh(
        &self,
        runtime: &ChannelRuntime,
        group_id: &str,
        force: bool,
    ) -> bool {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let now = OffsetDateTime::now_utc();
        let key = group_profile_refresh_key(runtime, group_id);
        if !force
            && let Some(last_refresh) = inner.group_profile_refreshes.get(&key)
            && *last_refresh > now - Duration::minutes(5)
        {
            return false;
        }
        inner.group_profile_refreshes.insert(key, now);
        true
    }

    fn apply_contact_profile(&self, input: ContactProfileApplyInput<'_>) {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let now = OffsetDateTime::now_utc();
        let contact = enrich_contact_inner(
            &mut inner,
            ContactEnrichmentInput {
                project_id: input.project_id,
                company_id: input.company_id,
                channel_id: Some(input.channel_id),
                contact_id: input.contact_id,
                push_name: input.push_name,
                profile: &input.profile,
                now,
            },
        );
        let aliases = contact_aliases(&contact, &input.profile);

        if let Some(conversation) = inner.conversations.get_mut(input.conversation_id)
            && conversation.conversation_type == "direct"
        {
            apply_contact_to_conversation(conversation, &contact);
        }
        for conversation in inner.conversations.values_mut() {
            if conversation.company_id == input.company_id
                && conversation.project_id == input.project_id
                && conversation.conversation_type == "direct"
                && aliases.iter().any(|alias| {
                    conversation.id == *alias
                        || conversation.contact_id.as_deref() == Some(alias.as_str())
                })
            {
                apply_contact_to_conversation(conversation, &contact);
            }
        }

        for messages in inner.messages_by_conversation.values_mut() {
            for message in messages {
                if message
                    .sender_contact_id
                    .as_deref()
                    .is_some_and(|sender| aliases.iter().any(|alias| alias == sender))
                {
                    message.sender_display_name = Some(contact.display_name.clone());
                    message.updated_at = now;
                }
            }
        }
        for message in inner.messages_by_id.values_mut() {
            if message
                .sender_contact_id
                .as_deref()
                .is_some_and(|sender| aliases.iter().any(|alias| alias == sender))
            {
                message.sender_display_name = Some(contact.display_name.clone());
                message.updated_at = now;
            }
        }

        for members in inner.group_members.values_mut() {
            for member in members.values_mut() {
                if aliases.iter().any(|alias| alias == &member.contact_id) {
                    member.display_name = contact.display_name.clone();
                    member.last_seen_at = now;
                }
            }
        }

        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "contact.updated",
                input.project_id,
                input.company_id,
                Some(input.channel_id.to_string()),
                Some(input.conversation_id.to_string()),
                None,
                None,
                json!({
                    "contact_id": contact.id,
                    "canonical_jid": contact.canonical_jid,
                    "lid": contact.lid,
                    "phone_e164": contact.phone_e164,
                    "profile_picture_url": contact.profile_picture_url
                }),
            ),
        );
        self.persist_locked(&inner);
    }

    fn apply_group_subject(
        &self,
        project_id: &str,
        company_id: &str,
        channel_id: &str,
        group_id: &str,
        subject: &str,
        created_at_wa: OffsetDateTime,
    ) {
        let Some(subject) = clean_group_subject(Some(subject)) else {
            return;
        };
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let record_snapshot = {
            let record = inner
                .groups
                .entry(group_id.to_string())
                .or_insert_with(empty_group_record);
            record.wa_jid = Some(group_id.to_string());
            record.subject = Some(subject.clone());
            record.clone()
        };
        let conversation = inner
            .conversations
            .entry(group_id.to_string())
            .or_insert_with(|| group_conversation(project_id, company_id, channel_id, group_id));
        apply_group_record_to_conversation(conversation, group_id, Some(&record_snapshot));
        conversation.updated_at = OffsetDateTime::now_utc();
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "group.updated",
                project_id,
                company_id,
                Some(channel_id.to_string()),
                Some(group_id.to_string()),
                None,
                None,
                json!({
                    "group_id": group_id,
                    "subject": subject,
                    "created_at_wa": ts(created_at_wa)
                }),
            ),
        );
        self.persist_locked(&inner);
    }

    fn apply_group_profile(
        &self,
        project_id: &str,
        company_id: &str,
        channel_id: &str,
        group_id: &str,
        profile: GroupProfile,
    ) {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let now = OffsetDateTime::now_utc();
        let subject = clean_group_subject(profile.subject.as_deref());
        let description = clean_optional_text(profile.description.as_deref());
        let owner_jid = profile.owner_jid.clone();
        let subject_owner_jid = profile.subject_owner_jid.clone();
        let created_at_wa = profile.created_at_wa_unix.and_then(|timestamp| {
            i64::try_from(timestamp)
                .ok()
                .and_then(|timestamp| OffsetDateTime::from_unix_timestamp(timestamp).ok())
        });
        let metadata_members_count = profile.size;
        let profile_picture_id = profile.profile_picture_id.clone();
        let profile_picture_url = profile.profile_picture_url.clone();
        let participants = profile.participants;
        let members_count = metadata_members_count
            .unwrap_or_else(|| u32::try_from(participants.len()).unwrap_or(u32::MAX));
        let admins_count = u32::try_from(
            participants
                .iter()
                .filter(|participant| {
                    participant.is_admin
                        || owner_jid.as_deref() == Some(participant.contact_id.as_str())
                })
                .count(),
        )
        .unwrap_or(u32::MAX);

        let record_snapshot = {
            let record = inner
                .groups
                .entry(group_id.to_string())
                .or_insert_with(empty_group_record);
            record.wa_jid = Some(profile.group_jid.clone());
            record.subject = subject.clone();
            record.description = description.clone();
            record.owner_jid = owner_jid.clone();
            record.subject_owner_jid = subject_owner_jid.clone();
            record.created_at_wa = created_at_wa;
            record.members_count = Some(members_count);
            record.admins_count = Some(admins_count);
            record.profile_picture_media_id = profile_picture_id.clone();
            record.avatar_url = profile_picture_url.clone();
            record.profile_picture_url = profile_picture_url.clone();
            record.clone()
        };

        for participant in participants {
            if let Some(phone_jid) = participant.phone_jid.as_deref() {
                upsert_contact_with_alt_inner(
                    &mut inner,
                    ContactUpsertWithAltInput {
                        contact: ContactUpsertInput {
                            project_id,
                            company_id,
                            channel_id: Some(channel_id),
                            contact_id: &participant.contact_id,
                            push_name: None,
                            profile_picture_url: None,
                            now,
                        },
                        alt_jid: Some(phone_jid),
                    },
                );
            }
            let display_name = group_participant_display_name(&inner, &participant);
            upsert_group_member_metadata_inner(
                &mut inner,
                group_id,
                &participant.contact_id,
                participant.phone_jid.as_deref(),
                &display_name,
                owner_jid.as_deref(),
                participant.is_admin,
                now,
            );
        }

        let conversation = inner
            .conversations
            .entry(group_id.to_string())
            .or_insert_with(|| group_conversation(project_id, company_id, channel_id, group_id));
        apply_group_record_to_conversation(conversation, group_id, Some(&record_snapshot));
        conversation.updated_at = now;

        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "group.updated",
                project_id,
                company_id,
                Some(channel_id.to_string()),
                Some(group_id.to_string()),
                None,
                None,
                json!({
                    "group_id": group_id,
                    "subject": record_snapshot.subject,
                    "description": record_snapshot.description,
                    "owner_jid": record_snapshot.owner_jid,
                    "subject_owner_jid": record_snapshot.subject_owner_jid,
                    "created_at_wa": record_snapshot.created_at_wa.map(ts),
                    "profile_picture_id": profile_picture_id,
                    "profile_picture_url": record_snapshot.profile_picture_url,
                    "members_count": members_count,
                    "admins_count": admins_count
                }),
            ),
        );
        self.persist_locked(&inner);
    }

    fn channel_connected_at(&self, channel_id: &str) -> Option<OffsetDateTime> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .channels
            .get(channel_id)
            .and_then(|channel| channel.get("connected_at"))
            .and_then(|value| value.as_str())
            .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
    }

    fn update_outbound_after_dispatch(
        &self,
        message_id: &str,
        wa_message_id: Option<String>,
        status: &str,
        error_message: Option<String>,
    ) -> ApiResult<Message> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let updated = {
            let message = inner
                .messages_by_id
                .get_mut(message_id)
                .ok_or_else(|| ApiError::NotFound(format!("message {message_id} not found")))?;
            if wa_message_id.is_some() {
                message.wa_message_id = wa_message_id;
            }
            message.status = status.to_string();
            message.error_message = error_message;
            message.updated_at = OffsetDateTime::now_utc();
            message.clone()
        };
        if let Some(messages) = inner
            .messages_by_conversation
            .get_mut(&updated.conversation_id)
            && let Some(existing) = messages.iter_mut().find(|message| message.id == updated.id)
        {
            *existing = updated.clone();
        }
        mark_dirty_inner(
            &mut inner,
            &updated.project_id,
            &updated.company_id,
            &updated.conversation_id,
            updated.conversation_seq,
            &updated.channel_account_id,
            Some(&self.events_tx),
            Some(&self.event_bus),
        );
        let event_type = match status {
            "sent_to_whatsapp" => "message.sent",
            "failed" => "message.failed",
            _ => "message.updated",
        };
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                event_type,
                &updated.project_id,
                &updated.company_id,
                Some(updated.channel_account_id.clone()),
                Some(updated.conversation_id.clone()),
                Some(updated.id.clone()),
                Some(updated.conversation_seq),
                json!({
                    "message_id": updated.id.clone(),
                    "wa_message_id": updated.wa_message_id.clone(),
                    "status": updated.status.clone(),
                    "delivery_state": updated.delivery_state(),
                    "error_message": updated.error_message.clone()
                }),
            ),
        );
        self.persist_locked(&inner);
        Ok(updated)
    }

    pub fn conversations(
        &self,
        project_id: &str,
        company_id: &str,
    ) -> Vec<crate::models::Conversation> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let mut conversations: Vec<_> = inner
            .conversations
            .values()
            .filter(|conversation| {
                conversation.project_id == project_id && conversation.company_id == company_id
            })
            .map(|conversation| {
                let mut conversation = conversation.clone();
                if conversation.conversation_type == "group" {
                    let group = inner.groups.get(&conversation.id);
                    let group_id = conversation.id.clone();
                    apply_group_record_to_conversation(&mut conversation, &group_id, group);
                }
                apply_public_conversation_fields(&inner, &mut conversation);
                conversation
            })
            .collect();
        conversations.sort_by(|a, b| b.last_message_at.cmp(&a.last_message_at));
        conversations
    }

    pub fn conversation(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
    ) -> ApiResult<crate::models::Conversation> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let mut conversation =
            conversation_for_inner(&inner, project_id, company_id, conversation_id)?.clone();
        if conversation.conversation_type == "group" {
            let group = inner.groups.get(&conversation.id);
            let group_id = conversation.id.clone();
            apply_group_record_to_conversation(&mut conversation, &group_id, group);
        }
        apply_public_conversation_fields(&inner, &mut conversation);
        Ok(conversation)
    }

    pub fn public_conversation_id_for_ref(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
    ) -> String {
        self.conversation(project_id, company_id, conversation_id)
            .map(|conversation| public_conversation_id(&conversation))
            .unwrap_or_else(|_| conversation_id.to_string())
    }

    pub fn contacts(&self, project_id: &str, company_id: &str) -> Vec<Value> {
        let mut contacts: Vec<_> = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .contacts
            .values()
            .filter(|contact| contact.project_id == project_id && contact.company_id == company_id)
            .map(contact_json)
            .collect();
        contacts.sort_by(|a, b| {
            b.get("last_contact_at")
                .and_then(Value::as_str)
                .cmp(&a.get("last_contact_at").and_then(Value::as_str))
        });
        contacts
    }

    pub fn contact(
        &self,
        project_id: &str,
        company_id: &str,
        contact_id: &str,
    ) -> ApiResult<Value> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let contact_id = resolve_contact_id_inner(&inner, project_id, company_id, contact_id);
        inner
            .contacts
            .get(&contact_id)
            .filter(|contact| contact.project_id == project_id && contact.company_id == company_id)
            .map(contact_json)
            .ok_or_else(|| ApiError::NotFound(format!("contact {contact_id} not found")))
    }

    pub async fn inspect_contact(
        &self,
        project_id: &str,
        company_id: &str,
        contact_id: &str,
    ) -> ApiResult<Value> {
        let context = self.contact_refresh_context(project_id, company_id, contact_id)?;
        if let Some(channel_id) = context.channel_id.as_deref()
            && self.channel_status(channel_id).as_deref() == Some("connected")
        {
            match self.whatsapp.contact_profile(channel_id, contact_id).await {
                Ok(profile) => {
                    let profile = merge_contact_profile_alt(profile, context.alt_jid.as_deref());
                    self.apply_contact_profile(ContactProfileApplyInput {
                        project_id,
                        company_id,
                        channel_id,
                        conversation_id: context.conversation_id.as_deref().unwrap_or(contact_id),
                        contact_id,
                        push_name: context.push_name.as_deref(),
                        profile,
                    });
                }
                Err(err) => {
                    tracing::debug!(
                        %channel_id,
                        %contact_id,
                        error = %err,
                        "best-effort contact inspection failed"
                    );
                }
            }
        }
        self.contact(project_id, company_id, contact_id)
    }

    pub fn contact_by_phone(
        &self,
        project_id: &str,
        company_id: &str,
        phone_e164: &str,
    ) -> ApiResult<Value> {
        let phone_number = phone_number_from_value(phone_e164)
            .ok_or_else(|| ApiError::BadRequest("phone number is empty".to_string()))?;
        let inner = self.inner.lock().expect("store lock poisoned");
        let contact_id =
            contact_id_for_phone_number_inner(&inner, project_id, company_id, &phone_number)
                .ok_or_else(|| {
                    ApiError::NotFound(format!("contact phone {phone_e164} not found"))
                })?;
        inner
            .contacts
            .get(&contact_id)
            .map(contact_json)
            .ok_or_else(|| ApiError::NotFound(format!("contact phone {phone_e164} not found")))
    }

    pub fn contact_conversations(
        &self,
        project_id: &str,
        company_id: &str,
        contact_id: &str,
    ) -> ApiResult<Vec<crate::models::Conversation>> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let aliases = contact_aliases_for_inner(&inner, project_id, company_id, contact_id)?;
        let mut conversations: Vec<_> = inner
            .conversations
            .values()
            .filter(|conversation| {
                conversation.project_id == project_id && conversation.company_id == company_id
            })
            .filter(|conversation| {
                if conversation.conversation_type == "direct" {
                    aliases.contains(&conversation.id)
                        || conversation
                            .contact_id
                            .as_ref()
                            .is_some_and(|contact_id| aliases.contains(contact_id))
                } else {
                    conversation
                        .group_id
                        .as_deref()
                        .and_then(|group_id| inner.group_members.get(group_id))
                        .is_some_and(|members| {
                            aliases.iter().any(|alias| members.contains_key(alias))
                        })
                }
            })
            .map(|conversation| {
                let mut conversation = conversation.clone();
                if conversation.conversation_type == "group" {
                    let group = inner.groups.get(&conversation.id);
                    let group_id = conversation.id.clone();
                    apply_group_record_to_conversation(&mut conversation, &group_id, group);
                }
                apply_public_conversation_fields(&inner, &mut conversation);
                conversation
            })
            .collect();
        conversations.sort_by(|a, b| b.last_message_at.cmp(&a.last_message_at));
        Ok(conversations)
    }

    pub fn media_for_contact(
        &self,
        project_id: &str,
        company_id: &str,
        contact_id: &str,
    ) -> ApiResult<Vec<MediaObject>> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let aliases = contact_aliases_for_inner(&inner, project_id, company_id, contact_id)?;
        let mut media: Vec<_> = inner
            .media
            .values()
            .filter(|media| media.project_id == project_id && media.company_id == company_id)
            .filter(|media| {
                let direct_conversation_matches = inner
                    .conversations
                    .get(&media.conversation_id)
                    .filter(|conversation| conversation.conversation_type == "direct")
                    .is_some_and(|conversation| {
                        aliases.contains(&conversation.id)
                            || conversation
                                .contact_id
                                .as_ref()
                                .is_some_and(|contact_id| aliases.contains(contact_id))
                    });
                let message_sender_matches = media
                    .message_id
                    .as_deref()
                    .and_then(|message_id| inner.messages_by_id.get(message_id))
                    .and_then(|message| message.sender_contact_id.as_ref())
                    .is_some_and(|sender| aliases.contains(sender));
                direct_conversation_matches || message_sender_matches
            })
            .cloned()
            .collect();
        media.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(media)
    }

    pub fn groups(&self, project_id: &str, company_id: &str) -> Vec<Value> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let mut groups: Vec<_> = inner
            .conversations
            .values()
            .filter(|conversation| {
                conversation.project_id == project_id
                    && conversation.company_id == company_id
                    && conversation.conversation_type == "group"
            })
            .map(|conversation| group_json(&inner, conversation))
            .collect();
        groups.sort_by(|a, b| {
            b.get("last_message_at")
                .and_then(Value::as_str)
                .cmp(&a.get("last_message_at").and_then(Value::as_str))
        });
        groups
    }

    pub fn group(&self, project_id: &str, company_id: &str, group_id: &str) -> ApiResult<Value> {
        let inner = self.inner.lock().expect("store lock poisoned");
        inner
            .conversations
            .get(group_id)
            .filter(|conversation| {
                conversation.project_id == project_id
                    && conversation.company_id == company_id
                    && conversation.conversation_type == "group"
            })
            .map(|conversation| group_json(&inner, conversation))
            .ok_or_else(|| ApiError::NotFound(format!("group {group_id} not found")))
    }

    pub async fn inspect_group(
        &self,
        project_id: &str,
        company_id: &str,
        group_id: &str,
    ) -> ApiResult<Value> {
        let channel_id = self.group_refresh_channel(project_id, company_id, group_id)?;
        if let Some(channel_id) = channel_id.as_deref()
            && self.channel_status(channel_id).as_deref() == Some("connected")
        {
            match self.whatsapp.group_profile(channel_id, group_id).await {
                Ok(profile) => {
                    self.apply_group_profile(project_id, company_id, channel_id, group_id, profile);
                }
                Err(err) => {
                    tracing::debug!(
                        %channel_id,
                        %group_id,
                        error = %err,
                        "best-effort group inspection failed"
                    );
                }
            }
        }
        self.group(project_id, company_id, group_id)
    }

    pub fn group_members(&self, group_id: &str) -> Vec<Value> {
        let inner = self.inner.lock().expect("store lock poisoned");
        group_member_values_for_inner(&inner, group_id)
    }

    pub fn group_members_for_group(
        &self,
        project_id: &str,
        company_id: &str,
        group_id: &str,
    ) -> ApiResult<Vec<Value>> {
        let inner = self.inner.lock().expect("store lock poisoned");
        inner
            .conversations
            .get(group_id)
            .filter(|conversation| {
                conversation.project_id == project_id
                    && conversation.company_id == company_id
                    && conversation.conversation_type == "group"
            })
            .ok_or_else(|| ApiError::NotFound(format!("group {group_id} not found")))?;
        Ok(group_member_values_for_inner(&inner, group_id))
    }

    pub fn message(&self, message_id: &str) -> ApiResult<Message> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .messages_by_id
            .get(message_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("message {message_id} not found")))
    }

    pub fn message_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        message_id: &str,
    ) -> ApiResult<Message> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .messages_by_id
            .get(message_id)
            .filter(|message| message.project_id == project_id && message.company_id == company_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("message {message_id} not found")))
    }

    pub fn mark_read(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
    ) -> ApiResult<crate::models::Conversation> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let conversation_id =
            resolve_conversation_id_inner(&inner, project_id, company_id, conversation_id);
        let conversation = inner
            .conversations
            .get_mut(&conversation_id)
            .filter(|conversation| {
                conversation_matches_tenant(conversation, project_id, company_id)
            })
            .ok_or_else(|| ApiError::NotFound("conversation not found".to_string()))?;
        conversation.unread_count = 0;
        conversation.updated_at = OffsetDateTime::now_utc();
        let updated = conversation.clone();
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "conversation.read",
                project_id,
                company_id,
                Some(updated.channel_account_id.clone()),
                Some(updated.id.clone()),
                None,
                Some(updated.last_seq),
                json!({ "unread_count": 0 }),
            ),
        );
        self.persist_locked(&inner);
        Ok(updated)
    }

    pub fn patch_conversation_metadata(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
        body: &Value,
    ) -> ApiResult<crate::models::Conversation> {
        let fields = body.as_object().ok_or_else(|| {
            ApiError::BadRequest("conversation patch body must be an object".to_string())
        })?;
        let allowed_fields = ["is_archived", "is_muted", "is_pinned", "control_mode"];
        for key in fields.keys() {
            if !allowed_fields.contains(&key.as_str()) {
                return Err(ApiError::BadRequest(format!(
                    "unsupported conversation patch field {key}"
                )));
            }
        }
        if fields.is_empty() {
            return Err(ApiError::BadRequest(
                "conversation patch body must include at least one supported field".to_string(),
            ));
        }

        let mut inner = self.inner.lock().expect("store lock poisoned");
        let conversation_id =
            resolve_conversation_id_inner(&inner, project_id, company_id, conversation_id);
        let conversation = inner
            .conversations
            .get_mut(&conversation_id)
            .filter(|conversation| {
                conversation_matches_tenant(conversation, project_id, company_id)
            })
            .ok_or_else(|| ApiError::NotFound("conversation not found".to_string()))?;
        if let Some(value) = fields.get("is_archived") {
            conversation.is_archived = value
                .as_bool()
                .ok_or_else(|| ApiError::BadRequest("is_archived must be a boolean".to_string()))?;
        }
        if let Some(value) = fields.get("is_muted") {
            conversation.is_muted = value
                .as_bool()
                .ok_or_else(|| ApiError::BadRequest("is_muted must be a boolean".to_string()))?;
        }
        if let Some(value) = fields.get("is_pinned") {
            conversation.is_pinned = value
                .as_bool()
                .ok_or_else(|| ApiError::BadRequest("is_pinned must be a boolean".to_string()))?;
        }
        if let Some(value) = fields.get("control_mode") {
            let control_mode = value
                .as_str()
                .ok_or_else(|| ApiError::BadRequest("control_mode must be a string".to_string()))?;
            if control_mode.trim().is_empty() {
                return Err(ApiError::BadRequest(
                    "control_mode must not be empty".to_string(),
                ));
            }
            conversation.control_mode = control_mode.to_string();
        }
        conversation.updated_at = OffsetDateTime::now_utc();
        let updated = conversation.clone();
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "conversation.updated",
                project_id,
                company_id,
                Some(updated.channel_account_id.clone()),
                Some(updated.id.clone()),
                None,
                Some(updated.last_seq),
                json!({
                    "conversation_id": updated.id.clone(),
                    "is_archived": updated.is_archived,
                    "is_muted": updated.is_muted,
                    "is_pinned": updated.is_pinned,
                    "control_mode": updated.control_mode.clone()
                }),
            ),
        );
        mark_dirty_inner(
            &mut inner,
            project_id,
            company_id,
            &updated.id,
            updated.last_seq,
            &updated.channel_account_id,
            Some(&self.events_tx),
            Some(&self.event_bus),
        );
        self.persist_locked(&inner);
        Ok(updated)
    }

    pub fn update_receipt(&self, message_id: &str, receipt_type: &str) -> ApiResult<Message> {
        self.update_receipt_with_context(message_id, receipt_type, None)
    }

    fn update_receipt_with_context(
        &self,
        message_id: &str,
        receipt_type: &str,
        created_at_wa: Option<OffsetDateTime>,
    ) -> ApiResult<Message> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let message = inner
            .messages_by_id
            .get_mut(message_id)
            .ok_or_else(|| ApiError::NotFound(format!("message {message_id} not found")))?;
        message.status = receipt_type.to_string();
        message.updated_at = OffsetDateTime::now_utc();
        let updated = message.clone();
        if let Some(messages) = inner
            .messages_by_conversation
            .get_mut(&updated.conversation_id)
            && let Some(existing) = messages.iter_mut().find(|message| message.id == updated.id)
        {
            *existing = updated.clone();
        }
        mark_dirty_inner(
            &mut inner,
            &updated.project_id,
            &updated.company_id,
            &updated.conversation_id,
            updated.conversation_seq,
            &updated.channel_account_id,
            Some(&self.events_tx),
            Some(&self.event_bus),
        );
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "message.receipt",
                &updated.project_id,
                &updated.company_id,
                Some(updated.channel_account_id.clone()),
                Some(updated.conversation_id.clone()),
                Some(updated.id.clone()),
                Some(updated.conversation_seq),
                json!({
                    "message_id": updated.id.clone(),
                    "wa_message_id": updated.wa_message_id.clone(),
                    "receipt_type": receipt_type,
                    "status": updated.status.clone(),
                    "delivery_state": updated.delivery_state(),
                    "created_at_wa": created_at_wa.map(ts)
                }),
            ),
        );
        self.persist_locked(&inner);
        Ok(updated)
    }

    pub fn update_receipt_by_wa_id(
        &self,
        wa_message_id: &str,
        receipt_type: &str,
    ) -> ApiResult<Message> {
        self.update_receipt_by_wa_id_with_context(wa_message_id, receipt_type, None)
    }

    fn update_receipt_by_wa_id_with_context(
        &self,
        wa_message_id: &str,
        receipt_type: &str,
        created_at_wa: Option<OffsetDateTime>,
    ) -> ApiResult<Message> {
        let internal_id = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .messages_by_id
            .values()
            .find(|message| message.wa_message_id.as_deref() == Some(wa_message_id))
            .map(|message| message.id.clone())
            .ok_or_else(|| ApiError::NotFound(format!("wa message {wa_message_id} not found")))?;
        self.update_receipt_with_context(&internal_id, receipt_type, created_at_wa)
    }

    pub fn set_message_flag(
        &self,
        message_id: &str,
        flag: &str,
        value: bool,
    ) -> ApiResult<Message> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let message = inner
            .messages_by_id
            .get_mut(message_id)
            .ok_or_else(|| ApiError::NotFound(format!("message {message_id} not found")))?;
        match flag {
            "pin" => message.is_pinned = value,
            "star" => message.is_starred = value,
            _ => {}
        }
        message.updated_at = OffsetDateTime::now_utc();
        let updated = message.clone();
        if let Some(messages) = inner
            .messages_by_conversation
            .get_mut(&updated.conversation_id)
            && let Some(existing) = messages.iter_mut().find(|message| message.id == updated.id)
        {
            *existing = updated.clone();
        }
        self.persist_locked(&inner);
        Ok(updated)
    }

    pub fn set_message_flag_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        message_id: &str,
        flag: &str,
        value: bool,
    ) -> ApiResult<Message> {
        self.message_for_company(project_id, company_id, message_id)?;
        self.set_message_flag(message_id, flag, value)
    }

    pub async fn react_to_message(
        &self,
        message_id: &str,
        emoji: Option<String>,
    ) -> ApiResult<Message> {
        let message = self.message(message_id)?;
        let wa_message_id = message.wa_message_id.clone().ok_or_else(|| {
            ApiError::BadRequest(format!(
                "message {message_id} does not have a WhatsApp id yet"
            ))
        })?;
        let reaction = emoji
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let send_result = tokio::time::timeout(
            StdDuration::from_secs(15),
            self.whatsapp.send_reaction(
                &message.channel_account_id,
                &message.conversation_id,
                &wa_message_id,
                message.direction == "outbound",
                message.sender_contact_id.as_deref(),
                reaction.as_deref(),
            ),
        )
        .await;
        match send_result {
            Ok(Ok(_)) => self.set_message_reaction(message_id, reaction),
            Ok(Err(err)) => Err(ApiError::ProviderError(err.to_string())),
            Err(_) => Err(ApiError::ProviderError(
                "WhatsApp reaction timed out".to_string(),
            )),
        }
    }

    pub async fn react_to_message_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        message_id: &str,
        emoji: Option<String>,
    ) -> ApiResult<Message> {
        self.message_for_company(project_id, company_id, message_id)?;
        self.react_to_message(message_id, emoji).await
    }

    fn set_message_reaction(
        &self,
        message_id: &str,
        reaction: Option<String>,
    ) -> ApiResult<Message> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let message = inner
            .messages_by_id
            .get_mut(message_id)
            .ok_or_else(|| ApiError::NotFound(format!("message {message_id} not found")))?;
        message.reaction = reaction;
        message.updated_at = OffsetDateTime::now_utc();
        let updated = message.clone();
        if let Some(messages) = inner
            .messages_by_conversation
            .get_mut(&updated.conversation_id)
            && let Some(existing) = messages.iter_mut().find(|message| message.id == updated.id)
        {
            *existing = updated.clone();
        }
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "message.reaction",
                &updated.project_id,
                &updated.company_id,
                Some(updated.channel_account_id.clone()),
                Some(updated.conversation_id.clone()),
                Some(updated.id.clone()),
                Some(updated.conversation_seq),
                json!({ "reaction": updated.reaction }),
            ),
        );
        self.persist_locked(&inner);
        Ok(updated)
    }

    pub fn media(&self, media_id: &str) -> ApiResult<MediaObject> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .media
            .get(media_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("media {media_id} not found")))
    }

    pub fn media_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        media_id: &str,
    ) -> ApiResult<MediaObject> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .media
            .get(media_id)
            .filter(|media| media.project_id == project_id && media.company_id == company_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("media {media_id} not found")))
    }

    async fn download_and_store_inbound_media(
        &self,
        runtime: &ChannelRuntime,
        message: &Message,
        conversation_id: &str,
        descriptor: InboundMediaDescriptor,
    ) -> ApiResult<MediaObject> {
        let limits = MediaLimits {
            quick_delete_threshold_mb: self.config.media_quick_delete_threshold_mb,
            reject_threshold_mb: self.config.media_reject_threshold_mb,
        };
        let media_id = format!("media_{}", Uuid::now_v7().simple());
        if limits.classify(descriptor.file_length) == MediaDecision::Rejected {
            return self.record_inbound_media_object(InboundMediaRecordInput {
                runtime,
                message,
                conversation_id,
                descriptor: &descriptor,
                media_id,
                size_bytes: descriptor.file_length,
                sha256: descriptor_sha256_hex(&descriptor),
                storage_status: "rejected",
                object_key: None,
            });
        }

        let writer = Cursor::new(Vec::with_capacity(
            usize::try_from(descriptor.file_length)
                .unwrap_or(0)
                .min(8 * 1024 * 1024),
        ));
        let writer = self
            .whatsapp
            .download_inbound_media_to_writer(&runtime.channel_id, &descriptor, writer)
            .await
            .map_err(|err| {
                ApiError::Internal(format!("failed to download WhatsApp media: {err}"))
            })?;
        let bytes = writer.into_inner();
        self.store_downloaded_inbound_media(runtime, message, conversation_id, descriptor, bytes)
    }

    fn store_downloaded_inbound_media(
        &self,
        runtime: &ChannelRuntime,
        message: &Message,
        conversation_id: &str,
        descriptor: InboundMediaDescriptor,
        bytes: Vec<u8>,
    ) -> ApiResult<MediaObject> {
        if bytes.is_empty() {
            return Err(ApiError::BadRequest(
                "downloaded WhatsApp media is empty".to_string(),
            ));
        }
        let size_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let limits = MediaLimits {
            quick_delete_threshold_mb: self.config.media_quick_delete_threshold_mb,
            reject_threshold_mb: self.config.media_reject_threshold_mb,
        };
        let decision = limits.classify(size_bytes);
        if decision == MediaDecision::Rejected {
            return self.record_inbound_media_object(InboundMediaRecordInput {
                runtime,
                message,
                conversation_id,
                descriptor: &descriptor,
                media_id: format!("media_{}", Uuid::now_v7().simple()),
                size_bytes,
                sha256: hex::encode(Sha256::digest(&bytes)),
                storage_status: "rejected",
                object_key: None,
            });
        }

        let media_id = format!("media_{}", Uuid::now_v7().simple());
        let staging_path = self.stage_media_bytes(&media_id, &bytes)?;
        if self.config.media_sniff_magic_bytes
            && !magic_matches_mime(Some(&descriptor.mime_type), &bytes)
        {
            let _ = std::fs::remove_file(&staging_path);
            return Err(ApiError::BadRequest(format!(
                "downloaded WhatsApp media magic bytes do not match declared MIME type {}",
                descriptor.mime_type
            )));
        }

        let storage_status = match decision {
            MediaDecision::Temp => "temp",
            MediaDecision::Quarantine => "quarantine",
            MediaDecision::Rejected => unreachable!("rejected media returned earlier"),
        };
        let ext = descriptor
            .filename
            .as_deref()
            .and_then(file_extension)
            .unwrap_or_else(|| extension_for_mime(&descriptor.mime_type));
        let object_key = Some(r2_object_key(R2ObjectKeyInput {
            base_prefix: &self.config.r2.base_prefix,
            class: storage_status,
            project_id: &runtime.project_id,
            company_id: &runtime.company_id,
            channel_id: &runtime.channel_id,
            conversation_id: Some(conversation_id),
            entity_type: None,
            entity_id: None,
            date: OffsetDateTime::now_utc().date(),
            media_id: &media_id,
            ext,
        }));
        if let Some(object_key) = object_key.as_deref()
            && let Err(err) =
                self.media_bytes
                    .put_blocking(object_key, &descriptor.mime_type, bytes.clone())
        {
            let _ = std::fs::remove_file(&staging_path);
            let _ = self.media_bytes.delete_blocking(object_key);
            self.record_media_storage_failed_event(
                runtime,
                message,
                conversation_id,
                &media_id,
                "byte_store_put_failed",
                &err,
            );
            return Err(ApiError::ProviderError(err));
        }
        let media = self.record_inbound_media_object(InboundMediaRecordInput {
            runtime,
            message,
            conversation_id,
            descriptor: &descriptor,
            media_id,
            size_bytes,
            sha256: hex::encode(Sha256::digest(&bytes)),
            storage_status,
            object_key: object_key.clone(),
        });
        let _ = std::fs::remove_file(&staging_path);
        media
    }

    fn record_media_storage_failed_event(
        &self,
        runtime: &ChannelRuntime,
        message: &Message,
        conversation_id: &str,
        media_id: &str,
        code: &str,
        error: &str,
    ) {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "media.storage_failed",
                &runtime.project_id,
                &runtime.company_id,
                Some(runtime.channel_id.clone()),
                Some(conversation_id.to_string()),
                Some(message.id.clone()),
                Some(message.conversation_seq),
                json!({
                    "message_id": message.id.clone(),
                    "media_id": media_id,
                    "code": code,
                    "error": error
                }),
            ),
        );
        self.persist_locked(&inner);
    }

    fn record_inbound_media_object(
        &self,
        input: InboundMediaRecordInput<'_>,
    ) -> ApiResult<MediaObject> {
        let media_type =
            normalize_inbound_media_type(&input.descriptor.media_type, &input.descriptor.mime_type);
        let mime_type = input.descriptor.mime_type.trim().to_string();
        let now = OffsetDateTime::now_utc();
        let public_object_url = input
            .object_key
            .as_deref()
            .and_then(|object_key| self.config.public_object_url(object_key));
        let local_url = (self.config.dev_mode && input.storage_status != "rejected")
            .then(|| self.config.dev_media_url(&input.media_id));
        let public_url = public_object_url.or(local_url);
        let thumbnail_url = (media_type == "image")
            .then(|| public_url.clone())
            .flatten();
        let media = MediaObject {
            id: input.media_id.clone(),
            project_id: input.runtime.project_id.clone(),
            company_id: input.runtime.company_id.clone(),
            conversation_id: input.conversation_id.to_string(),
            message_id: Some(input.message.id.clone()),
            media_type: media_type.clone(),
            mime_type: mime_type.clone(),
            original_filename: input.descriptor.filename.clone(),
            size_bytes: input.size_bytes,
            sha256: input.sha256,
            storage_status: input.storage_status.to_string(),
            bucket: input.object_key.as_ref().map(|_| self.media_bucket_name()),
            object_key: input.object_key.clone(),
            permanent_object_key: None,
            public_url,
            thumbnail_url,
            width: input.descriptor.width,
            height: input.descriptor.height,
            duration_seconds: input.descriptor.duration_seconds,
            expires_at: (input.storage_status != "rejected")
                .then(|| now + Duration::days(self.config.media_temp_retention_days.into())),
            saved_at: None,
            created_at: now,
            updated_at: now,
        };
        let mut inner = self.inner.lock().expect("store lock poisoned");
        inner.media.insert(media.id.clone(), media.clone());
        let message = attach_media_to_message_inner(&mut inner, &input.message.id, &media)
            .unwrap_or_else(|| input.message.clone());
        if media.storage_status != "rejected" {
            push_event_inner(
                &mut inner,
                Some(&self.events_tx),
                Some(&self.event_bus),
                new_event(
                    "media.stored",
                    &input.runtime.project_id,
                    &input.runtime.company_id,
                    Some(input.runtime.channel_id.clone()),
                    Some(input.conversation_id.to_string()),
                    Some(message.id.clone()),
                    Some(message.conversation_seq),
                    json!({
                        "message_id": message.id.clone(),
                        "media_id": media.id.clone(),
                        "media_type": media.media_type.clone(),
                        "mime_type": media.mime_type.clone(),
                        "storage_status": media.storage_status.clone(),
                        "size_bytes": media.size_bytes
                    }),
                ),
            );
        }
        if media.media_type == "audio" && media.storage_status != "rejected" {
            let transcript = transcript_with_lifecycle(
                &input.runtime.project_id,
                &input.runtime.company_id,
                &message.id,
                Some(media.id.clone()),
                TranscriptLifecycle::Pending,
                None,
            );
            inner
                .transcripts
                .insert(message.id.clone(), transcript.clone());
            push_event_inner(
                &mut inner,
                Some(&self.events_tx),
                Some(&self.event_bus),
                new_event(
                    "audio.transcription.requested",
                    &input.runtime.project_id,
                    &input.runtime.company_id,
                    Some(input.runtime.channel_id.clone()),
                    Some(input.conversation_id.to_string()),
                    Some(message.id.clone()),
                    Some(message.conversation_seq),
                    json!({
                        "message_id": message.id.clone(),
                        "media_id": media.id.clone(),
                        "transcript_id": transcript.id.clone()
                    }),
                ),
            );
        }
        self.persist_locked(&inner);
        Ok(media)
    }

    pub async fn media_blob(&self, media_id: &str) -> ApiResult<Option<(MediaObject, Vec<u8>)>> {
        let media = self.media(media_id)?;
        let object_key = media
            .object_key
            .as_deref()
            .or(media.permanent_object_key.as_deref());
        let Some(object_key) = object_key else {
            return Ok(None);
        };
        match self.media_bytes.get(object_key).await {
            Ok(bytes) => Ok(Some((media, bytes))),
            Err(err)
                if err.contains("No such file")
                    || err.contains("not found")
                    || err.contains("os error 2") =>
            {
                Ok(None)
            }
            Err(err) => Err(ApiError::ProviderError(err)),
        }
    }

    pub async fn copy_media_bytes(
        &self,
        source_object_key: &str,
        destination_object_key: &str,
        content_type: &str,
    ) -> ApiResult<()> {
        self.media_bytes
            .copy(source_object_key, destination_object_key, content_type)
            .await
            .map_err(ApiError::ProviderError)
    }

    pub async fn delete_media_bytes(&self, object_key: &str) -> ApiResult<()> {
        self.media_bytes
            .delete(object_key)
            .await
            .map_err(ApiError::ProviderError)
    }

    pub async fn upload_outbound_media(
        &self,
        project_id: &str,
        company_id: &str,
        upload: OutboundMediaUpload,
    ) -> ApiResult<MediaObject> {
        if upload.bytes.is_empty() {
            return Err(ApiError::BadRequest("upload file is empty".to_string()));
        }
        let conversation_id = upload.conversation_id.trim();
        if conversation_id.is_empty() {
            return Err(ApiError::BadRequest(
                "conversation_id is required".to_string(),
            ));
        }
        let conversation_id = {
            let inner = self.inner.lock().expect("store lock poisoned");
            resolve_conversation_id_inner(&inner, project_id, company_id, conversation_id)
        };
        let conversation_id = conversation_id.as_str();
        let size_bytes = u64::try_from(upload.bytes.len()).unwrap_or(u64::MAX);
        let limits = MediaLimits {
            quick_delete_threshold_mb: self.config.media_quick_delete_threshold_mb,
            reject_threshold_mb: self.config.media_reject_threshold_mb,
        };
        let decision = limits.classify(size_bytes);
        if decision == MediaDecision::Rejected {
            return Err(ApiError::PayloadTooLarge(format!(
                "media upload is larger than {} MB",
                self.config.media_reject_threshold_mb
            )));
        }
        let mime_type = upload
            .mime_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("application/octet-stream")
            .to_string();
        let media_type = upload
            .media_type
            .as_deref()
            .map(normalize_media_type)
            .transpose()?
            .unwrap_or_else(|| infer_media_type(&mime_type));
        let now = OffsetDateTime::now_utc();
        let media_id = format!("media_{}", Uuid::now_v7().simple());
        let staging_path = self.stage_media_bytes(&media_id, &upload.bytes)?;
        if self.config.media_sniff_magic_bytes
            && !magic_matches_mime(Some(&mime_type), &upload.bytes)
        {
            let _ = std::fs::remove_file(&staging_path);
            return Err(ApiError::BadRequest(format!(
                "upload magic bytes do not match declared MIME type {mime_type}"
            )));
        }
        let channel_id = {
            let inner = self.inner.lock().expect("store lock poisoned");
            inner
                .conversations
                .get(conversation_id)
                .map(|conversation| conversation.channel_account_id.clone())
                .or_else(|| connected_channel_for_project_company(&inner, project_id, company_id))
                .unwrap_or_else(|| "channel_dev".to_string())
        };
        let storage_class = match decision {
            MediaDecision::Temp => "outbound-temp",
            MediaDecision::Quarantine => "quarantine",
            MediaDecision::Rejected => unreachable!("rejected uploads returned earlier"),
        };
        let filename = upload
            .filename
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let ext = filename
            .as_deref()
            .and_then(file_extension)
            .unwrap_or_else(|| extension_for_mime(&mime_type));
        let object_key = r2_object_key(R2ObjectKeyInput {
            base_prefix: &self.config.r2.base_prefix,
            class: storage_class,
            project_id,
            company_id,
            channel_id: &channel_id,
            conversation_id: Some(conversation_id),
            entity_type: None,
            entity_id: None,
            date: now.date(),
            media_id: &media_id,
            ext,
        });
        let dev_url = self
            .config
            .dev_mode
            .then(|| self.config.dev_media_url(&media_id));
        let public_url = dev_url.clone();
        let thumbnail_url = (media_type == "image").then(|| dev_url.clone()).flatten();
        let media = MediaObject {
            id: media_id.clone(),
            project_id: project_id.to_string(),
            company_id: company_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: None,
            media_type,
            mime_type: mime_type.clone(),
            original_filename: filename,
            size_bytes,
            sha256: hex::encode(Sha256::digest(&upload.bytes)),
            storage_status: storage_class.to_string(),
            bucket: Some(self.media_bucket_name()),
            object_key: Some(object_key.clone()),
            permanent_object_key: None,
            public_url,
            thumbnail_url,
            width: None,
            height: None,
            duration_seconds: None,
            expires_at: Some(now + Duration::days(self.config.media_temp_retention_days.into())),
            saved_at: None,
            created_at: now,
            updated_at: now,
        };
        if let Err(err) = self
            .media_bytes
            .put(&object_key, &mime_type, upload.bytes)
            .await
        {
            let _ = std::fs::remove_file(&staging_path);
            let _ = self.media_bytes.delete(&object_key).await;
            return Err(ApiError::ProviderError(err));
        }
        let mut inner = self.inner.lock().expect("store lock poisoned");
        ensure_conversation_inner(
            &mut inner,
            project_id,
            company_id,
            conversation_id,
            &channel_id,
            "manual",
        );
        inner.media.insert(media_id.clone(), media.clone());
        self.persist_locked(&inner);
        let _ = std::fs::remove_file(staging_path);
        Ok(media)
    }

    fn stage_media_bytes(&self, media_id: &str, bytes: &[u8]) -> ApiResult<PathBuf> {
        let preferred_dir = self.config.media_local_temp_dir.join("staging");
        let staging_dir = if let Err(err) = std::fs::create_dir_all(&preferred_dir) {
            #[cfg(test)]
            {
                let fallback_dir = std::env::temp_dir()
                    .join("rustzap-test-media")
                    .join("staging");
                std::fs::create_dir_all(&fallback_dir).map_err(|fallback_err| {
                    ApiError::Internal(format!(
                        "failed to create media staging directory {} ({err}); fallback {} failed: {fallback_err}",
                        preferred_dir.display(),
                        fallback_dir.display()
                    ))
                })?;
                fallback_dir
            }
            #[cfg(not(test))]
            {
                return Err(ApiError::Internal(format!(
                    "failed to create media staging directory {}: {err}",
                    preferred_dir.display()
                )));
            }
        } else {
            preferred_dir
        };
        std::fs::create_dir_all(&staging_dir).map_err(|err| {
            ApiError::Internal(format!(
                "failed to create media staging directory {}: {err}",
                staging_dir.display()
            ))
        })?;
        let staging_path = staging_dir.join(format!("{media_id}.upload"));
        std::fs::write(&staging_path, bytes).map_err(|err| {
            ApiError::Internal(format!(
                "failed to write media staging file {}: {err}",
                staging_path.display()
            ))
        })?;
        let written = std::fs::metadata(&staging_path)
            .map(|metadata| metadata.len())
            .map_err(|err| {
                ApiError::Internal(format!(
                    "failed to stat media staging file {}: {err}",
                    staging_path.display()
                ))
            })?;
        if written != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
            let _ = std::fs::remove_file(&staging_path);
            return Err(ApiError::Internal(
                "staged media size does not match upload size".to_string(),
            ));
        }
        Ok(staging_path)
    }

    pub fn media_for_conversation(&self, conversation_id: &str) -> Vec<MediaObject> {
        let mut media: Vec<_> = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .media
            .values()
            .filter(|media| media.conversation_id == conversation_id)
            .cloned()
            .collect();
        media.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        media
    }

    pub fn media_for_project_conversation(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
    ) -> ApiResult<Vec<MediaObject>> {
        let conversation = self.conversation(project_id, company_id, conversation_id)?;
        let public_id = public_conversation_id(&conversation);
        let mut media = self.media_for_conversation(&conversation.id);
        for item in &mut media {
            item.conversation_id = public_id.clone();
        }
        Ok(media)
    }

    pub fn media_for_group(
        &self,
        project_id: &str,
        company_id: &str,
        group_id: &str,
    ) -> ApiResult<Vec<MediaObject>> {
        self.group(project_id, company_id, group_id)?;
        let mut media: Vec<_> = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .media
            .values()
            .filter(|media| {
                media.project_id == project_id
                    && media.company_id == company_id
                    && media.conversation_id == group_id
            })
            .cloned()
            .collect();
        media.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(media)
    }

    pub fn starred_messages_for_conversation(&self, conversation_id: &str) -> Vec<Message> {
        let mut messages: Vec<_> = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .messages_by_conversation
            .get(conversation_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|message| message.is_starred)
            .collect();
        messages.sort_by_key(|message| message.conversation_seq);
        messages
    }

    pub fn starred_messages_for_project_conversation(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
    ) -> ApiResult<Vec<Message>> {
        let conversation = self.conversation(project_id, company_id, conversation_id)?;
        let public_id = public_conversation_id(&conversation);
        let mut messages = self.starred_messages_for_conversation(&conversation.id);
        for message in &mut messages {
            message.conversation_id = public_id.clone();
        }
        Ok(messages)
    }

    pub fn starred_messages_for_group(
        &self,
        project_id: &str,
        company_id: &str,
        group_id: &str,
    ) -> ApiResult<Vec<Message>> {
        self.group(project_id, company_id, group_id)?;
        Ok(self.starred_messages_for_conversation(group_id))
    }

    pub fn search_messages_for_conversation(
        &self,
        conversation_id: &str,
        query: &str,
        limit: usize,
    ) -> Vec<Message> {
        let needle = query.trim().to_lowercase();
        let mut messages: Vec<_> = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .messages_by_conversation
            .get(conversation_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|message| {
                needle.is_empty()
                    || message
                        .text
                        .as_deref()
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains(&needle)
            })
            .take(limit)
            .collect();
        messages.sort_by_key(|message| message.conversation_seq);
        messages
    }

    pub fn search_messages_for_project_conversation(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
        query: &str,
        limit: usize,
    ) -> ApiResult<Vec<Message>> {
        let conversation = self.conversation(project_id, company_id, conversation_id)?;
        let public_id = public_conversation_id(&conversation);
        let mut messages = self.search_messages_for_conversation(&conversation.id, query, limit);
        for message in &mut messages {
            message.conversation_id = public_id.clone();
        }
        Ok(messages)
    }

    pub fn search_messages_for_group(
        &self,
        project_id: &str,
        company_id: &str,
        group_id: &str,
        query: &str,
        limit: usize,
    ) -> ApiResult<Vec<Message>> {
        self.group(project_id, company_id, group_id)?;
        Ok(self.search_messages_for_conversation(group_id, query, limit))
    }

    pub fn save_media(
        &self,
        media_id: &str,
        entity_type: &str,
        entity_id: &str,
    ) -> ApiResult<MediaObject> {
        self.save_media_with_permanent_object_key(media_id, entity_type, entity_id, None)
    }

    fn save_media_with_permanent_object_key(
        &self,
        media_id: &str,
        entity_type: &str,
        entity_id: &str,
        permanent_object_key: Option<String>,
    ) -> ApiResult<MediaObject> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let media = inner
            .media
            .get_mut(media_id)
            .ok_or_else(|| ApiError::NotFound(format!("media {media_id} not found")))?;
        let now = OffsetDateTime::now_utc();
        media.storage_status = "permanent".to_string();
        let object_key = permanent_object_key.unwrap_or_else(|| {
            r2_object_key(R2ObjectKeyInput {
                base_prefix: &self.config.r2.base_prefix,
                class: "permanent",
                project_id: &media.project_id,
                company_id: &media.company_id,
                channel_id: "channel_dev",
                conversation_id: Some(&media.conversation_id),
                entity_type: Some(entity_type),
                entity_id: Some(entity_id),
                date: now.date(),
                media_id,
                ext: "bin",
            })
        });
        media.permanent_object_key = Some(object_key.clone());
        media.public_url = self.config.public_object_url(&object_key);
        if media.media_type == "image" {
            media.thumbnail_url = media.public_url.clone().or_else(|| {
                self.config
                    .dev_mode
                    .then(|| self.config.dev_media_url(media_id))
            });
        }
        media.saved_at = Some(now);
        media.updated_at = now;
        let updated = media.clone();
        self.persist_locked(&inner);
        Ok(updated)
    }

    pub fn save_media_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        media_id: &str,
        entity_type: &str,
        entity_id: &str,
    ) -> ApiResult<MediaObject> {
        self.media_for_company(project_id, company_id, media_id)?;
        self.save_media(media_id, entity_type, entity_id)
    }

    pub fn save_media_with_permanent_object_key_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        media_id: &str,
        entity_type: &str,
        entity_id: &str,
        permanent_object_key: String,
    ) -> ApiResult<MediaObject> {
        self.media_for_company(project_id, company_id, media_id)?;
        self.save_media_with_permanent_object_key(
            media_id,
            entity_type,
            entity_id,
            Some(permanent_object_key),
        )
    }

    pub fn delete_media_for_company(
        &self,
        project_id: &str,
        company_id: &str,
        media_id: &str,
    ) -> ApiResult<MediaObject> {
        self.media_for_company(project_id, company_id, media_id)?;
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let media = inner
            .media
            .get_mut(media_id)
            .ok_or_else(|| ApiError::NotFound(format!("media {media_id} not found")))?;
        media.storage_status = "deleted".to_string();
        media.public_url = None;
        media.thumbnail_url = None;
        media.expires_at = None;
        media.updated_at = OffsetDateTime::now_utc();
        let updated = media.clone();
        self.persist_locked(&inner);
        Ok(updated)
    }

    pub fn cleanup_expired_temp_media(&self) -> usize {
        let now = OffsetDateTime::now_utc();
        let inner = self.inner.lock().expect("store lock poisoned");
        let expired_media: Vec<_> = inner
            .media
            .iter()
            .filter(|(_, media)| {
                matches!(
                    media.storage_status.as_str(),
                    "temp" | "quarantine" | "outbound-temp"
                ) && media.expires_at.is_some_and(|expires_at| expires_at <= now)
            })
            .map(|(_, media)| media.clone())
            .collect();
        drop(inner);
        let deleted_media: Vec<_> = expired_media
            .into_iter()
            .filter(|media| self.delete_media_object_bytes_best_effort(media))
            .collect();
        let expired_ids: HashSet<_> = deleted_media.iter().map(|media| media.id.clone()).collect();
        let mut inner = self.inner.lock().expect("store lock poisoned");
        for media_id in &expired_ids {
            inner.media.remove(media_id);
        }
        if !expired_ids.is_empty() {
            self.persist_locked(&inner);
        }
        expired_ids.len()
    }

    pub fn apply_retention_once(&self) -> RetentionSummary {
        let now = OffsetDateTime::now_utc();
        let companies: Vec<_> = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .companies
            .iter()
            .map(|((project_id, company_id), value)| {
                (
                    project_id.clone(),
                    company_id.clone(),
                    company_privacy_policy(value),
                )
            })
            .collect();

        let mut summary = RetentionSummary::default();
        for (project_id, company_id, policy) in companies {
            let company_summary =
                self.apply_company_retention(&project_id, &company_id, &policy, now);
            if company_summary.messages_removed
                + company_summary.transcripts_removed
                + company_summary.transcripts_redacted
                + company_summary.media_removed
                > 0
            {
                self.audit_log(
                    &project_id,
                    &company_id,
                    "retention.apply",
                    "company",
                    Some(&company_id),
                    json!({
                        "messages_removed": company_summary.messages_removed,
                        "transcripts_removed": company_summary.transcripts_removed,
                        "transcripts_redacted": company_summary.transcripts_redacted,
                        "media_removed": company_summary.media_removed
                    }),
                );
            }
            summary.messages_removed += company_summary.messages_removed;
            summary.transcripts_removed += company_summary.transcripts_removed;
            summary.transcripts_redacted += company_summary.transcripts_redacted;
            summary.media_removed += company_summary.media_removed;
        }
        summary.media_removed += self.cleanup_expired_temp_media();
        summary
    }

    fn apply_company_retention(
        &self,
        project_id: &str,
        company_id: &str,
        policy: &CompanyPrivacyPolicy,
        now: OffsetDateTime,
    ) -> RetentionSummary {
        let message_cutoff = retention_cutoff(now, policy.message_retention_days);
        let transcript_cutoff = retention_cutoff(now, policy.transcript_retention_days);
        let (candidate_message_ids, candidate_media) = {
            let inner = self.inner.lock().expect("store lock poisoned");
            let mut message_ids = HashSet::new();
            if let Some(cutoff) = message_cutoff {
                for (message_id, message) in &inner.messages_by_id {
                    if message.project_id == project_id
                        && message.company_id == company_id
                        && message.created_at <= cutoff
                    {
                        message_ids.insert(message_id.clone());
                    }
                }
            }
            let media_temp_cutoff = retention_cutoff(now, policy.media_temp_retention_days);
            let media: Vec<MediaObject> = inner
                .media
                .values()
                .filter(|media| {
                    let message_expired = media
                        .message_id
                        .as_deref()
                        .is_some_and(|message_id| message_ids.contains(message_id));
                    let temp_expired = matches!(
                        media.storage_status.as_str(),
                        "temp" | "quarantine" | "outbound-temp"
                    ) && media.project_id == project_id
                        && media.company_id == company_id
                        && media_temp_cutoff.is_some_and(|cutoff| media.created_at <= cutoff);
                    message_expired || temp_expired
                })
                .cloned()
                .collect();
            (message_ids, media)
        };

        let mut deleted_media = Vec::new();
        let mut failed_message_ids = HashSet::new();
        for media in candidate_media {
            if self.delete_media_object_bytes_best_effort(&media) {
                deleted_media.push(media);
            } else if let Some(message_id) = media.message_id.as_ref() {
                failed_message_ids.insert(message_id.clone());
            }
        }
        let removable_message_ids: HashSet<_> = candidate_message_ids
            .difference(&failed_message_ids)
            .cloned()
            .collect();
        let removable_media_ids: HashSet<_> = deleted_media
            .iter()
            .filter(|media| {
                media
                    .message_id
                    .as_ref()
                    .is_none_or(|message_id| !failed_message_ids.contains(message_id))
            })
            .map(|media| media.id.clone())
            .collect();

        let mut summary = RetentionSummary {
            messages_removed: removable_message_ids.len(),
            media_removed: removable_media_ids.len(),
            ..RetentionSummary::default()
        };
        if !removable_message_ids.is_empty() || !removable_media_ids.is_empty() {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            inner
                .messages_by_id
                .retain(|message_id, _| !removable_message_ids.contains(message_id));
            for messages in inner.messages_by_conversation.values_mut() {
                messages.retain(|message| !removable_message_ids.contains(&message.id));
            }
            inner
                .transcripts
                .retain(|message_id, _| !removable_message_ids.contains(message_id));
            inner
                .media
                .retain(|media_id, _| !removable_media_ids.contains(media_id));
            self.persist_locked(&inner);
        }

        if let Some(cutoff) = transcript_cutoff {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            let before = inner.transcripts.len();
            inner.transcripts.retain(|_, transcript| {
                !(transcript.project_id == project_id
                    && transcript.company_id == company_id
                    && transcript.updated_at <= cutoff)
            });
            let removed = before.saturating_sub(inner.transcripts.len());
            if removed > 0 {
                summary.transcripts_removed += removed;
                self.persist_locked(&inner);
            }
        }

        if !policy.allow_transcript_storage {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            for transcript in inner.transcripts.values_mut() {
                if transcript.project_id == project_id
                    && transcript.company_id == company_id
                    && (transcript.text.is_some() || !transcript.raw_response_json.is_null())
                {
                    transcript.text = None;
                    transcript.raw_response_json = json!({"redacted": true});
                    transcript.updated_at = now;
                    summary.transcripts_redacted += 1;
                }
            }
            if summary.transcripts_redacted > 0 {
                self.persist_locked(&inner);
            }
        }
        summary
    }

    fn delete_media_object_bytes_best_effort(&self, media: &MediaObject) -> bool {
        let mut ok = true;
        if let Some(object_key) = media.object_key.as_deref()
            && let Err(err) = self.media_bytes.delete_blocking(object_key)
        {
            tracing::warn!(media_id = %media.id, object_key, error = %err, "failed to delete media object bytes");
            ok = false;
        }
        if let Some(object_key) = media.permanent_object_key.as_deref()
            && media.object_key.as_deref() != Some(object_key)
            && let Err(err) = self.media_bytes.delete_blocking(object_key)
        {
            tracing::warn!(media_id = %media.id, object_key, error = %err, "failed to delete permanent media object bytes");
            ok = false;
        }
        ok
    }

    pub fn cleanup_staging_files(&self) -> usize {
        let staging_dir = self.config.media_local_temp_dir.join("staging");
        let Ok(entries) = std::fs::read_dir(staging_dir) else {
            return 0;
        };
        let mut removed = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && std::fs::remove_file(path).is_ok() {
                removed += 1;
            }
        }
        removed
    }

    pub fn transcript(&self, message_id: &str) -> ApiResult<Transcript> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .transcripts
            .get(message_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("transcript for {message_id} not found")))
    }

    pub fn create_transcript(
        &self,
        project_id: &str,
        company_id: &str,
        message_id: &str,
    ) -> Transcript {
        let transcript = mock_transcript(project_id, company_id, message_id, None);
        {
            let mut inner = self.inner.lock().expect("store lock poisoned");
            inner
                .transcripts
                .insert(message_id.to_string(), transcript.clone());
            self.persist_locked(&inner);
        }
        transcript
    }

    pub fn request_transcript(
        &self,
        project_id: &str,
        company_id: &str,
        message_id: &str,
    ) -> ApiResult<Transcript> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let message = inner
            .messages_by_id
            .get(message_id)
            .filter(|message| message.project_id == project_id && message.company_id == company_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("message {message_id} not found")))?;
        let Some(media_id) = message.media_id.clone() else {
            return Err(ApiError::BadRequest(format!(
                "message {message_id} has no media to transcribe"
            )));
        };
        let media = inner
            .media
            .get(&media_id)
            .filter(|media| media.project_id == project_id && media.company_id == company_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("media {media_id} not found")))?;
        if !media.mime_type.starts_with("audio/") {
            return Err(ApiError::BadRequest(format!(
                "media {} is not an audio object",
                media.id
            )));
        }
        if let Some(transcript) = inner.transcripts.get(message_id).filter(|transcript| {
            matches!(
                transcript.status.as_str(),
                "pending"
                    | "processing"
                    | "completed"
                    | "skipped_size_limit"
                    | "skipped_unsupported_type"
            )
        }) {
            return Ok(transcript.clone());
        }
        let transcript = inner
            .transcripts
            .get(message_id)
            .filter(|transcript| {
                matches!(
                    transcript.status.as_str(),
                    "pending" | "processing" | "completed"
                )
            })
            .cloned()
            .unwrap_or_else(|| {
                transcript_with_lifecycle(
                    project_id,
                    company_id,
                    message_id,
                    Some(media.id.clone()),
                    TranscriptLifecycle::Pending,
                    None,
                )
            });
        inner
            .transcripts
            .insert(message_id.to_string(), transcript.clone());
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "audio.transcription.requested",
                project_id,
                company_id,
                Some(message.channel_account_id.clone()),
                Some(message.conversation_id.clone()),
                Some(message.id.clone()),
                Some(message.conversation_seq),
                json!({
                    "message_id": message.id.clone(),
                    "media_id": media.id.clone(),
                    "transcript_id": transcript.id.clone()
                }),
            ),
        );
        self.persist_locked(&inner);
        Ok(transcript)
    }

    pub async fn process_outbound_send_request(
        &self,
        event: &crate::models::CommonEvent,
    ) -> ApiResult<Message> {
        let message_id = event
            .payload
            .get("message_id")
            .and_then(Value::as_str)
            .or(event.message_id.as_deref())
            .ok_or_else(|| {
                ApiError::BadRequest("outbound send event missing message_id".to_string())
            })?;
        let message = self.message_for_company(&event.project_id, &event.company_id, message_id)?;
        if matches!(message.status.as_str(), "sent_to_whatsapp" | "failed") {
            return Ok(message);
        }
        let text = event
            .payload
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| message.text.clone());
        self.dispatch_prepared_message(
            &SendMessageOutcome {
                message,
                should_dispatch: true,
            },
            text.as_deref(),
        )
        .await
    }

    pub async fn process_transcription_request(
        &self,
        event: &crate::models::CommonEvent,
    ) -> ApiResult<Transcript> {
        self.metrics
            .stt_requests_total
            .fetch_add(1, Ordering::Relaxed);
        let message_id = event
            .payload
            .get("message_id")
            .and_then(Value::as_str)
            .or(event.message_id.as_deref())
            .ok_or_else(|| {
                ApiError::BadRequest("transcription event missing message_id".to_string())
            })?;
        let message = self.message_for_company(&event.project_id, &event.company_id, message_id)?;
        if let Some(transcript) = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .transcripts
            .get(message_id)
            .filter(|transcript| {
                matches!(
                    transcript.status.as_str(),
                    "completed" | "skipped_size_limit" | "skipped_unsupported_type"
                )
            })
            .cloned()
        {
            return Ok(transcript);
        }
        let media_id = event
            .payload
            .get("media_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| message.media_id.clone())
            .ok_or_else(|| {
                ApiError::BadRequest(format!("message {message_id} has no media to transcribe"))
            })?;
        let media = self.media_for_company(&event.project_id, &event.company_id, &media_id)?;
        if !media.mime_type.starts_with("audio/") {
            return self.set_transcript_lifecycle(
                &message,
                &media,
                TranscriptLifecycle::SkippedUnsupportedType,
                None,
            );
        }
        let max_bytes = self
            .config
            .groq
            .stt_max_audio_mb
            .saturating_mul(1024 * 1024);
        if media.size_bytes > max_bytes {
            return self.set_transcript_lifecycle(
                &message,
                &media,
                TranscriptLifecycle::SkippedSizeLimit,
                None,
            );
        }

        self.set_transcript_lifecycle(&message, &media, TranscriptLifecycle::Processing, None)?;
        let bytes = match self.media_blob(&media_id).await {
            Ok(Some((_media, bytes))) => bytes,
            Ok(None) => {
                return self.set_transcript_lifecycle(
                    &message,
                    &media,
                    TranscriptLifecycle::Failed(format!(
                        "media {media_id} has no stored bytes available for STT"
                    )),
                    None,
                );
            }
            Err(err) => {
                return self.set_transcript_lifecycle(
                    &message,
                    &media,
                    TranscriptLifecycle::Failed(err.to_string()),
                    None,
                );
            }
        };
        let filename = media
            .original_filename
            .as_deref()
            .unwrap_or("audio.ogg")
            .to_string();
        let client = GroqSttClient::new(self.config.groq.clone());
        match client
            .transcribe_bytes(&filename, &media.mime_type, bytes)
            .await
        {
            Ok(result) => self.set_completed_groq_transcript(&message, &media, result),
            Err(error) => self.set_transcript_lifecycle(
                &message,
                &media,
                TranscriptLifecycle::Failed(error),
                None,
            ),
        }
    }

    fn set_transcript_lifecycle(
        &self,
        message: &Message,
        media: &MediaObject,
        lifecycle: TranscriptLifecycle,
        text: Option<String>,
    ) -> ApiResult<Transcript> {
        let mut transcript = transcript_with_lifecycle(
            &message.project_id,
            &message.company_id,
            &message.id,
            Some(media.id.clone()),
            lifecycle,
            text,
        );
        if !self.company_allows_transcript_storage(&message.project_id, &message.company_id) {
            transcript.text = None;
            transcript.raw_response_json = json!({"redacted": true});
        }
        let mut inner = self.inner.lock().expect("store lock poisoned");
        if let Some(existing) = inner.transcripts.get(&message.id) {
            transcript.id = existing.id.clone();
            transcript.created_at = existing.created_at;
        }
        inner
            .transcripts
            .insert(message.id.clone(), transcript.clone());
        if transcript.status == "completed" {
            push_event_inner(
                &mut inner,
                Some(&self.events_tx),
                Some(&self.event_bus),
                new_event(
                    "audio.transcribed",
                    &message.project_id,
                    &message.company_id,
                    Some(message.channel_account_id.clone()),
                    Some(message.conversation_id.clone()),
                    Some(message.id.clone()),
                    Some(message.conversation_seq),
                    json!({
                        "message_id": message.id.clone(),
                        "media_id": media.id.clone(),
                        "transcript_id": transcript.id.clone(),
                        "status": transcript.status.clone()
                    }),
                ),
            );
            mark_dirty_inner(
                &mut inner,
                &message.project_id,
                &message.company_id,
                &message.conversation_id,
                message.conversation_seq,
                &message.channel_account_id,
                Some(&self.events_tx),
                Some(&self.event_bus),
            );
        }
        self.persist_locked(&inner);
        Ok(transcript)
    }

    fn set_completed_groq_transcript(
        &self,
        message: &Message,
        media: &MediaObject,
        result: crate::transcription::GroqTranscription,
    ) -> ApiResult<Transcript> {
        let mut transcript = groq_transcript_from_result(
            &message.project_id,
            &message.company_id,
            &message.id,
            Some(media.id.clone()),
            result,
        );
        if !self.company_allows_transcript_storage(&message.project_id, &message.company_id) {
            transcript.text = None;
            transcript.raw_response_json = json!({"redacted": true});
        }
        let mut inner = self.inner.lock().expect("store lock poisoned");
        if let Some(existing) = inner.transcripts.get(&message.id) {
            transcript.id = existing.id.clone();
            transcript.created_at = existing.created_at;
        }
        inner
            .transcripts
            .insert(message.id.clone(), transcript.clone());
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "audio.transcribed",
                &message.project_id,
                &message.company_id,
                Some(message.channel_account_id.clone()),
                Some(message.conversation_id.clone()),
                Some(message.id.clone()),
                Some(message.conversation_seq),
                json!({
                    "message_id": message.id.clone(),
                    "media_id": media.id.clone(),
                    "transcript_id": transcript.id.clone()
                }),
            ),
        );
        push_event_inner(
            &mut inner,
            Some(&self.events_tx),
            Some(&self.event_bus),
            new_event(
                "transcript.completed",
                &message.project_id,
                &message.company_id,
                Some(message.channel_account_id.clone()),
                Some(message.conversation_id.clone()),
                Some(message.id.clone()),
                Some(message.conversation_seq),
                json!({
                    "message_id": message.id.clone(),
                    "media_id": media.id.clone(),
                    "transcript_id": transcript.id.clone()
                }),
            ),
        );
        mark_dirty_inner(
            &mut inner,
            &message.project_id,
            &message.company_id,
            &message.conversation_id,
            message.conversation_seq,
            &message.channel_account_id,
            Some(&self.events_tx),
            Some(&self.event_bus),
        );
        self.persist_locked(&inner);
        Ok(transcript)
    }

    pub fn list_dirty(
        &self,
        project_id: &str,
        company_id: &str,
        consumer_id: &str,
        limit: usize,
    ) -> Vec<DirtyConversationItem> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let now = OffsetDateTime::now_utc();
        let mut items = Vec::new();
        let consumer_state = inner.consumer_state.clone();
        let mut leases = Vec::new();
        let mut consumer_entries = Vec::new();
        for ((dirty_project, dirty_company, conversation_id), dirty) in inner.dirty.iter_mut() {
            if dirty_project != project_id || dirty_company != company_id {
                continue;
            }
            let processed = consumer_state
                .get(&(
                    project_id.to_string(),
                    company_id.to_string(),
                    consumer_id.to_string(),
                    conversation_id.clone(),
                ))
                .copied()
                .unwrap_or(0);
            if processed >= dirty.max_seq {
                continue;
            }
            let lease_token = format!("lease_{}", Uuid::now_v7().simple());
            let locked_until = now + Duration::seconds(60);
            leases.push((
                (
                    project_id.to_string(),
                    company_id.to_string(),
                    consumer_id.to_string(),
                    conversation_id.clone(),
                ),
                DirtyLeaseRecord {
                    lease_token: lease_token.clone(),
                    locked_until,
                    max_seq: dirty.max_seq,
                },
            ));
            consumer_entries.push((
                (
                    project_id.to_string(),
                    company_id.to_string(),
                    consumer_id.to_string(),
                    conversation_id.clone(),
                ),
                processed,
            ));
            items.push(DirtyConversationItem {
                conversation_id: dirty.conversation_id.clone(),
                max_seq: dirty.max_seq,
                reason: dirty.reason.clone(),
                priority: dirty.priority,
                available_at: dirty.available_at,
                lease_token,
                locked_until,
            });
            if items.len() >= limit {
                break;
            }
        }
        inner.dirty_leases.extend(leases);
        for (key, processed) in consumer_entries {
            inner.consumer_state.entry(key).or_insert(processed);
        }
        self.persist_locked(&inner);
        items.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.available_at.cmp(&b.available_at))
        });
        items
    }

    pub fn ack_dirty(
        &self,
        project_id: &str,
        company_id: &str,
        conversation_id: &str,
        request: DirtyAckRequest,
    ) -> ApiResult<Value> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let key = (
            project_id.to_string(),
            company_id.to_string(),
            conversation_id.to_string(),
        );
        let dirty = inner
            .dirty
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound("dirty conversation not found".to_string()))?;
        let lease_key = (
            project_id.to_string(),
            company_id.to_string(),
            request.consumer_id.clone(),
            conversation_id.to_string(),
        );
        let lease = inner
            .dirty_leases
            .get(&lease_key)
            .cloned()
            .ok_or_else(|| ApiError::Conflict("lease_token missing".to_string()))?;
        let now = OffsetDateTime::now_utc();
        if lease.lease_token != request.lease_token {
            return Err(ApiError::Conflict("lease_token mismatch".to_string()));
        }
        if lease.locked_until < now {
            return Err(ApiError::Conflict("lease_token expired".to_string()));
        }

        inner.consumer_state.insert(
            (
                project_id.to_string(),
                company_id.to_string(),
                request.consumer_id.clone(),
                conversation_id.to_string(),
            ),
            request.processed_until_seq,
        );
        inner.dirty_leases.remove(&lease_key);

        let all_registered_consumers_done = inner
            .consumer_state
            .iter()
            .filter(
                |((state_project, state_company, _, state_conversation), _)| {
                    state_project == project_id
                        && state_company == company_id
                        && state_conversation == conversation_id
                },
            )
            .all(|(_, processed_until_seq)| *processed_until_seq >= dirty.max_seq);

        if request.processed_until_seq >= dirty.max_seq && all_registered_consumers_done {
            inner.dirty.remove(&key);
            self.persist_locked(&inner);
            Ok(json!({"acked": true, "remaining_dirty": false}))
        } else {
            self.persist_locked(&inner);
            Ok(json!({"acked": true, "remaining_dirty": true, "current_max_seq": dirty.max_seq}))
        }
    }

    pub fn events(&self) -> Vec<crate::models::CommonEvent> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .events
            .clone()
    }

    pub fn list_callbacks(&self, project_id: &str, company_id: &str) -> Vec<Value> {
        let mut callbacks: Vec<_> = self
            .inner
            .lock()
            .expect("store lock poisoned")
            .callbacks
            .values()
            .filter(|callback| {
                callback["project_id"].as_str() == Some(project_id)
                    && callback["company_id"].as_str() == Some(company_id)
            })
            .map(sanitize_callback)
            .collect();
        callbacks.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
        callbacks
    }

    pub fn upsert_callback(
        &self,
        project_id: &str,
        company_id: &str,
        callback_id: Option<&str>,
        body: Value,
    ) -> ApiResult<Value> {
        let now = OffsetDateTime::now_utc();
        let callback_id = callback_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("callback_{}", Uuid::now_v7().simple()));
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let existing = inner.callbacks.get(&callback_id).cloned();
        if existing
            .as_ref()
            .is_some_and(|callback| !callback_belongs_to_company(callback, project_id, company_id))
        {
            return Err(ApiError::NotFound(format!(
                "callback {callback_id} not found"
            )));
        }
        let created_at = existing
            .as_ref()
            .and_then(|value| value["created_at"].as_str())
            .map(str::to_string)
            .unwrap_or_else(|| ts(now));
        let callback_url = body
            .get("url")
            .and_then(Value::as_str)
            .or_else(|| existing.as_ref().and_then(|value| value["url"].as_str()))
            .unwrap_or("http://localhost:3000/webhook");
        validate_callback_url(callback_url, self.config.dev_mode)?;
        let encrypted_secret = if let Some(secret) = body.get("secret").and_then(Value::as_str) {
            Some(self.encrypt_callback_secret(secret)?)
        } else if let Some(existing_secret) = existing
            .as_ref()
            .and_then(|value| value["encrypted_secret"].as_str())
            .map(str::to_string)
        {
            Some(existing_secret)
        } else if let Some(legacy_secret) =
            existing.as_ref().and_then(|value| value["secret"].as_str())
        {
            Some(self.encrypt_callback_secret(legacy_secret)?)
        } else if self.config.secret_master_key.is_some() {
            Some(self.encrypt_callback_secret(&format!("whsec_{}", Uuid::now_v7().simple()))?)
        } else if self.config.webhook.delivery_enabled && self.webhook_signal_enabled() {
            return Err(ApiError::BadRequest(
                "RUSTZAP_SECRET_MASTER_KEY is required for webhook delivery secrets".to_string(),
            ));
        } else {
            None
        };
        let callback = json!({
            "id": callback_id,
            "project_id": project_id,
            "company_id": company_id,
            "url": callback_url,
            "encrypted_secret": encrypted_secret,
            "enabled": body.get("enabled").and_then(Value::as_bool).unwrap_or(true),
            "events": body.get("events").cloned().unwrap_or_else(|| json!(["conversation.dirty"])),
            "max_batch_size": body.get("max_batch_size").and_then(Value::as_u64).unwrap_or(self.config.webhook.max_batch_size as u64),
            "timeout_seconds": body.get("timeout_seconds").and_then(Value::as_u64).unwrap_or(self.config.webhook.timeout_seconds),
            "created_at": created_at,
            "updated_at": ts(now)
        });
        inner.callbacks.insert(callback_id, callback.clone());
        self.persist_locked(&inner);
        drop(inner);
        self.audit_log(
            project_id,
            company_id,
            "callback.upsert",
            "consumer_callback",
            callback["id"].as_str(),
            json!({"enabled": callback["enabled"]}),
        );
        Ok(sanitize_callback(&callback))
    }

    fn encrypt_callback_secret(&self, secret: &str) -> ApiResult<String> {
        let key = self.config.secret_master_key.as_ref().ok_or_else(|| {
            ApiError::BadRequest(
                "RUSTZAP_SECRET_MASTER_KEY is required to store webhook secrets".to_string(),
            )
        })?;
        crate::secrets::encrypt_secret(key, secret)
            .map_err(|err| ApiError::Internal(format!("failed to encrypt webhook secret: {err}")))
    }

    fn decrypt_callback_secret(&self, encrypted_secret: &str) -> ApiResult<String> {
        let key = self.config.secret_master_key.as_ref().ok_or_else(|| {
            ApiError::Internal(
                "RUSTZAP_SECRET_MASTER_KEY is required to deliver webhook secrets".to_string(),
            )
        })?;
        crate::secrets::decrypt_secret(key, encrypted_secret)
            .map_err(|err| ApiError::Internal(format!("failed to decrypt webhook secret: {err}")))
    }

    fn upgrade_legacy_callback_secrets(&self) {
        let Some(key) = self.config.secret_master_key.as_ref() else {
            return;
        };
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let mut upgraded = 0_usize;
        for callback in inner.callbacks.values_mut() {
            if callback
                .get("encrypted_secret")
                .and_then(Value::as_str)
                .is_some()
            {
                if let Some(object) = callback.as_object_mut() {
                    object.remove("secret");
                }
                continue;
            }
            let Some(legacy_secret) = callback
                .get("secret")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                continue;
            };
            match crate::secrets::encrypt_secret(key, &legacy_secret) {
                Ok(encrypted_secret) => {
                    if let Some(object) = callback.as_object_mut() {
                        object.insert("encrypted_secret".to_string(), json!(encrypted_secret));
                        object.remove("secret");
                        upgraded += 1;
                    }
                }
                Err(err) => {
                    tracing::error!(error = %err, "failed to upgrade legacy webhook secret");
                }
            }
        }
        if upgraded > 0 {
            self.persist_locked(&inner);
            tracing::info!(upgraded, "upgraded legacy webhook callback secrets");
        }
    }

    pub fn delete_callback(
        &self,
        project_id: &str,
        company_id: &str,
        callback_id: &str,
    ) -> ApiResult<Value> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let callback = inner
            .callbacks
            .get(callback_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("callback {callback_id} not found")))?;
        if !callback_belongs_to_company(&callback, project_id, company_id) {
            return Err(ApiError::NotFound(format!(
                "callback {callback_id} not found"
            )));
        }
        let callback = inner
            .callbacks
            .remove(callback_id)
            .expect("callback existence checked before remove");
        self.persist_locked(&inner);
        drop(inner);
        self.audit_log(
            project_id,
            company_id,
            "callback.delete",
            "consumer_callback",
            Some(callback_id),
            json!({"deleted": true}),
        );
        Ok(json!({"deleted": true, "callback": sanitize_callback(&callback)}))
    }

    pub fn webhook_delivery_attempts(&self, project_id: &str, company_id: &str) -> Vec<Value> {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .webhook_delivery_attempts
            .iter()
            .filter(|attempt| attempt.project_id == project_id && attempt.company_id == company_id)
            .map(|attempt| json!(attempt))
            .collect()
    }

    pub async fn deliver_pending_webhooks_once(&self) -> ApiResult<usize> {
        if !self.config.webhook.delivery_enabled || !self.webhook_signal_enabled() {
            return Ok(0);
        }
        let jobs = self.pending_webhook_jobs();
        let mut delivered = 0;
        for job in jobs {
            self.deliver_webhook_job(job).await?;
            delivered += 1;
        }
        Ok(delivered)
    }

    fn webhook_signal_enabled(&self) -> bool {
        matches!(
            self.config.consumer_signal_mode,
            ConsumerSignalMode::Webhook | ConsumerSignalMode::WebhookAndPolling
        )
    }

    fn pending_webhook_jobs(&self) -> Vec<WebhookDeliveryJob> {
        let now = OffsetDateTime::now_utc();
        let inner = self.inner.lock().expect("store lock poisoned");
        let mut jobs = Vec::new();
        for callback in inner.callbacks.values() {
            if callback["enabled"].as_bool() != Some(true) {
                continue;
            }
            let project_id = callback["project_id"].as_str().unwrap_or_default();
            let company_id = callback["company_id"].as_str().unwrap_or_default();
            for event in &inner.events {
                if event.project_id != project_id || event.company_id != company_id {
                    continue;
                }
                if !callback_subscribes_to_event(callback, &event.event_type) {
                    continue;
                }
                let callback_id = callback["id"].as_str().unwrap_or_default();
                let attempts: Vec<_> = inner
                    .webhook_delivery_attempts
                    .iter()
                    .filter(|attempt| {
                        attempt.callback_id == callback_id && attempt.event_id == event.event_id
                    })
                    .collect();
                if attempts.iter().any(|attempt| attempt.status == "success") {
                    continue;
                }
                let Some(last_attempt) = attempts.iter().max_by_key(|attempt| attempt.attempt)
                else {
                    jobs.push(WebhookDeliveryJob {
                        callback: callback.clone(),
                        event: event.clone(),
                        attempt: 1,
                        first_failed_at: None,
                    });
                    continue;
                };
                if last_attempt.attempt >= self.config.webhook.max_retries {
                    continue;
                }
                if last_attempt
                    .next_retry_at
                    .is_some_and(|next_retry_at| next_retry_at > now)
                {
                    continue;
                }
                jobs.push(WebhookDeliveryJob {
                    callback: callback.clone(),
                    event: event.clone(),
                    attempt: last_attempt.attempt.saturating_add(1),
                    first_failed_at: last_attempt
                        .first_failed_at
                        .or(Some(last_attempt.created_at)),
                });
            }
        }
        jobs
    }

    async fn deliver_webhook_job(&self, job: WebhookDeliveryJob) -> ApiResult<()> {
        let callback_id = job.callback["id"].as_str().unwrap_or_default().to_string();
        let url = job.callback["url"]
            .as_str()
            .ok_or_else(|| ApiError::BadRequest("callback url is required".to_string()))?
            .to_string();
        let encrypted_secret = job
            .callback
            .get("encrypted_secret")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::Internal(format!(
                    "callback {callback_id} is missing encrypted_secret"
                ))
            })?;
        let secret = self.decrypt_callback_secret(encrypted_secret)?;
        let body = serde_json::to_vec(&json!({
            "events": [compact_webhook_event(&job.event)]
        }))
        .map_err(|err| ApiError::Internal(format!("failed to encode webhook body: {err}")))?;
        let timestamp = OffsetDateTime::now_utc().unix_timestamp().to_string();
        let signature = crate::security::webhook_signature(&secret, &timestamp, &body);
        let timeout_seconds = job
            .callback
            .get("timeout_seconds")
            .and_then(Value::as_u64)
            .unwrap_or(self.config.webhook.timeout_seconds);
        let response = reqwest::Client::builder()
            .timeout(StdDuration::from_secs(timeout_seconds.max(1)))
            .build()
            .map_err(|err| ApiError::Internal(format!("failed to build webhook client: {err}")))?
            .post(&url)
            .header("content-type", "application/json")
            .header(&self.config.webhook.signing_header, signature)
            .header(&self.config.webhook.timestamp_header, timestamp)
            .header(&self.config.webhook.event_id_header, &job.event.event_id)
            .body(body)
            .send()
            .await;

        match response {
            Ok(response) if response.status().is_success() => {
                self.record_webhook_attempt(WebhookAttemptInput {
                    callback_id,
                    event: &job.event,
                    attempt: job.attempt,
                    status: "success",
                    http_status: Some(response.status().as_u16()),
                    error_message: None,
                    first_failed_at: None,
                    next_retry_at: None,
                });
            }
            Ok(response) => {
                self.record_webhook_attempt(WebhookAttemptInput {
                    callback_id,
                    event: &job.event,
                    attempt: job.attempt,
                    status: "failed",
                    http_status: Some(response.status().as_u16()),
                    error_message: Some(format!("webhook returned {}", response.status())),
                    first_failed_at: job.first_failed_at,
                    next_retry_at: Some(next_webhook_retry_at(
                        job.attempt,
                        self.config.webhook.retry_base_seconds,
                        self.config.webhook.retry_max_seconds,
                    )),
                });
            }
            Err(err) => {
                self.record_webhook_attempt(WebhookAttemptInput {
                    callback_id,
                    event: &job.event,
                    attempt: job.attempt,
                    status: "failed",
                    http_status: None,
                    error_message: Some(err.to_string()),
                    first_failed_at: job.first_failed_at,
                    next_retry_at: Some(next_webhook_retry_at(
                        job.attempt,
                        self.config.webhook.retry_base_seconds,
                        self.config.webhook.retry_max_seconds,
                    )),
                });
            }
        }
        Ok(())
    }

    fn record_webhook_attempt(&self, input: WebhookAttemptInput<'_>) {
        let now = OffsetDateTime::now_utc();
        self.metrics
            .webhook_delivery_attempts_total
            .fetch_add(1, Ordering::Relaxed);
        if input.status == "success" {
            self.metrics
                .webhook_delivery_successes_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let mut inner = self.inner.lock().expect("store lock poisoned");
        inner
            .webhook_delivery_attempts
            .push(WebhookDeliveryAttempt {
                id: Uuid::now_v7().to_string(),
                project_id: input.event.project_id.clone(),
                company_id: input.event.company_id.clone(),
                callback_id: input.callback_id,
                event_id: input.event.event_id.clone(),
                attempt: input.attempt,
                status: input.status.to_string(),
                http_status: input.http_status,
                error_message: input.error_message,
                first_failed_at: if input.status == "failed" {
                    input.first_failed_at.or(Some(now))
                } else {
                    None
                },
                last_failed_at: (input.status == "failed").then_some(now),
                next_retry_at: input.next_retry_at,
                created_at: now,
            });
        self.persist_locked(&inner);
    }

    pub fn privacy_export_contact(
        &self,
        project_id: &str,
        company_id: &str,
        contact_id: &str,
    ) -> ApiResult<Value> {
        let inner = self.inner.lock().expect("store lock poisoned");
        let contact_id = resolve_contact_id_inner(&inner, project_id, company_id, contact_id);
        let contact = inner
            .contacts
            .get(&contact_id)
            .filter(|contact| contact.project_id == project_id && contact.company_id == company_id)
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("contact {contact_id} not found")))?;
        let aliases = contact_aliases_for_record(&contact);
        let conversations: Vec<_> = inner
            .conversations
            .values()
            .filter(|conversation| {
                conversation.project_id == project_id
                    && conversation.company_id == company_id
                    && conversation
                        .contact_id
                        .as_deref()
                        .is_some_and(|id| aliases.iter().any(|alias| alias == id))
            })
            .cloned()
            .collect();
        let conversation_ids: HashSet<_> = conversations
            .iter()
            .map(|conversation| conversation.id.clone())
            .collect();
        let messages: Vec<_> = inner
            .messages_by_id
            .values()
            .filter(|message| {
                message.project_id == project_id
                    && message.company_id == company_id
                    && (conversation_ids.contains(&message.conversation_id)
                        || message
                            .sender_contact_id
                            .as_deref()
                            .is_some_and(|id| aliases.iter().any(|alias| alias == id)))
            })
            .cloned()
            .collect();
        let message_ids: HashSet<_> = messages.iter().map(|message| message.id.clone()).collect();
        let media: Vec<_> = inner
            .media
            .values()
            .filter(|media| {
                media.project_id == project_id
                    && media.company_id == company_id
                    && (conversation_ids.contains(&media.conversation_id)
                        || media
                            .message_id
                            .as_deref()
                            .is_some_and(|id| message_ids.contains(id)))
            })
            .cloned()
            .collect();
        let transcripts: Vec<_> = inner
            .transcripts
            .values()
            .filter(|transcript| {
                transcript.project_id == project_id
                    && transcript.company_id == company_id
                    && message_ids.contains(&transcript.message_id)
            })
            .cloned()
            .collect();
        Ok(json!({
            "contact": contact_json(&contact),
            "conversations": conversations,
            "messages": messages,
            "media": media,
            "transcripts": transcripts
        }))
    }

    pub fn anonymize_contact(
        &self,
        project_id: &str,
        company_id: &str,
        contact_id: &str,
        delete_messages: bool,
    ) -> ApiResult<Value> {
        let mut inner = self.inner.lock().expect("store lock poisoned");
        let now = OffsetDateTime::now_utc();
        let resolved_contact_id =
            resolve_contact_id_inner(&inner, project_id, company_id, contact_id);
        let contact = inner
            .contacts
            .get_mut(&resolved_contact_id)
            .filter(|contact| contact.project_id == project_id && contact.company_id == company_id)
            .ok_or_else(|| {
                ApiError::NotFound(format!("contact {resolved_contact_id} not found"))
            })?;
        let aliases = contact_aliases_for_record(contact);
        contact.phone_e164 = None;
        contact.push_name = None;
        contact.display_name = format!(
            "anonymous_{}",
            &contact.id.chars().take(8).collect::<String>()
        );
        contact.business_description = None;
        contact.avatar_url = None;
        contact.profile_picture_url = None;
        contact.last_contact_at = now;
        let anonymous_name = contact.display_name.clone();

        for conversation in inner.conversations.values_mut() {
            if conversation.project_id == project_id
                && conversation.company_id == company_id
                && conversation
                    .contact_id
                    .as_deref()
                    .is_some_and(|id| aliases.iter().any(|alias| alias == id))
            {
                conversation.display_name = Some(anonymous_name.clone());
                conversation.display_phone = None;
                conversation.phone_number = None;
                conversation.avatar_url = None;
                conversation.profile_picture_url = None;
                conversation.updated_at = now;
            }
        }

        for message in inner.messages_by_id.values_mut() {
            if message.project_id == project_id
                && message.company_id == company_id
                && message
                    .sender_contact_id
                    .as_deref()
                    .is_some_and(|id| aliases.iter().any(|alias| alias == id))
            {
                message.sender_display_name = Some(anonymous_name.clone());
                if delete_messages {
                    message.text = None;
                }
                message.updated_at = now;
            }
        }
        for messages in inner.messages_by_conversation.values_mut() {
            for message in messages {
                if message.project_id == project_id
                    && message.company_id == company_id
                    && message
                        .sender_contact_id
                        .as_deref()
                        .is_some_and(|id| aliases.iter().any(|alias| alias == id))
                {
                    message.sender_display_name = Some(anonymous_name.clone());
                    if delete_messages {
                        message.text = None;
                    }
                    message.updated_at = now;
                }
            }
        }

        self.persist_locked(&inner);
        drop(inner);
        self.audit_log(
            project_id,
            company_id,
            if delete_messages {
                "privacy.delete"
            } else {
                "privacy.anonymize"
            },
            "contact",
            Some(&resolved_contact_id),
            json!({"pii_redacted": true, "message_text_deleted": delete_messages}),
        );
        Ok(json!({
            "contact_id": contact_id,
            "technical_contact_id": resolved_contact_id,
            "anonymized": true,
            "deleted": delete_messages,
            "pii_redacted": true
        }))
    }

    pub fn push_event(&self, event: crate::models::CommonEvent) {
        let _ = self.events_tx.send(event.clone());
        self.event_bus.publish_background(event.clone());
        let mut inner = self.inner.lock().expect("store lock poisoned");
        inner.events.push(event);
        self.persist_locked(&inner);
    }

    pub async fn bootstrap_existing_sessions(&self) -> ApiResult<usize> {
        let base = self.config.wa_session_sqlite_dir.clone();
        if !base.exists() {
            return Ok(0);
        }
        let mut started = 0;
        for (project_id, company_id, channel_id) in discover_session_channels(&base) {
            self.upsert_project(project_id.clone(), project_id.clone());
            self.upsert_company(project_id.clone(), company_id.clone(), company_id.clone());
            if !self.channel_exists(&channel_id) {
                self.create_channel(
                    &project_id,
                    &company_id,
                    Some(channel_id.clone()),
                    Some("WhatsApp Restored".to_string()),
                    None,
                )?;
            }
            if self
                .connect_channel(&project_id, &company_id, &channel_id)
                .await
                .is_ok()
            {
                started += 1;
            }
        }
        Ok(started)
    }

    pub fn reset_dev(&self) {
        *self.inner.lock().expect("store lock poisoned") = StoreInner::default();
        if let Some(path) = self.dev_state_path.as_deref() {
            let _ = std::fs::remove_file(path);
        }
    }

    fn channel_exists(&self, channel_id: &str) -> bool {
        self.inner
            .lock()
            .expect("store lock poisoned")
            .channels
            .contains_key(channel_id)
    }
}

fn discover_session_channels(base: &Path) -> Vec<(String, String, String)> {
    let mut channels = Vec::new();
    let Ok(projects) = std::fs::read_dir(base) else {
        return channels;
    };
    for project in projects.flatten() {
        let project_path = project.path();
        if !project_path.is_dir() {
            continue;
        }
        let project_id = project.file_name().to_string_lossy().to_string();
        let Ok(companies) = std::fs::read_dir(project_path) else {
            continue;
        };
        for company in companies.flatten() {
            let company_path = company.path();
            if !company_path.is_dir() {
                continue;
            }
            let company_id = company.file_name().to_string_lossy().to_string();
            let Ok(channel_dirs) = std::fs::read_dir(company_path) else {
                continue;
            };
            for channel in channel_dirs.flatten() {
                let channel_path = channel.path();
                if channel_path.join("session.sqlite").exists() {
                    channels.push((
                        project_id.clone(),
                        company_id.clone(),
                        channel.file_name().to_string_lossy().to_string(),
                    ));
                }
            }
        }
    }
    channels
}

#[allow(clippy::too_many_arguments)]
fn record_message_inner(
    inner: &mut StoreInner,
    project_id: &str,
    company_id: &str,
    conversation_id: &str,
    direction: &str,
    message_type: &str,
    text: Option<String>,
    status: &str,
    quoted_message_id: Option<String>,
    channel_account_id: Option<String>,
    sender_contact_id: Option<String>,
    sender_display_name: Option<String>,
    sender_profile_picture_url: Option<String>,
    sent_by_external_user_id: Option<String>,
    events_tx: Option<&broadcast::Sender<crate::models::CommonEvent>>,
    event_bus: Option<&EventBusHandle>,
) -> Message {
    let now = OffsetDateTime::now_utc();
    let channel_id = channel_account_id.unwrap_or_else(|| "channel_dev".to_string());
    let is_group = conversation_id.ends_with("@g.us");
    let contact_id = if direction == "inbound" {
        sender_contact_id.clone().or_else(|| {
            if is_group {
                None
            } else {
                Some(conversation_id.to_string())
            }
        })
    } else if is_group {
        None
    } else {
        Some(conversation_id.to_string())
    };
    let contact = contact_id.as_ref().map(|contact_id| {
        upsert_contact_inner(
            inner,
            ContactUpsertInput {
                project_id,
                company_id,
                channel_id: Some(&channel_id),
                contact_id,
                push_name: sender_display_name.as_deref(),
                profile_picture_url: sender_profile_picture_url.clone(),
                now,
            },
        )
    });
    if is_group && let Some(sender_contact_id) = sender_contact_id.as_deref() {
        let display_name = sender_display_name
            .clone()
            .or_else(|| contact.as_ref().map(|contact| contact.display_name.clone()))
            .unwrap_or_else(|| display_name_for_jid(sender_contact_id));
        upsert_group_member_inner(
            inner,
            conversation_id,
            sender_contact_id,
            &display_name,
            now,
        );
    }
    let group_record = if is_group {
        inner.groups.get(conversation_id).cloned()
    } else {
        None
    };
    let conversation_display_name = if is_group {
        Some(
            group_record
                .as_ref()
                .and_then(|group| group.subject.clone())
                .unwrap_or_else(|| group_subject_for_jid(conversation_id)),
        )
    } else {
        contact.as_ref().map(|contact| contact.display_name.clone())
    };
    let conversation_display_phone = if is_group {
        None
    } else {
        contact
            .as_ref()
            .and_then(|contact| contact.phone_e164.clone())
    };
    let conversation = inner
        .conversations
        .entry(conversation_id.to_string())
        .or_insert_with(|| crate::models::Conversation {
            id: conversation_id.to_string(),
            project_id: project_id.to_string(),
            company_id: company_id.to_string(),
            channel_account_id: channel_id.clone(),
            conversation_type: if is_group { "group" } else { "direct" }.to_string(),
            contact_id: if is_group { None } else { contact_id.clone() },
            group_id: is_group.then(|| conversation_id.to_string()),
            display_name: conversation_display_name.clone(),
            display_phone: conversation_display_phone.clone(),
            phone_number: conversation_display_phone
                .as_deref()
                .and_then(phone_number_from_e164),
            avatar_url: if is_group {
                group_record
                    .as_ref()
                    .and_then(|group| group.avatar_url.clone())
            } else {
                contact
                    .as_ref()
                    .and_then(|contact| contact.avatar_url.clone())
            },
            profile_picture_url: if is_group {
                group_record
                    .as_ref()
                    .and_then(|group| group.profile_picture_url.clone())
            } else {
                contact
                    .as_ref()
                    .and_then(|contact| contact.profile_picture_url.clone())
            },
            last_seq: 0,
            last_message_at: None,
            unread_count: 0,
            is_archived: false,
            is_muted: false,
            is_pinned: false,
            control_mode: "autopilot".to_string(),
            created_at: now,
            updated_at: now,
        });
    if is_group {
        apply_group_record_to_conversation(conversation, conversation_id, group_record.as_ref());
    } else {
        if conversation_display_name.is_some() {
            conversation.display_name = conversation_display_name;
        }
        if conversation_display_phone.is_some() {
            conversation.phone_number = conversation_display_phone
                .as_deref()
                .and_then(phone_number_from_e164);
            conversation.display_phone = conversation_display_phone;
        }
    }
    conversation.last_seq += 1;
    conversation.last_message_at = Some(now);
    conversation.updated_at = now;
    if direction == "inbound" {
        conversation.unread_count += 1;
    }
    let conversation_seq = conversation.last_seq;
    let message = Message {
        id: format!("msg_{}", Uuid::now_v7().simple()),
        project_id: project_id.to_string(),
        company_id: company_id.to_string(),
        conversation_id: conversation_id.to_string(),
        channel_account_id: channel_id.clone(),
        conversation_seq,
        wa_message_id: Some(format!("wa_{}", Uuid::now_v7().simple())),
        direction: direction.to_string(),
        sender_contact_id,
        sender_display_name: sender_display_name
            .or_else(|| contact.as_ref().map(|contact| contact.display_name.clone())),
        message_type: message_type.to_string(),
        text,
        media_id: None,
        media_url: None,
        thumbnail_url: None,
        mime_type: None,
        file_name: None,
        quoted_message_id,
        status: status.to_string(),
        error_message: None,
        is_starred: false,
        is_pinned: false,
        reaction: None,
        sent_by_source: (direction == "outbound").then(|| "rustzap_api".to_string()),
        sent_by_external_user_id: (direction == "outbound")
            .then_some(sent_by_external_user_id)
            .flatten(),
        created_at_wa: now,
        created_at: now,
        updated_at: now,
    };
    inner
        .messages_by_conversation
        .entry(conversation_id.to_string())
        .or_default()
        .push(message.clone());
    inner
        .messages_by_id
        .insert(message.id.clone(), message.clone());
    mark_dirty_inner(
        inner,
        project_id,
        company_id,
        conversation_id,
        conversation_seq,
        &channel_id,
        events_tx,
        event_bus,
    );
    push_event_inner(
        inner,
        events_tx,
        event_bus,
        new_event(
            if direction == "inbound" {
                "message.received"
            } else {
                "message.queued"
            },
            project_id,
            company_id,
            Some(channel_id),
            Some(conversation_id.to_string()),
            Some(message.id.clone()),
            Some(conversation_seq),
            json!({
                "message_id": message.id.clone(),
                "status": message.status.clone(),
                "delivery_state": message.delivery_state()
            }),
        ),
    );
    message
}

fn ensure_conversation_inner(
    inner: &mut StoreInner,
    project_id: &str,
    company_id: &str,
    conversation_id: &str,
    channel_id: &str,
    control_mode: &str,
) {
    if inner.conversations.contains_key(conversation_id) {
        return;
    }

    let now = OffsetDateTime::now_utc();
    let is_group = conversation_id.ends_with("@g.us");
    let contact = (!is_group).then(|| {
        upsert_contact_inner(
            inner,
            ContactUpsertInput {
                project_id,
                company_id,
                channel_id: Some(channel_id),
                contact_id: conversation_id,
                push_name: None,
                profile_picture_url: None,
                now,
            },
        )
    });
    let group_record = if is_group {
        inner.groups.get(conversation_id).cloned()
    } else {
        None
    };
    let display_name = if is_group {
        Some(
            group_record
                .as_ref()
                .and_then(|group| group.subject.clone())
                .unwrap_or_else(|| group_subject_for_jid(conversation_id)),
        )
    } else {
        contact.as_ref().map(|contact| contact.display_name.clone())
    };

    inner.conversations.insert(
        conversation_id.to_string(),
        crate::models::Conversation {
            id: conversation_id.to_string(),
            project_id: project_id.to_string(),
            company_id: company_id.to_string(),
            channel_account_id: channel_id.to_string(),
            conversation_type: if is_group { "group" } else { "direct" }.to_string(),
            contact_id: if is_group {
                None
            } else {
                Some(conversation_id.to_string())
            },
            group_id: is_group.then(|| conversation_id.to_string()),
            display_name,
            display_phone: if is_group {
                None
            } else {
                contact
                    .as_ref()
                    .and_then(|contact| contact.phone_e164.clone())
            },
            phone_number: if is_group {
                None
            } else {
                contact.as_ref().and_then(contact_phone_number)
            },
            avatar_url: if is_group {
                group_record
                    .as_ref()
                    .and_then(|group| group.avatar_url.clone())
            } else {
                contact
                    .as_ref()
                    .and_then(|contact| contact.avatar_url.clone())
            },
            profile_picture_url: if is_group {
                group_record
                    .as_ref()
                    .and_then(|group| group.profile_picture_url.clone())
            } else {
                contact
                    .as_ref()
                    .and_then(|contact| contact.profile_picture_url.clone())
            },
            last_seq: 0,
            last_message_at: None,
            unread_count: 0,
            is_archived: false,
            is_muted: false,
            is_pinned: false,
            control_mode: control_mode.to_string(),
            created_at: now,
            updated_at: now,
        },
    );
}

fn attach_media_to_message_inner(
    inner: &mut StoreInner,
    message_id: &str,
    media: &MediaObject,
) -> Option<Message> {
    let message = inner.messages_by_id.get_mut(message_id)?;
    message.media_id = Some(media.id.clone());
    message.media_url = media.public_url.clone();
    message.thumbnail_url = media.thumbnail_url.clone();
    message.mime_type = Some(media.mime_type.clone());
    message.file_name = media.original_filename.clone();
    message.updated_at = OffsetDateTime::now_utc();
    let updated = message.clone();
    if let Some(messages) = inner
        .messages_by_conversation
        .get_mut(&updated.conversation_id)
        && let Some(existing) = messages.iter_mut().find(|message| message.id == updated.id)
    {
        *existing = updated.clone();
    }
    Some(updated)
}

struct ContactProfileApplyInput<'a> {
    project_id: &'a str,
    company_id: &'a str,
    channel_id: &'a str,
    conversation_id: &'a str,
    contact_id: &'a str,
    push_name: Option<&'a str>,
    profile: ContactProfile,
}

struct ContactUpsertInput<'a> {
    project_id: &'a str,
    company_id: &'a str,
    channel_id: Option<&'a str>,
    contact_id: &'a str,
    push_name: Option<&'a str>,
    profile_picture_url: Option<String>,
    now: OffsetDateTime,
}

struct ContactUpsertWithAltInput<'a> {
    contact: ContactUpsertInput<'a>,
    alt_jid: Option<&'a str>,
}

struct ContactEnrichmentInput<'a> {
    project_id: &'a str,
    company_id: &'a str,
    channel_id: Option<&'a str>,
    contact_id: &'a str,
    push_name: Option<&'a str>,
    profile: &'a ContactProfile,
    now: OffsetDateTime,
}

fn upsert_contact_inner(inner: &mut StoreInner, input: ContactUpsertInput<'_>) -> ContactRecord {
    let phone_e164 = phone_from_jid(input.contact_id);
    let display_name = input
        .push_name
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| phone_e164.clone())
        .unwrap_or_else(|| display_name_for_jid(input.contact_id));
    let entry = inner
        .contacts
        .entry(input.contact_id.to_string())
        .or_insert_with(|| ContactRecord {
            id: input.contact_id.to_string(),
            canonical_jid: (!input.contact_id.ends_with("@lid"))
                .then(|| input.contact_id.to_string()),
            lid: input
                .contact_id
                .ends_with("@lid")
                .then(|| input.contact_id.to_string()),
            project_id: input.project_id.to_string(),
            company_id: input.company_id.to_string(),
            channel_account_id: input.channel_id.map(str::to_string),
            phone_e164: phone_e164.clone(),
            push_name: input.push_name.map(str::to_string),
            display_name: display_name.clone(),
            profile_picture_media_id: None,
            business_description: None,
            avatar_url: input.profile_picture_url.clone(),
            profile_picture_url: input.profile_picture_url.clone(),
            first_contact_at: input.now,
            last_contact_at: input.now,
        });
    entry.project_id = input.project_id.to_string();
    entry.company_id = input.company_id.to_string();
    if let Some(channel_id) = input.channel_id {
        entry.channel_account_id = Some(channel_id.to_string());
    }
    entry.last_contact_at = input.now;
    if phone_e164.is_some() {
        entry.phone_e164 = phone_e164;
    }
    if let Some(push_name) = input.push_name.filter(|value| !value.trim().is_empty()) {
        entry.push_name = Some(push_name.to_string());
        entry.display_name = push_name.to_string();
    } else if entry.display_name == entry.id {
        entry.display_name = display_name;
    }
    if input.profile_picture_url.is_some() {
        entry.avatar_url = input.profile_picture_url.clone();
        entry.profile_picture_url = input.profile_picture_url;
    }
    entry.clone()
}

fn upsert_contact_with_alt_inner(
    inner: &mut StoreInner,
    input: ContactUpsertWithAltInput<'_>,
) -> ContactRecord {
    if let Some(profile) = contact_profile_from_alt(input.contact.contact_id, input.alt_jid) {
        let mut profile = profile;
        if input.contact.profile_picture_url.is_some() {
            profile.profile_picture_url = input.contact.profile_picture_url.clone();
        }
        enrich_contact_inner(
            inner,
            ContactEnrichmentInput {
                project_id: input.contact.project_id,
                company_id: input.contact.company_id,
                channel_id: input.contact.channel_id,
                contact_id: input.contact.contact_id,
                push_name: input.contact.push_name,
                profile: &profile,
                now: input.contact.now,
            },
        )
    } else {
        upsert_contact_inner(inner, input.contact)
    }
}

fn contact_profile_from_alt(contact_id: &str, alt_jid: Option<&str>) -> Option<ContactProfile> {
    let alt_jid = alt_jid
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| *value != contact_id)?;
    let canonical_jid = [contact_id, alt_jid]
        .into_iter()
        .find(|jid| phone_from_jid(jid).is_some())
        .map(str::to_string);
    let lid = [contact_id, alt_jid]
        .into_iter()
        .find(|jid| jid.ends_with("@lid"))
        .map(str::to_string);
    let phone_e164 = canonical_jid
        .as_deref()
        .and_then(phone_from_jid)
        .or_else(|| phone_from_jid(contact_id))
        .or_else(|| phone_from_jid(alt_jid));
    if canonical_jid.is_none() && lid.is_none() && phone_e164.is_none() {
        return None;
    }
    Some(ContactProfile {
        requested_jid: contact_id.to_string(),
        resolved_jid: canonical_jid,
        lid,
        phone_e164,
        business_description: None,
        profile_picture_id: None,
        profile_picture_url: None,
    })
}

fn merge_contact_profile_alt(mut profile: ContactProfile, alt_jid: Option<&str>) -> ContactProfile {
    if let Some(fallback) = contact_profile_from_alt(&profile.requested_jid, alt_jid) {
        if profile.resolved_jid.is_none() {
            profile.resolved_jid = fallback.resolved_jid;
        }
        if profile.lid.is_none() {
            profile.lid = fallback.lid;
        }
        if profile.phone_e164.is_none() {
            profile.phone_e164 = fallback.phone_e164;
        }
    }
    profile
}

fn enrich_contact_inner(
    inner: &mut StoreInner,
    input: ContactEnrichmentInput<'_>,
) -> ContactRecord {
    let profile_picture_url = input.profile.profile_picture_url.clone();
    upsert_contact_inner(
        inner,
        ContactUpsertInput {
            project_id: input.project_id,
            company_id: input.company_id,
            channel_id: input.channel_id,
            contact_id: input.contact_id,
            push_name: input.push_name,
            profile_picture_url: profile_picture_url.clone(),
            now: input.now,
        },
    );

    let canonical_jid = input
        .profile
        .resolved_jid
        .clone()
        .filter(|jid| !jid.ends_with("@lid"))
        .or_else(|| (!input.contact_id.ends_with("@lid")).then(|| input.contact_id.to_string()));
    let lid = input.profile.lid.clone().or_else(|| {
        input
            .contact_id
            .ends_with("@lid")
            .then(|| input.contact_id.to_string())
    });
    let phone_e164 = input
        .profile
        .phone_e164
        .clone()
        .or_else(|| canonical_jid.as_deref().and_then(phone_from_jid));

    let enriched = {
        let entry = inner
            .contacts
            .get_mut(input.contact_id)
            .expect("contact inserted before enrichment");
        entry.canonical_jid = canonical_jid.clone();
        entry.lid = lid.clone();
        if phone_e164.is_some() {
            entry.phone_e164 = phone_e164.clone();
        }
        if let Some(profile_picture_url) = profile_picture_url.clone() {
            entry.avatar_url = Some(profile_picture_url.clone());
            entry.profile_picture_url = Some(profile_picture_url);
        }
        if let Some(description) = input
            .profile
            .business_description
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            entry.business_description = Some(description.to_string());
        }
        if let Some(push_name) = input.push_name.filter(|value| !value.trim().is_empty()) {
            entry.push_name = Some(push_name.to_string());
            entry.display_name = push_name.to_string();
        } else if (entry.display_name == entry.id
            || entry.display_name.ends_with("@lid")
            || entry.display_name == display_name_for_jid(input.contact_id))
            && let Some(phone_e164) = phone_e164.as_deref()
        {
            entry.display_name = phone_e164.to_string();
        }
        entry.last_contact_at = input.now;
        entry.clone()
    };

    for alias in [canonical_jid, lid].into_iter().flatten() {
        if alias != input.contact_id {
            let mut alias_record = enriched.clone();
            alias_record.id = alias.clone();
            inner.contacts.insert(alias, alias_record);
        }
    }

    enriched
}

fn contact_aliases(contact: &ContactRecord, profile: &ContactProfile) -> Vec<String> {
    let mut aliases = vec![contact.id.clone(), profile.requested_jid.clone()];
    if let Some(canonical_jid) = contact.canonical_jid.clone() {
        aliases.push(canonical_jid);
    }
    if let Some(lid) = contact.lid.clone() {
        aliases.push(lid);
    }
    if let Some(resolved_jid) = profile.resolved_jid.clone() {
        aliases.push(resolved_jid);
    }
    aliases.sort();
    aliases.dedup();
    aliases
}

fn contact_aliases_for_record(contact: &ContactRecord) -> Vec<String> {
    let mut aliases = vec![contact.id.clone()];
    if let Some(canonical_jid) = contact.canonical_jid.clone() {
        aliases.push(canonical_jid);
    }
    if let Some(lid) = contact.lid.clone() {
        aliases.push(lid);
    }
    aliases.sort();
    aliases.dedup();
    aliases
}

fn contact_aliases_for_inner(
    inner: &StoreInner,
    project_id: &str,
    company_id: &str,
    contact_id: &str,
) -> ApiResult<Vec<String>> {
    let contact_id = resolve_contact_id_inner(inner, project_id, company_id, contact_id);
    inner
        .contacts
        .get(&contact_id)
        .filter(|contact| contact.project_id == project_id && contact.company_id == company_id)
        .map(contact_aliases_for_record)
        .ok_or_else(|| ApiError::NotFound(format!("contact {contact_id} not found")))
}

fn contact_id_for_phone_number_inner(
    inner: &StoreInner,
    project_id: &str,
    company_id: &str,
    phone_number: &str,
) -> Option<String> {
    let mut ids: Vec<_> = inner
        .contacts
        .values()
        .filter(|contact| {
            contact.project_id == project_id
                && contact.company_id == company_id
                && contact_phone_number(contact).as_deref() == Some(phone_number)
        })
        .map(|contact| contact.id.clone())
        .collect();
    ids.sort_by_key(|id| (!id.ends_with("@lid"), id.clone()));
    ids.into_iter().next()
}

fn resolve_contact_id_inner(
    inner: &StoreInner,
    project_id: &str,
    company_id: &str,
    contact_id: &str,
) -> String {
    let trimmed = contact_id.trim();
    if inner
        .contacts
        .get(trimmed)
        .is_some_and(|contact| contact.project_id == project_id && contact.company_id == company_id)
    {
        return trimmed.to_string();
    }
    let phone_number = phone_number_from_jid(trimmed).or_else(|| {
        (!trimmed.contains('@'))
            .then(|| phone_number_from_value(trimmed))
            .flatten()
    });
    if let Some(phone_number) = phone_number
        && let Some(contact_id) =
            contact_id_for_phone_number_inner(inner, project_id, company_id, &phone_number)
    {
        return contact_id;
    }
    trimmed.to_string()
}

fn connected_channel_for_project_company(
    inner: &StoreInner,
    project_id: &str,
    company_id: &str,
) -> Option<String> {
    inner.channels.iter().find_map(|(id, channel)| {
        (channel.get("project_id").and_then(Value::as_str) == Some(project_id)
            && channel.get("company_id").and_then(Value::as_str) == Some(company_id)
            && channel.get("status").and_then(Value::as_str) == Some("connected"))
        .then(|| id.clone())
    })
}

fn apply_contact_to_conversation(
    conversation: &mut crate::models::Conversation,
    contact: &ContactRecord,
) {
    conversation.contact_id = Some(contact.id.clone());
    conversation.display_name = Some(contact.display_name.clone());
    if contact.phone_e164.is_some() {
        conversation.display_phone = contact.phone_e164.clone();
    }
    if let Some(phone_number) = contact_phone_number(contact) {
        conversation.phone_number = Some(phone_number);
    }
    if contact.avatar_url.is_some() {
        conversation.avatar_url = contact.avatar_url.clone();
    }
    if contact.profile_picture_url.is_some() {
        conversation.profile_picture_url = contact.profile_picture_url.clone();
    }
    conversation.updated_at = OffsetDateTime::now_utc();
}

fn group_profile_refresh_key(runtime: &ChannelRuntime, group_id: &str) -> String {
    format!(
        "{}|{}|{}|{}",
        runtime.project_id, runtime.company_id, runtime.channel_id, group_id
    )
}

fn clean_group_subject(subject: Option<&str>) -> Option<String> {
    subject
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn clean_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn empty_group_record() -> GroupRecord {
    GroupRecord {
        wa_jid: None,
        subject: None,
        description: None,
        owner_jid: None,
        subject_owner_jid: None,
        profile_picture_media_id: None,
        avatar_url: None,
        profile_picture_url: None,
        created_at_wa: None,
        members_count: None,
        admins_count: None,
    }
}

fn group_conversation(
    project_id: &str,
    company_id: &str,
    channel_id: &str,
    group_id: &str,
) -> crate::models::Conversation {
    let now = OffsetDateTime::now_utc();
    crate::models::Conversation {
        id: group_id.to_string(),
        project_id: project_id.to_string(),
        company_id: company_id.to_string(),
        channel_account_id: channel_id.to_string(),
        conversation_type: "group".to_string(),
        contact_id: None,
        group_id: Some(group_id.to_string()),
        display_name: Some(group_subject_for_jid(group_id)),
        display_phone: None,
        phone_number: None,
        avatar_url: None,
        profile_picture_url: None,
        last_seq: 0,
        last_message_at: None,
        unread_count: 0,
        is_archived: false,
        is_muted: false,
        is_pinned: false,
        control_mode: "manual".to_string(),
        created_at: now,
        updated_at: now,
    }
}

fn apply_group_record_to_conversation(
    conversation: &mut crate::models::Conversation,
    group_id: &str,
    group: Option<&GroupRecord>,
) {
    conversation.conversation_type = "group".to_string();
    conversation.contact_id = None;
    conversation.group_id = Some(group_id.to_string());
    conversation.display_phone = None;
    conversation.phone_number = None;
    conversation.display_name = Some(
        group
            .and_then(|group| group.subject.clone())
            .unwrap_or_else(|| group_subject_for_jid(group_id)),
    );
    conversation.avatar_url = group.and_then(|group| group.avatar_url.clone());
    conversation.profile_picture_url = group.and_then(|group| group.profile_picture_url.clone());
}

fn group_participant_display_name(
    inner: &StoreInner,
    participant: &GroupParticipantProfile,
) -> String {
    inner
        .contacts
        .get(&participant.contact_id)
        .map(|contact| contact.display_name.clone())
        .or_else(|| participant.phone_jid.as_deref().and_then(phone_from_jid))
        .unwrap_or_else(|| display_name_for_jid(&participant.contact_id))
}

fn upsert_group_member_inner(
    inner: &mut StoreInner,
    group_id: &str,
    contact_id: &str,
    display_name: &str,
    now: OffsetDateTime,
) {
    inner
        .group_members
        .entry(group_id.to_string())
        .or_default()
        .entry(contact_id.to_string())
        .and_modify(|member| {
            member.wa_jid = Some(contact_id.to_string());
            member.phone_e164 = phone_from_jid(contact_id);
            member.display_name = display_name.to_string();
            member.updated_at = now;
            member.last_seen_at = now;
        })
        .or_insert_with(|| GroupMemberRecord {
            group_id: group_id.to_string(),
            contact_id: contact_id.to_string(),
            wa_jid: Some(contact_id.to_string()),
            phone_e164: phone_from_jid(contact_id),
            display_name: display_name.to_string(),
            role: "member".to_string(),
            is_admin: false,
            joined_at: now,
            updated_at: now,
            last_seen_at: now,
        });
}

fn upsert_group_member_metadata_inner(
    inner: &mut StoreInner,
    group_id: &str,
    contact_id: &str,
    phone_jid: Option<&str>,
    display_name: &str,
    owner_jid: Option<&str>,
    is_admin: bool,
    now: OffsetDateTime,
) {
    let role = if owner_jid == Some(contact_id) {
        "owner"
    } else if is_admin {
        "admin"
    } else {
        "member"
    };
    inner
        .group_members
        .entry(group_id.to_string())
        .or_default()
        .entry(contact_id.to_string())
        .and_modify(|member| {
            member.wa_jid = Some(contact_id.to_string());
            member.phone_e164 = phone_jid
                .and_then(phone_from_jid)
                .or_else(|| phone_from_jid(contact_id));
            if should_replace_group_member_name(&member.display_name, contact_id) {
                member.display_name = display_name.to_string();
            }
            member.role = role.to_string();
            member.is_admin = is_admin || role == "owner";
            member.updated_at = now;
            member.last_seen_at = now;
        })
        .or_insert_with(|| GroupMemberRecord {
            group_id: group_id.to_string(),
            contact_id: contact_id.to_string(),
            wa_jid: Some(contact_id.to_string()),
            phone_e164: phone_jid
                .and_then(phone_from_jid)
                .or_else(|| phone_from_jid(contact_id)),
            display_name: display_name.to_string(),
            role: role.to_string(),
            is_admin: is_admin || role == "owner",
            joined_at: now,
            updated_at: now,
            last_seen_at: now,
        });
}

fn should_replace_group_member_name(current: &str, contact_id: &str) -> bool {
    let current = current.trim();
    current.is_empty()
        || current == contact_id
        || current == display_name_for_jid(contact_id)
        || current.ends_with("@lid")
        || current.ends_with("@s.whatsapp.net")
}

fn phone_to_jid(phone_e164: &str) -> String {
    let digits: String = phone_e164
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        phone_e164.to_string()
    } else {
        format!("{digits}@s.whatsapp.net")
    }
}

fn phone_number_from_value(value: &str) -> Option<String> {
    let digits: String = value.chars().filter(|ch| ch.is_ascii_digit()).collect();
    (!digits.is_empty()).then_some(digits)
}

fn phone_from_jid(jid: &str) -> Option<String> {
    let local = jid.split('@').next().unwrap_or(jid);
    if jid.ends_with("@s.whatsapp.net") && local.chars().all(|ch| ch.is_ascii_digit()) {
        Some(format!("+{local}"))
    } else {
        None
    }
}

fn phone_number_from_jid(jid: &str) -> Option<String> {
    let local = jid.split('@').next().unwrap_or(jid);
    if jid.ends_with("@s.whatsapp.net") && local.chars().all(|ch| ch.is_ascii_digit()) {
        Some(local.to_string())
    } else {
        None
    }
}

fn phone_number_from_e164(phone_e164: &str) -> Option<String> {
    phone_number_from_value(phone_e164)
}

fn contact_phone_number(contact: &ContactRecord) -> Option<String> {
    contact
        .phone_e164
        .as_deref()
        .and_then(phone_number_from_e164)
        .or_else(|| {
            contact
                .canonical_jid
                .as_deref()
                .and_then(phone_number_from_jid)
        })
        .or_else(|| phone_number_from_jid(&contact.id))
}

fn display_name_for_jid(jid: &str) -> String {
    phone_from_jid(jid).unwrap_or_else(|| jid.split('@').next().unwrap_or(jid).to_string())
}

fn group_subject_for_jid(jid: &str) -> String {
    let short = jid.split('@').next().unwrap_or(jid);
    format!("Grupo {short}")
}

fn normalize_media_type(value: &str) -> ApiResult<String> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "image" | "audio" | "document" => Ok(normalized),
        "file" | "application" => Ok("document".to_string()),
        "" => Err(ApiError::BadRequest("media type is empty".to_string())),
        other => Err(ApiError::BadRequest(format!(
            "unsupported outbound media type {other}"
        ))),
    }
}

fn infer_media_type(mime_type: &str) -> String {
    if mime_type.starts_with("image/") {
        "image".to_string()
    } else if mime_type.starts_with("audio/") {
        "audio".to_string()
    } else {
        "document".to_string()
    }
}

fn normalize_inbound_media_type(value: &str, mime_type: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "image" | "audio" | "video" | "document" => value.trim().to_ascii_lowercase(),
        "file" | "application" => "document".to_string(),
        _ if mime_type.starts_with("video/") => "video".to_string(),
        _ => infer_media_type(mime_type),
    }
}

fn descriptor_sha256_hex(descriptor: &InboundMediaDescriptor) -> String {
    BASE64_STANDARD
        .decode(descriptor.file_sha256_b64.as_bytes())
        .map(hex::encode)
        .unwrap_or_default()
}

fn file_extension(filename: &str) -> Option<&str> {
    filename
        .rsplit_once('.')
        .map(|(_, ext)| ext.trim())
        .filter(|ext| !ext.is_empty() && ext.len() <= 12)
}

fn extension_for_mime(mime_type: &str) -> &'static str {
    match mime_type.split(';').next().unwrap_or(mime_type).trim() {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/svg+xml" => "svg",
        "audio/ogg" => "ogg",
        "audio/webm" => "webm",
        "audio/mpeg" => "mp3",
        "audio/mp4" => "m4a",
        "video/mp4" => "mp4",
        "video/quicktime" => "mov",
        "application/pdf" => "pdf",
        "text/plain" => "txt",
        _ => "bin",
    }
}

pub fn dev_preview_svg(media: &MediaObject, message: &Message) -> String {
    let title = media
        .original_filename
        .as_deref()
        .unwrap_or(media.media_type.as_str());
    let subtitle = message
        .text
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or(media.mime_type.as_str());
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 540" role="img" aria-label="{title}">
<defs><linearGradient id="g" x1="0" x2="1" y1="0" y2="1"><stop stop-color="#128c7e"/><stop offset="1" stop-color="#f5c542"/></linearGradient></defs>
<rect width="960" height="540" fill="url(#g)"/>
<rect x="72" y="72" width="816" height="396" rx="24" fill="rgba(255,255,255,.82)"/>
<circle cx="214" cy="222" r="74" fill="#075e54"/>
<path d="M332 352 452 236l82 72 70-62 142 106z" fill="#25d366"/>
<text x="92" y="438" font-family="Arial, sans-serif" font-size="34" font-weight="700" fill="#111b21">{title}</text>
<text x="92" y="484" font-family="Arial, sans-serif" font-size="24" fill="#34443f">{subtitle}</text>
</svg>"##,
        title = xml_escape(title),
        subtitle = xml_escape(subtitle)
    )
}

fn dev_media_bytes(media: &MediaObject, message: &Message) -> Vec<u8> {
    let mime_type = media
        .mime_type
        .split(';')
        .next()
        .unwrap_or(&media.mime_type)
        .trim();
    match mime_type {
        "image/png" => BASE64_STANDARD
            .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=")
            .unwrap_or_else(|_| dev_preview_svg(media, message).into_bytes()),
        "image/svg+xml" => dev_preview_svg(media, message).into_bytes(),
        "audio/ogg" | "audio/webm" => {
            let mut bytes = b"OggS\0\x02rustzap-dev-audio:".to_vec();
            bytes.extend_from_slice(media.id.as_bytes());
            bytes
        }
        "application/pdf" => format!(
            "%PDF-1.4\n% RustZap dev media {}\n1 0 obj<</Type/Catalog>>endobj\n%%EOF\n",
            media.id
        )
        .into_bytes(),
        _ => format!("RustZap dev media {}\nmessage={}\n", media.id, message.id).into_bytes(),
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn contact_json(contact: &ContactRecord) -> Value {
    let phone_number = contact_phone_number(contact);
    json!({
        "id": phone_number.as_deref().unwrap_or(&contact.id),
        "technical_id": contact.id,
        "canonical_jid": contact.canonical_jid,
        "lid": contact.lid,
        "phone_e164": contact.phone_e164,
        "phone_number": phone_number,
        "push_name": contact.push_name,
        "display_name": contact.display_name,
        "profile_picture_media_id": contact.profile_picture_media_id,
        "business_description": contact.business_description,
        "avatar_url": contact.avatar_url,
        "profile_picture_url": contact.profile_picture_url,
        "first_contact_at": ts(contact.first_contact_at),
        "last_contact_at": ts(contact.last_contact_at)
    })
}

fn group_json(inner: &StoreInner, conversation: &crate::models::Conversation) -> Value {
    let members = inner
        .group_members
        .get(&conversation.id)
        .map_or(0, HashMap::len);
    let admins = inner
        .group_members
        .get(&conversation.id)
        .map(|members| members.values().filter(|member| member.is_admin).count())
        .unwrap_or_default();
    let group = inner.groups.get(&conversation.id);
    let subject = group
        .and_then(|group| group.subject.clone())
        .unwrap_or_else(|| group_subject_for_jid(&conversation.id));
    let description = group.and_then(|group| group.description.clone());
    let owner_jid = group.and_then(|group| group.owner_jid.clone());
    let subject_owner_jid = group.and_then(|group| group.subject_owner_jid.clone());
    let created_at_wa = group.and_then(|group| group.created_at_wa.map(ts));
    let profile_picture_media_id = group.and_then(|group| group.profile_picture_media_id.clone());
    let avatar_url = group.and_then(|group| group.avatar_url.clone());
    let profile_picture_url = group.and_then(|group| group.profile_picture_url.clone());
    json!({
        "id": conversation.id,
        "subject": subject,
        "description": description,
        "owner_jid": owner_jid,
        "subject_owner_jid": subject_owner_jid,
        "created_at_wa": created_at_wa,
        "members_count": group
            .and_then(|group| group.members_count)
            .unwrap_or_else(|| u32::try_from(members).unwrap_or(u32::MAX)),
        "admins_count": group
            .and_then(|group| group.admins_count)
            .unwrap_or_else(|| u32::try_from(admins).unwrap_or(u32::MAX)),
        "last_message_at": conversation.last_message_at.map(ts),
        "conversation_id": conversation.id,
        "profile_picture_media_id": profile_picture_media_id,
        "avatar_url": avatar_url,
        "profile_picture_url": profile_picture_url
    })
}

fn is_updates_event_surface(
    chat_jid: &str,
    sender_jid: &str,
    sender_alt_jid: Option<&str>,
    recipient_jid: Option<&str>,
    recipient_alt_jid: Option<&str>,
) -> bool {
    is_updates_surface_jid(chat_jid)
        || is_updates_surface_jid(sender_jid)
        || sender_alt_jid.is_some_and(is_updates_surface_jid)
        || recipient_jid.is_some_and(is_updates_surface_jid)
        || recipient_alt_jid.is_some_and(is_updates_surface_jid)
}

fn conversation_matches_tenant(
    conversation: &crate::models::Conversation,
    project_id: &str,
    company_id: &str,
) -> bool {
    conversation.project_id == project_id && conversation.company_id == company_id
}

fn conversation_phone_number_for_inner(
    inner: &StoreInner,
    conversation: &crate::models::Conversation,
) -> Option<String> {
    if conversation.conversation_type == "group" {
        return None;
    }
    conversation
        .phone_number
        .clone()
        .or_else(|| {
            conversation
                .display_phone
                .as_deref()
                .and_then(phone_number_from_e164)
        })
        .or_else(|| phone_number_from_jid(&conversation.id))
        .or_else(|| {
            conversation
                .contact_id
                .as_deref()
                .and_then(|contact_id| inner.contacts.get(contact_id))
                .and_then(contact_phone_number)
        })
        .or_else(|| {
            inner
                .contacts
                .get(&conversation.id)
                .and_then(contact_phone_number)
        })
}

fn apply_public_conversation_fields(
    inner: &StoreInner,
    conversation: &mut crate::models::Conversation,
) {
    if conversation.conversation_type == "group" {
        conversation.phone_number = None;
        return;
    }
    if let Some(phone_number) = conversation_phone_number_for_inner(inner, conversation) {
        if conversation.display_phone.is_none() {
            conversation.display_phone = Some(format!("+{phone_number}"));
        }
        conversation.phone_number = Some(phone_number);
    }
}

fn public_conversation_id(conversation: &crate::models::Conversation) -> String {
    conversation
        .phone_number
        .clone()
        .unwrap_or_else(|| conversation.id.clone())
}

fn conversation_id_for_phone_number_inner(
    inner: &StoreInner,
    project_id: &str,
    company_id: &str,
    phone_number: &str,
) -> Option<String> {
    let mut aliases = Vec::new();
    for contact in inner.contacts.values().filter(|contact| {
        contact.project_id == project_id
            && contact.company_id == company_id
            && contact_phone_number(contact).as_deref() == Some(phone_number)
    }) {
        if let Some(lid) = contact.lid.as_deref() {
            aliases.push(lid.to_string());
        }
        if let Some(canonical_jid) = contact.canonical_jid.as_deref() {
            aliases.push(canonical_jid.to_string());
        }
        aliases.push(contact.id.clone());
    }
    aliases.push(phone_to_jid(phone_number));
    aliases.sort();
    aliases.dedup();
    aliases.into_iter().find(|alias| {
        inner.conversations.get(alias).is_some_and(|conversation| {
            conversation_matches_tenant(conversation, project_id, company_id)
        })
    })
}

fn resolve_conversation_id_inner(
    inner: &StoreInner,
    project_id: &str,
    company_id: &str,
    conversation_id: &str,
) -> String {
    let trimmed = conversation_id.trim();
    if inner
        .conversations
        .get(trimmed)
        .is_some_and(|conversation| {
            conversation_matches_tenant(conversation, project_id, company_id)
        })
    {
        return trimmed.to_string();
    }

    let phone_number = phone_number_from_jid(trimmed).or_else(|| {
        (!trimmed.contains('@'))
            .then(|| phone_number_from_value(trimmed))
            .flatten()
    });
    if let Some(phone_number) = phone_number {
        return conversation_id_for_phone_number_inner(
            inner,
            project_id,
            company_id,
            &phone_number,
        )
        .unwrap_or_else(|| phone_to_jid(&phone_number));
    }

    trimmed.to_string()
}

fn conversation_for_inner<'a>(
    inner: &'a StoreInner,
    project_id: &str,
    company_id: &str,
    conversation_id: &str,
) -> ApiResult<&'a crate::models::Conversation> {
    let conversation_id =
        resolve_conversation_id_inner(inner, project_id, company_id, conversation_id);
    inner
        .conversations
        .get(&conversation_id)
        .filter(|conversation| conversation_matches_tenant(conversation, project_id, company_id))
        .ok_or_else(|| ApiError::NotFound("conversation not found".to_string()))
}

fn group_member_values_for_inner(inner: &StoreInner, group_id: &str) -> Vec<Value> {
    let mut members: Vec<GroupMemberRecord> = inner
        .group_members
        .get(group_id)
        .map(|members| members.values().cloned().collect())
        .unwrap_or_default();
    members.sort_by(|a: &GroupMemberRecord, b: &GroupMemberRecord| {
        a.display_name
            .to_lowercase()
            .cmp(&b.display_name.to_lowercase())
            .then_with(|| a.contact_id.cmp(&b.contact_id))
    });
    members.iter().map(group_member_json).collect()
}

fn group_member_json(member: &GroupMemberRecord) -> Value {
    let phone_number = member
        .phone_e164
        .as_deref()
        .and_then(phone_number_from_e164)
        .or_else(|| member.wa_jid.as_deref().and_then(phone_number_from_jid))
        .or_else(|| phone_number_from_jid(&member.contact_id));
    json!({
        "group_id": member.group_id,
        "contact_id": phone_number.as_deref().unwrap_or(&member.contact_id),
        "technical_contact_id": member.contact_id,
        "wa_jid": member.wa_jid,
        "phone_e164": member.phone_e164,
        "phone_number": phone_number,
        "name": member.display_name,
        "display_name": member.display_name,
        "role": member.role,
        "is_admin": member.is_admin,
        "joined_at": ts(member.joined_at),
        "updated_at": ts(member.updated_at),
        "last_seen_at": ts(member.last_seen_at)
    })
}

#[allow(clippy::too_many_arguments)]
fn mark_dirty_inner(
    inner: &mut StoreInner,
    project_id: &str,
    company_id: &str,
    conversation_id: &str,
    max_seq: i64,
    channel_id: &str,
    events_tx: Option<&broadcast::Sender<crate::models::CommonEvent>>,
    event_bus: Option<&EventBusHandle>,
) {
    let now = OffsetDateTime::now_utc();
    let key = (
        project_id.to_string(),
        company_id.to_string(),
        conversation_id.to_string(),
    );
    inner
        .dirty
        .entry(key)
        .and_modify(|dirty| {
            dirty.max_seq = dirty.max_seq.max(max_seq);
            dirty.updated(now);
        })
        .or_insert_with(|| DirtyRecord {
            conversation_id: conversation_id.to_string(),
            max_seq,
            reason: "new_message".to_string(),
            priority: 100,
            available_at: now,
            lease_token: None,
            locked_until: None,
        });
    push_event_inner(
        inner,
        events_tx,
        event_bus,
        dirty_signal(
            project_id,
            company_id,
            channel_id,
            conversation_id,
            max_seq,
            "new_message",
            100,
        ),
    );
}

fn push_event_inner(
    inner: &mut StoreInner,
    events_tx: Option<&broadcast::Sender<crate::models::CommonEvent>>,
    event_bus: Option<&EventBusHandle>,
    event: crate::models::CommonEvent,
) {
    if let Some(events_tx) = events_tx {
        let _ = events_tx.send(event.clone());
    }
    if let Some(event_bus) = event_bus {
        event_bus.publish_background(event.clone());
    }
    inner.events.push(event);
}

fn internal_work_event_type(event_type: &str) -> bool {
    matches!(
        event_type,
        "outbound.send.requested" | "audio.transcription.requested"
    )
}

fn sanitize_callback(callback: &Value) -> Value {
    let mut sanitized = callback.clone();
    if let Some(object) = sanitized.as_object_mut() {
        object.remove("secret");
        object.remove("encrypted_secret");
    }
    sanitized
}

fn callback_belongs_to_company(callback: &Value, project_id: &str, company_id: &str) -> bool {
    callback.get("project_id").and_then(Value::as_str) == Some(project_id)
        && callback.get("company_id").and_then(Value::as_str) == Some(company_id)
}

fn validate_callback_url(url: &str, dev_mode: bool) -> ApiResult<()> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|err| ApiError::BadRequest(format!("invalid callback url: {err}")))?;
    match parsed.scheme() {
        "https" => {}
        "http" if dev_mode => {}
        "http" => {
            return Err(ApiError::BadRequest(
                "callback url must use https outside dev mode".to_string(),
            ));
        }
        _ => {
            return Err(ApiError::BadRequest(
                "callback url scheme must be https".to_string(),
            ));
        }
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| ApiError::BadRequest("callback url must include a host".to_string()))?;
    if callback_host_is_localhost(host) && !dev_mode {
        return Err(ApiError::BadRequest(
            "callback url host is not allowed outside dev mode".to_string(),
        ));
    }
    if let Ok(ip) = host.parse::<IpAddr>()
        && callback_ip_is_disallowed(ip, dev_mode)
    {
        return Err(ApiError::BadRequest(
            "callback url host is not allowed outside dev mode".to_string(),
        ));
    }
    Ok(())
}

fn callback_host_is_localhost(host: &str) -> bool {
    let normalized = host.trim_end_matches('.').to_ascii_lowercase();
    normalized == "localhost" || normalized.ends_with(".localhost")
}

fn callback_ip_is_disallowed(ip: IpAddr, dev_mode: bool) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_multicast()
                || (!dev_mode && (ip.is_loopback() || ip.is_private() || ip.is_link_local()))
        }
        IpAddr::V6(ip) => {
            let first_segment = ip.segments()[0];
            let unique_local = (first_segment & 0xfe00) == 0xfc00;
            let link_local = (first_segment & 0xffc0) == 0xfe80;
            ip.is_unspecified()
                || ip.is_multicast()
                || (!dev_mode && (ip.is_loopback() || unique_local || link_local))
        }
    }
}

fn callback_subscribes_to_event(callback: &Value, event_type: &str) -> bool {
    callback
        .get("events")
        .and_then(Value::as_array)
        .is_none_or(|events| {
            events.iter().any(|event| {
                event
                    .as_str()
                    .is_some_and(|name| name == "*" || name == event_type)
            })
        })
}

fn compact_webhook_event(event: &crate::models::CommonEvent) -> Value {
    json!({
        "event_id": event.event_id,
        "event_type": event.event_type,
        "project_id": event.project_id,
        "company_id": event.company_id,
        "channel_id": event.channel_id,
        "conversation_id": event.conversation_id,
        "message_id": event.message_id,
        "conversation_seq": event.conversation_seq,
        "occurred_at": ts(event.occurred_at),
        "payload": compact_payload(&event.payload)
    })
}

fn compact_payload(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut compact = serde_json::Map::new();
            for (key, value) in object {
                if matches!(
                    key.as_str(),
                    "text" | "caption" | "bytes" | "raw_response_json" | "transcript" | "media"
                ) {
                    continue;
                }
                compact.insert(key.clone(), compact_payload(value));
            }
            Value::Object(compact)
        }
        Value::Array(items) => Value::Array(items.iter().map(compact_payload).collect()),
        other => other.clone(),
    }
}

fn next_webhook_retry_at(
    attempt: u32,
    retry_base_seconds: u64,
    retry_max_seconds: u64,
) -> OffsetDateTime {
    let exponent = attempt.saturating_sub(1).min(16);
    let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let delay = retry_base_seconds
        .saturating_mul(multiplier)
        .min(retry_max_seconds.max(1));
    OffsetDateTime::now_utc() + Duration::seconds(i64::try_from(delay).unwrap_or(i64::MAX))
}

impl DirtyRecord {
    fn updated(&mut self, now: OffsetDateTime) {
        self.available_at = self.available_at.min(now);
    }
}

fn ts(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).expect("RFC3339 format works")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::config::AppConfig;

    #[test]
    fn idempotency_replays_same_hash_and_conflicts_on_different_hash() {
        let state = AppState::new(AppConfig::from_env());
        let req = SendMessageRequest {
            message_type: "text".to_string(),
            text: Some("oi".to_string()),
            media_id: None,
            caption: None,
            filename: None,
            quoted_message_id: None,
            metadata: None,
        };
        let first = state
            .send_message("p", "c", "conv", "idem-1", req.clone())
            .unwrap();
        let replay = state.send_message("p", "c", "conv", "idem-1", req).unwrap();
        assert_eq!(first.id, replay.id);

        let changed = SendMessageRequest {
            message_type: "text".to_string(),
            text: Some("mudou".to_string()),
            media_id: None,
            caption: None,
            filename: None,
            quoted_message_id: None,
            metadata: None,
        };
        let err = state
            .send_message("p", "c", "conv", "idem-1", changed)
            .unwrap_err();
        assert!(matches!(err, ApiError::IdempotencyConflict(_)));
    }

    #[test]
    fn prepare_send_message_enqueues_outbound_send_request() {
        let state = AppState::new(AppConfig::from_env());
        let req = SendMessageRequest {
            message_type: "text".to_string(),
            text: Some("oi async".to_string()),
            media_id: None,
            caption: None,
            filename: None,
            quoted_message_id: None,
            metadata: None,
        };

        let outcome = state
            .prepare_send_message("p", "c", "conv", "idem-async", req)
            .unwrap();

        assert!(outcome.should_dispatch);
        assert_eq!(outcome.message.status, "queued");
        let serialized_message = serde_json::to_value(&outcome.message).unwrap();
        assert_eq!(serialized_message["delivery_state"], "pending");
        let events = state.events();
        let queued = events
            .iter()
            .find(|event| {
                event.event_type == "message.queued"
                    && event.message_id.as_deref() == Some(outcome.message.id.as_str())
            })
            .expect("queued message event should be emitted");
        assert_eq!(queued.payload["status"], "queued");
        assert_eq!(queued.payload["delivery_state"], "pending");
        let requested = events
            .iter()
            .find(|event| event.event_type == "outbound.send.requested")
            .expect("outbound send request event should be emitted");
        assert_eq!(
            requested.payload["message_id"].as_str(),
            Some(outcome.message.id.as_str())
        );
        assert!(requested.payload.get("text").is_none());
        assert!(requested.payload.get("idempotency_key").is_none());
        assert!(
            !events.iter().any(|event| {
                event.event_type == "message.sent"
                    && event.message_id.as_deref() == Some(outcome.message.id.as_str())
            }),
            "queued API sends must not emit message.sent before the Kafka worker dispatches them"
        );
    }

    #[tokio::test]
    async fn outbound_send_worker_marks_disconnected_channel_failed_and_emits_terminal_event() {
        let state = AppState::new(AppConfig::from_env());
        let req = SendMessageRequest {
            message_type: "text".to_string(),
            text: Some("worker path".to_string()),
            media_id: None,
            caption: None,
            filename: None,
            quoted_message_id: None,
            metadata: None,
        };

        let outcome = state
            .prepare_send_message("p", "c", "conv_worker", "idem-worker", req)
            .unwrap();
        let event = state
            .events()
            .into_iter()
            .find(|event| {
                event.event_type == "outbound.send.requested"
                    && event.message_id.as_deref() == Some(outcome.message.id.as_str())
            })
            .expect("outbound send event should exist");

        let updated = state.process_outbound_send_request(&event).await.unwrap();

        assert_eq!(updated.status, "failed");
        let serialized_message = serde_json::to_value(&updated).unwrap();
        assert_eq!(serialized_message["delivery_state"], "failed");
        assert!(
            updated
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("not connected"))
        );
        let events = state.events();
        assert!(events.iter().any(|event| {
            event.event_type == "message.failed"
                && event.message_id.as_deref() == Some(outcome.message.id.as_str())
                && event.payload["status"] == "failed"
                && event.payload["delivery_state"] == "failed"
        }));
    }

    #[test]
    fn receipt_updates_delivery_state_and_emits_public_payload() {
        let state = AppState::new(AppConfig::from_env());
        let outcome = state
            .prepare_send_message(
                "p",
                "c",
                "conv_receipt",
                "idem-receipt",
                SendMessageRequest {
                    message_type: "text".to_string(),
                    text: Some("receipt path".to_string()),
                    media_id: None,
                    caption: None,
                    filename: None,
                    quoted_message_id: None,
                    metadata: None,
                },
            )
            .unwrap();

        state
            .update_outbound_after_dispatch(
                &outcome.message.id,
                Some("wa_receipt_1".to_string()),
                "sent_to_whatsapp",
                None,
            )
            .unwrap();
        let updated = state
            .update_receipt_by_wa_id("wa_receipt_1", "played")
            .unwrap();
        assert_eq!(updated.status, "played");
        let serialized_message = serde_json::to_value(&updated).unwrap();
        assert_eq!(serialized_message["delivery_state"], "read");

        let receipt = state
            .events()
            .into_iter()
            .find(|event| {
                event.event_type == "message.receipt"
                    && event.message_id.as_deref() == Some(updated.id.as_str())
            })
            .expect("receipt event should be emitted");
        assert_eq!(receipt.payload["status"], "played");
        assert_eq!(receipt.payload["delivery_state"], "read");
        assert_eq!(receipt.payload["receipt_type"], "played");
    }

    #[tokio::test]
    async fn outbound_send_worker_is_idempotent_after_terminal_status() {
        let state = AppState::new(AppConfig::from_env());
        let outcome = state
            .prepare_send_message(
                "p",
                "c",
                "conv_worker_terminal",
                "idem-worker-terminal",
                SendMessageRequest {
                    message_type: "text".to_string(),
                    text: Some("terminal path".to_string()),
                    media_id: None,
                    caption: None,
                    filename: None,
                    quoted_message_id: None,
                    metadata: None,
                },
            )
            .unwrap();
        let event = state
            .events()
            .into_iter()
            .find(|event| {
                event.event_type == "outbound.send.requested"
                    && event.message_id.as_deref() == Some(outcome.message.id.as_str())
            })
            .unwrap();

        state.process_outbound_send_request(&event).await.unwrap();
        let terminal_events = state
            .events()
            .iter()
            .filter(|event| {
                event.event_type == "message.failed"
                    && event.message_id.as_deref() == Some(outcome.message.id.as_str())
            })
            .count();
        state.process_outbound_send_request(&event).await.unwrap();

        let after_replay = state
            .events()
            .iter()
            .filter(|event| {
                event.event_type == "message.failed"
                    && event.message_id.as_deref() == Some(outcome.message.id.as_str())
            })
            .count();
        assert_eq!(after_replay, terminal_events);
    }

    #[test]
    fn request_transcript_enqueues_pending_audio_transcription_request() {
        let state = AppState::new(AppConfig::from_env());
        let (message, _media, _transcript) = state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some("conv_audio".to_string()),
                channel_id: Some("ch_audio".to_string()),
                from_phone_e164: Some("+15550000000".to_string()),
                sender_name: Some("Audio Sender".to_string()),
                profile_picture_url: None,
                media_type: Some("audio".to_string()),
                mime_type: Some("audio/ogg".to_string()),
                filename: Some("voice.ogg".to_string()),
                caption: None,
                size_bytes: Some(128_000),
            },
        );

        let transcript = state
            .request_transcript("p", "c", &message.id)
            .expect("transcript request should be accepted");

        assert_eq!(transcript.status, "pending");
        let events = state.events();
        let requested = events
            .iter()
            .find(|event| event.event_type == "audio.transcription.requested")
            .expect("audio transcription request should be emitted");
        assert_eq!(requested.message_id.as_deref(), Some(message.id.as_str()));
        assert_eq!(
            requested.payload["transcript_id"].as_str(),
            Some(transcript.id.as_str())
        );
    }

    #[tokio::test]
    async fn downloaded_inbound_audio_media_is_stored_and_queues_transcription() {
        let mut config = AppConfig::from_env();
        config.media_local_temp_dir = std::env::temp_dir().join(format!(
            "rustzap-inbound-media-test-{}",
            Uuid::now_v7().simple()
        ));
        let state = AppState::new(config);
        let runtime = test_runtime();
        let message = state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("conv_downloaded_audio".to_string()),
                channel_id: Some(runtime.channel_id.clone()),
                from_phone_e164: Some("+15550000003".to_string()),
                sender_name: Some("Audio Sender".to_string()),
                profile_picture_url: None,
                text: "voice note".to_string(),
            },
        );
        let bytes = b"OggS\x00\x02rustzap-audio".to_vec();
        let descriptor = InboundMediaDescriptor {
            media_type: "audio".to_string(),
            mime_type: "audio/ogg".to_string(),
            filename: Some("voice.ogg".to_string()),
            caption: None,
            direct_path: "/v/t62/test".to_string(),
            media_key_b64: String::new(),
            file_sha256_b64: String::new(),
            file_enc_sha256_b64: String::new(),
            file_length: bytes.len() as u64,
            width: None,
            height: None,
            duration_seconds: Some(1.5),
        };

        let media = state
            .store_downloaded_inbound_media(
                &runtime,
                &message,
                &message.conversation_id,
                descriptor,
                bytes.clone(),
            )
            .unwrap();

        assert_eq!(media.message_id.as_deref(), Some(message.id.as_str()));
        assert_eq!(media.media_type, "audio");
        assert_eq!(media.storage_status, "temp");
        assert_eq!(media.duration_seconds, Some(1.5));
        let (_stored, stored_bytes) = state.media_blob(&media.id).await.unwrap().unwrap();
        assert_eq!(stored_bytes, bytes);
        let events = state.events();
        assert!(events.iter().any(|event| {
            event.event_type == "media.stored"
                && event.payload["media_id"].as_str() == Some(media.id.as_str())
        }));
        assert!(events.iter().any(|event| {
            event.event_type == "audio.transcription.requested"
                && event.payload["media_id"].as_str() == Some(media.id.as_str())
        }));
    }

    #[tokio::test]
    async fn transcription_worker_preserves_transcript_id_when_status_changes() {
        let state = AppState::new(AppConfig::from_env());
        let (message, _media, _transcript) = state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some("conv_audio_worker".to_string()),
                channel_id: Some("ch_audio_worker".to_string()),
                from_phone_e164: Some("+15550000001".to_string()),
                sender_name: Some("Audio Sender".to_string()),
                profile_picture_url: None,
                media_type: Some("audio".to_string()),
                mime_type: Some("audio/ogg".to_string()),
                filename: Some("voice.ogg".to_string()),
                caption: None,
                size_bytes: Some(128_000),
            },
        );
        let requested = state
            .request_transcript("p", "c", &message.id)
            .expect("transcript request should be accepted");
        let event = state
            .events()
            .into_iter()
            .rev()
            .find(|event| {
                event.event_type == "audio.transcription.requested"
                    && event.message_id.as_deref() == Some(message.id.as_str())
            })
            .expect("transcription event should exist");

        let updated = state.process_transcription_request(&event).await.unwrap();

        assert_eq!(updated.id, requested.id);
        assert_eq!(updated.status, "failed");
    }

    #[tokio::test]
    async fn transcription_worker_does_not_downgrade_completed_transcript_on_replay() {
        let state = AppState::new(AppConfig::from_env());
        let (message, media, _transcript) = state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some("conv_audio_completed".to_string()),
                channel_id: Some("ch_audio_completed".to_string()),
                from_phone_e164: Some("+15550000002".to_string()),
                sender_name: Some("Audio Sender".to_string()),
                profile_picture_url: None,
                media_type: Some("audio".to_string()),
                mime_type: Some("audio/ogg".to_string()),
                filename: Some("voice.ogg".to_string()),
                caption: None,
                size_bytes: Some(128_000),
            },
        );
        let requested = state.request_transcript("p", "c", &message.id).unwrap();
        let event = state
            .events()
            .into_iter()
            .rev()
            .find(|event| {
                event.event_type == "audio.transcription.requested"
                    && event.message_id.as_deref() == Some(message.id.as_str())
            })
            .unwrap();
        let completed = state
            .set_transcript_lifecycle(
                &message,
                &media,
                TranscriptLifecycle::Completed,
                Some("texto final".to_string()),
            )
            .unwrap();
        assert_eq!(completed.id, requested.id);

        let replayed = state.process_transcription_request(&event).await.unwrap();

        assert_eq!(replayed.id, requested.id);
        assert_eq!(replayed.status, "completed");
        assert_eq!(replayed.text.as_deref(), Some("texto final"));
    }

    #[tokio::test]
    async fn raw_whatsapp_worker_persists_inbound_message_after_enqueue() {
        let state = AppState::new(AppConfig::from_env());
        let raw = state
            .enqueue_whatsapp_raw_event(
                test_runtime(),
                WhatsappEvent::Message {
                    wa_message_id: "wa_raw_1".to_string(),
                    chat_jid: "5511999000000@s.whatsapp.net".to_string(),
                    sender_jid: "5511999000000@s.whatsapp.net".to_string(),
                    sender_alt_jid: None,
                    recipient_jid: None,
                    recipient_alt_jid: None,
                    push_name: Some("Raw Sender".to_string()),
                    text: Some("raw inbound".to_string()),
                    message_type: "text".to_string(),
                    media: None,
                    created_at_wa: OffsetDateTime::now_utc(),
                    is_from_me: false,
                },
            )
            .unwrap();
        assert!(state.conversations("p", "c").is_empty());
        assert_eq!(raw.event_type, "whatsapp.raw.inbound");
        assert!(raw.payload["runtime"].get("session_path").is_none());

        state.process_whatsapp_raw_event(&raw).await.unwrap();

        let page = state.list_messages("5511999000000@s.whatsapp.net", None, None, 10);
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.messages[0].text.as_deref(), Some("raw inbound"));
        assert!(state.events().iter().any(|event| {
            event.event_type == "message.received"
                && event.message_id.as_deref() == Some(page.messages[0].id.as_str())
        }));
    }

    #[tokio::test]
    async fn in_memory_dispatcher_processes_outbound_send_requests_after_queueing() {
        let mut config = AppConfig::from_env();
        config.event_bus = EventBusMode::InMemory;
        let state = AppState::from_config(config).await.unwrap();
        let outcome = state
            .prepare_send_message(
                "p",
                "c",
                "conv_local_dispatch",
                "idem-local-dispatch",
                SendMessageRequest {
                    message_type: "text".to_string(),
                    text: Some("queued local dispatch".to_string()),
                    media_id: None,
                    caption: None,
                    filename: None,
                    quoted_message_id: None,
                    metadata: None,
                },
            )
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(75)).await;

        let message = state
            .message_for_company("p", "c", &outcome.message.id)
            .unwrap();
        assert_eq!(message.status, "failed");
        assert!(
            message
                .error_message
                .as_deref()
                .is_some_and(|error| error.contains("not connected"))
        );
    }

    #[tokio::test]
    async fn persisted_store_serializes_media_metadata_without_media_bytes() {
        let mut config = AppConfig::from_env();
        config.media_local_temp_dir = std::env::temp_dir().join(format!(
            "rustzap-no-media-blobs-test-{}",
            Uuid::now_v7().simple()
        ));
        let state = AppState::new(config);
        let media = state
            .upload_outbound_media(
                "p",
                "c",
                OutboundMediaUpload {
                    conversation_id: "conv_bytes".to_string(),
                    media_type: Some("image".to_string()),
                    mime_type: Some("image/png".to_string()),
                    filename: Some("photo.png".to_string()),
                    caption: None,
                    bytes: b"\x89PNG\r\n\x1a\nsensitive-media-bytes".to_vec(),
                },
            )
            .await
            .unwrap();
        let inner = state.inner.lock().expect("store lock poisoned");
        let snapshot = PersistedStore::from_inner(&inner);
        let json = serde_json::to_value(snapshot).unwrap();

        assert!(json.get("media_blobs").is_none());
        assert_eq!(
            json["media"][&media.id]["object_key"].as_str(),
            media.object_key.as_deref()
        );
        assert!(!json.to_string().contains("sensitive-media-bytes"));
    }

    #[test]
    fn persisted_store_accepts_legacy_message_time_arrays_and_rewrites_rfc3339() {
        let message = json!({
            "id": "msg_legacy_time",
            "project_id": "p",
            "company_id": "c",
            "conversation_id": "conv",
            "channel_account_id": "ch",
            "conversation_seq": 1,
            "wa_message_id": null,
            "direction": "inbound",
            "sender_contact_id": null,
            "sender_display_name": null,
            "message_type": "text",
            "text": "oi",
            "media_id": null,
            "media_url": null,
            "thumbnail_url": null,
            "mime_type": null,
            "file_name": null,
            "quoted_message_id": null,
            "status": "received",
            "error_message": null,
            "is_starred": false,
            "is_pinned": false,
            "reaction": null,
            "sent_by_source": null,
            "sent_by_external_user_id": null,
            "created_at_wa": [2026, 133, 17, 37, 21, 0, 0, 0, 0],
            "created_at": [2026, 133, 17, 37, 21, 767582565, 0, 0, 0],
            "updated_at": [2026, 133, 17, 37, 21, 767582565, 0, 0, 0]
        });
        let store: PersistedStore = serde_json::from_value(json!({
            "messages_by_id": {
                "msg_legacy_time": message.clone()
            },
            "messages_by_conversation": {
                "conv": [message]
            }
        }))
        .unwrap();

        let serialized = serde_json::to_value(store).unwrap();
        assert!(serialized["messages_by_id"]["msg_legacy_time"]["created_at"].is_string());
        assert!(serialized["messages_by_conversation"]["conv"][0]["updated_at"].is_string());
    }

    #[test]
    fn persisted_store_creates_placeholder_conversation_for_orphan_media() {
        let now = OffsetDateTime::now_utc();
        let mut inner = StoreInner::default();
        let conversation_id = "5511888777666@s.whatsapp.net";
        inner.channels.insert(
            "ch_live".to_string(),
            json!({
                "id": "ch_live",
                "project_id": "p",
                "company_id": "c",
                "provider": "whatsapp-rust",
                "status": "connected",
                "created_at": ts(now),
                "updated_at": ts(now)
            }),
        );
        inner.media.insert(
            "media_orphan".to_string(),
            MediaObject {
                id: "media_orphan".to_string(),
                project_id: "p".to_string(),
                company_id: "c".to_string(),
                conversation_id: conversation_id.to_string(),
                message_id: None,
                media_type: "image".to_string(),
                mime_type: "image/png".to_string(),
                original_filename: Some("probe.png".to_string()),
                size_bytes: 68,
                sha256: "sha".to_string(),
                storage_status: "outbound-temp".to_string(),
                bucket: Some("devbucket".to_string()),
                object_key: Some("object-key".to_string()),
                permanent_object_key: None,
                public_url: None,
                thumbnail_url: None,
                width: None,
                height: None,
                duration_seconds: None,
                expires_at: None,
                saved_at: None,
                created_at: now,
                updated_at: now,
            },
        );

        let persisted = PersistedStore::from_inner(&inner);
        let conversation = persisted.conversations.get(conversation_id).unwrap();

        assert_eq!(conversation.project_id, "p");
        assert_eq!(conversation.company_id, "c");
        assert_eq!(conversation.channel_account_id, "ch_live");
        assert_eq!(conversation.contact_id.as_deref(), Some(conversation_id));
        assert_eq!(conversation.last_seq, 0);
        assert_eq!(conversation.control_mode, "manual");
    }

    #[test]
    fn callback_secret_is_persisted_encrypted_not_plaintext() {
        let mut config = AppConfig::from_env();
        config.secret_master_key = Some(crate::secrets::SecretMasterKey::from_raw_32(
            *b"0123456789abcdef0123456789abcdef",
        ));
        let state = AppState::new(config);

        let callback = state
            .upsert_callback(
                "p",
                "c",
                Some("callback_secret_test"),
                json!({
                    "url": "http://localhost/hook",
                    "secret": "webhook_secret",
                    "enabled": true
                }),
            )
            .unwrap();

        assert!(callback.get("secret").is_none());
        assert!(callback.get("encrypted_secret").is_none());
        let inner = state.inner.lock().expect("store lock poisoned");
        let stored = inner.callbacks.get("callback_secret_test").unwrap();
        assert!(stored.get("secret").is_none());
        let encrypted = stored["encrypted_secret"].as_str().unwrap();
        assert!(encrypted.starts_with("rzsec:v1:"));
        assert!(!encrypted.contains("webhook_secret"));
    }

    #[test]
    fn callback_upsert_encrypts_legacy_plaintext_secret() {
        let mut config = AppConfig::from_env();
        config.secret_master_key = Some(crate::secrets::SecretMasterKey::from_raw_32(
            *b"0123456789abcdef0123456789abcdef",
        ));
        let state = AppState::new(config);
        state
            .inner
            .lock()
            .expect("store lock poisoned")
            .callbacks
            .insert(
                "legacy_callback".to_string(),
                json!({
                    "id": "legacy_callback",
                    "project_id": "p",
                    "company_id": "c",
                    "url": "http://localhost/hook",
                    "secret": "legacy_secret",
                    "enabled": true,
                    "events": ["conversation.dirty"]
                }),
            );

        state
            .upsert_callback(
                "p",
                "c",
                Some("legacy_callback"),
                json!({"url": "http://localhost/hook"}),
            )
            .unwrap();

        let inner = state.inner.lock().expect("store lock poisoned");
        let stored = inner.callbacks.get("legacy_callback").unwrap();
        assert!(stored.get("secret").is_none());
        assert!(
            stored["encrypted_secret"]
                .as_str()
                .unwrap()
                .starts_with("rzsec:v1:")
        );
        assert!(!stored.to_string().contains("legacy_secret"));
    }

    #[test]
    fn startup_upgrade_encrypts_legacy_callback_secret_without_upsert() {
        let mut config = AppConfig::from_env();
        config.secret_master_key = Some(crate::secrets::SecretMasterKey::from_raw_32(
            *b"0123456789abcdef0123456789abcdef",
        ));
        let state = AppState::new(config);
        state
            .inner
            .lock()
            .expect("store lock poisoned")
            .callbacks
            .insert(
                "legacy_startup_callback".to_string(),
                json!({
                    "id": "legacy_startup_callback",
                    "project_id": "p",
                    "company_id": "c",
                    "url": "http://localhost/hook",
                    "secret": "legacy_secret",
                    "enabled": true,
                    "events": ["conversation.dirty"]
                }),
            );

        state.upgrade_legacy_callback_secrets();

        let inner = state.inner.lock().expect("store lock poisoned");
        let stored = inner.callbacks.get("legacy_startup_callback").unwrap();
        assert!(stored.get("secret").is_none());
        assert!(
            stored["encrypted_secret"]
                .as_str()
                .unwrap()
                .starts_with("rzsec:v1:")
        );
    }

    #[test]
    fn retention_removes_old_messages_and_audits_aggregate_counts() {
        let state = AppState::new(AppConfig::from_env());
        state.upsert_company("p".to_string(), "c".to_string(), "Company".to_string());
        let message = state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("conv_retention".to_string()),
                channel_id: Some("ch_retention".to_string()),
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                text: "old private text".to_string(),
            },
        );
        {
            let mut inner = state.inner.lock().expect("store lock poisoned");
            inner
                .companies
                .get_mut(&("p".to_string(), "c".to_string()))
                .unwrap()["privacy"]["message_retention_days"] = json!(1);
            let old = OffsetDateTime::now_utc() - Duration::days(2);
            inner
                .messages_by_id
                .get_mut(&message.id)
                .unwrap()
                .created_at = old;
            inner
                .messages_by_conversation
                .get_mut("conv_retention")
                .unwrap()[0]
                .created_at = old;
        }

        let summary = state.apply_retention_once();

        assert_eq!(summary.messages_removed, 1);
        assert!(state.message_for_company("p", "c", &message.id).is_err());
        let audit_logs = &state.inner.lock().expect("store lock poisoned").audit_logs;
        assert!(audit_logs.iter().any(|entry| {
            entry["action"] == "retention.apply"
                && entry["response_json"]["messages_removed"] == json!(1)
                && !entry.to_string().contains("old private text")
        }));
    }

    #[tokio::test]
    async fn retention_removes_local_media_bytes_with_metadata() {
        let mut config = AppConfig::from_env();
        config.local_storage_dir = std::env::temp_dir().join(format!(
            "rustzap-retention-byte-delete-test-{}",
            Uuid::now_v7().simple()
        ));
        let state = AppState::new(config);
        state.upsert_company("p".to_string(), "c".to_string(), "Company".to_string());
        let media = state
            .upload_outbound_media(
                "p",
                "c",
                OutboundMediaUpload {
                    conversation_id: "conv_media_retention".to_string(),
                    media_type: Some("document".to_string()),
                    mime_type: Some("text/plain".to_string()),
                    filename: Some("file.txt".to_string()),
                    caption: None,
                    bytes: b"retained bytes".to_vec(),
                },
            )
            .await
            .unwrap();
        let object_key = media.object_key.clone().unwrap();
        let object_path = state.config.local_storage_dir.join(&object_key);
        assert!(object_path.exists());
        {
            let mut inner = state.inner.lock().expect("store lock poisoned");
            inner
                .companies
                .get_mut(&("p".to_string(), "c".to_string()))
                .unwrap()["privacy"]["media_temp_retention_days"] = json!(1);
            inner.media.get_mut(&media.id).unwrap().created_at =
                OffsetDateTime::now_utc() - Duration::days(2);
        }

        let summary = state.apply_retention_once();

        assert_eq!(summary.media_removed, 1);
        assert!(state.media(&media.id).is_err());
        assert!(!object_path.exists());
    }

    #[test]
    fn transcript_storage_policy_redacts_completed_text_and_raw_response() {
        let state = AppState::new(AppConfig::from_env());
        state.upsert_company("p".to_string(), "c".to_string(), "Company".to_string());
        let (message, media, _transcript) = state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some("conv_transcript_policy".to_string()),
                channel_id: Some("ch_transcript_policy".to_string()),
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                media_type: Some("audio".to_string()),
                mime_type: Some("audio/ogg".to_string()),
                filename: Some("voice.ogg".to_string()),
                caption: None,
                size_bytes: Some(100),
            },
        );
        state
            .inner
            .lock()
            .expect("store lock poisoned")
            .companies
            .get_mut(&("p".to_string(), "c".to_string()))
            .unwrap()["privacy"]["allow_transcript_storage"] = json!(false);

        let transcript = state
            .set_completed_groq_transcript(
                &message,
                &media,
                crate::transcription::GroqTranscription {
                    text: "private transcript".to_string(),
                    raw_response_json: json!({"text": "private transcript"}),
                },
            )
            .unwrap();

        assert_eq!(transcript.status, "completed");
        assert!(transcript.text.is_none());
        assert_eq!(transcript.raw_response_json, json!({"redacted": true}));
    }

    #[test]
    fn raw_whatsapp_enqueue_rejects_qr_pairing_secrets() {
        let state = AppState::new(AppConfig::from_env());
        let err = state
            .enqueue_whatsapp_raw_event(
                test_runtime(),
                WhatsappEvent::PairingQrCode {
                    code: "secret-qr".to_string(),
                    timeout: std::time::Duration::from_secs(30),
                },
            )
            .unwrap_err();

        assert!(matches!(err, ApiError::BadRequest(_)));
        assert!(state.events().is_empty());
    }

    #[tokio::test]
    async fn outbound_upload_creates_placeholder_conversation_for_new_jid() {
        let state = AppState::new(AppConfig::from_env());
        let conversation_id = "5511888777666@s.whatsapp.net";
        let bytes = b"rustzap outbound bytes".to_vec();

        let media = state
            .upload_outbound_media(
                "p",
                "c",
                OutboundMediaUpload {
                    conversation_id: conversation_id.to_string(),
                    media_type: Some("document".to_string()),
                    mime_type: Some("text/plain".to_string()),
                    filename: Some("arquivo.txt".to_string()),
                    caption: None,
                    bytes: bytes.clone(),
                },
            )
            .await
            .unwrap();

        let conversation = state.conversation("p", "c", conversation_id).unwrap();
        assert_eq!(conversation.id, conversation_id);
        assert_eq!(conversation.channel_account_id, "channel_dev");
        assert_eq!(conversation.last_seq, 0);
        assert_eq!(media.conversation_id, conversation_id);
        assert_eq!(media.sha256, hex::encode(Sha256::digest(&bytes)));
    }

    #[tokio::test]
    async fn outbound_upload_attaches_to_prepared_message() {
        let state = AppState::new(AppConfig::from_env());
        let conversation_id = "5511999999999@s.whatsapp.net";
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(conversation_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );

        let bytes = b"rustzap file bytes".to_vec();
        let media = state
            .upload_outbound_media(
                "p",
                "c",
                OutboundMediaUpload {
                    conversation_id: conversation_id.to_string(),
                    media_type: Some("document".to_string()),
                    mime_type: Some("text/plain".to_string()),
                    filename: Some("arquivo.txt".to_string()),
                    caption: None,
                    bytes: bytes.clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(state.media_blob(&media.id).await.unwrap().unwrap().1, bytes);
        assert!(
            media
                .object_key
                .as_deref()
                .unwrap()
                .starts_with("rustyzap/outbound-temp/")
        );

        let outcome = state
            .prepare_send_message(
                "p",
                "c",
                conversation_id,
                "media-idem",
                SendMessageRequest {
                    message_type: "document".to_string(),
                    text: None,
                    media_id: Some(media.id.clone()),
                    caption: Some("segue".to_string()),
                    filename: Some("arquivo.txt".to_string()),
                    quoted_message_id: None,
                    metadata: None,
                },
            )
            .unwrap();

        assert_eq!(outcome.message.media_id.as_deref(), Some(media.id.as_str()));
        assert_eq!(outcome.message.mime_type.as_deref(), Some("text/plain"));
        assert_eq!(outcome.message.file_name.as_deref(), Some("arquivo.txt"));
        assert_eq!(outcome.message.text.as_deref(), Some("segue"));
        assert_eq!(
            state.media(&media.id).unwrap().message_id.as_deref(),
            Some(outcome.message.id.as_str())
        );
    }

    #[tokio::test]
    async fn reaction_is_not_faked_when_provider_is_unavailable() {
        let state = AppState::new(AppConfig::from_env());
        let message = state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("5511999999999@s.whatsapp.net".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );

        let err = state
            .react_to_message(&message.id, Some("👍".to_string()))
            .await
            .unwrap_err();

        assert!(matches!(err, ApiError::ProviderError(_)));
        assert_eq!(state.message(&message.id).unwrap().reaction, None);
    }

    #[tokio::test]
    async fn conversation_seq_is_unique_under_concurrent_inserts() {
        let state = AppState::new(AppConfig::from_env());
        let mut tasks = Vec::new();
        for idx in 0..50 {
            let state = state.clone();
            tasks.push(tokio::spawn(async move {
                state.receive_inbound_text(
                    "p",
                    "c",
                    SimulateInboundTextRequest {
                        conversation_id: Some("conv".to_string()),
                        channel_id: None,
                        from_phone_e164: None,
                        sender_name: None,
                        profile_picture_url: None,
                        text: format!("msg {idx}"),
                    },
                )
            }));
        }
        let mut seqs = BTreeSet::new();
        for task in tasks {
            let message = task.await.unwrap();
            seqs.insert(message.conversation_seq);
        }
        assert_eq!(seqs.len(), 50);
        assert_eq!(seqs.first().copied(), Some(1));
        assert_eq!(seqs.last().copied(), Some(50));
    }

    #[test]
    fn dirty_ack_keeps_dirty_when_new_max_seq_exists() {
        let state = AppState::new(AppConfig::from_env());
        let first = state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("conv".to_string()),
                channel_id: None,
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                text: "one".to_string(),
            },
        );
        let lease = state.list_dirty("p", "c", "consumer", 10).remove(0);
        let second = state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("conv".to_string()),
                channel_id: None,
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                text: "two".to_string(),
            },
        );

        let ack = state
            .ack_dirty(
                "p",
                "c",
                "conv",
                DirtyAckRequest {
                    consumer_id: "consumer".to_string(),
                    processed_until_seq: first.conversation_seq,
                    lease_token: lease.lease_token,
                },
            )
            .unwrap();

        assert_eq!(ack["remaining_dirty"], true);
        assert!(second.conversation_seq > first.conversation_seq);
        let dirty = state.list_dirty("p", "c", "consumer", 10).remove(0);
        assert_eq!(dirty.max_seq, second.conversation_seq);
    }

    #[test]
    fn dirty_ack_waits_for_other_registered_consumers() {
        let state = AppState::new(AppConfig::from_env());
        let message = state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("conv".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );

        let alpha_lease = state.list_dirty("p", "c", "alpha", 10).remove(0);
        let beta_lease = state.list_dirty("p", "c", "beta", 10).remove(0);
        let alpha_ack = state
            .ack_dirty(
                "p",
                "c",
                "conv",
                DirtyAckRequest {
                    consumer_id: "alpha".to_string(),
                    processed_until_seq: message.conversation_seq,
                    lease_token: alpha_lease.lease_token,
                },
            )
            .unwrap();

        assert_eq!(alpha_ack["remaining_dirty"], true);
        let still_dirty = state.list_dirty("p", "c", "beta", 10).remove(0);
        assert_eq!(still_dirty.max_seq, message.conversation_seq);
        assert_ne!(still_dirty.lease_token, beta_lease.lease_token);

        let beta_ack = state
            .ack_dirty(
                "p",
                "c",
                "conv",
                DirtyAckRequest {
                    consumer_id: "beta".to_string(),
                    processed_until_seq: message.conversation_seq,
                    lease_token: still_dirty.lease_token,
                },
            )
            .unwrap();

        assert_eq!(beta_ack["remaining_dirty"], false);
        assert!(state.list_dirty("p", "c", "alpha", 10).is_empty());
    }

    #[tokio::test]
    async fn webhook_delivery_sends_compact_signed_events_and_records_attempt() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<(axum::http::HeaderMap, Vec<u8>)>();
        let app = axum::Router::new().route(
            "/hook",
            axum::routing::post(
                move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                    let tx = tx.clone();
                    async move {
                        tx.send((headers, body.to_vec())).unwrap();
                        axum::http::StatusCode::NO_CONTENT
                    }
                },
            ),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut config = AppConfig::from_env();
        config.consumer_signal_mode = crate::config::ConsumerSignalMode::Webhook;
        config.webhook.delivery_enabled = true;
        config.secret_master_key = Some(crate::secrets::SecretMasterKey::from_raw_32(
            *b"0123456789abcdef0123456789abcdef",
        ));
        let state = AppState::new(config);
        state
            .upsert_callback(
                "p",
                "c",
                None,
                json!({
                    "url": format!("http://{addr}/hook"),
                    "secret": "webhook_secret",
                    "events": ["conversation.dirty"],
                    "enabled": true
                }),
            )
            .unwrap();
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("conv".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                text: "private text must not be delivered".to_string(),
            },
        );

        state.deliver_pending_webhooks_once().await.unwrap();

        let (headers, raw_body) =
            tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
        let timestamp = headers
            .get("X-RustZap-Timestamp")
            .and_then(|value| value.to_str().ok())
            .unwrap();
        let signature = headers
            .get("X-RustZap-Signature")
            .and_then(|value| value.to_str().ok())
            .unwrap();
        assert_eq!(
            signature,
            crate::security::webhook_signature("webhook_secret", timestamp, &raw_body)
        );
        assert!(headers.get("X-RustZap-Event-Id").is_some());
        let body: Value = serde_json::from_slice(&raw_body).unwrap();
        assert_eq!(body["events"].as_array().unwrap().len(), 1);
        assert_eq!(body["events"][0]["event_type"], "conversation.dirty");
        assert!(
            body.to_string()
                .contains("private text must not be delivered")
                == false
        );

        let attempts = state.webhook_delivery_attempts("p", "c");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0]["status"], "success");

        server.abort();
    }

    #[test]
    fn message_cursor_pagination_uses_after_seq_and_limit_cap() {
        let state = AppState::new(AppConfig::from_env());
        for idx in 0..5 {
            state.receive_inbound_text(
                "p",
                "c",
                SimulateInboundTextRequest {
                    conversation_id: Some("conv".to_string()),
                    channel_id: None,
                    from_phone_e164: None,
                    sender_name: None,
                    profile_picture_url: None,
                    text: format!("msg {idx}"),
                },
            );
        }
        let page = state.list_messages("conv", Some(2), None, 2);
        assert_eq!(page.messages.len(), 2);
        assert_eq!(page.from_seq, Some(3));
        assert_eq!(page.to_seq, Some(4));
        assert!(page.has_more);
    }

    #[tokio::test]
    async fn history_sync_event_does_not_create_messages() {
        let state = AppState::new(AppConfig::from_env());
        state
            .handle_whatsapp_event(test_runtime(), WhatsappEvent::HistorySyncIgnored)
            .await;

        assert!(state.conversations("p", "c").is_empty());
        assert!(
            state
                .events()
                .iter()
                .any(|event| event.event_type == "history_sync.ignored")
        );
    }

    #[tokio::test]
    async fn system_message_without_visible_content_is_ignored() {
        let state = AppState::new(AppConfig::from_env());
        state
            .handle_whatsapp_event(
                test_runtime(),
                WhatsappEvent::Message {
                    wa_message_id: "wa_system".to_string(),
                    chat_jid: "5511999999999@s.whatsapp.net".to_string(),
                    sender_jid: "5511999999999@s.whatsapp.net".to_string(),
                    sender_alt_jid: None,
                    recipient_jid: None,
                    recipient_alt_jid: None,
                    push_name: None,
                    text: None,
                    message_type: "system".to_string(),
                    media: None,
                    created_at_wa: OffsetDateTime::now_utc(),
                    is_from_me: false,
                },
            )
            .await;

        assert!(state.conversations("p", "c").is_empty());
        assert!(
            state
                .events()
                .iter()
                .any(|event| event.event_type == "message.system_ignored")
        );
    }

    #[tokio::test]
    async fn whatsapp_updates_surfaces_do_not_create_local_records() {
        let state = AppState::new(AppConfig::from_env());
        state
            .handle_whatsapp_event(
                test_runtime(),
                WhatsappEvent::Message {
                    wa_message_id: "wa_status".to_string(),
                    chat_jid: "status@broadcast".to_string(),
                    sender_jid: "5511999999999@s.whatsapp.net".to_string(),
                    sender_alt_jid: None,
                    recipient_jid: None,
                    recipient_alt_jid: None,
                    push_name: Some("Status Sender".to_string()),
                    text: Some("status caption".to_string()),
                    message_type: "image".to_string(),
                    media: None,
                    created_at_wa: OffsetDateTime::now_utc(),
                    is_from_me: false,
                },
            )
            .await;
        state
            .handle_whatsapp_event(
                test_runtime(),
                WhatsappEvent::Message {
                    wa_message_id: "wa_newsletter".to_string(),
                    chat_jid: "120363000000000000@newsletter".to_string(),
                    sender_jid: "120363000000000000@newsletter".to_string(),
                    sender_alt_jid: None,
                    recipient_jid: None,
                    recipient_alt_jid: None,
                    push_name: Some("Canal".to_string()),
                    text: Some("canal update".to_string()),
                    message_type: "video".to_string(),
                    media: None,
                    created_at_wa: OffsetDateTime::now_utc(),
                    is_from_me: false,
                },
            )
            .await;

        assert!(state.conversations("p", "c").is_empty());
        assert!(state.contacts("p", "c").is_empty());
        assert!(state.media_for_conversation("status@broadcast").is_empty());
        assert_eq!(
            state
                .events()
                .iter()
                .filter(|event| event.event_type == "whatsapp_updates.ignored")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn dispatch_fails_fast_when_channel_not_connected() {
        let state = AppState::new(AppConfig::from_env());
        state
            .create_channel("p", "c", Some("ch".to_string()), None, None)
            .unwrap();
        state.set_channel_status("ch", "waiting_qr", None);
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("5511999999999@s.whatsapp.net".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente RustZap".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );
        let request = SendMessageRequest {
            message_type: "text".to_string(),
            text: Some("resposta".to_string()),
            media_id: None,
            caption: None,
            filename: None,
            quoted_message_id: None,
            metadata: None,
        };
        let outcome = state
            .prepare_send_message(
                "p",
                "c",
                "5511999999999@s.whatsapp.net",
                "send-fast-fail",
                request,
            )
            .unwrap();

        let message = state
            .dispatch_prepared_message(&outcome, Some("resposta"))
            .await
            .unwrap();

        assert_eq!(message.channel_account_id, "ch");
        assert_eq!(message.status, "failed");
        assert!(
            message
                .error_message
                .as_deref()
                .unwrap()
                .contains("not connected")
        );
    }

    #[tokio::test]
    async fn dispatch_marks_stale_connected_channel_disconnected() {
        let state = AppState::new(AppConfig::from_env());
        state
            .create_channel("p", "c", Some("ch".to_string()), None, None)
            .unwrap();
        let connected_at = OffsetDateTime::now_utc();
        state.set_channel_status("ch", "connected", Some(connected_at));
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("5511999999999@s.whatsapp.net".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente RustZap".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );
        let request = SendMessageRequest {
            message_type: "text".to_string(),
            text: Some("resposta".to_string()),
            media_id: None,
            caption: None,
            filename: None,
            quoted_message_id: None,
            metadata: None,
        };
        let outcome = state
            .prepare_send_message(
                "p",
                "c",
                "5511999999999@s.whatsapp.net",
                "send-stale-channel",
                request,
            )
            .unwrap();

        let message = state
            .dispatch_prepared_message(&outcome, Some("resposta"))
            .await
            .unwrap();

        assert_eq!(message.status, "failed");
        assert!(
            message
                .error_message
                .as_deref()
                .unwrap()
                .contains("not connected")
        );
        assert_eq!(state.channel("ch")["status"], "disconnected");
        assert_eq!(state.channel_connected_at("ch"), None);
        assert_eq!(state.qr("ch").status, "disconnected");
    }

    #[test]
    fn channel_status_reports_disconnected_when_runtime_is_gone() {
        let state = AppState::new(AppConfig::from_env());
        state
            .create_channel("p", "c", Some("ch".to_string()), None, None)
            .unwrap();
        let connected_at = OffsetDateTime::now_utc();
        state.set_channel_status("ch", "connected", Some(connected_at));

        let channel = state.channel("ch");

        assert_eq!(channel["status"], "disconnected");
        assert_eq!(channel["connected_at"], Value::Null);
        assert_eq!(state.qr("ch").status, "disconnected");
        assert!(state.events().iter().any(|event| {
            event.event_type == "channel.disconnected" && event.channel_id.as_deref() == Some("ch")
        }));
    }

    #[test]
    fn qr_status_reports_disconnected_when_runtime_is_gone() {
        let state = AppState::new(AppConfig::from_env());
        state
            .create_channel("p", "c", Some("ch".to_string()), None, None)
            .unwrap();
        let connected_at = OffsetDateTime::now_utc();
        state.set_channel_status("ch", "connected", Some(connected_at));

        let qr = state.qr("ch");

        assert_eq!(qr.status, "disconnected");
        assert_eq!(qr.qr_code_text, None);
        assert_eq!(state.channel("ch")["status"], "disconnected");
    }

    #[test]
    fn contact_display_and_group_members_are_recorded_from_messages() {
        let state = AppState::new(AppConfig::from_env());
        let direct = state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: None,
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente RustZap".to_string()),
                profile_picture_url: Some("https://example.test/profile.svg".to_string()),
                text: "oi".to_string(),
            },
        );
        assert_eq!(
            direct.sender_contact_id.as_deref(),
            Some("5511999999999@s.whatsapp.net")
        );
        let conversation = state.conversations("p", "c").remove(0);
        assert_eq!(
            conversation.display_name.as_deref(),
            Some("Cliente RustZap")
        );
        assert_eq!(
            conversation.display_phone.as_deref(),
            Some("+5511999999999")
        );
        assert_eq!(
            conversation.profile_picture_url.as_deref(),
            Some("https://example.test/profile.svg")
        );

        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("120363000000000000@g.us".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511888888888".to_string()),
                sender_name: Some("Pessoa do Grupo".to_string()),
                profile_picture_url: None,
                text: "grupo".to_string(),
            },
        );
        let groups = state.groups("p", "c");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["id"], "120363000000000000@g.us");
        let members = state.group_members("120363000000000000@g.us");
        assert_eq!(members[0]["display_name"], "Pessoa do Grupo");
    }

    #[tokio::test]
    async fn outbound_lid_dm_uses_recipient_contact_not_self_sender() {
        let state = AppState::new(AppConfig::from_env());
        state
            .handle_whatsapp_event(
                test_runtime(),
                WhatsappEvent::Message {
                    wa_message_id: "wa_self_echo".to_string(),
                    chat_jid: "153580027813893@lid".to_string(),
                    sender_jid: "241570603368695:34@lid".to_string(),
                    sender_alt_jid: None,
                    recipient_jid: Some("153580027813893@lid".to_string()),
                    recipient_alt_jid: Some("5511999999999@s.whatsapp.net".to_string()),
                    push_name: Some("João Merlin".to_string()),
                    text: Some("mensagem para outra pessoa".to_string()),
                    message_type: "text".to_string(),
                    media: None,
                    created_at_wa: OffsetDateTime::now_utc(),
                    is_from_me: true,
                },
            )
            .await;

        let conversation = state.conversations("p", "c").remove(0);
        assert_eq!(conversation.id, "153580027813893@lid");
        assert_eq!(
            conversation.contact_id.as_deref(),
            Some("153580027813893@lid")
        );
        assert_ne!(conversation.display_name.as_deref(), Some("João Merlin"));
        assert_eq!(
            conversation.display_phone.as_deref(),
            Some("+5511999999999")
        );

        let message = state
            .list_messages("153580027813893@lid", None, None, 10)
            .messages
            .remove(0);
        assert_eq!(message.direction, "outbound");
        assert_eq!(message.sender_contact_id, None);
        assert_eq!(message.sender_display_name, None);
        assert_eq!(message.sent_by_external_user_id, None);
    }

    #[test]
    fn contact_profile_enrichment_updates_lid_conversation() {
        let state = AppState::new(AppConfig::from_env());
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("200889293889773@lid".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: None,
                sender_name: Some("Tetoz".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );

        state.apply_contact_profile(ContactProfileApplyInput {
            project_id: "p",
            company_id: "c",
            channel_id: "ch",
            conversation_id: "200889293889773@lid",
            contact_id: "200889293889773@lid",
            push_name: Some("Tetoz"),
            profile: ContactProfile {
                requested_jid: "200889293889773@lid".to_string(),
                resolved_jid: Some("5511999999999@s.whatsapp.net".to_string()),
                lid: Some("200889293889773@lid".to_string()),
                phone_e164: Some("+5511999999999".to_string()),
                business_description: None,
                profile_picture_id: Some("pic_1".to_string()),
                profile_picture_url: Some("https://example.test/profile.jpg".to_string()),
            },
        });

        let conversation = state.conversations("p", "c").remove(0);
        assert_eq!(conversation.display_name.as_deref(), Some("Tetoz"));
        assert_eq!(
            conversation.display_phone.as_deref(),
            Some("+5511999999999")
        );
        assert_eq!(
            conversation.profile_picture_url.as_deref(),
            Some("https://example.test/profile.jpg")
        );
        assert_eq!(conversation.unread_count, 1);

        let serialized = serde_json::to_value(&conversation).unwrap();
        assert_eq!(serialized["phone_number"], "5511999999999");

        let contact = state.contact_by_phone("p", "c", "5511999999999").unwrap();
        assert_eq!(contact["id"], "5511999999999");
        assert_eq!(contact["phone_number"], "5511999999999");
        assert_eq!(contact["technical_id"], "200889293889773@lid");

        let resolved = state.conversation("p", "c", "5511999999999").unwrap();
        assert_eq!(resolved.id, "200889293889773@lid");
        assert_eq!(resolved.display_name.as_deref(), Some("Tetoz"));

        let messages = state
            .list_messages_for_conversation("p", "c", "5511999999999", None, None, 10)
            .unwrap();
        assert_eq!(messages.messages.len(), 1);
        assert_eq!(messages.conversation_id, "5511999999999");
        assert_eq!(messages.messages[0].conversation_id, "5511999999999");
    }

    #[test]
    fn contact_profile_enrichment_persists_business_description_and_picture() {
        let state = AppState::new(AppConfig::from_env());
        let contact_id = "5511999999999@s.whatsapp.net";
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(contact_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Loja RustZap".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );

        state.apply_contact_profile(ContactProfileApplyInput {
            project_id: "p",
            company_id: "c",
            channel_id: "ch",
            conversation_id: contact_id,
            contact_id,
            push_name: Some("Loja RustZap"),
            profile: ContactProfile {
                requested_jid: contact_id.to_string(),
                resolved_jid: Some(contact_id.to_string()),
                lid: None,
                phone_e164: Some("+5511999999999".to_string()),
                business_description: Some("Atendimento imobiliario".to_string()),
                profile_picture_id: Some("pic_contact".to_string()),
                profile_picture_url: Some("https://example.test/contact.jpg".to_string()),
            },
        });

        let contact = state.contact("p", "c", contact_id).unwrap();
        assert_eq!(contact["business_description"], "Atendimento imobiliario");
        assert_eq!(
            contact["profile_picture_url"],
            "https://example.test/contact.jpg"
        );
        assert_eq!(contact["canonical_jid"], contact_id);
    }

    #[test]
    fn contact_profile_enrichment_does_not_overwrite_group_conversation() {
        let state = AppState::new(AppConfig::from_env());
        let group_id = "120363000000000000@g.us";
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(group_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511888888888".to_string()),
                sender_name: Some("Pessoa do Grupo".to_string()),
                profile_picture_url: None,
                text: "grupo".to_string(),
            },
        );

        state.apply_contact_profile(ContactProfileApplyInput {
            project_id: "p",
            company_id: "c",
            channel_id: "ch",
            conversation_id: group_id,
            contact_id: "5511888888888@s.whatsapp.net",
            push_name: Some("Pessoa Perfil"),
            profile: ContactProfile {
                requested_jid: "5511888888888@s.whatsapp.net".to_string(),
                resolved_jid: None,
                lid: None,
                phone_e164: Some("+5511888888888".to_string()),
                business_description: None,
                profile_picture_id: Some("pic_sender".to_string()),
                profile_picture_url: Some("https://example.test/sender.jpg".to_string()),
            },
        });

        let conversation = state.conversations("p", "c").remove(0);
        assert_eq!(conversation.conversation_type, "group");
        assert_eq!(conversation.contact_id, None);
        assert_eq!(conversation.display_phone, None);
        assert_eq!(
            conversation.display_name.as_deref(),
            Some("Grupo 120363000000000000")
        );
        assert_eq!(conversation.profile_picture_url, None);

        let message = state
            .list_messages(group_id, None, None, 10)
            .messages
            .remove(0);
        assert_eq!(
            message.sender_display_name.as_deref(),
            Some("Pessoa Perfil")
        );
        let members = state.group_members(group_id);
        assert_eq!(members[0]["display_name"], "Pessoa Perfil");
    }

    #[test]
    fn group_profile_updates_subject_avatar_and_member_roles() {
        let state = AppState::new(AppConfig::from_env());
        let group_id = "120363000000000000@g.us";
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(group_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511888888888".to_string()),
                sender_name: Some("Pessoa do Grupo".to_string()),
                profile_picture_url: None,
                text: "grupo".to_string(),
            },
        );

        state.apply_group_profile(
            "p",
            "c",
            "ch",
            group_id,
            GroupProfile {
                group_jid: group_id.to_string(),
                subject: Some("Equipe RustZap".to_string()),
                description: Some("Grupo de atendimento".to_string()),
                owner_jid: Some("5511888888888@s.whatsapp.net".to_string()),
                subject_owner_jid: Some("5511888888888@s.whatsapp.net".to_string()),
                created_at_wa_unix: Some(1_700_000_000),
                size: Some(2),
                profile_picture_id: Some("pic_group".to_string()),
                profile_picture_url: Some("https://example.test/group.jpg".to_string()),
                participants: vec![
                    GroupParticipantProfile {
                        contact_id: "5511888888888@s.whatsapp.net".to_string(),
                        phone_jid: None,
                        is_admin: true,
                    },
                    GroupParticipantProfile {
                        contact_id: "200889293889773@lid".to_string(),
                        phone_jid: Some("5511999999999@s.whatsapp.net".to_string()),
                        is_admin: false,
                    },
                ],
            },
        );

        let conversation = state.conversations("p", "c").remove(0);
        assert_eq!(conversation.conversation_type, "group");
        assert_eq!(conversation.contact_id, None);
        assert_eq!(conversation.display_phone, None);
        assert_eq!(conversation.display_name.as_deref(), Some("Equipe RustZap"));
        assert_eq!(
            conversation.profile_picture_url.as_deref(),
            Some("https://example.test/group.jpg")
        );

        let group = state.group("p", "c", group_id).unwrap();
        assert_eq!(group["subject"], "Equipe RustZap");
        assert_eq!(group["description"], "Grupo de atendimento");
        assert_eq!(group["owner_jid"], "5511888888888@s.whatsapp.net");
        assert_eq!(group["subject_owner_jid"], "5511888888888@s.whatsapp.net");
        assert_eq!(group["members_count"], 2);
        assert_eq!(group["admins_count"], 1);
        assert_eq!(
            group["profile_picture_url"],
            "https://example.test/group.jpg"
        );

        let members = state.group_members(group_id);
        let existing = members
            .iter()
            .find(|member| member["contact_id"] == "5511888888888")
            .unwrap();
        assert_eq!(
            existing["technical_contact_id"],
            "5511888888888@s.whatsapp.net"
        );
        assert_eq!(existing["display_name"], "Pessoa do Grupo");
        assert_eq!(existing["is_admin"], true);
        assert_eq!(existing["role"], "owner");
        let lid = members
            .iter()
            .find(|member| member["contact_id"] == "5511999999999")
            .unwrap();
        assert_eq!(lid["technical_contact_id"], "200889293889773@lid");
        assert_eq!(lid["display_name"], "+5511999999999");
    }

    #[test]
    fn contact_media_and_conversations_are_filtered_by_contact() {
        let state = AppState::new(AppConfig::from_env());
        let contact_a = "5511999999999@s.whatsapp.net";
        let contact_b = "5511888888888@s.whatsapp.net";
        let (_, media_a, _) = state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some(contact_a.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Contato A".to_string()),
                media_type: Some("image".to_string()),
                mime_type: Some("image/png".to_string()),
                size_bytes: Some(1000),
                filename: Some("a.png".to_string()),
                caption: Some("foto a".to_string()),
                profile_picture_url: None,
            },
        );
        state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some(contact_b.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511888888888".to_string()),
                sender_name: Some("Contato B".to_string()),
                media_type: Some("image".to_string()),
                mime_type: Some("image/png".to_string()),
                size_bytes: Some(1000),
                filename: Some("b.png".to_string()),
                caption: Some("foto b".to_string()),
                profile_picture_url: None,
            },
        );

        let media = state.media_for_contact("p", "c", contact_a).unwrap();
        assert_eq!(media.len(), 1);
        assert_eq!(media[0].id, media_a.id);

        let conversations = state.contact_conversations("p", "c", contact_a).unwrap();
        assert_eq!(conversations.len(), 1);
        assert_eq!(conversations[0].id, contact_a);
    }

    #[test]
    fn group_media_search_and_starred_are_filtered_by_group() {
        let state = AppState::new(AppConfig::from_env());
        let group_a = "120363000000000000@g.us";
        let group_b = "120363000000000001@g.us";
        let message_a = state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(group_a.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Pessoa A".to_string()),
                profile_picture_url: None,
                text: "contrato do grupo alfa".to_string(),
            },
        );
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(group_b.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511888888888".to_string()),
                sender_name: Some("Pessoa B".to_string()),
                profile_picture_url: None,
                text: "contrato do grupo beta".to_string(),
            },
        );
        let (_, media_a, _) = state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some(group_a.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Pessoa A".to_string()),
                media_type: Some("document".to_string()),
                mime_type: Some("application/pdf".to_string()),
                size_bytes: Some(2048),
                filename: Some("alfa.pdf".to_string()),
                caption: Some("midia alfa".to_string()),
                profile_picture_url: None,
            },
        );
        state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some(group_b.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511888888888".to_string()),
                sender_name: Some("Pessoa B".to_string()),
                media_type: Some("document".to_string()),
                mime_type: Some("application/pdf".to_string()),
                size_bytes: Some(2048),
                filename: Some("beta.pdf".to_string()),
                caption: Some("midia beta".to_string()),
                profile_picture_url: None,
            },
        );
        state.set_message_flag(&message_a.id, "star", true).unwrap();

        let media = state.media_for_group("p", "c", group_a).unwrap();
        assert_eq!(media.len(), 1);
        assert_eq!(media[0].id, media_a.id);

        let search = state
            .search_messages_for_group("p", "c", group_a, "contrato", 10)
            .unwrap();
        assert_eq!(search.len(), 1);
        assert_eq!(search[0].conversation_id, group_a);

        let starred = state.starred_messages_for_group("p", "c", group_a).unwrap();
        assert_eq!(starred.len(), 1);
        assert_eq!(starred[0].id, message_a.id);
    }

    #[tokio::test]
    async fn group_subject_update_event_keeps_group_fields_clean() {
        let state = AppState::new(AppConfig::from_env());
        let group_id = "120363000000000000@g.us";
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(group_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511888888888".to_string()),
                sender_name: Some("Pessoa do Grupo".to_string()),
                profile_picture_url: None,
                text: "grupo".to_string(),
            },
        );

        state
            .handle_whatsapp_event(
                test_runtime(),
                WhatsappEvent::GroupUpdate {
                    group_jid: group_id.to_string(),
                    subject: Some("Novo Nome do Grupo".to_string()),
                    created_at_wa: OffsetDateTime::now_utc(),
                },
            )
            .await;

        let conversation = state.conversations("p", "c").remove(0);
        assert_eq!(
            conversation.display_name.as_deref(),
            Some("Novo Nome do Grupo")
        );
        assert_eq!(conversation.contact_id, None);
        assert_eq!(conversation.display_phone, None);
        assert_eq!(
            state.group("p", "c", group_id).unwrap()["subject"],
            "Novo Nome do Grupo"
        );
    }

    #[test]
    fn mark_read_clears_unread_count() {
        let state = AppState::new(AppConfig::from_env());
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some("5511999999999@s.whatsapp.net".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente RustZap".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );
        assert_eq!(state.conversations("p", "c")[0].unread_count, 1);

        let conversation = state
            .mark_read("p", "c", "5511999999999@s.whatsapp.net")
            .unwrap();

        assert_eq!(conversation.unread_count, 0);
        assert_eq!(state.conversations("p", "c")[0].unread_count, 0);
        assert!(
            state
                .events()
                .iter()
                .any(|event| event.event_type == "conversation.read")
        );
    }

    #[test]
    fn inbound_image_gets_rustyzap_key_and_preview_url() {
        let mut config = AppConfig::from_env();
        config.storage_provider = crate::config::StorageProvider::LocalFs;
        config.r2.bucket = "devbucket".to_string();
        config.r2.base_prefix = "rustyzap".to_string();
        config.r2.public_url = Some("https://pub.example".to_string());
        let state = AppState::new(config);
        let (message, media, _) = state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some("5511999999999@s.whatsapp.net".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente RustZap".to_string()),
                media_type: Some("image".to_string()),
                mime_type: Some("image/svg+xml".to_string()),
                size_bytes: Some(8192),
                filename: Some("foto.svg".to_string()),
                caption: Some("foto".to_string()),
                profile_picture_url: None,
            },
        );

        assert_eq!(message.media_id.as_deref(), Some(media.id.as_str()));
        assert_eq!(message.thumbnail_url, media.thumbnail_url);
        assert!(
            media
                .object_key
                .as_deref()
                .unwrap()
                .starts_with("rustyzap/")
        );
        assert_eq!(media.bucket.as_deref(), Some("devbucket"));
        assert!(
            media
                .thumbnail_url
                .as_deref()
                .unwrap()
                .contains("/dev-media/")
        );
    }

    #[test]
    fn list_messages_normalizes_local_media_urls() {
        let mut config = AppConfig::from_env();
        config.storage_provider = crate::config::StorageProvider::LocalFs;
        config.r2.public_url = Some("https://pub.example".to_string());
        let state = AppState::new(config);
        let (message, _, _) = state.receive_inbound_media(
            "p",
            "c",
            SimulateInboundMediaRequest {
                conversation_id: Some("5511999999999@s.whatsapp.net".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente RustZap".to_string()),
                media_type: Some("image".to_string()),
                mime_type: Some("image/jpeg".to_string()),
                size_bytes: Some(8192),
                filename: Some("foto.jpg".to_string()),
                caption: None,
                profile_picture_url: None,
            },
        );
        {
            let mut inner = state.inner.lock().expect("store lock poisoned");
            let stored = inner.messages_by_id.get_mut(&message.id).unwrap();
            stored.media_url = Some("https://pub.example/bad.jpg".to_string());
            stored.thumbnail_url = Some("https://pub.example/bad.jpg".to_string());
            let conversation_messages = inner
                .messages_by_conversation
                .get_mut(&message.conversation_id)
                .unwrap();
            let stored = conversation_messages
                .iter_mut()
                .find(|stored| stored.id == message.id)
                .unwrap();
            stored.media_url = Some("https://pub.example/bad.jpg".to_string());
            stored.thumbnail_url = Some("https://pub.example/bad.jpg".to_string());
        }

        let page = state.list_messages(&message.conversation_id, None, None, 10);
        let listed = page.messages.first().unwrap();
        assert!(listed.media_url.as_deref().unwrap().contains("/dev-media/"));
        assert!(
            listed
                .thumbnail_url
                .as_deref()
                .unwrap()
                .contains("/dev-media/")
        );
    }

    #[test]
    fn set_channel_status_clears_connected_at_when_disconnected() {
        let state = AppState::new(AppConfig::from_env());
        state
            .create_channel("p", "c", Some("ch".to_string()), None, None)
            .unwrap();
        let connected_at = OffsetDateTime::now_utc();
        state.set_channel_status("ch", "connected", Some(connected_at));

        state.set_channel_status("ch", "disconnected", None);

        assert_eq!(state.channel("ch")["status"], "disconnected");
        assert_eq!(state.channel_connected_at("ch"), None);
    }

    #[test]
    fn qr_does_not_return_expired_pairing_secret() {
        let state = AppState::new(AppConfig::from_env());
        state
            .create_channel("p", "c", Some("ch".to_string()), None, None)
            .unwrap();
        state.set_qr(
            "ch",
            "waiting_qr",
            Some("expired-pairing-secret".to_string()),
            OffsetDateTime::now_utc() - Duration::seconds(1),
        );

        let qr = state.qr("ch");

        assert_eq!(qr.status, "disconnected");
        assert_eq!(qr.qr_code_text, None);
    }

    #[test]
    fn persisted_group_refresh_keys_do_not_contain_nul() {
        let mut inner = StoreInner::default();
        inner.group_profile_refreshes.insert(
            "p\x00c\x00ch\x001203630000000000000@g.us".to_string(),
            OffsetDateTime::now_utc(),
        );

        let persisted = PersistedStore::from_inner(&inner);
        let restored = persisted.into_inner();

        assert!(
            restored
                .group_profile_refreshes
                .keys()
                .all(|key| !key.contains('\0'))
        );
    }

    #[test]
    fn persisted_store_drops_updates_surfaces() {
        let now = OffsetDateTime::now_utc();
        let mut inner = StoreInner::default();
        inner.conversations.insert(
            "status@broadcast".to_string(),
            crate::models::Conversation {
                id: "status@broadcast".to_string(),
                project_id: "p".to_string(),
                company_id: "c".to_string(),
                channel_account_id: "ch".to_string(),
                conversation_type: "direct".to_string(),
                contact_id: Some("status@broadcast".to_string()),
                group_id: None,
                display_name: Some("Status".to_string()),
                display_phone: None,
                phone_number: None,
                avatar_url: None,
                profile_picture_url: None,
                last_seq: 1,
                last_message_at: Some(now),
                unread_count: 1,
                is_archived: false,
                is_muted: false,
                is_pinned: false,
                control_mode: "manual".to_string(),
                created_at: now,
                updated_at: now,
            },
        );
        let message = Message {
            id: "msg_status".to_string(),
            project_id: "p".to_string(),
            company_id: "c".to_string(),
            conversation_id: "status@broadcast".to_string(),
            channel_account_id: "ch".to_string(),
            conversation_seq: 1,
            wa_message_id: Some("wa_status".to_string()),
            direction: "inbound".to_string(),
            sender_contact_id: Some("5511999999999@s.whatsapp.net".to_string()),
            sender_display_name: Some("Pessoa".to_string()),
            message_type: "image".to_string(),
            text: Some("status caption".to_string()),
            media_id: None,
            media_url: None,
            thumbnail_url: None,
            mime_type: None,
            file_name: None,
            quoted_message_id: None,
            status: "received".to_string(),
            error_message: None,
            is_starred: false,
            is_pinned: false,
            reaction: None,
            sent_by_source: None,
            sent_by_external_user_id: None,
            created_at_wa: now,
            created_at: now,
            updated_at: now,
        };
        inner
            .messages_by_conversation
            .insert("status@broadcast".to_string(), vec![message.clone()]);
        inner.messages_by_id.insert(message.id.clone(), message);
        inner.contacts.insert(
            "status@broadcast".to_string(),
            ContactRecord {
                id: "status@broadcast".to_string(),
                canonical_jid: None,
                lid: None,
                project_id: "p".to_string(),
                company_id: "c".to_string(),
                channel_account_id: Some("ch".to_string()),
                phone_e164: None,
                push_name: Some("Status".to_string()),
                display_name: "Status".to_string(),
                profile_picture_media_id: None,
                business_description: None,
                avatar_url: None,
                profile_picture_url: None,
                first_contact_at: now,
                last_contact_at: now,
            },
        );

        let restored = PersistedStore::from_inner(&inner).into_inner();

        assert!(restored.conversations.is_empty());
        assert!(restored.messages_by_conversation.is_empty());
        assert!(restored.messages_by_id.is_empty());
        assert!(restored.contacts.is_empty());
    }

    #[tokio::test]
    async fn whatsapp_message_before_connected_at_is_ignored() {
        let state = AppState::new(AppConfig::from_env());
        state
            .create_channel("p", "c", Some("ch".to_string()), None, None)
            .unwrap();
        let connected_at = OffsetDateTime::now_utc();
        state.set_channel_status("ch", "connected", Some(connected_at));

        state
            .handle_whatsapp_event(
                test_runtime(),
                WhatsappEvent::Message {
                    wa_message_id: "wa_old".to_string(),
                    chat_jid: "5511999999999@s.whatsapp.net".to_string(),
                    sender_jid: "5511999999999@s.whatsapp.net".to_string(),
                    sender_alt_jid: None,
                    recipient_jid: None,
                    recipient_alt_jid: None,
                    push_name: Some("Old Contact".to_string()),
                    text: Some("old".to_string()),
                    message_type: "text".to_string(),
                    media: None,
                    created_at_wa: connected_at - Duration::seconds(1),
                    is_from_me: false,
                },
            )
            .await;

        assert!(state.conversations("p", "c").is_empty());
        assert!(
            state
                .events()
                .iter()
                .any(|event| event.event_type == "ignored_old_message")
        );
    }

    fn test_runtime() -> ChannelRuntime {
        ChannelRuntime {
            project_id: "p".to_string(),
            company_id: "c".to_string(),
            channel_id: "ch".to_string(),
            session_path: "/tmp/rustzap-test-session.sqlite".into(),
        }
    }
}
