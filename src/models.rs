use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub service: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub ok: bool,
    pub checks: Vec<ReadyCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyCheck {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyPath {
    pub project_id: String,
    pub company_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectPath {
    pub project_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyChannelPath {
    pub project_id: String,
    pub company_id: String,
    pub channel_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyConversationPath {
    pub project_id: String,
    pub company_id: String,
    pub conversation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyMessagePath {
    pub project_id: String,
    pub company_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyMediaPath {
    pub project_id: String,
    pub company_id: String,
    pub media_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyContactPath {
    pub project_id: String,
    pub company_id: String,
    pub contact_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyGroupPath {
    pub project_id: String,
    pub company_id: String,
    pub group_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyGroupMemberPath {
    pub project_id: String,
    pub company_id: String,
    pub group_id: String,
    pub contact_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PageQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub q: Option<String>,
    pub direction: Option<String>,
    pub before_seq: Option<i64>,
    pub after_seq: Option<i64>,
    pub order: Option<String>,
    pub consumer_id: Option<String>,
    pub mode: Option<String>,
}

impl PageQuery {
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(50).clamp(1, 500)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectRequest {
    pub id: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompanyRequest {
    pub id: Option<String>,
    pub external_company_id: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelAccountRequest {
    pub id: Option<String>,
    pub label: Option<String>,
    pub phone_e164: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
    pub project_id: String,
    pub company_id: String,
    pub channel_account_id: String,
    #[serde(rename = "type")]
    pub conversation_type: String,
    pub contact_id: Option<String>,
    pub group_id: Option<String>,
    pub display_name: Option<String>,
    pub display_phone: Option<String>,
    pub avatar_url: Option<String>,
    pub profile_picture_url: Option<String>,
    pub last_seq: i64,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_message_at: Option<OffsetDateTime>,
    pub unread_count: i64,
    pub is_archived: bool,
    pub is_muted: bool,
    pub is_pinned: bool,
    pub control_mode: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub project_id: String,
    pub company_id: String,
    pub conversation_id: String,
    pub channel_account_id: String,
    pub conversation_seq: i64,
    pub wa_message_id: Option<String>,
    pub direction: String,
    pub sender_contact_id: Option<String>,
    pub sender_display_name: Option<String>,
    pub message_type: String,
    pub text: Option<String>,
    pub media_id: Option<String>,
    pub media_url: Option<String>,
    pub thumbnail_url: Option<String>,
    pub mime_type: Option<String>,
    pub file_name: Option<String>,
    pub quoted_message_id: Option<String>,
    pub status: String,
    pub error_message: Option<String>,
    pub is_starred: bool,
    pub is_pinned: bool,
    pub reaction: Option<String>,
    pub sent_by_source: Option<String>,
    pub sent_by_external_user_id: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at_wa: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageRequest {
    #[serde(rename = "type")]
    pub message_type: String,
    pub text: Option<String>,
    pub media_id: Option<String>,
    pub caption: Option<String>,
    pub filename: Option<String>,
    pub quoted_message_id: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulateInboundTextRequest {
    pub conversation_id: Option<String>,
    pub channel_id: Option<String>,
    pub from_phone_e164: Option<String>,
    pub sender_name: Option<String>,
    pub profile_picture_url: Option<String>,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulateInboundMediaRequest {
    pub conversation_id: Option<String>,
    pub channel_id: Option<String>,
    pub from_phone_e164: Option<String>,
    pub sender_name: Option<String>,
    pub media_type: Option<String>,
    pub mime_type: Option<String>,
    pub size_bytes: Option<u64>,
    pub filename: Option<String>,
    pub caption: Option<String>,
    pub profile_picture_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiptRequest {
    pub message_id: String,
    pub receipt_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagesPage {
    pub conversation_id: String,
    pub from_seq: Option<i64>,
    pub to_seq: Option<i64>,
    pub has_more: bool,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureCapability {
    pub supported: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_admin: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guaranteed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitiesResponse {
    pub provider: String,
    pub features: std::collections::BTreeMap<String, FeatureCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QrState {
    pub channel_id: String,
    pub status: String,
    pub qr_code_text: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaObject {
    pub id: String,
    pub project_id: String,
    pub company_id: String,
    pub conversation_id: String,
    pub message_id: Option<String>,
    pub media_type: String,
    pub mime_type: String,
    pub original_filename: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
    pub storage_status: String,
    pub bucket: Option<String>,
    pub object_key: Option<String>,
    pub permanent_object_key: Option<String>,
    pub public_url: Option<String>,
    pub thumbnail_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<f64>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub saved_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transcript {
    pub id: String,
    pub project_id: String,
    pub company_id: String,
    pub message_id: String,
    pub media_id: Option<String>,
    pub provider: String,
    pub model: String,
    pub language: String,
    pub text: Option<String>,
    pub raw_response_json: Value,
    pub status: String,
    pub error_message: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyConversationItem {
    pub conversation_id: String,
    pub max_seq: i64,
    pub reason: String,
    pub priority: i32,
    #[serde(with = "time::serde::rfc3339")]
    pub available_at: OffsetDateTime,
    pub lease_token: String,
    #[serde(with = "time::serde::rfc3339")]
    pub locked_until: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyListResponse {
    pub items: Vec<DirtyConversationItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyAckRequest {
    pub consumer_id: String,
    pub processed_until_seq: i64,
    pub lease_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDeliveryAttempt {
    pub id: String,
    pub project_id: String,
    pub company_id: String,
    pub callback_id: String,
    pub event_id: String,
    pub attempt: u32,
    pub status: String,
    pub http_status: Option<u16>,
    pub error_message: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub first_failed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_failed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub next_retry_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonEvent {
    pub event_id: String,
    pub event_type: String,
    pub project_id: String,
    pub company_id: String,
    pub channel_id: Option<String>,
    pub conversation_id: Option<String>,
    pub message_id: Option<String>,
    pub conversation_seq: Option<i64>,
    pub trace_id: String,
    pub causation_id: Option<String>,
    pub correlation_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub produced_at: OffsetDateTime,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    #[serde(rename = "type")]
    pub kind: String,
    pub project_id: String,
    pub company_id: String,
    pub topics: Vec<String>,
}
