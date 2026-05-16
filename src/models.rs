use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use serde_json::Value;
use time::OffsetDateTime;

pub(crate) mod serde_time_compat {
    use std::collections::HashMap;

    use serde::{Deserialize, Deserializer, Serializer, de, ser::SerializeMap};
    use serde_json::Value;
    use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

    pub(crate) fn to_rfc3339<E>(value: &OffsetDateTime) -> Result<String, E>
    where
        E: serde::ser::Error,
    {
        value
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(E::custom)
    }

    pub(crate) fn from_json_value<E>(value: Value) -> Result<OffsetDateTime, E>
    where
        E: de::Error,
    {
        match value {
            Value::String(value) => {
                OffsetDateTime::parse(&value, &time::format_description::well_known::Rfc3339)
                    .map_err(E::custom)
            }
            Value::Array(values) => {
                let mut parts = Vec::with_capacity(values.len());
                for value in values {
                    let Some(part) = value.as_i64() else {
                        return Err(E::custom(
                            "legacy OffsetDateTime array contains non-integer",
                        ));
                    };
                    parts.push(part);
                }
                legacy_array_to_offset_datetime(&parts).map_err(E::custom)
            }
            _ => Err(E::custom(
                "expected RFC3339 string or legacy OffsetDateTime array",
            )),
        }
    }

    fn legacy_array_to_offset_datetime(parts: &[i64]) -> Result<OffsetDateTime, String> {
        if parts.len() != 9 {
            return Err(format!(
                "legacy OffsetDateTime array must have 9 values, got {}",
                parts.len()
            ));
        }
        let year = i32::try_from(parts[0]).map_err(|_| "year out of range")?;
        let ordinal = u16::try_from(parts[1]).map_err(|_| "ordinal out of range")?;
        let hour = u8::try_from(parts[2]).map_err(|_| "hour out of range")?;
        let minute = u8::try_from(parts[3]).map_err(|_| "minute out of range")?;
        let second = u8::try_from(parts[4]).map_err(|_| "second out of range")?;
        let nanos = u32::try_from(parts[5]).map_err(|_| "nanoseconds out of range")?;
        let offset_hour = i8::try_from(parts[6]).map_err(|_| "offset hour out of range")?;
        let offset_minute = i8::try_from(parts[7]).map_err(|_| "offset minute out of range")?;
        let offset_second = i8::try_from(parts[8]).map_err(|_| "offset second out of range")?;

        let date = Date::from_ordinal_date(year, ordinal).map_err(|err| err.to_string())?;
        let time =
            Time::from_hms_nano(hour, minute, second, nanos).map_err(|err| err.to_string())?;
        let offset = UtcOffset::from_hms(offset_hour, offset_minute, offset_second)
            .map_err(|err| err.to_string())?;
        Ok(PrimitiveDateTime::new(date, time).assume_offset(offset))
    }

    pub(crate) fn serialize<S>(value: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_rfc3339(value)?)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        from_json_value(Value::deserialize(deserializer)?)
    }

    pub(crate) mod option {
        use super::*;

        pub(crate) fn serialize<S>(
            value: &Option<OffsetDateTime>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match value {
                Some(value) => serializer.serialize_some(&to_rfc3339(value)?),
                None => serializer.serialize_none(),
            }
        }

        pub(crate) fn deserialize<'de, D>(
            deserializer: D,
        ) -> Result<Option<OffsetDateTime>, D::Error>
        where
            D: Deserializer<'de>,
        {
            match Option::<Value>::deserialize(deserializer)? {
                Some(Value::Null) | None => Ok(None),
                Some(value) => from_json_value(value).map(Some),
            }
        }
    }

    pub(crate) mod map {
        use super::*;

        pub(crate) fn serialize<S>(
            value: &HashMap<String, OffsetDateTime>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut map = serializer.serialize_map(Some(value.len()))?;
            for (key, value) in value {
                map.serialize_entry(key, &to_rfc3339(value)?)?;
            }
            map.end()
        }

        pub(crate) fn deserialize<'de, D>(
            deserializer: D,
        ) -> Result<HashMap<String, OffsetDateTime>, D::Error>
        where
            D: Deserializer<'de>,
        {
            HashMap::<String, Value>::deserialize(deserializer)?
                .into_iter()
                .map(|(key, value)| from_json_value(value).map(|value| (key, value)))
                .collect()
        }
    }
}

pub const INTERNAL_PROJECT_ID: &str = "rustzap_internal";

pub fn internal_project_id() -> String {
    INTERNAL_PROJECT_ID.to_string()
}

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
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyChannelPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub channel_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyConversationPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub conversation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyMessagePath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyMediaPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub media_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyContactPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub contact_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyGroupPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub group_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyGroupMemberPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub group_id: String,
    pub contact_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyContactPhonePath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub phone_e164: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyGroupJoinRequestPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub group_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectCompanyCallbackPath {
    #[serde(default = "internal_project_id")]
    pub project_id: String,
    pub company_id: String,
    pub callback_id: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone_number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_picture_media_id: Option<String>,
    pub avatar_url: Option<String>,
    pub profile_picture_url: Option<String>,
    pub last_seq: i64,
    #[serde(with = "crate::models::serde_time_compat::option")]
    pub last_message_at: Option<OffsetDateTime>,
    pub unread_count: i64,
    pub is_archived: bool,
    pub is_muted: bool,
    pub is_pinned: bool,
    pub control_mode: String,
    #[serde(with = "crate::models::serde_time_compat")]
    pub created_at: OffsetDateTime,
    #[serde(with = "crate::models::serde_time_compat")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    NotApplicable,
    Pending,
    Sent,
    Delivered,
    Read,
    Failed,
}

impl DeliveryState {
    pub fn for_message(direction: &str, message_type: &str, status: &str) -> Self {
        if !matches!(direction, "outbound" | "out") || message_type == "system" {
            return Self::NotApplicable;
        }
        match status {
            "queued" => Self::Pending,
            "sent_to_whatsapp" | "server_ack" => Self::Sent,
            "delivered" => Self::Delivered,
            "read" | "played" => Self::Read,
            "failed" => Self::Failed,
            _ => Self::NotApplicable,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
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
    #[serde(with = "crate::models::serde_time_compat")]
    pub created_at_wa: OffsetDateTime,
    #[serde(with = "crate::models::serde_time_compat")]
    pub created_at: OffsetDateTime,
    #[serde(with = "crate::models::serde_time_compat")]
    pub updated_at: OffsetDateTime,
}

impl Message {
    pub fn delivery_state(&self) -> DeliveryState {
        DeliveryState::for_message(&self.direction, &self.message_type, &self.status)
    }
}

impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Message", 29)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("project_id", &self.project_id)?;
        state.serialize_field("company_id", &self.company_id)?;
        state.serialize_field("conversation_id", &self.conversation_id)?;
        state.serialize_field("channel_account_id", &self.channel_account_id)?;
        state.serialize_field("conversation_seq", &self.conversation_seq)?;
        state.serialize_field("wa_message_id", &self.wa_message_id)?;
        state.serialize_field("direction", &self.direction)?;
        state.serialize_field("sender_contact_id", &self.sender_contact_id)?;
        state.serialize_field("sender_display_name", &self.sender_display_name)?;
        state.serialize_field("message_type", &self.message_type)?;
        state.serialize_field("text", &self.text)?;
        state.serialize_field("media_id", &self.media_id)?;
        state.serialize_field("media_url", &self.media_url)?;
        state.serialize_field("thumbnail_url", &self.thumbnail_url)?;
        state.serialize_field("mime_type", &self.mime_type)?;
        state.serialize_field("file_name", &self.file_name)?;
        state.serialize_field("quoted_message_id", &self.quoted_message_id)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("delivery_state", &self.delivery_state())?;
        state.serialize_field("error_message", &self.error_message)?;
        state.serialize_field("is_starred", &self.is_starred)?;
        state.serialize_field("is_pinned", &self.is_pinned)?;
        state.serialize_field("reaction", &self.reaction)?;
        state.serialize_field("sent_by_source", &self.sent_by_source)?;
        state.serialize_field("sent_by_external_user_id", &self.sent_by_external_user_id)?;
        state.serialize_field(
            "created_at_wa",
            &serde_time_compat::to_rfc3339::<S::Error>(&self.created_at_wa)?,
        )?;
        state.serialize_field(
            "created_at",
            &serde_time_compat::to_rfc3339::<S::Error>(&self.created_at)?,
        )?;
        state.serialize_field(
            "updated_at",
            &serde_time_compat::to_rfc3339::<S::Error>(&self.updated_at)?,
        )?;
        state.end()
    }
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
    #[serde(with = "crate::models::serde_time_compat")]
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
    #[serde(with = "crate::models::serde_time_compat::option")]
    pub expires_at: Option<OffsetDateTime>,
    #[serde(with = "crate::models::serde_time_compat::option")]
    pub saved_at: Option<OffsetDateTime>,
    #[serde(with = "crate::models::serde_time_compat")]
    pub created_at: OffsetDateTime,
    #[serde(with = "crate::models::serde_time_compat")]
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
    #[serde(with = "crate::models::serde_time_compat")]
    pub created_at: OffsetDateTime,
    #[serde(with = "crate::models::serde_time_compat")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyConversationItem {
    pub conversation_id: String,
    pub max_seq: i64,
    pub reason: String,
    pub priority: i32,
    #[serde(with = "crate::models::serde_time_compat")]
    pub available_at: OffsetDateTime,
    pub lease_token: String,
    #[serde(with = "crate::models::serde_time_compat")]
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
    #[serde(with = "crate::models::serde_time_compat::option")]
    pub first_failed_at: Option<OffsetDateTime>,
    #[serde(with = "crate::models::serde_time_compat::option")]
    pub last_failed_at: Option<OffsetDateTime>,
    #[serde(with = "crate::models::serde_time_compat::option")]
    pub next_retry_at: Option<OffsetDateTime>,
    #[serde(with = "crate::models::serde_time_compat")]
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
    #[serde(with = "crate::models::serde_time_compat")]
    pub occurred_at: OffsetDateTime,
    #[serde(with = "crate::models::serde_time_compat")]
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

#[cfg(test)]
mod tests {
    use super::DeliveryState;

    #[test]
    fn delivery_state_maps_public_message_states() {
        assert_eq!(
            DeliveryState::for_message("inbound", "text", "read"),
            DeliveryState::NotApplicable
        );
        assert_eq!(
            DeliveryState::for_message("outbound", "system", "read"),
            DeliveryState::NotApplicable
        );
        assert_eq!(
            DeliveryState::for_message("outbound", "text", "queued"),
            DeliveryState::Pending
        );
        assert_eq!(
            DeliveryState::for_message("outbound", "text", "sent_to_whatsapp"),
            DeliveryState::Sent
        );
        assert_eq!(
            DeliveryState::for_message("outbound", "text", "server_ack"),
            DeliveryState::Sent
        );
        assert_eq!(
            DeliveryState::for_message("outbound", "text", "delivered"),
            DeliveryState::Delivered
        );
        assert_eq!(
            DeliveryState::for_message("outbound", "text", "read"),
            DeliveryState::Read
        );
        assert_eq!(
            DeliveryState::for_message("outbound", "audio", "played"),
            DeliveryState::Read
        );
        assert_eq!(
            DeliveryState::for_message("outbound", "text", "failed"),
            DeliveryState::Failed
        );
    }
}
