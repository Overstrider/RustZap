use std::{
    collections::HashMap,
    fs,
    io::{Seek, Write},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};

use crate::models::{CapabilitiesResponse, FeatureCapability};
use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use wacore::{
    download::MediaType,
    proto_helpers::MessageExt,
    stanza::groups::GroupNotificationAction,
    types::{events::Event, message::MessageInfo, presence::ReceiptType},
};
use waproto::whatsapp as wa;
use whatsapp_rust::{Client, Jid, TokioRuntime, bot::Bot, store::SqliteStore};
use whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory;
use whatsapp_rust_ureq_http_client::UreqHttpClient;

pub fn session_sqlite_path(
    base_dir: impl Into<PathBuf>,
    project_id: &str,
    company_id: &str,
    channel_id: &str,
) -> PathBuf {
    base_dir
        .into()
        .join(project_id)
        .join(company_id)
        .join(channel_id)
        .join("session.sqlite")
}

pub fn capabilities() -> CapabilitiesResponse {
    let mut features = std::collections::BTreeMap::new();
    features.insert("send_text".to_string(), supported());
    features.insert("send_media".to_string(), supported());
    features.insert("send_reaction".to_string(), supported());
    features.insert(
        "pair_code".to_string(),
        FeatureCapability {
            supported: false,
            reason: Some("pair-code API is not available in the active adapter".to_string()),
            requires_admin: None,
            guaranteed: None,
        },
    );
    features.insert(
        "pin_message".to_string(),
        FeatureCapability {
            supported: false,
            reason: Some("pin is not available in the active adapter".to_string()),
            requires_admin: None,
            guaranteed: None,
        },
    );
    features.insert(
        "star_message".to_string(),
        FeatureCapability {
            supported: false,
            reason: Some("star is not available in the active adapter".to_string()),
            requires_admin: None,
            guaranteed: None,
        },
    );
    features.insert(
        "mark_read".to_string(),
        FeatureCapability {
            supported: true,
            reason: None,
            requires_admin: None,
            guaranteed: Some(false),
        },
    );
    features.insert(
        "read_receipts".to_string(),
        FeatureCapability {
            supported: true,
            reason: None,
            requires_admin: None,
            guaranteed: Some(false),
        },
    );
    features.insert(
        "groups_manage".to_string(),
        FeatureCapability {
            supported: false,
            reason: Some(
                "group admin commands are not implemented by the active adapter".to_string(),
            ),
            requires_admin: Some(true),
            guaranteed: None,
        },
    );
    features.insert("groups_read".to_string(), supported());
    features.insert(
        "group_exit".to_string(),
        unsupported_group_admin("group exit"),
    );
    features.insert(
        "group_member_add".to_string(),
        unsupported_group_admin("group member add"),
    );
    features.insert(
        "group_member_remove".to_string(),
        unsupported_group_admin("group member remove"),
    );
    features.insert(
        "group_invite_accept".to_string(),
        unsupported_group_admin("group invite accept"),
    );
    features.insert(
        "group_member_promote".to_string(),
        unsupported_group_admin("group member promote"),
    );
    features.insert(
        "group_member_demote".to_string(),
        unsupported_group_admin("group member demote"),
    );
    features.insert(
        "group_join_request_accept".to_string(),
        unsupported_group_admin("group join request accept"),
    );
    features.insert(
        "group_join_request_reject".to_string(),
        unsupported_group_admin("group join request reject"),
    );

    CapabilitiesResponse {
        provider: "whatsapp-rust".to_string(),
        features,
    }
}

pub fn is_updates_surface_jid(jid: &str) -> bool {
    let normalized = jid.trim().to_ascii_lowercase();
    normalized == "status@broadcast"
        || normalized.ends_with("@newsletter")
        || normalized.ends_with("@broadcast")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelRuntime {
    pub project_id: String,
    pub company_id: String,
    pub channel_id: String,
    pub session_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ContactProfile {
    pub requested_jid: String,
    pub resolved_jid: Option<String>,
    pub lid: Option<String>,
    pub phone_e164: Option<String>,
    pub business_description: Option<String>,
    pub profile_picture_id: Option<String>,
    pub profile_picture_media_id: Option<String>,
    pub profile_picture_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GroupProfile {
    pub group_jid: String,
    pub subject: Option<String>,
    pub description: Option<String>,
    pub owner_jid: Option<String>,
    pub subject_owner_jid: Option<String>,
    pub created_at_wa_unix: Option<u64>,
    pub size: Option<u32>,
    pub profile_picture_id: Option<String>,
    pub profile_picture_media_id: Option<String>,
    pub profile_picture_url: Option<String>,
    pub participants: Vec<GroupParticipantProfile>,
}

#[derive(Debug, Clone)]
pub struct GroupParticipantProfile {
    pub contact_id: String,
    pub phone_jid: Option<String>,
    pub is_admin: bool,
}

#[derive(Debug, Clone)]
pub struct OutboundMediaMessage {
    pub media_type: String,
    pub mime_type: String,
    pub filename: Option<String>,
    pub caption: Option<String>,
    pub bytes: Vec<u8>,
    pub ptt: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundMediaDescriptor {
    pub media_type: String,
    pub mime_type: String,
    pub filename: Option<String>,
    pub caption: Option<String>,
    pub direct_path: String,
    pub media_key_b64: String,
    pub file_sha256_b64: String,
    pub file_enc_sha256_b64: String,
    pub file_length: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_seconds: Option<f64>,
}

#[derive(Clone, Default)]
pub struct WhatsappManager {
    supervisors: Arc<Mutex<HashMap<String, ChannelSupervisor>>>,
    intents: Arc<Mutex<HashMap<String, ReconnectIntent>>>,
}

struct ChannelSupervisor {
    client: Arc<Client>,
    task: Arc<tokio::task::JoinHandle<()>>,
}

/// Per-channel reconnection bookkeeping. `desired` distinguishes an operator
/// connect (keep the channel up, respawn on runtime death) from an explicit
/// disconnect or a WhatsApp logout (stay down). `needs_session_reset` marks
/// credentials invalidated by a LoggedOut event: the session sqlite must be
/// wiped before the next pairing attempt or the client re-loads the dead
/// credentials and is immediately logged out again.
#[derive(Default, Clone)]
struct ReconnectIntent {
    desired: bool,
    needs_session_reset: bool,
    attempts: u32,
}

const RECONNECT_BASE_DELAY_SECS: u64 = 5;
const RECONNECT_MAX_DELAY_SECS: u64 = 300;

/// Removes the session sqlite (plus -shm/-wal sidecars) for a channel so the
/// next connect performs a fresh QR pairing instead of resuming dead
/// credentials.
pub fn wipe_session_files(session_path: &std::path::Path) {
    for suffix in ["", "-shm", "-wal"] {
        let mut os_path = session_path.as_os_str().to_owned();
        os_path.push(suffix);
        let path = PathBuf::from(os_path);
        match fs::remove_file(&path) {
            Ok(()) => tracing::info!(path = %path.display(), "removed WhatsApp session file"),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(path = %path.display(), %err, "failed to remove WhatsApp session file");
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WhatsappEvent {
    Connected,
    Disconnected,
    LoggedOut {
        reason: String,
    },
    PairingQrCode {
        code: String,
        timeout: std::time::Duration,
    },
    HistorySyncIgnored,
    Message {
        wa_message_id: String,
        chat_jid: String,
        sender_jid: String,
        sender_alt_jid: Option<String>,
        recipient_jid: Option<String>,
        recipient_alt_jid: Option<String>,
        push_name: Option<String>,
        text: Option<String>,
        message_type: String,
        media: Option<Box<InboundMediaDescriptor>>,
        created_at_wa: OffsetDateTime,
        is_from_me: bool,
    },
    Receipt {
        wa_message_ids: Vec<String>,
        receipt_type: String,
        chat_jid: String,
        created_at_wa: OffsetDateTime,
    },
    GroupUpdate {
        group_jid: String,
        subject: Option<String>,
        created_at_wa: OffsetDateTime,
    },
    Diagnostic {
        event_type: String,
        payload: serde_json::Value,
    },
}

impl WhatsappManager {
    pub fn set_desired(&self, channel_id: &str, desired: bool) {
        self.intents
            .lock()
            .expect("whatsapp intents lock poisoned")
            .entry(channel_id.to_string())
            .or_default()
            .desired = desired;
    }

    pub fn is_desired(&self, channel_id: &str) -> bool {
        self.intents
            .lock()
            .expect("whatsapp intents lock poisoned")
            .get(channel_id)
            .is_some_and(|intent| intent.desired)
    }

    pub fn mark_needs_session_reset(&self, channel_id: &str) {
        self.intents
            .lock()
            .expect("whatsapp intents lock poisoned")
            .entry(channel_id.to_string())
            .or_default()
            .needs_session_reset = true;
    }

    /// Returns and clears the pending session-reset flag for the channel.
    pub fn take_needs_session_reset(&self, channel_id: &str) -> bool {
        let mut intents = self.intents.lock().expect("whatsapp intents lock poisoned");
        match intents.get_mut(channel_id) {
            Some(intent) if intent.needs_session_reset => {
                intent.needs_session_reset = false;
                true
            }
            _ => false,
        }
    }

    /// Increments the retry counter and returns the delay to wait before the
    /// next reconnect attempt: 5s doubling up to a 300s ceiling.
    pub fn next_reconnect_delay_secs(&self, channel_id: &str) -> u64 {
        let mut intents = self.intents.lock().expect("whatsapp intents lock poisoned");
        let intent = intents.entry(channel_id.to_string()).or_default();
        let attempt = intent.attempts.min(16);
        intent.attempts = intent.attempts.saturating_add(1);
        (RECONNECT_BASE_DELAY_SECS << attempt).min(RECONNECT_MAX_DELAY_SECS)
    }

    pub fn reset_reconnect_backoff(&self, channel_id: &str) {
        if let Some(intent) = self
            .intents
            .lock()
            .expect("whatsapp intents lock poisoned")
            .get_mut(channel_id)
        {
            intent.attempts = 0;
        }
    }

    pub fn is_channel_active(&self, channel_id: &str) -> bool {
        self.supervisors
            .lock()
            .expect("whatsapp manager lock poisoned")
            .get(channel_id)
            .is_some_and(|supervisor| !supervisor.task.is_finished())
    }

    pub fn is_channel_connected(&self, channel_id: &str) -> bool {
        self.supervisors
            .lock()
            .expect("whatsapp manager lock poisoned")
            .get(channel_id)
            .is_some_and(|supervisor| {
                !supervisor.task.is_finished() && supervisor.client.is_connected()
            })
    }

    pub fn stop_channel(&self, channel_id: &str) {
        if let Some(supervisor) = self
            .supervisors
            .lock()
            .expect("whatsapp manager lock poisoned")
            .remove(channel_id)
        {
            supervisor.task.abort();
        }
    }

    pub async fn start_channel<F, Fut>(&self, runtime: ChannelRuntime, on_event: F) -> Result<()>
    where
        F: Fn(ChannelRuntime, WhatsappEvent) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        {
            let mut supervisors = self
                .supervisors
                .lock()
                .expect("whatsapp manager lock poisoned");
            let existing_is_connected =
                supervisors
                    .get(&runtime.channel_id)
                    .is_some_and(|supervisor| {
                        !supervisor.task.is_finished() && supervisor.client.is_connected()
                    });
            if existing_is_connected {
                return Ok(());
            }
            if let Some(supervisor) = supervisors.remove(&runtime.channel_id)
                && !supervisor.task.is_finished()
            {
                supervisor.task.abort();
            }
        }

        if let Some(parent) = runtime.session_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let database_url = runtime.session_path.to_string_lossy().to_string();
        let backend = Arc::new(
            SqliteStore::new(&database_url)
                .await
                .with_context(|| format!("failed to open WhatsApp session {}", database_url))?,
        );

        let event_runtime = runtime.clone();
        let event_handler = on_event.clone();
        let mut bot = Bot::builder()
            .with_backend(backend)
            .with_transport_factory(TokioWebSocketTransportFactory::new())
            .with_http_client(UreqHttpClient::new())
            .with_runtime(TokioRuntime)
            .skip_history_sync()
            .on_event(move |event, client| {
                let runtime = event_runtime.clone();
                let handler = event_handler.clone();
                async move {
                    client.set_skip_history_sync(true);
                    if let Some(mapped) = map_event(event) {
                        handler(runtime, mapped).await;
                    }
                }
            })
            .build()
            .await
            .context("failed to build WhatsApp bot")?;

        let client = bot.client();
        client.set_skip_history_sync(true);
        let channel_id = runtime.channel_id.clone();
        let task_runtime = runtime.clone();
        let task_handler = on_event.clone();
        let task = tokio::spawn(async move {
            let reason = match bot.run().await {
                Ok(handle) => match handle.await {
                    Ok(()) => "bot_task_finished".to_string(),
                    Err(err) => {
                        tracing::warn!(%channel_id, %err, "WhatsApp bot stopped");
                        format!("bot_task_join_error: {err}")
                    }
                },
                Err(err) => {
                    tracing::error!(%channel_id, error = %err, "failed to run WhatsApp bot");
                    format!("bot_run_error: {err}")
                }
            };
            task_handler(task_runtime.clone(), WhatsappEvent::Disconnected).await;
            task_handler(
                task_runtime,
                WhatsappEvent::Diagnostic {
                    event_type: "channel.runtime_stopped".to_string(),
                    payload: serde_json::json!({ "reason": reason }),
                },
            )
            .await;
        });

        self.supervisors
            .lock()
            .expect("whatsapp manager lock poisoned")
            .insert(
                runtime.channel_id,
                ChannelSupervisor {
                    client,
                    task: Arc::new(task),
                },
            );
        Ok(())
    }

    pub async fn send_text(
        &self,
        channel_id: &str,
        conversation_id: &str,
        text: &str,
    ) -> Result<String> {
        let client = self.active_client(channel_id)?;
        let to = Jid::from_str(conversation_id)
            .with_context(|| format!("conversation_id {conversation_id} is not a WhatsApp JID"))?;
        let message = wa::Message {
            conversation: Some(text.to_string()),
            ..Default::default()
        };
        client.send_message(to, message).await
    }

    pub async fn send_media(
        &self,
        channel_id: &str,
        conversation_id: &str,
        payload: OutboundMediaMessage,
    ) -> Result<String> {
        let client = self.active_client(channel_id)?;
        let to = Jid::from_str(conversation_id)
            .with_context(|| format!("conversation_id {conversation_id} is not a WhatsApp JID"))?;
        let OutboundMediaMessage {
            media_type,
            mime_type,
            filename,
            caption,
            bytes,
            ptt,
        } = payload;
        let wa_media_type = match media_type.as_str() {
            "image" => MediaType::Image,
            "audio" => MediaType::Audio,
            "document" => MediaType::Document,
            other => return Err(anyhow!("unsupported outbound media type {other}")),
        };
        let upload = client
            .upload(bytes, wa_media_type)
            .await
            .with_context(|| format!("failed to upload {media_type} to WhatsApp"))?;
        let media_key_timestamp = Some(OffsetDateTime::now_utc().unix_timestamp());
        let caption = caption
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let message = match wa_media_type {
            MediaType::Image => wa::Message {
                image_message: Some(Box::new(wa::message::ImageMessage {
                    url: Some(upload.url),
                    mimetype: Some(mime_type),
                    caption,
                    file_sha256: Some(upload.file_sha256),
                    file_length: Some(upload.file_length),
                    media_key: Some(upload.media_key),
                    file_enc_sha256: Some(upload.file_enc_sha256),
                    direct_path: Some(upload.direct_path),
                    media_key_timestamp,
                    ..Default::default()
                })),
                ..Default::default()
            },
            MediaType::Audio => wa::Message {
                audio_message: Some(Box::new(wa::message::AudioMessage {
                    url: Some(upload.url),
                    mimetype: Some(mime_type),
                    file_sha256: Some(upload.file_sha256),
                    file_length: Some(upload.file_length),
                    ptt: Some(ptt),
                    media_key: Some(upload.media_key),
                    file_enc_sha256: Some(upload.file_enc_sha256),
                    direct_path: Some(upload.direct_path),
                    media_key_timestamp,
                    ..Default::default()
                })),
                ..Default::default()
            },
            MediaType::Document => {
                let file_name = filename.unwrap_or_else(|| "rustzap-file".to_string());
                wa::Message {
                    document_message: Some(Box::new(wa::message::DocumentMessage {
                        url: Some(upload.url),
                        mimetype: Some(mime_type),
                        title: Some(file_name.clone()),
                        file_sha256: Some(upload.file_sha256),
                        file_length: Some(upload.file_length),
                        media_key: Some(upload.media_key),
                        file_name: Some(file_name),
                        file_enc_sha256: Some(upload.file_enc_sha256),
                        direct_path: Some(upload.direct_path),
                        media_key_timestamp,
                        caption,
                        ..Default::default()
                    })),
                    ..Default::default()
                }
            }
            _ => unreachable!("wa_media_type is constrained above"),
        };
        client.send_message(to, message).await
    }

    pub async fn download_inbound_media_to_writer<W>(
        &self,
        channel_id: &str,
        descriptor: &InboundMediaDescriptor,
        writer: W,
    ) -> Result<W>
    where
        W: Write + Seek + Send + 'static,
    {
        let client = self.active_client(channel_id)?;
        let media_type = match descriptor.media_type.as_str() {
            "image" => MediaType::Image,
            "audio" => MediaType::Audio,
            "document" => MediaType::Document,
            "video" => MediaType::Video,
            other => return Err(anyhow!("unsupported inbound media type {other}")),
        };
        let media_key = BASE64
            .decode(&descriptor.media_key_b64)
            .context("invalid inbound media_key")?;
        let file_sha256 = BASE64
            .decode(&descriptor.file_sha256_b64)
            .context("invalid inbound file_sha256")?;
        let file_enc_sha256 = BASE64
            .decode(&descriptor.file_enc_sha256_b64)
            .context("invalid inbound file_enc_sha256")?;
        client
            .download_from_params_to_writer(
                &descriptor.direct_path,
                &media_key,
                &file_sha256,
                &file_enc_sha256,
                descriptor.file_length,
                media_type,
                writer,
            )
            .await
    }

    pub async fn send_reaction(
        &self,
        channel_id: &str,
        conversation_id: &str,
        target_wa_message_id: &str,
        target_from_me: bool,
        target_participant: Option<&str>,
        emoji: Option<&str>,
    ) -> Result<String> {
        let client = self.active_client(channel_id)?;
        let to = Jid::from_str(conversation_id)
            .with_context(|| format!("conversation_id {conversation_id} is not a WhatsApp JID"))?;
        let participant = conversation_id
            .ends_with("@g.us")
            .then(|| target_participant.map(str::to_string))
            .flatten();
        let reaction = wa::message::ReactionMessage {
            key: Some(wa::MessageKey {
                remote_jid: Some(conversation_id.to_string()),
                from_me: Some(target_from_me),
                id: Some(target_wa_message_id.to_string()),
                participant,
            }),
            text: Some(emoji.unwrap_or("").to_string()),
            sender_timestamp_ms: Some(OffsetDateTime::now_utc().unix_timestamp() * 1000),
            ..Default::default()
        };
        client
            .send_message(
                to,
                wa::Message {
                    reaction_message: Some(reaction),
                    ..Default::default()
                },
            )
            .await
    }

    pub async fn contact_profile(&self, channel_id: &str, jid: &str) -> Result<ContactProfile> {
        let client = self.active_client(channel_id)?;
        let requested =
            Jid::from_str(jid).with_context(|| format!("{jid} is not a WhatsApp JID"))?;
        let mut profile = ContactProfile {
            requested_jid: requested.to_string(),
            resolved_jid: None,
            lid: jid.ends_with("@lid").then(|| jid.to_string()),
            phone_e164: phone_from_jid_str(jid),
            business_description: None,
            profile_picture_id: None,
            profile_picture_media_id: None,
            profile_picture_url: None,
        };

        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client
                .contacts()
                .get_user_info(std::slice::from_ref(&requested)),
        )
        .await
        {
            Ok(Ok(infos)) => {
                if let Some(info) = infos.get(&requested).or_else(|| infos.values().next()) {
                    let resolved = info.jid.to_string();
                    profile.phone_e164 = phone_from_jid_str(&resolved).or(profile.phone_e164);
                    profile.resolved_jid = Some(resolved);
                    profile.lid = info.lid.as_ref().map(ToString::to_string).or(profile.lid);
                    profile.profile_picture_id = info.picture_id.clone();
                }
            }
            Ok(Err(err)) => {
                tracing::debug!(%channel_id, %jid, error = %err, "WhatsApp contact info lookup failed");
            }
            Err(_) => {
                tracing::debug!(%channel_id, %jid, "WhatsApp contact info lookup timed out");
            }
        }

        let mut candidates = Vec::new();
        if let Some(resolved_jid) = profile.resolved_jid.as_deref()
            && let Ok(jid) = Jid::from_str(resolved_jid)
        {
            candidates.push(jid);
        }
        candidates.push(requested);

        for candidate in candidates.clone() {
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                client.get_business_profile(&candidate),
            )
            .await
            {
                Ok(Ok(Some(business))) => {
                    let description = business.description.trim();
                    if !description.is_empty() {
                        profile.business_description = Some(description.to_string());
                        break;
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(err)) => {
                    tracing::debug!(
                        %channel_id,
                        jid = %candidate,
                        error = %err,
                        "WhatsApp business profile lookup failed"
                    );
                }
                Err(_) => {
                    tracing::debug!(
                        %channel_id,
                        jid = %candidate,
                        "WhatsApp business profile lookup timed out"
                    );
                }
            }
        }

        for candidate in candidates {
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                client.contacts().get_profile_picture(&candidate, true),
            )
            .await
            {
                Ok(Ok(Some(picture))) => {
                    profile.profile_picture_id = Some(picture.id);
                    profile.profile_picture_url = Some(picture.url);
                    break;
                }
                Ok(Ok(None)) => {}
                Ok(Err(err)) => {
                    tracing::debug!(
                        %channel_id,
                        jid = %candidate,
                        error = %err,
                        "WhatsApp profile picture lookup failed"
                    );
                }
                Err(_) => {
                    tracing::debug!(
                        %channel_id,
                        jid = %candidate,
                        "WhatsApp profile picture lookup timed out"
                    );
                }
            }
        }

        Ok(profile)
    }

    pub async fn group_profile(&self, channel_id: &str, group_jid: &str) -> Result<GroupProfile> {
        let client = self.active_client(channel_id)?;
        let requested = Jid::from_str(group_jid)
            .with_context(|| format!("{group_jid} is not a WhatsApp group JID"))?;
        let metadata = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.groups().get_metadata(&requested),
        )
        .await
        .map_err(|_| anyhow!("WhatsApp group metadata lookup timed out for {group_jid}"))?
        .with_context(|| format!("WhatsApp group metadata lookup failed for {group_jid}"))?;

        let subject = if metadata.subject.trim().is_empty() {
            None
        } else {
            Some(metadata.subject.trim().to_string())
        };
        let participants = metadata
            .participants
            .into_iter()
            .map(|participant| GroupParticipantProfile {
                contact_id: participant.jid.to_string(),
                phone_jid: participant.phone_number.as_ref().map(ToString::to_string),
                is_admin: participant.is_admin,
            })
            .collect();

        let mut profile = GroupProfile {
            group_jid: metadata.id.to_string(),
            subject,
            description: metadata
                .description
                .map(|description| description.trim().to_string())
                .filter(|description| !description.is_empty()),
            owner_jid: metadata.creator.as_ref().map(ToString::to_string),
            subject_owner_jid: metadata.subject_owner.as_ref().map(ToString::to_string),
            created_at_wa_unix: metadata.creation_time,
            size: metadata.size,
            profile_picture_id: None,
            profile_picture_media_id: None,
            profile_picture_url: None,
            participants,
        };

        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.contacts().get_profile_picture(&requested, true),
        )
        .await
        {
            Ok(Ok(Some(picture))) => {
                profile.profile_picture_id = Some(picture.id);
                profile.profile_picture_url = Some(picture.url);
            }
            Ok(Ok(None)) => {}
            Ok(Err(err)) => {
                tracing::debug!(
                    %channel_id,
                    jid = %requested,
                    error = %err,
                    "WhatsApp group profile picture lookup failed"
                );
            }
            Err(_) => {
                tracing::debug!(
                    %channel_id,
                    jid = %requested,
                    "WhatsApp group profile picture lookup timed out"
                );
            }
        }

        Ok(profile)
    }

    fn active_client(&self, channel_id: &str) -> Result<Arc<Client>> {
        self.supervisors
            .lock()
            .expect("whatsapp manager lock poisoned")
            .get(channel_id)
            .filter(|supervisor| !supervisor.task.is_finished())
            .map(|supervisor| supervisor.client.clone())
            .with_context(|| format!("WhatsApp channel {channel_id} is not connected"))
    }
}

fn phone_from_jid_str(jid: &str) -> Option<String> {
    let local = jid.split('@').next().unwrap_or(jid);
    if jid.ends_with("@s.whatsapp.net") && local.chars().all(|ch| ch.is_ascii_digit()) {
        Some(format!("+{local}"))
    } else {
        None
    }
}

fn supported() -> FeatureCapability {
    FeatureCapability {
        supported: true,
        reason: None,
        requires_admin: None,
        guaranteed: None,
    }
}

fn unsupported_group_admin(command: &str) -> FeatureCapability {
    FeatureCapability {
        supported: false,
        reason: Some(format!(
            "{command} is not implemented by the active adapter"
        )),
        requires_admin: Some(true),
        guaranteed: None,
    }
}

fn map_event(event: Event) -> Option<WhatsappEvent> {
    match event {
        Event::Connected(_) | Event::PairSuccess(_) => Some(WhatsappEvent::Connected),
        Event::Disconnected(_) => Some(WhatsappEvent::Disconnected),
        Event::LoggedOut(logged_out) => Some(WhatsappEvent::LoggedOut {
            reason: format!("{:?}", logged_out.reason),
        }),
        Event::PairingQrCode { code, timeout } => {
            Some(WhatsappEvent::PairingQrCode { code, timeout })
        }
        Event::HistorySync(_) => Some(WhatsappEvent::HistorySyncIgnored),
        Event::Message(message, info) => {
            if message_info_has_updates_surface(&info) {
                return Some(updates_ignored_event());
            }
            let mapped = map_message_event(*message, info);
            match &mapped {
                WhatsappEvent::Message {
                    message_type, text, ..
                } if message_type == "system" && text.as_deref().unwrap_or_default().is_empty() => {
                    None
                }
                _ => Some(mapped),
            }
        }
        Event::Receipt(receipt) => {
            if is_updates_surface_jid(&receipt.source.chat.to_string()) {
                return Some(updates_ignored_event());
            }
            Some(WhatsappEvent::Receipt {
                wa_message_ids: receipt
                    .message_ids
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect(),
                receipt_type: receipt_type_name(&receipt.r#type).to_string(),
                chat_jid: receipt.source.chat.to_string(),
                created_at_wa: offset_from_unix(receipt.timestamp.timestamp()),
            })
        }
        Event::GroupUpdate(update) => {
            let group_jid = update.group_jid.to_string();
            if is_updates_surface_jid(&group_jid) {
                return Some(updates_ignored_event());
            }
            let created_at_wa = offset_from_unix(update.timestamp.timestamp());
            let subject = match update.action {
                GroupNotificationAction::Subject { subject, .. } => Some(subject),
                _ => None,
            };
            Some(WhatsappEvent::GroupUpdate {
                group_jid,
                subject,
                created_at_wa,
            })
        }
        Event::StreamReplaced(_) => Some(WhatsappEvent::Diagnostic {
            event_type: "channel.stream_replaced".to_string(),
            payload: serde_json::json!({}),
        }),
        Event::ConnectFailure(failure) => Some(WhatsappEvent::Diagnostic {
            event_type: "channel.connect_failure".to_string(),
            payload: serde_json::json!({
                "reason": format!("{:?}", failure.reason),
                "message": failure.message
            }),
        }),
        _ => None,
    }
}

fn updates_ignored_event() -> WhatsappEvent {
    WhatsappEvent::Diagnostic {
        event_type: "whatsapp_updates.ignored".to_string(),
        payload: serde_json::json!({ "reason": "updates_surface" }),
    }
}

fn message_info_has_updates_surface(info: &MessageInfo) -> bool {
    is_updates_surface_jid(&info.source.chat.to_string())
        || is_updates_surface_jid(&info.source.sender.to_string())
        || info
            .source
            .sender_alt
            .as_ref()
            .is_some_and(|jid| is_updates_surface_jid(&jid.to_string()))
        || info
            .source
            .recipient
            .as_ref()
            .is_some_and(|jid| is_updates_surface_jid(&jid.to_string()))
        || info
            .source
            .recipient_alt
            .as_ref()
            .is_some_and(|jid| is_updates_surface_jid(&jid.to_string()))
}

fn map_message_event(message: wa::Message, info: MessageInfo) -> WhatsappEvent {
    let text = message
        .text_content()
        .map(str::to_string)
        .or_else(|| message.get_caption().map(str::to_string));
    let message_type = whatsapp_message_type(&message, text.as_deref());
    let media = inbound_media_descriptor(&message, &message_type);
    WhatsappEvent::Message {
        wa_message_id: info.id.to_string(),
        chat_jid: info.source.chat.to_string(),
        sender_jid: info.source.sender.to_string(),
        sender_alt_jid: info.source.sender_alt.as_ref().map(ToString::to_string),
        recipient_jid: info.source.recipient.as_ref().map(ToString::to_string),
        recipient_alt_jid: info.source.recipient_alt.as_ref().map(ToString::to_string),
        push_name: (!info.push_name.is_empty()).then_some(info.push_name),
        text,
        message_type,
        media: media.map(Box::new),
        created_at_wa: offset_from_unix(info.timestamp.timestamp()),
        is_from_me: info.source.is_from_me,
    }
}

fn inbound_media_descriptor(
    message: &wa::Message,
    message_type: &str,
) -> Option<InboundMediaDescriptor> {
    match message_type {
        "image" => {
            let image = message.image_message.as_ref()?;
            downloadable_media_descriptor(
                "image",
                image.mimetype.as_deref().unwrap_or("image/jpeg"),
                None,
                image.caption.clone(),
                image.direct_path.clone(),
                image.media_key.clone(),
                image.file_sha256.clone(),
                image.file_enc_sha256.clone(),
                image.file_length,
                image.width,
                image.height,
                None,
            )
        }
        "audio" => {
            let audio = message.audio_message.as_ref()?;
            downloadable_media_descriptor(
                "audio",
                audio.mimetype.as_deref().unwrap_or("audio/ogg"),
                None,
                None,
                audio.direct_path.clone(),
                audio.media_key.clone(),
                audio.file_sha256.clone(),
                audio.file_enc_sha256.clone(),
                audio.file_length,
                None,
                None,
                audio.seconds.map(f64::from),
            )
        }
        "document" => {
            let document = message.document_message.as_ref()?;
            downloadable_media_descriptor(
                "document",
                document
                    .mimetype
                    .as_deref()
                    .unwrap_or("application/octet-stream"),
                document.file_name.clone().or(document.title.clone()),
                document.caption.clone(),
                document.direct_path.clone(),
                document.media_key.clone(),
                document.file_sha256.clone(),
                document.file_enc_sha256.clone(),
                document.file_length,
                None,
                None,
                None,
            )
        }
        "video" => {
            let video = message.video_message.as_ref()?;
            downloadable_media_descriptor(
                "video",
                video.mimetype.as_deref().unwrap_or("video/mp4"),
                None,
                video.caption.clone(),
                video.direct_path.clone(),
                video.media_key.clone(),
                video.file_sha256.clone(),
                video.file_enc_sha256.clone(),
                video.file_length,
                video.width,
                video.height,
                video.seconds.map(f64::from),
            )
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn downloadable_media_descriptor(
    media_type: &str,
    mime_type: &str,
    filename: Option<String>,
    caption: Option<String>,
    direct_path: Option<String>,
    media_key: Option<Vec<u8>>,
    file_sha256: Option<Vec<u8>>,
    file_enc_sha256: Option<Vec<u8>>,
    file_length: Option<u64>,
    width: Option<u32>,
    height: Option<u32>,
    duration_seconds: Option<f64>,
) -> Option<InboundMediaDescriptor> {
    Some(InboundMediaDescriptor {
        media_type: media_type.to_string(),
        mime_type: mime_type.to_string(),
        filename,
        caption,
        direct_path: direct_path?,
        media_key_b64: BASE64.encode(media_key?),
        file_sha256_b64: BASE64.encode(file_sha256?),
        file_enc_sha256_b64: BASE64.encode(file_enc_sha256?),
        file_length: file_length?,
        width,
        height,
        duration_seconds,
    })
}

fn whatsapp_message_type(message: &wa::Message, text: Option<&str>) -> String {
    if text.is_some() {
        return "text".to_string();
    }
    if message.image_message.is_some() {
        return "image".to_string();
    }
    if message.audio_message.is_some() {
        return "audio".to_string();
    }
    if message.document_message.is_some() {
        return "document".to_string();
    }
    if message.video_message.is_some() {
        return "video".to_string();
    }
    "system".to_string()
}

fn receipt_type_name(receipt_type: &ReceiptType) -> &'static str {
    match receipt_type {
        ReceiptType::Delivered | ReceiptType::Sender => "delivered",
        ReceiptType::Read | ReceiptType::ReadSelf => "read",
        ReceiptType::Played | ReceiptType::PlayedSelf => "played",
        ReceiptType::ServerError | ReceiptType::Inactive => "failed",
        _ => "server_ack",
    }
}

fn offset_from_unix(timestamp: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(timestamp).unwrap_or_else(|_| OffsetDateTime::now_utc())
}

pub fn qr_expires_at(timeout: std::time::Duration) -> OffsetDateTime {
    OffsetDateTime::now_utc()
        + Duration::seconds(i64::try_from(timeout.as_secs()).unwrap_or(20).clamp(1, 300))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_path_is_local_sqlite() {
        let path = session_sqlite_path("/data/rustzap/wa-sessions", "p", "c", "ch");
        assert_eq!(
            path.to_string_lossy(),
            "/data/rustzap/wa-sessions/p/c/ch/session.sqlite"
        );
    }

    #[test]
    fn default_manager_has_no_active_channel() {
        let manager = WhatsappManager::default();

        assert!(!manager.is_channel_active("ch"));
        assert!(!manager.is_channel_connected("ch"));
    }

    #[test]
    fn reconnect_delay_doubles_and_caps_at_max() {
        let manager = WhatsappManager::default();

        assert_eq!(manager.next_reconnect_delay_secs("ch"), 5);
        assert_eq!(manager.next_reconnect_delay_secs("ch"), 10);
        assert_eq!(manager.next_reconnect_delay_secs("ch"), 20);
        assert_eq!(manager.next_reconnect_delay_secs("ch"), 40);
        assert_eq!(manager.next_reconnect_delay_secs("ch"), 80);
        assert_eq!(manager.next_reconnect_delay_secs("ch"), 160);
        assert_eq!(manager.next_reconnect_delay_secs("ch"), 300);
        assert_eq!(manager.next_reconnect_delay_secs("ch"), 300);

        manager.reset_reconnect_backoff("ch");
        assert_eq!(manager.next_reconnect_delay_secs("ch"), 5);
    }

    #[test]
    fn desired_flag_tracks_connect_intent() {
        let manager = WhatsappManager::default();

        assert!(!manager.is_desired("ch"));
        manager.set_desired("ch", true);
        assert!(manager.is_desired("ch"));
        manager.set_desired("ch", false);
        assert!(!manager.is_desired("ch"));
    }

    #[test]
    fn session_reset_flag_is_consumed_once() {
        let manager = WhatsappManager::default();

        assert!(!manager.take_needs_session_reset("ch"));
        manager.mark_needs_session_reset("ch");
        assert!(manager.take_needs_session_reset("ch"));
        assert!(!manager.take_needs_session_reset("ch"));
    }

    #[test]
    fn wipe_session_files_removes_sqlite_and_sidecars() {
        let dir = std::env::temp_dir().join(format!(
            "rustzap-wipe-test-{}",
            uuid::Uuid::now_v7().simple()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let session = dir.join("session.sqlite");
        for suffix in ["", "-shm", "-wal"] {
            let mut os_path = session.as_os_str().to_owned();
            os_path.push(suffix);
            fs::write(PathBuf::from(os_path), b"x").expect("write session file");
        }

        wipe_session_files(&session);

        for suffix in ["", "-shm", "-wal"] {
            let mut os_path = session.as_os_str().to_owned();
            os_path.push(suffix);
            assert!(!PathBuf::from(os_path).exists());
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn group_admin_capabilities_do_not_claim_unsupported_commands() {
        let capabilities = capabilities();

        for feature in [
            "groups_manage",
            "group_exit",
            "group_member_add",
            "group_member_remove",
            "group_invite_accept",
            "group_member_promote",
            "group_member_demote",
            "group_join_request_accept",
            "group_join_request_reject",
        ] {
            let capability = capabilities
                .features
                .get(feature)
                .expect("feature should be advertised");
            assert!(!capability.supported, "{feature} should not claim support");
            assert!(capability.reason.is_some(), "{feature} should explain why");
        }
    }
}
