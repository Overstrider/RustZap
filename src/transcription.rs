use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::config::GroqConfig;
use crate::models::Transcript;

#[derive(Debug, Clone)]
pub enum TranscriptLifecycle {
    Pending,
    Processing,
    Completed,
    Failed(String),
    SkippedSizeLimit,
    SkippedUnsupportedType,
}

impl TranscriptLifecycle {
    pub fn status(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Completed => "completed",
            Self::Failed(_) => "failed",
            Self::SkippedSizeLimit => "skipped_size_limit",
            Self::SkippedUnsupportedType => "skipped_unsupported_type",
        }
    }
}

#[derive(Clone)]
pub struct GroqSttClient {
    config: GroqConfig,
    http: reqwest::Client,
}

impl GroqSttClient {
    pub fn new(config: GroqConfig) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(config.stt_timeout_seconds))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            config,
        }
    }

    pub async fn transcribe_bytes(
        &self,
        filename: &str,
        mime_type: &str,
        bytes: Vec<u8>,
    ) -> Result<String, String> {
        if !mime_type.starts_with("audio/") {
            return Err("unsupported audio MIME type".to_string());
        }
        let max_bytes = self.config.stt_max_audio_mb * 1024 * 1024;
        if bytes.len() as u64 > max_bytes {
            return Err("audio exceeds Groq STT size limit".to_string());
        }
        let api_key = self
            .config
            .api_key
            .as_deref()
            .ok_or_else(|| "GROQ_API_KEY missing".to_string())?;
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(filename.to_string())
            .mime_str(mime_type)
            .map_err(|err| format!("invalid audio MIME type: {err}"))?;
        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model", self.config.stt_model.clone())
            .text("language", self.config.stt_language.clone())
            .text("response_format", self.config.stt_response_format.clone());
        let response = self
            .http
            .post("https://api.groq.com/openai/v1/audio/transcriptions")
            .bearer_auth(api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|err| format!("Groq STT request failed: {err}"))?;
        let status = response.status();
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|err| format!("Groq STT response decode failed: {err}"))?;
        if !status.is_success() {
            return Err(format!("Groq STT returned {status}: {body}"));
        }
        body.get("text")
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .ok_or_else(|| "Groq STT response missing text".to_string())
    }
}

pub fn mock_transcript(
    project_id: &str,
    company_id: &str,
    message_id: &str,
    media_id: Option<String>,
) -> Transcript {
    let now = OffsetDateTime::now_utc();
    Transcript {
        id: format!("tr_{}", Uuid::now_v7().simple()),
        project_id: project_id.to_string(),
        company_id: company_id.to_string(),
        message_id: message_id.to_string(),
        media_id,
        provider: "groq".to_string(),
        model: "whisper-large-v3-turbo".to_string(),
        language: "pt".to_string(),
        text: Some("Transcricao simulada para audio de desenvolvimento.".to_string()),
        raw_response_json: json!({"mock": true}),
        status: "completed".to_string(),
        error_message: None,
        created_at: now,
        updated_at: now,
    }
}

pub fn transcript_with_lifecycle(
    project_id: &str,
    company_id: &str,
    message_id: &str,
    media_id: Option<String>,
    lifecycle: TranscriptLifecycle,
    text: Option<String>,
) -> Transcript {
    let now = OffsetDateTime::now_utc();
    let error_message = match &lifecycle {
        TranscriptLifecycle::Failed(message) => Some(message.clone()),
        _ => None,
    };
    Transcript {
        id: format!("tr_{}", Uuid::now_v7().simple()),
        project_id: project_id.to_string(),
        company_id: company_id.to_string(),
        message_id: message_id.to_string(),
        media_id,
        provider: "groq".to_string(),
        model: "whisper-large-v3-turbo".to_string(),
        language: "pt".to_string(),
        text,
        raw_response_json: json!({"lifecycle": lifecycle.status()}),
        status: lifecycle.status().to_string(),
        error_message,
        created_at: now,
        updated_at: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groq_mock_transcript_completes() {
        let transcript = mock_transcript("p", "c", "m", Some("media".to_string()));
        assert_eq!(transcript.provider, "groq");
        assert_eq!(transcript.status, "completed");
        assert!(transcript.text.unwrap().contains("Transcricao"));
    }

    #[test]
    fn transcript_lifecycle_supports_skipped_statuses() {
        let transcript = transcript_with_lifecycle(
            "p",
            "c",
            "m",
            Some("media".to_string()),
            TranscriptLifecycle::SkippedSizeLimit,
            None,
        );
        assert_eq!(transcript.status, "skipped_size_limit");
        assert!(transcript.text.is_none());
    }
}
