use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::config::GroqConfig;
use crate::models::Transcript;

#[derive(Debug, Clone, PartialEq)]
pub struct GroqTranscription {
    pub text: String,
    pub raw_response_json: serde_json::Value,
}

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
    ) -> Result<GroqTranscription, String> {
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
        let (upload_filename, upload_mime_type, upload_bytes) =
            if self.config.stt_enable_preprocessing {
                preprocess_audio_with_ffmpeg(&self.config, filename, bytes)?
            } else {
                (filename.to_string(), mime_type.to_string(), bytes)
            };
        let part = reqwest::multipart::Part::bytes(upload_bytes)
            .file_name(upload_filename)
            .mime_str(&upload_mime_type)
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
        parse_groq_transcription_response(status, body)
    }
}

fn preprocess_audio_with_ffmpeg(
    config: &GroqConfig,
    filename: &str,
    bytes: Vec<u8>,
) -> Result<(String, String, Vec<u8>), String> {
    let temp_dir = std::env::temp_dir().join("rustzap-stt");
    std::fs::create_dir_all(&temp_dir)
        .map_err(|err| format!("failed to create STT temp dir: {err}"))?;
    let nonce = Uuid::now_v7().simple().to_string();
    let input_ext = filename
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .filter(|ext| !ext.trim().is_empty())
        .unwrap_or("audio");
    let input_path = temp_dir.join(format!("{nonce}.{input_ext}"));
    let output_path = temp_dir.join(format!("{nonce}.wav"));
    std::fs::write(&input_path, bytes)
        .map_err(|err| format!("failed to write STT temp input: {err}"))?;
    let output = std::process::Command::new(&config.ffmpeg_path)
        .arg("-y")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(&input_path)
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg("16000")
        .arg(&output_path)
        .output()
        .map_err(|err| format!("failed to run ffmpeg for STT preprocessing: {err}"))?;
    let _ = std::fs::remove_file(&input_path);
    if !output.status.success() {
        let _ = std::fs::remove_file(&output_path);
        return Err(format!(
            "ffmpeg STT preprocessing failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let preprocessed = std::fs::read(&output_path)
        .map_err(|err| format!("failed to read STT preprocessed audio: {err}"))?;
    let _ = std::fs::remove_file(&output_path);
    Ok((
        "rustzap-stt.wav".to_string(),
        "audio/wav".to_string(),
        preprocessed,
    ))
}

pub fn parse_groq_transcription_response(
    status: reqwest::StatusCode,
    body: serde_json::Value,
) -> Result<GroqTranscription, String> {
    if !status.is_success() {
        return Err(format!("Groq STT returned {status}: {body}"));
    }
    let text = body
        .get("text")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .ok_or_else(|| "Groq STT response missing text".to_string())?;
    Ok(GroqTranscription {
        text,
        raw_response_json: body,
    })
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

pub fn groq_transcript_from_result(
    project_id: &str,
    company_id: &str,
    message_id: &str,
    media_id: Option<String>,
    result: GroqTranscription,
) -> Transcript {
    let mut transcript = transcript_with_lifecycle(
        project_id,
        company_id,
        message_id,
        media_id,
        TranscriptLifecycle::Completed,
        Some(result.text),
    );
    transcript.raw_response_json = result.raw_response_json;
    transcript
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_groq_verbose_json_without_losing_raw_response() {
        let raw = json!({
            "text": "teste de transcricao",
            "duration": 1.25,
            "segments": [{"id": 0, "text": "teste de transcricao"}]
        });

        let parsed = parse_groq_transcription_response(reqwest::StatusCode::OK, raw.clone())
            .expect("verbose_json response should parse");

        assert_eq!(parsed.text, "teste de transcricao");
        assert_eq!(parsed.raw_response_json, raw);
    }

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
