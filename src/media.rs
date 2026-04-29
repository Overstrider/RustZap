use time::{Date, macros::format_description};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaDecision {
    Temp,
    Quarantine,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct MediaLimits {
    pub quick_delete_threshold_mb: u64,
    pub reject_threshold_mb: u64,
}

impl MediaLimits {
    pub fn classify(&self, size_bytes: u64) -> MediaDecision {
        let quick = self.quick_delete_threshold_mb * 1024 * 1024;
        let reject = self.reject_threshold_mb * 1024 * 1024;
        if size_bytes > reject {
            MediaDecision::Rejected
        } else if size_bytes > quick {
            MediaDecision::Quarantine
        } else {
            MediaDecision::Temp
        }
    }
}

pub fn sniff_mime_from_magic(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.starts_with(b"%PDF-") {
        Some("application/pdf")
    } else if bytes.starts_with(b"OggS") {
        Some("audio/ogg")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WAVE") {
        Some("audio/wav")
    } else if bytes.starts_with(b"ID3") || bytes.starts_with(&[0xff, 0xfb]) {
        Some("audio/mpeg")
    } else if bytes.len() >= 12 && bytes.get(4..8) == Some(b"ftyp") {
        Some("video/mp4")
    } else {
        None
    }
}

pub fn magic_matches_mime(declared_mime: Option<&str>, bytes: &[u8]) -> bool {
    let Some(sniffed) = sniff_mime_from_magic(bytes) else {
        return true;
    };
    let Some(declared) = declared_mime else {
        return true;
    };
    let declared = declared
        .split(';')
        .next()
        .unwrap_or(declared)
        .trim()
        .to_ascii_lowercase();
    declared == sniffed
        || (declared == "audio/mp4" && sniffed == "video/mp4")
        || (declared == "video/quicktime" && sniffed == "video/mp4")
}

#[derive(Debug, Clone)]
pub struct R2ObjectKeyInput<'a> {
    pub base_prefix: &'a str,
    pub class: &'a str,
    pub project_id: &'a str,
    pub company_id: &'a str,
    pub channel_id: &'a str,
    pub conversation_id: Option<&'a str>,
    pub entity_type: Option<&'a str>,
    pub entity_id: Option<&'a str>,
    pub date: Date,
    pub media_id: &'a str,
    pub ext: &'a str,
}

pub fn r2_object_key(input: R2ObjectKeyInput<'_>) -> String {
    let date = input
        .date
        .format(format_description!("[year]-[month]-[day]"))
        .expect("date format is valid");
    let ext = input.ext.trim_start_matches('.');
    match input.class {
        "permanent" => format!(
            "{}/permanent/project={}/company={}/entity={}/entity_id={}/date={}/media={}.{}",
            input.base_prefix,
            input.project_id,
            input.company_id,
            input.entity_type.unwrap_or("unknown"),
            input.entity_id.unwrap_or("unknown"),
            date,
            input.media_id,
            ext
        ),
        "outbound-temp" => format!(
            "{}/outbound-temp/project={}/company={}/channel={}/date={}/upload={}.{}",
            input.base_prefix,
            input.project_id,
            input.company_id,
            input.channel_id,
            date,
            input.media_id,
            ext
        ),
        class => format!(
            "{}/{}/project={}/company={}/channel={}/conversation={}/date={}/media={}.{}",
            input.base_prefix,
            class,
            input.project_id,
            input.company_id,
            input.channel_id,
            input.conversation_id.unwrap_or("unknown"),
            date,
            input.media_id,
            ext
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::date;

    #[test]
    fn media_limits_classify_temp_quarantine_rejected() {
        let limits = MediaLimits {
            quick_delete_threshold_mb: 25,
            reject_threshold_mb: 100,
        };
        assert_eq!(limits.classify(10 * 1024 * 1024), MediaDecision::Temp);
        assert_eq!(limits.classify(26 * 1024 * 1024), MediaDecision::Quarantine);
        assert_eq!(limits.classify(101 * 1024 * 1024), MediaDecision::Rejected);
    }

    #[test]
    fn r2_key_uses_ids_not_pii() {
        let key = r2_object_key(R2ObjectKeyInput {
            base_prefix: "rustzap",
            class: "temp",
            project_id: "tetoz",
            company_id: "company_123",
            channel_id: "channel_123",
            conversation_id: Some("conv_123"),
            entity_type: None,
            entity_id: None,
            date: date!(2026 - 04 - 26),
            media_id: "media_123",
            ext: ".ogg",
        });

        assert_eq!(
            key,
            "rustzap/temp/project=tetoz/company=company_123/channel=channel_123/conversation=conv_123/date=2026-04-26/media=media_123.ogg"
        );
        assert!(!key.contains("+55"));
        assert!(!key.contains("maria"));
    }

    #[test]
    fn magic_sniff_rejects_mime_mismatch() {
        let png = b"\x89PNG\r\n\x1a\nrest";

        assert_eq!(sniff_mime_from_magic(png), Some("image/png"));
        assert!(magic_matches_mime(Some("image/png"), png));
        assert!(!magic_matches_mime(Some("audio/ogg"), png));
    }
}
