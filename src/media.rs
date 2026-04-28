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
}
