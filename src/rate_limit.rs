use std::{
    collections::HashMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitDecision {
    pub key: String,
    pub retry_after_seconds: u64,
}

#[derive(Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<String, RateLimitBucket>>,
}

#[derive(Debug, Clone)]
struct RateLimitBucket {
    window_minute: u64,
    count: u32,
}

impl RateLimiter {
    pub fn check(&self, keys: &[String], max_per_minute: u32) -> Result<(), RateLimitDecision> {
        if max_per_minute == 0 {
            return Err(RateLimitDecision {
                key: keys
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "global".to_string()),
                retry_after_seconds: seconds_until_next_minute(),
            });
        }

        let window_minute = current_window_minute();
        let mut buckets = self.buckets.lock().expect("rate limiter lock poisoned");
        for key in keys {
            if let Some(bucket) = buckets.get_mut(key) {
                if bucket.window_minute != window_minute {
                    bucket.window_minute = window_minute;
                    bucket.count = 0;
                }
                if bucket.count >= max_per_minute {
                    return Err(RateLimitDecision {
                        key: key.clone(),
                        retry_after_seconds: seconds_until_next_minute(),
                    });
                }
            }
        }

        for key in keys {
            let bucket = buckets.entry(key.clone()).or_insert(RateLimitBucket {
                window_minute,
                count: 0,
            });
            bucket.count = bucket.count.saturating_add(1);
        }
        Ok(())
    }
}

fn current_window_minute() -> u64 {
    unix_seconds() / 60
}

fn seconds_until_next_minute() -> u64 {
    60 - (unix_seconds() % 60)
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limiter_blocks_when_any_key_exceeds_limit() {
        let limiter = RateLimiter::default();
        let keys = vec!["project:p".to_string(), "api_key:k".to_string()];

        assert!(limiter.check(&keys, 1).is_ok());
        let err = limiter.check(&keys, 1).unwrap_err();

        assert!(keys.contains(&err.key));
        assert!((1..=60).contains(&err.retry_after_seconds));
    }
}
