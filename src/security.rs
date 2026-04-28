use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, OnceLock},
};

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::{config::AppConfig, error::ApiError};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct Principal {
    pub api_key_id: String,
    pub scopes: HashSet<String>,
    pub project_id: Option<String>,
    pub company_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ApiKeyPrincipal {
    pub api_key_id: String,
    pub project_id: String,
    pub company_id: Option<String>,
    pub scopes: HashSet<String>,
    pub revoked: bool,
}

static API_KEYS: OnceLock<Mutex<HashMap<String, ApiKeyPrincipal>>> = OnceLock::new();

pub fn authorize(
    config: &AppConfig,
    headers: &HeaderMap,
    required_scope: &str,
) -> Result<Principal, ApiError> {
    let token = bearer_token(headers)
        .ok_or_else(|| ApiError::Unauthorized("missing bearer token".to_string()))?;
    principal_for_token(config, &token, required_scope)
}

pub fn authorize_project(
    config: &AppConfig,
    headers: &HeaderMap,
    required_scope: &str,
    project_id: &str,
) -> Result<Principal, ApiError> {
    let principal = authorize(config, headers, required_scope)?;
    enforce_project_tenant(&principal, project_id, false)?;
    Ok(principal)
}

pub fn authorize_company(
    config: &AppConfig,
    headers: &HeaderMap,
    required_scope: &str,
    project_id: &str,
    company_id: &str,
) -> Result<Principal, ApiError> {
    let principal = authorize(config, headers, required_scope)?;
    enforce_principal_tenant(&principal, project_id, Some(company_id))?;
    Ok(principal)
}

pub fn authorize_token(
    config: &AppConfig,
    token: &str,
    required_scope: &str,
) -> Result<Principal, ApiError> {
    principal_for_token(config, token, required_scope)
}

fn principal_for_token(
    config: &AppConfig,
    token: &str,
    required_scope: &str,
) -> Result<Principal, ApiError> {
    let token_hash = sha256_hex(token);
    if let Some(principal) = API_KEYS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("api key registry lock poisoned")
        .get(&token_hash)
        .cloned()
    {
        if principal.revoked {
            return Err(ApiError::Unauthorized("api key revoked".to_string()));
        }
        if has_scope(&principal.scopes, required_scope) {
            return Ok(Principal {
                api_key_id: principal.api_key_id,
                scopes: principal.scopes,
                project_id: Some(principal.project_id),
                company_id: principal.company_id,
            });
        }
        return Err(ApiError::Forbidden(format!(
            "missing required scope {required_scope}"
        )));
    }

    let scopes = if config.dev_mode && token == config.admin_api_key {
        ["admin:*"].into_iter().map(String::from).collect()
    } else if token == config.project_api_key || token == "dev_ws_token" {
        if !config.dev_mode {
            return Err(ApiError::Unauthorized(
                "fixed development tokens are disabled outside dev mode".to_string(),
            ));
        }
        [
            "projects:write",
            "companies:write",
            "channels:read",
            "channels:write",
            "contacts:read",
            "conversations:read",
            "messages:read",
            "messages:send",
            "messages:manage",
            "media:read",
            "media:write",
            "transcripts:read",
            "transcripts:write",
            "groups:read",
            "groups:manage",
            "dirty:read",
            "dirty:ack",
            "websocket:connect",
            "dev:simulate",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    } else {
        return Err(ApiError::Unauthorized("invalid bearer token".to_string()));
    };

    if has_scope(&scopes, required_scope) {
        Ok(Principal {
            api_key_id: stable_key_id(token),
            scopes,
            project_id: None,
            company_id: None,
        })
    } else {
        Err(ApiError::Forbidden(format!(
            "missing required scope {required_scope}"
        )))
    }
}

pub fn has_scope(scopes: &HashSet<String>, required_scope: &str) -> bool {
    scopes.contains("admin:*") || scopes.contains(required_scope)
}

pub fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_string)
}

pub fn idempotency_key(headers: &HeaderMap) -> Result<String, ApiError> {
    headers
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::BadRequest("missing Idempotency-Key".to_string()))
}

pub fn sha256_json<T: serde::Serialize>(value: &T) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|err| ApiError::BadRequest(format!("invalid JSON body: {err}")))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub fn sha256_hex(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

pub fn generate_api_key() -> (String, String, String) {
    let plaintext = format!("rzp_{}", uuid::Uuid::now_v7().simple());
    let key_hash = sha256_hex(&plaintext);
    let api_key_id = stable_key_id(&plaintext);
    (plaintext, key_hash, api_key_id)
}

pub fn register_project_api_key(
    key_hash: String,
    api_key_id: String,
    project_id: String,
    company_id: Option<String>,
    scopes: HashSet<String>,
    revoked: bool,
) {
    API_KEYS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("api key registry lock poisoned")
        .insert(
            key_hash,
            ApiKeyPrincipal {
                api_key_id,
                project_id,
                company_id,
                scopes,
                revoked,
            },
        );
}

pub fn enforce_principal_tenant(
    principal: &Principal,
    project_id: &str,
    company_id: Option<&str>,
) -> Result<(), ApiError> {
    enforce_project_tenant(principal, project_id, true)?;
    if let (Some(principal_company_id), Some(company_id)) =
        (principal.company_id.as_deref(), company_id)
        && principal_company_id != company_id
    {
        return Err(ApiError::Forbidden("api key company mismatch".to_string()));
    }
    Ok(())
}

pub fn enforce_project_tenant(
    principal: &Principal,
    project_id: &str,
    allow_company_scoped: bool,
) -> Result<(), ApiError> {
    let Some(principal_project_id) = principal.project_id.as_deref() else {
        return Ok(());
    };
    if principal_project_id != project_id {
        return Err(ApiError::Forbidden("api key project mismatch".to_string()));
    }
    if !allow_company_scoped && principal.company_id.is_some() {
        return Err(ApiError::Forbidden(
            "api key company scope cannot access project-wide route".to_string(),
        ));
    }
    Ok(())
}

pub fn webhook_signature(secret: &str, timestamp: &str, raw_body: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(raw_body);
    hex::encode(mac.finalize().into_bytes())
}

fn stable_key_id(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    format!("key_{}", hex::encode(&digest[..8]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use axum::http::{HeaderMap, HeaderValue};

    fn test_config() -> AppConfig {
        let mut config = AppConfig::from_env();
        config.dev_mode = true;
        config.admin_api_key = "dev_admin_key".to_string();
        config.project_api_key = "dev_project_key".to_string();
        config
    }

    fn headers_for(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    fn register_test_key(token: &str, project_id: &str, company_id: Option<&str>, scopes: &[&str]) {
        register_project_api_key(
            sha256_hex(token),
            format!("key_{token}"),
            project_id.to_string(),
            company_id.map(str::to_string),
            scopes.iter().map(|scope| (*scope).to_string()).collect(),
            false,
        );
    }

    #[test]
    fn scope_missing_returns_forbidden() {
        let config = test_config();
        let headers = headers_for("dev_project_key");

        let err = authorize(&config, &headers, "unknown:scope").unwrap_err();
        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn project_key_accepts_same_project() {
        let config = test_config();
        let token = "project_key_accepts_same_project";
        register_test_key(token, "project_a", None, &["conversations:read"]);

        let principal = authorize_project(
            &config,
            &headers_for(token),
            "conversations:read",
            "project_a",
        )
        .unwrap();

        assert_eq!(principal.project_id.as_deref(), Some("project_a"));
        assert_eq!(principal.company_id, None);
    }

    #[test]
    fn project_key_rejects_other_project() {
        let config = test_config();
        let token = "project_key_rejects_other_project";
        register_test_key(token, "project_a", None, &["conversations:read"]);

        let err = authorize_project(
            &config,
            &headers_for(token),
            "conversations:read",
            "project_b",
        )
        .unwrap_err();

        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn company_key_accepts_same_company() {
        let config = test_config();
        let token = "company_key_accepts_same_company";
        register_test_key(token, "project_a", Some("company_a"), &["messages:read"]);

        let principal = authorize_company(
            &config,
            &headers_for(token),
            "messages:read",
            "project_a",
            "company_a",
        )
        .unwrap();

        assert_eq!(principal.project_id.as_deref(), Some("project_a"));
        assert_eq!(principal.company_id.as_deref(), Some("company_a"));
    }

    #[test]
    fn company_key_rejects_other_company() {
        let config = test_config();
        let token = "company_key_rejects_other_company";
        register_test_key(token, "project_a", Some("company_a"), &["messages:read"]);

        let err = authorize_company(
            &config,
            &headers_for(token),
            "messages:read",
            "project_a",
            "company_b",
        )
        .unwrap_err();

        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn company_key_rejects_project_wide_route() {
        let config = test_config();
        let token = "company_key_rejects_project_wide_route";
        register_test_key(token, "project_a", Some("company_a"), &["projects:write"]);

        let err = authorize_project(&config, &headers_for(token), "projects:write", "project_a")
            .unwrap_err();

        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn global_tokens_are_not_tenant_blocked() {
        let config = test_config();

        authorize_company(
            &config,
            &headers_for("dev_project_key"),
            "messages:read",
            "project_a",
            "company_a",
        )
        .unwrap();
        authorize_company(
            &config,
            &headers_for("dev_admin_key"),
            "messages:read",
            "project_b",
            "company_b",
        )
        .unwrap();
    }

    #[test]
    fn webhook_signature_uses_timestamp_dot_body() {
        let sig = webhook_signature("secret", "123", br#"{"ok":true}"#);
        let sig_again = webhook_signature("secret", "123", br#"{"ok":true}"#);
        let sig_changed = webhook_signature("secret", "124", br#"{"ok":true}"#);
        assert_eq!(sig, sig_again);
        assert_ne!(sig, sig_changed);
    }
}
