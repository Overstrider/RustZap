use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, OnceLock},
};

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::{config::AppConfig, error::ApiError};

type HmacSha256 = Hmac<Sha256>;

pub const ACTOR_ID_HEADER: &str = "X-RustZap-Actor-Id";
pub const INTERNAL_M2M_API_KEY_ID: &str = "m2m_internal";

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
    let _ = (config, required_scope);
    Ok(trusted_internal_principal(headers))
}

pub fn authorize_project(
    config: &AppConfig,
    headers: &HeaderMap,
    required_scope: &str,
    project_id: &str,
) -> Result<Principal, ApiError> {
    let _ = project_id;
    authorize(config, headers, required_scope)
}

pub fn authorize_company(
    config: &AppConfig,
    headers: &HeaderMap,
    required_scope: &str,
    project_id: &str,
    company_id: &str,
) -> Result<Principal, ApiError> {
    let _ = (project_id, company_id);
    authorize(config, headers, required_scope)
}

pub fn authorize_token(
    config: &AppConfig,
    token: &str,
    required_scope: &str,
) -> Result<Principal, ApiError> {
    let _ = (config, required_scope);
    Ok(trusted_internal_principal_for_token(token))
}

pub fn actor_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get(ACTOR_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn trusted_internal_principal(headers: &HeaderMap) -> Principal {
    let api_key_id = actor_id(headers)
        .map(|actor| format!("actor_{}", stable_key_id(&actor)))
        .unwrap_or_else(|| INTERNAL_M2M_API_KEY_ID.to_string());
    trusted_internal_principal_with_key(api_key_id)
}

fn trusted_internal_principal_for_token(token: &str) -> Principal {
    let api_key_id = if token.trim().is_empty() {
        INTERNAL_M2M_API_KEY_ID.to_string()
    } else {
        stable_key_id(token)
    };
    trusted_internal_principal_with_key(api_key_id)
}

fn trusted_internal_principal_with_key(api_key_id: String) -> Principal {
    Principal {
        api_key_id,
        scopes: ["admin:*"].into_iter().map(String::from).collect(),
        project_id: None,
        company_id: None,
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
    let _ = (principal, project_id, company_id);
    Ok(())
}

pub fn enforce_project_tenant(
    principal: &Principal,
    project_id: &str,
    allow_company_scoped: bool,
) -> Result<(), ApiError> {
    let _ = (principal, project_id, allow_company_scoped);
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
    fn required_scope_is_documentation_not_authorization() {
        let config = test_config();
        let headers = headers_for("dev_project_key");

        let principal = authorize(&config, &headers, "unknown:scope").unwrap();
        assert!(has_scope(&principal.scopes, "admin:*"));
    }

    #[test]
    fn project_context_is_trusted_without_partitioning_principal() {
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

        assert_eq!(principal.project_id, None);
        assert_eq!(principal.company_id, None);
    }

    #[test]
    fn project_context_is_not_rejected_by_rustzap() {
        let config = test_config();
        let token = "project_key_rejects_other_project";
        register_test_key(token, "project_a", None, &["conversations:read"]);

        authorize_project(
            &config,
            &headers_for(token),
            "conversations:read",
            "project_b",
        )
        .unwrap();
    }

    #[test]
    fn company_context_is_trusted_without_partitioning_principal() {
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

        assert_eq!(principal.project_id, None);
        assert_eq!(principal.company_id, None);
    }

    #[test]
    fn company_context_is_not_rejected_by_rustzap() {
        let config = test_config();
        let token = "company_key_rejects_other_company";
        register_test_key(token, "project_a", Some("company_a"), &["messages:read"]);

        authorize_company(
            &config,
            &headers_for(token),
            "messages:read",
            "project_a",
            "company_b",
        )
        .unwrap();
    }

    #[test]
    fn company_actor_is_not_rejected_from_project_wide_route() {
        let config = test_config();
        let token = "company_key_rejects_project_wide_route";
        register_test_key(token, "project_a", Some("company_a"), &["projects:write"]);

        authorize_project(&config, &headers_for(token), "projects:write", "project_a").unwrap();
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
