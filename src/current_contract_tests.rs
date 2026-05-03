use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode, header},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    AppState, build_router,
    config::{AppConfig, EventBusMode, MetadataDbMode, StorageProvider},
    models::INTERNAL_PROJECT_ID,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct TestResponse {
    status: StatusCode,
    headers: HeaderMap,
    json: Value,
}

fn unique_dir(label: &str) -> PathBuf {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rustzap-current-contract-{label}-{}-{id}",
        std::process::id()
    ))
}

fn test_config(dev_simulation_enabled: bool) -> AppConfig {
    let mut config = AppConfig::from_env();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    config.env = "test".to_string();
    config.dev_mode = true;
    config.dev_simulation_enabled = dev_simulation_enabled;
    config.metadata_db = MetadataDbMode::InMemory;
    config.database_url = None;
    config.event_bus = EventBusMode::InMemory;
    config.storage_provider = StorageProvider::LocalFs;
    config.local_storage_dir = unique_dir("local-storage");
    config.media_local_temp_dir = unique_dir("media-temp");
    config.wa_session_sqlite_dir = unique_dir("wa-session");
    config.r2.base_prefix = format!("rustzap-test-{id}");
    config.webhook.delivery_enabled = false;
    config.media_sniff_magic_bytes = false;
    config.rate_limits.requests_per_minute_per_project = 1_000_000;
    config.rate_limits.send_message_per_minute_per_channel = 1_000_000;
    config.rate_limits.send_message_per_minute_per_conversation = 1_000_000;
    config.rate_limits.media_downloads_per_minute_per_channel = 1_000_000;
    config.rate_limits.stt_per_minute_per_project = 1_000_000;
    config.rate_limits.admin_requests_per_minute = 1_000_000;
    config
}

fn test_app(dev_simulation_enabled: bool) -> Router {
    build_router(AppState::new(test_config(dev_simulation_enabled)))
}

async fn request_json(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> TestResponse {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let body = match body {
        Some(body) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|err| {
            panic!(
                "response body is not JSON: {err}; body={}",
                String::from_utf8_lossy(&bytes)
            )
        })
    };
    TestResponse {
        status,
        headers,
        json,
    }
}

async fn get_json(app: &Router, uri: &str) -> TestResponse {
    request_json(app, Method::GET, uri, None, &[]).await
}

async fn post_json(app: &Router, uri: &str, body: Value) -> TestResponse {
    request_json(app, Method::POST, uri, Some(body), &[]).await
}

async fn post_empty(app: &Router, uri: &str) -> TestResponse {
    request_json(app, Method::POST, uri, None, &[]).await
}

fn assert_error(response: &TestResponse, status: StatusCode, code: &str) {
    assert_eq!(response.status, status, "{:?}", response.json);
    assert_eq!(response.json["error"]["code"], code);
    assert!(response.json["error"]["request_id"].as_str().is_some());
}

#[tokio::test]
async fn current_m2m_contract_trusts_internal_company_context_and_isolates_by_company() {
    let app = test_app(true);

    let create = post_json(
        &app,
        "/v1/companies",
        json!({"id": "company_a", "name": "Company A"}),
    )
    .await;
    assert_eq!(create.status, StatusCode::OK, "{:?}", create.json);
    assert_eq!(create.json["id"], "company_a");
    assert_eq!(create.json["project_id"], INTERNAL_PROJECT_ID);

    let inbound = request_json(
        &app,
        Method::POST,
        "/v1/dev/companies/company_a/simulate/inbound-text",
        Some(json!({
            "conversation_id": "5511999000000@s.whatsapp.net",
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999000000",
            "sender_name": "Contact A",
            "text": "hello from company a"
        })),
        &[("authorization", "Bearer invalid-but-trusted-m2m")],
    )
    .await;
    assert_eq!(inbound.status, StatusCode::OK, "{:?}", inbound.json);
    assert_eq!(inbound.json["project_id"], INTERNAL_PROJECT_ID);
    assert_eq!(inbound.json["company_id"], "company_a");
    assert_eq!(inbound.json["direction"], "inbound");

    let company_a = get_json(&app, "/v1/companies/company_a/conversations").await;
    assert_eq!(company_a.status, StatusCode::OK, "{:?}", company_a.json);
    assert_eq!(company_a.json["items"].as_array().unwrap().len(), 1);
    assert_eq!(company_a.json["items"][0]["company_id"], "company_a");
    assert_eq!(company_a.json["items"][0]["phone_number"], "5511999000000");

    let company_b = get_json(&app, "/v1/companies/company_b/conversations").await;
    assert_eq!(company_b.status, StatusCode::OK, "{:?}", company_b.json);
    assert_eq!(company_b.json["items"].as_array().unwrap().len(), 0);

    let openapi = get_json(&app, "/openapi.json").await;
    let paths = openapi.json["paths"].as_object().unwrap();
    assert!(
        paths.contains_key("/v1/companies/{company_id}/conversations/{conversation_id}/messages")
    );
    assert!(!paths.contains_key(
        "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/messages"
    ));
}

#[tokio::test]
async fn outbound_send_records_actor_and_enforces_idempotency_on_current_route_shape() {
    let app = test_app(true);
    let uri = "/v1/companies/company_send/conversations/5511999112233/messages";
    let request_body = json!({"type": "text", "text": "idempotent message", "metadata": {"source": "contract_test"}});

    let first = request_json(
        &app,
        Method::POST,
        uri,
        Some(request_body.clone()),
        &[
            ("Idempotency-Key", "send-current-contract-1"),
            ("X-RustZap-Actor-Id", "actor_contract"),
        ],
    )
    .await;
    assert_eq!(first.status, StatusCode::ACCEPTED, "{:?}", first.json);
    assert_eq!(first.json["accepted"], true);
    assert_eq!(first.json["status"], "queued");
    assert_eq!(first.json["delivery_state"], "pending");
    assert_eq!(first.json["message"]["conversation_id"], "5511999112233");
    assert_eq!(
        first.json["message"]["sent_by_external_user_id"],
        "actor_contract"
    );
    let command_id = first.json["command_id"].as_str().unwrap().to_string();

    let replay = request_json(
        &app,
        Method::POST,
        uri,
        Some(request_body),
        &[("Idempotency-Key", "send-current-contract-1")],
    )
    .await;
    assert_eq!(replay.status, StatusCode::ACCEPTED, "{:?}", replay.json);
    assert_eq!(replay.json["command_id"], command_id);
    assert_eq!(
        replay.json["message"]["sent_by_external_user_id"],
        "actor_contract"
    );

    let conflict = request_json(
        &app,
        Method::POST,
        uri,
        Some(json!({"type": "text", "text": "different body"})),
        &[("Idempotency-Key", "send-current-contract-1")],
    )
    .await;
    assert_error(&conflict, StatusCode::CONFLICT, "idempotency_conflict");

    let messages = get_json(
        &app,
        "/v1/companies/company_send/conversations/5511999112233/messages?after_seq=0",
    )
    .await;
    assert_eq!(messages.status, StatusCode::OK, "{:?}", messages.json);
    assert_eq!(messages.json["messages"].as_array().unwrap().len(), 1);
    assert_eq!(messages.json["messages"][0]["id"], command_id);
}

#[tokio::test]
async fn dev_simulation_endpoints_are_forbidden_when_disabled() {
    let app = test_app(false);

    let response = post_json(
        &app,
        "/v1/dev/companies/company_disabled/simulate/inbound-text",
        json!({
            "conversation_id": "conv_disabled",
            "from_phone_e164": "+5511999000001",
            "text": "blocked"
        }),
    )
    .await;
    assert_error(&response, StatusCode::FORBIDDEN, "forbidden");
}

#[tokio::test]
async fn dev_simulation_covers_text_audio_receipts_qr_groups_and_reset() {
    let app = test_app(true);

    let text = post_json(
        &app,
        "/v1/dev/companies/company_dev/simulate/inbound-text",
        json!({
            "conversation_id": "5511999330000@s.whatsapp.net",
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999330000",
            "sender_name": "Dev Contact",
            "text": "dev text"
        }),
    )
    .await;
    assert_eq!(text.status, StatusCode::OK, "{:?}", text.json);
    assert_eq!(text.json["status"], "received");
    let message_id = text.json["id"].as_str().unwrap().to_string();

    let receipt = post_json(
        &app,
        "/v1/dev/companies/company_dev/simulate/receipt",
        json!({"message_id": message_id, "receipt_type": "delivered"}),
    )
    .await;
    assert_eq!(receipt.status, StatusCode::OK, "{:?}", receipt.json);
    assert_eq!(receipt.json["status"], "delivered");

    let audio = post_json(
        &app,
        "/v1/dev/companies/company_dev/simulate/inbound-audio",
        json!({
            "conversation_id": "5511999330000@s.whatsapp.net",
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999330000",
            "sender_name": "Dev Contact",
            "mime_type": "audio/ogg",
            "size_bytes": 4096,
            "filename": "voice.ogg",
            "caption": "audio caption"
        }),
    )
    .await;
    assert_eq!(audio.status, StatusCode::OK, "{:?}", audio.json);
    assert_eq!(audio.json["message"]["message_type"], "audio");
    assert_eq!(audio.json["media"]["storage_status"], "temp");
    assert_eq!(audio.json["transcript"]["status"], "pending");

    let qr = post_empty(&app, "/v1/dev/companies/company_dev/simulate/qr-rotate").await;
    assert_eq!(qr.status, StatusCode::OK, "{:?}", qr.json);
    assert_eq!(qr.json["status"], "waiting_qr");
    assert!(
        qr.json["qr_code_text"]
            .as_str()
            .unwrap()
            .starts_with("rustzap-dev-qr-channel_dev-")
    );

    let group_id = "120363333333333333@g.us";
    let group = post_json(
        &app,
        "/v1/dev/companies/company_dev/simulate/group-event",
        json!({
            "group_id": group_id,
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999330000",
            "member_name": "Group Member",
            "text": "group text"
        }),
    )
    .await;
    assert_eq!(group.status, StatusCode::OK, "{:?}", group.json);
    assert_eq!(group.json["event"], "group.updated");
    assert_eq!(group.json["group_id"], group_id);

    let groups = get_json(&app, "/v1/companies/company_dev/groups").await;
    assert_eq!(groups.status, StatusCode::OK, "{:?}", groups.json);
    assert_eq!(groups.json["items"].as_array().unwrap().len(), 1);
    assert_eq!(groups.json["items"][0]["id"], group_id);

    let reset = post_empty(&app, "/v1/dev/companies/company_dev/simulate/reset").await;
    assert_eq!(reset.status, StatusCode::OK, "{:?}", reset.json);
    assert_eq!(reset.json["reset"], true);

    let conversations = get_json(&app, "/v1/companies/company_dev/conversations").await;
    assert_eq!(
        conversations.status,
        StatusCode::OK,
        "{:?}",
        conversations.json
    );
    assert_eq!(conversations.json["items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn dirty_leases_require_tokens_and_preserve_new_max_seq_races() {
    let app = test_app(true);
    let conversation_id = "conv_dirty_contract";

    let first = post_json(
        &app,
        "/v1/dev/companies/company_dirty/simulate/inbound-text",
        json!({
            "conversation_id": conversation_id,
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999440001",
            "text": "first dirty"
        }),
    )
    .await;
    assert_eq!(first.status, StatusCode::OK, "{:?}", first.json);
    let seq1 = first.json["conversation_seq"].as_i64().unwrap();

    let dirty = get_json(
        &app,
        "/v1/companies/company_dirty/dirty-conversations?consumer_id=alpha&limit=10",
    )
    .await;
    assert_eq!(dirty.status, StatusCode::OK, "{:?}", dirty.json);
    assert_eq!(dirty.json["items"].as_array().unwrap().len(), 1);
    assert_eq!(dirty.json["items"][0]["conversation_id"], conversation_id);
    assert_eq!(dirty.json["items"][0]["max_seq"], seq1);
    let old_lease = dirty.json["items"][0]["lease_token"]
        .as_str()
        .unwrap()
        .to_string();

    let second = post_json(
        &app,
        "/v1/dev/companies/company_dirty/simulate/inbound-text",
        json!({
            "conversation_id": conversation_id,
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999440001",
            "text": "second dirty"
        }),
    )
    .await;
    assert_eq!(second.status, StatusCode::OK, "{:?}", second.json);
    let seq2 = second.json["conversation_seq"].as_i64().unwrap();
    assert!(seq2 > seq1);

    let old_ack = post_json(
        &app,
        "/v1/companies/company_dirty/dirty-conversations/conv_dirty_contract/ack",
        json!({
            "consumer_id": "alpha",
            "processed_until_seq": seq1,
            "lease_token": old_lease
        }),
    )
    .await;
    assert_eq!(old_ack.status, StatusCode::OK, "{:?}", old_ack.json);
    assert_eq!(old_ack.json["acked"], true);
    assert_eq!(old_ack.json["remaining_dirty"], true);
    assert_eq!(old_ack.json["current_max_seq"], seq2);

    let dirty_again = get_json(
        &app,
        "/v1/companies/company_dirty/dirty-conversations?consumer_id=alpha&limit=10",
    )
    .await;
    assert_eq!(dirty_again.status, StatusCode::OK, "{:?}", dirty_again.json);
    assert_eq!(dirty_again.json["items"].as_array().unwrap().len(), 1);
    assert_eq!(dirty_again.json["items"][0]["max_seq"], seq2);
    let new_lease = dirty_again.json["items"][0]["lease_token"]
        .as_str()
        .unwrap()
        .to_string();

    let wrong_token = post_json(
        &app,
        "/v1/companies/company_dirty/dirty-conversations/conv_dirty_contract/ack",
        json!({
            "consumer_id": "alpha",
            "processed_until_seq": seq2,
            "lease_token": "lease_wrong"
        }),
    )
    .await;
    assert_error(&wrong_token, StatusCode::CONFLICT, "conflict");

    let final_ack = post_json(
        &app,
        "/v1/companies/company_dirty/dirty-conversations/conv_dirty_contract/ack",
        json!({
            "consumer_id": "alpha",
            "processed_until_seq": seq2,
            "lease_token": new_lease
        }),
    )
    .await;
    assert_eq!(final_ack.status, StatusCode::OK, "{:?}", final_ack.json);
    assert_eq!(final_ack.json["remaining_dirty"], false);
}

#[tokio::test]
async fn media_simulation_covers_temp_quarantine_rejected_and_pii_safe_keys() {
    let app = test_app(true);

    let temp = post_json(
        &app,
        "/v1/dev/companies/company_media/simulate/inbound-image",
        json!({
            "conversation_id": "5511999550000@s.whatsapp.net",
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999550000",
            "sender_name": "Maria",
            "mime_type": "image/png",
            "size_bytes": 1024,
            "filename": "documento-maria.png",
            "caption": "document"
        }),
    )
    .await;
    assert_eq!(temp.status, StatusCode::OK, "{:?}", temp.json);
    assert_eq!(temp.json["media"]["storage_status"], "temp");
    let object_key = temp.json["media"]["object_key"].as_str().unwrap();
    assert!(object_key.contains("conversation_hash="));
    assert!(!object_key.contains("5511999550000"));
    assert!(!object_key.contains("maria"));
    assert!(!object_key.contains("@s.whatsapp.net"));

    let quarantine = post_json(
        &app,
        "/v1/dev/companies/company_media/simulate/inbound-image",
        json!({
            "conversation_id": "5511999550001@s.whatsapp.net",
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999550001",
            "mime_type": "application/pdf",
            "size_bytes": 27262976,
            "filename": "large.pdf"
        }),
    )
    .await;
    assert_eq!(quarantine.status, StatusCode::OK, "{:?}", quarantine.json);
    assert_eq!(quarantine.json["media"]["storage_status"], "quarantine");
    assert!(quarantine.json["media"]["object_key"].as_str().is_some());

    let rejected = post_json(
        &app,
        "/v1/dev/companies/company_media/simulate/inbound-image",
        json!({
            "conversation_id": "5511999550002@s.whatsapp.net",
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999550002",
            "mime_type": "application/pdf",
            "size_bytes": 105906176,
            "filename": "too-large.pdf"
        }),
    )
    .await;
    assert_eq!(rejected.status, StatusCode::OK, "{:?}", rejected.json);
    assert_eq!(rejected.json["media"]["storage_status"], "rejected");
    assert!(rejected.json["media"]["object_key"].is_null());
}

#[tokio::test]
async fn unsupported_provider_commands_return_standard_not_supported_errors() {
    let app = test_app(true);

    let inbound = post_json(
        &app,
        "/v1/dev/companies/company_caps/simulate/inbound-text",
        json!({
            "conversation_id": "conv_caps",
            "channel_id": "channel_dev",
            "from_phone_e164": "+5511999660000",
            "text": "capability target"
        }),
    )
    .await;
    assert_eq!(inbound.status, StatusCode::OK, "{:?}", inbound.json);
    let message_id = inbound.json["id"].as_str().unwrap().to_string();

    let capabilities = get_json(
        &app,
        "/v1/companies/company_caps/channels/whatsapp/accounts/channel_dev/capabilities",
    )
    .await;
    assert_eq!(
        capabilities.status,
        StatusCode::OK,
        "{:?}",
        capabilities.json
    );
    assert_eq!(
        capabilities.json["features"]["pin_message"]["supported"],
        false
    );
    assert_eq!(
        capabilities.json["features"]["star_message"]["supported"],
        false
    );
    assert_eq!(
        capabilities.json["features"]["mark_read"]["supported"],
        true
    );
    assert_eq!(
        capabilities.json["features"]["mark_read"]["guaranteed"],
        false
    );

    let pair = post_empty(
        &app,
        "/v1/companies/company_caps/channels/whatsapp/accounts/channel_dev/pair-code",
    )
    .await;
    assert_error(&pair, StatusCode::NOT_IMPLEMENTED, "not_supported");

    let pin = post_empty(
        &app,
        &format!("/v1/companies/company_caps/messages/{message_id}/pin"),
    )
    .await;
    assert_error(&pin, StatusCode::NOT_IMPLEMENTED, "not_supported");

    let star = post_empty(
        &app,
        &format!("/v1/companies/company_caps/messages/{message_id}/star"),
    )
    .await;
    assert_error(&star, StatusCode::NOT_IMPLEMENTED, "not_supported");

    let mark_read = post_empty(
        &app,
        "/v1/companies/company_caps/conversations/conv_caps/mark-read",
    )
    .await;
    assert_error(&mark_read, StatusCode::NOT_IMPLEMENTED, "not_supported");

    let group_id = "120363444444444444@g.us";
    let group = post_json(
        &app,
        "/v1/dev/companies/company_caps/simulate/group-event",
        json!({"group_id": group_id, "channel_id": "channel_dev", "text": "group"}),
    )
    .await;
    assert_eq!(group.status, StatusCode::OK, "{:?}", group.json);

    let group_exit = post_empty(
        &app,
        "/v1/companies/company_caps/groups/120363444444444444%40g.us/exit",
    )
    .await;
    assert_error(&group_exit, StatusCode::NOT_IMPLEMENTED, "not_supported");
}

#[tokio::test]
async fn standard_error_envelope_uses_request_id_header_when_present() {
    let app = test_app(true);

    let response = request_json(
        &app,
        Method::POST,
        "/v1/companies/company_errors/conversations/5511999770000/messages",
        Some(json!({"type": "text", "text": "missing idempotency"})),
        &[("x-request-id", "req_contract_fixed")],
    )
    .await;
    assert_error(&response, StatusCode::BAD_REQUEST, "bad_request");
    assert_eq!(
        response
            .headers
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap(),
        "req_contract_fixed"
    );
    assert_eq!(
        response.json["error"]["request_id"].as_str().unwrap(),
        "req_contract_fixed"
    );
}
