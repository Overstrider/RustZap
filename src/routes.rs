use std::collections::{HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{
        Multipart, Path, Query, Request, State, WebSocketUpgrade,
        ws::{Message as WsMessage, WebSocket},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use serde::Serialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::{
    config::{EventBusMode, MetadataDbMode},
    error::{ApiError, ApiResult},
    eventbus,
    media::{R2ObjectKeyInput, r2_object_key},
    models::{
        ChannelAccountRequest, CompanyRequest, DirtyAckRequest, DirtyListResponse, PageQuery,
        ProjectCompanyChannelPath, ProjectCompanyContactPath, ProjectCompanyConversationPath,
        ProjectCompanyGroupMemberPath, ProjectCompanyGroupPath, ProjectCompanyMediaPath,
        ProjectCompanyMessagePath, ProjectCompanyPath, ProjectPath, ProjectRequest, ReadyCheck,
        ReadyResponse, ReceiptRequest, SendMessageRequest, SimulateInboundMediaRequest,
        SimulateInboundTextRequest, SubscribeRequest,
    },
    security::{
        Principal, authorize, authorize_company, authorize_project, authorize_token,
        generate_api_key, idempotency_key, register_project_api_key,
    },
    state::{AppState, OutboundMediaUpload, RateLimitScope},
    storage::{copy_r2_object, delete_r2_object, presigned_r2_get_url},
    whatsapp,
};

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(docs))
        .route("/debug/kafka", get(debug_kafka))
        .route("/debug/kafka/deadletters", get(debug_kafka_deadletters))
        .route(
            "/debug/kafka/deadletters/{deadletter_id}/replay",
            post(replay_kafka_deadletter),
        )
        .route("/debug/dirty", get(debug_dirty))
        .route("/debug/channels", get(debug_channels))
        .route("/dev-media/{media_id}", get(dev_media_preview))
        .route("/ws/v1", get(websocket_handler))
        .route("/v1/projects", post(create_project))
        .route("/v1/projects/{project_id}/api-keys", post(create_api_key))
        .route("/v1/projects/{project_id}/companies", post(create_company))
        .route(
            "/v1/projects/{project_id}/companies/{company_id}",
            get(get_company),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts",
            post(create_channel_account),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/connect",
            post(connect_channel),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/disconnect",
            post(disconnect_channel),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}",
            get(get_channel),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/qr",
            get(get_qr),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/pair-code",
            post(pair_code),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/capabilities",
            get(capabilities),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/contacts",
            get(list_contacts),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/contacts/by-phone/{phone_e164}",
            get(get_contact_by_phone),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}",
            get(get_contact),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}/media",
            get(contact_media),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}/conversations",
            get(contact_conversations),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/conversations",
            get(list_conversations),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}",
            get(get_conversation).patch(patch_conversation),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/messages",
            get(list_messages).post(send_message),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/search",
            get(search_messages),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/media",
            get(conversation_media),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/starred",
            get(conversation_starred),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/mark-read",
            post(mark_read),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/typing",
            post(typing),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}",
            get(get_message),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/react",
            post(react_message).delete(delete_react_message),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/pin",
            post(pin_message).delete(unpin_message),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/star",
            post(star_message).delete(unstar_message),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/media/upload-outbound",
            post(upload_outbound),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/media/{media_id}",
            get(get_media).delete(delete_media),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/media/{media_id}/download-url",
            get(download_url),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/media/{media_id}/save",
            post(save_media),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/transcript",
            get(get_transcript),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/transcribe",
            post(transcribe_message),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups",
            get(list_groups),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}",
            get(get_group),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members",
            get(group_members).post(add_group_member),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/media",
            get(group_media),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/starred",
            get(group_starred),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/search",
            get(group_search),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/exit",
            post(group_exit),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}",
            delete(remove_group_member),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}/promote",
            post(promote_group_member),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}/demote",
            post(demote_group_member),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/accept",
            post(accept_join_request),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/reject",
            post(reject_join_request),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/dirty-conversations",
            get(list_dirty),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/dirty-conversations/{conversation_id}/ack",
            post(ack_dirty),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/consumer-callbacks",
            get(list_callbacks).post(create_callback),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/consumer-callbacks/{callback_id}",
            patch(update_callback).delete(delete_callback),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}/export",
            post(privacy_export),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}",
            delete(privacy_delete),
        )
        .route(
            "/v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}/anonymize",
            post(privacy_anonymize),
        )
        .route(
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-text",
            post(dev_inbound_text),
        )
        .route(
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-audio",
            post(dev_inbound_audio),
        )
        .route(
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-image",
            post(dev_inbound_image),
        )
        .route(
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/receipt",
            post(dev_receipt),
        )
        .route(
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/qr-rotate",
            post(dev_qr_rotate),
        )
        .route(
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/group-event",
            post(dev_group_event),
        )
        .route(
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/reset",
            post(dev_reset),
        )
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(middleware::from_fn(request_id_middleware))
        .layer(TraceLayer::new_for_http())
}

async fn request_id_middleware(request: Request, next: Next) -> Response {
    let request_id = request
        .headers()
        .get("X-Request-Id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(crate::error::new_request_id);
    let mut response = crate::error::scope_request_id(request_id.clone(), next.run(request)).await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("X-Request-Id", value.clone());
        response.headers_mut().insert("X-Correlation-Id", value);
    }
    response
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true, "service": "rustzap"}))
}

async fn ready(State(state): State<AppState>) -> Json<ReadyResponse> {
    let session_dir_ok = std::fs::create_dir_all(&state.config.wa_session_sqlite_dir).is_ok();
    let metadata_check = state.metadata_ready().await;
    let event_bus_check = state.event_bus_ready().await;
    let metadata_ok = match state.config.metadata_db {
        MetadataDbMode::InMemory => true,
        MetadataDbMode::Postgres => {
            state.config.database_url.as_deref().is_some() && metadata_check.is_ok()
        }
    };
    Json(ReadyResponse {
        ok: metadata_ok && session_dir_ok && event_bus_check.ok,
        checks: vec![
            ReadyCheck {
                name: "metadata_db".to_string(),
                ok: metadata_ok,
                detail: match state.config.metadata_db {
                    MetadataDbMode::InMemory => "in-memory dev store".to_string(),
                    MetadataDbMode::Postgres => state
                        .config
                        .database_url
                        .as_ref()
                        .map(|_| {
                            metadata_check
                                .as_ref()
                                .map(|_| "postgres ready".to_string())
                                .unwrap_or_else(|err| format!("postgres not ready: {err}"))
                        })
                        .unwrap_or_else(|| "DATABASE_URL missing".to_string()),
                },
            },
            ReadyCheck {
                name: "event_bus".to_string(),
                ok: event_bus_check.ok,
                detail: event_bus_check.detail,
            },
            ReadyCheck {
                name: "wa_session_dir".to_string(),
                ok: session_dir_ok,
                detail: state.config.wa_session_sqlite_dir.display().to_string(),
            },
        ],
    })
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let event_bus = state.event_bus_snapshot();
    let runtime = state.runtime_metrics_snapshot();
    format!(
        "rustzap_up 1\nrustzap_eventbus_publish_attempts {}\nrustzap_eventbus_publish_successes {}\nrustzap_eventbus_publish_failures {}\nrustzap_eventbus_deadletter_attempts {}\nrustzap_eventbus_deadletter_successes {}\nrustzap_rate_limited_total {}\nrustzap_webhook_delivery_attempts_total {}\nrustzap_webhook_delivery_successes_total {}\nrustzap_stt_requests_total {}\nrustzap_websocket_subscriptions_total {}\n",
        event_bus.publish_attempts,
        event_bus.publish_successes,
        event_bus.publish_failures,
        event_bus.deadletter_attempts,
        event_bus.deadletter_successes,
        runtime.rate_limited_total,
        runtime.webhook_delivery_attempts_total,
        runtime.webhook_delivery_successes_total,
        runtime.stt_requests_total,
        runtime.websocket_subscriptions_total
    )
}

async fn docs() -> Html<&'static str> {
    Html(
        "<!doctype html><title>RustZap API</title><h1>RustZap API</h1><a href=\"/openapi.json\">OpenAPI JSON</a>",
    )
}

async fn openapi_json() -> Json<Value> {
    let mut spec = json!({
        "openapi": "3.1.0",
        "info": {"title": "RustZap", "version": "0.1.0"},
        "components": {
            "securitySchemes": {
                "bearerAuth": {"type": "http", "scheme": "bearer"}
            },
            "schemas": {
                "ErrorEnvelope": {
                    "type": "object",
                    "required": ["error"],
                    "properties": {
                        "error": {
                            "type": "object",
                            "required": ["code", "message", "details", "request_id"],
                            "properties": {
                                "code": {"type": "string"},
                                "message": {"type": "string"},
                                "details": {"type": "object"},
                                "request_id": {"type": "string"}
                            }
                        }
                    }
                },
                "PaginatedResponse": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array", "items": {}},
                        "next_cursor": {"type": ["string", "null"]},
                        "has_more": {"type": "boolean"},
                        "total": {"type": "integer"}
                    }
                }
            }
        },
        "paths": {
            "/health": {"get": {"summary": "Liveness"}},
            "/ready": {"get": {"summary": "Readiness"}},
            "/ws/v1": {"get": {"summary": "WebSocket events"}},
            "/v1/projects/{project_id}/companies/{company_id}/contacts": {
                "get": {
                    "summary": "List contacts",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["contacts:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}": {
                "get": {"summary": "Inspect contact", "security": [{"bearerAuth": ["contacts:read"]}]}
            },
            "/v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}/media": {
                "get": {
                    "summary": "List contact media",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["media:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}/conversations": {
                "get": {
                    "summary": "List contact conversations",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["contacts:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/conversations": {
                "get": {
                    "summary": "List conversations",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["conversations:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/messages": {
                "get": {"summary": "Read messages by cursor"},
                "post": {"summary": "Send idempotent message"}
            },
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/search": {
                "get": {
                    "summary": "Search conversation messages",
                    "parameters": [{"name": "q", "in": "query"}, {"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["messages:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/media": {
                "get": {
                    "summary": "List conversation media",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["media:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/starred": {
                "get": {
                    "summary": "List starred conversation messages",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["messages:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/groups": {
                "get": {
                    "summary": "List groups",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["groups:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}": {
                "get": {"summary": "Inspect group", "security": [{"bearerAuth": ["groups:read"]}]}
            },
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members": {
                "get": {
                    "summary": "List group members",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["groups:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/media": {
                "get": {
                    "summary": "List group media",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["media:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/starred": {
                "get": {
                    "summary": "List starred group messages",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["messages:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/search": {
                "get": {
                    "summary": "Search group messages",
                    "parameters": [{"name": "q", "in": "query"}, {"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                    "security": [{"bearerAuth": ["messages:read"]}]
                }
            },
            "/v1/projects/{project_id}/companies/{company_id}/dirty-conversations": {
                "get": {"summary": "List compact dirty signals"}
            },
            "/v1/projects/{project_id}/companies/{company_id}/consumer-callbacks": {
                "get": {"summary": "List persisted callbacks"},
                "post": {"summary": "Create persisted callback"}
            },
            "/v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}/export": {
                "post": {"summary": "Export contact privacy data"}
            },
            "/v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}": {
                "delete": {"summary": "Delete and redact contact privacy data"}
            },
            "/v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}/anonymize": {
                "post": {"summary": "Anonymize contact privacy data"}
            }
        }
    });
    if let Some(paths) = spec.get_mut("paths").and_then(Value::as_object_mut) {
        add_openapi_paths(paths);
    }
    Json(spec)
}

fn add_openapi_paths(paths: &mut serde_json::Map<String, Value>) {
    for (path, methods) in [
        (
            "/metrics",
            json!({"get": {"summary": "Prometheus metrics"}}),
        ),
        ("/docs", json!({"get": {"summary": "API docs"}})),
        (
            "/debug/kafka",
            json!({"get": {"summary": "Kafka debug", "security": [{"bearerAuth": ["admin:*"]}]}}),
        ),
        (
            "/debug/kafka/deadletters",
            json!({"get": {"summary": "List Kafka deadletters", "security": [{"bearerAuth": ["admin:*"]}]}}),
        ),
        (
            "/debug/kafka/deadletters/{deadletter_id}/replay",
            json!({"post": {"summary": "Replay Kafka deadletter", "security": [{"bearerAuth": ["admin:*"]}]}}),
        ),
        (
            "/debug/dirty",
            json!({"get": {"summary": "Debug dirty events", "security": [{"bearerAuth": ["admin:*"]}]}}),
        ),
        (
            "/debug/channels",
            json!({"get": {"summary": "Debug channels", "security": [{"bearerAuth": ["admin:*"]}]}}),
        ),
        (
            "/dev-media/{media_id}",
            json!({"get": {"summary": "Development media preview"}}),
        ),
        (
            "/v1/projects",
            json!({"post": {"summary": "Create project", "security": [{"bearerAuth": ["projects:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/api-keys",
            json!({"post": {"summary": "Create API key", "security": [{"bearerAuth": ["projects:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies",
            json!({"post": {"summary": "Create company", "security": [{"bearerAuth": ["companies:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}",
            json!({"get": {"summary": "Get company", "security": [{"bearerAuth": ["companies:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts",
            json!({"post": {"summary": "Create WhatsApp account", "security": [{"bearerAuth": ["channels:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}",
            json!({"get": {"summary": "Get WhatsApp account", "security": [{"bearerAuth": ["channels:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/connect",
            json!({"post": {"summary": "Connect WhatsApp account", "security": [{"bearerAuth": ["channels:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/disconnect",
            json!({"post": {"summary": "Disconnect WhatsApp account", "security": [{"bearerAuth": ["channels:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/qr",
            json!({"get": {"summary": "Get WhatsApp QR", "security": [{"bearerAuth": ["channels:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/pair-code",
            json!({"post": {"summary": "Request pair code", "security": [{"bearerAuth": ["channels:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/capabilities",
            json!({"get": {"summary": "Get channel capabilities", "security": [{"bearerAuth": ["channels:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/contacts/by-phone/{phone_e164}",
            json!({"get": {"summary": "Get contact by phone", "security": [{"bearerAuth": ["contacts:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}",
            json!({"get": {"summary": "Get conversation", "security": [{"bearerAuth": ["conversations:read"]}]}, "patch": {"summary": "Patch conversation", "security": [{"bearerAuth": ["conversations:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/mark-read",
            json!({"post": {"summary": "Mark conversation read", "security": [{"bearerAuth": ["messages:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/typing",
            json!({"post": {"summary": "Send typing state", "security": [{"bearerAuth": ["messages:send"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}",
            json!({"get": {"summary": "Get message", "security": [{"bearerAuth": ["messages:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/react",
            json!({"post": {"summary": "React to message", "security": [{"bearerAuth": ["messages:manage"]}]}, "delete": {"summary": "Delete reaction", "security": [{"bearerAuth": ["messages:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/pin",
            json!({"post": {"summary": "Pin message", "security": [{"bearerAuth": ["messages:manage"]}]}, "delete": {"summary": "Unpin message", "security": [{"bearerAuth": ["messages:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/star",
            json!({"post": {"summary": "Star message", "security": [{"bearerAuth": ["messages:manage"]}]}, "delete": {"summary": "Unstar message", "security": [{"bearerAuth": ["messages:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/media/upload-outbound",
            json!({"post": {"summary": "Upload outbound media", "security": [{"bearerAuth": ["media:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/media/{media_id}",
            json!({"get": {"summary": "Get media", "security": [{"bearerAuth": ["media:read"]}]}, "delete": {"summary": "Delete media", "security": [{"bearerAuth": ["media:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/media/{media_id}/download-url",
            json!({"get": {"summary": "Get media download URL", "security": [{"bearerAuth": ["media:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/media/{media_id}/save",
            json!({"post": {"summary": "Save media permanently", "security": [{"bearerAuth": ["media:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/transcript",
            json!({"get": {"summary": "Get transcript", "security": [{"bearerAuth": ["transcripts:read"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/transcribe",
            json!({"post": {"summary": "Request transcription", "security": [{"bearerAuth": ["transcripts:write"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/exit",
            json!({"post": {"summary": "Exit group", "security": [{"bearerAuth": ["groups:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}",
            json!({"delete": {"summary": "Remove group member", "security": [{"bearerAuth": ["groups:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}/promote",
            json!({"post": {"summary": "Promote group member", "security": [{"bearerAuth": ["groups:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}/demote",
            json!({"post": {"summary": "Demote group member", "security": [{"bearerAuth": ["groups:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/accept",
            json!({"post": {"summary": "Accept join request", "security": [{"bearerAuth": ["groups:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/reject",
            json!({"post": {"summary": "Reject join request", "security": [{"bearerAuth": ["groups:manage"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/dirty-conversations/{conversation_id}/ack",
            json!({"post": {"summary": "Ack dirty conversation", "security": [{"bearerAuth": ["dirty:ack"]}]}}),
        ),
        (
            "/v1/projects/{project_id}/companies/{company_id}/consumer-callbacks/{callback_id}",
            json!({"patch": {"summary": "Update callback", "security": [{"bearerAuth": ["admin:*"]}]}, "delete": {"summary": "Delete callback", "security": [{"bearerAuth": ["admin:*"]}]}}),
        ),
        (
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-text",
            json!({"post": {"summary": "Simulate inbound text", "security": [{"bearerAuth": ["dev:simulate"]}]}}),
        ),
        (
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-audio",
            json!({"post": {"summary": "Simulate inbound audio", "security": [{"bearerAuth": ["dev:simulate"]}]}}),
        ),
        (
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-image",
            json!({"post": {"summary": "Simulate inbound image", "security": [{"bearerAuth": ["dev:simulate"]}]}}),
        ),
        (
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/receipt",
            json!({"post": {"summary": "Simulate receipt", "security": [{"bearerAuth": ["dev:simulate"]}]}}),
        ),
        (
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/qr-rotate",
            json!({"post": {"summary": "Rotate dev QR", "security": [{"bearerAuth": ["dev:simulate"]}]}}),
        ),
        (
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/group-event",
            json!({"post": {"summary": "Simulate group event", "security": [{"bearerAuth": ["dev:simulate"]}]}}),
        ),
        (
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/reset",
            json!({"post": {"summary": "Reset dev state", "security": [{"bearerAuth": ["dev:simulate"]}]}}),
        ),
    ] {
        paths.entry(path.to_string()).or_insert(methods);
    }
}

fn paginated_items<T>(items: Vec<T>, query: &PageQuery) -> ApiResult<Value>
where
    T: Serialize,
{
    let limit = query.limit();
    let offset = page_offset(query)?;
    let total = items.len();
    let page: Vec<_> = items.into_iter().skip(offset).take(limit).collect();
    let next_index = offset.saturating_add(page.len());
    let has_more = next_index < total;
    Ok(json!({
        "items": page,
        "next_cursor": has_more.then(|| next_index.to_string()),
        "has_more": has_more,
        "total": total
    }))
}

fn page_offset(query: &PageQuery) -> ApiResult<usize> {
    query
        .cursor
        .as_deref()
        .map(str::trim)
        .filter(|cursor| !cursor.is_empty())
        .map(|cursor| {
            cursor.parse::<usize>().map_err(|_| {
                ApiError::BadRequest("cursor must be a non-negative offset".to_string())
            })
        })
        .transpose()
        .map(|offset| offset.unwrap_or_default())
}

async fn debug_kafka(headers: HeaderMap, State(state): State<AppState>) -> ApiResult<Json<Value>> {
    authorize(&state.config, &headers, "admin:*")?;
    let sample = eventbus::dirty_signal(
        "project",
        "company",
        "channel",
        "conversation",
        1,
        "debug",
        1,
    );
    let health = state.event_bus_ready().await;
    let runtime = state.event_bus_snapshot();
    let outbox_backlog = state.kafka_outbox_backlog().await?;
    Ok(Json(json!({
        "event_bus": format!("{:?}", state.config.event_bus),
        "brokers_configured": state.config.kafka.brokers.is_some(),
        "topic_prefix": state.config.kafka.topic_prefix.clone(),
        "retry_topic": eventbus::retry_topic(&state.config.kafka),
        "deadletter_topic": eventbus::deadletter_topic(&state.config.kafka),
        "required_topics": runtime.required_topics.clone(),
        "runtime": runtime,
        "outbox_backlog": outbox_backlog,
        "sample_topic": eventbus::topic_for_event(&state.config.kafka, &sample.event_type),
        "sample_partition_key": eventbus::partition_key(&sample),
        "raw_media_bytes": eventbus::event_has_raw_media_bytes(&sample),
        "health": {
            "ok": health.ok,
            "detail": health.detail
        },
        "kafka_required": state.config.event_bus == EventBusMode::Kafka
    })))
}

async fn debug_kafka_deadletters(
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize(&state.config, &headers, "admin:*")?;
    let limit = query.limit();
    let offset = page_offset(&query)?;
    let deadletters = state.kafka_deadletters(limit, offset).await?;
    if !deadletters.is_empty() {
        return Ok(Json(json!({
            "count": deadletters.len(),
            "deadletters": deadletters,
            "source": "postgres"
        })));
    }
    let runtime = state.event_bus_snapshot();
    Ok(Json(json!({
        "count": runtime.recent_deadletters.len(),
        "deadletters": runtime.recent_deadletters,
        "source": "runtime"
    })))
}

async fn replay_kafka_deadletter(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(deadletter_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let principal = authorize(&state.config, &headers, "admin:*")?;
    let event_id = state
        .replay_kafka_deadletter(&deadletter_id, &principal.api_key_id)
        .await?;
    Ok(Json(json!({
        "replayed": true,
        "deadletter_id": deadletter_id,
        "event_id": event_id
    })))
}

async fn debug_dirty(headers: HeaderMap, State(state): State<AppState>) -> ApiResult<Json<Value>> {
    authorize(&state.config, &headers, "admin:*")?;
    Ok(Json(json!({"events": state.events()})))
}

async fn debug_channels(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize(&state.config, &headers, "admin:*")?;
    Ok(Json(
        json!({"status": "debug-ready", "session_dir": state.config.wa_session_sqlite_dir}),
    ))
}

async fn dev_media_preview(
    Path(media_id): Path<String>,
    State(state): State<AppState>,
) -> Response {
    match state.media(&media_id) {
        Ok(media) => {
            let message = media
                .message_id
                .as_deref()
                .and_then(|message_id| state.message(message_id).ok())
                .unwrap_or_else(|| crate::models::Message {
                    id: "preview".to_string(),
                    project_id: media.project_id.clone(),
                    company_id: media.company_id.clone(),
                    conversation_id: media.conversation_id.clone(),
                    channel_account_id: "channel_dev".to_string(),
                    conversation_seq: 0,
                    wa_message_id: None,
                    direction: "inbound".to_string(),
                    sender_contact_id: None,
                    sender_display_name: None,
                    message_type: media.media_type.clone(),
                    text: None,
                    media_id: Some(media.id.clone()),
                    media_url: media.public_url.clone(),
                    thumbnail_url: media.thumbnail_url.clone(),
                    mime_type: Some(media.mime_type.clone()),
                    file_name: media.original_filename.clone(),
                    quoted_message_id: None,
                    status: media.storage_status.clone(),
                    error_message: None,
                    is_starred: false,
                    is_pinned: false,
                    reaction: None,
                    sent_by_source: None,
                    sent_by_external_user_id: None,
                    created_at_wa: media.created_at,
                    created_at: media.created_at,
                    updated_at: media.updated_at,
                });
            if let Ok(Some((media, bytes))) = state.media_blob(&media_id) {
                ([(header::CONTENT_TYPE, media.mime_type)], bytes).into_response()
            } else {
                (
                    [(header::CONTENT_TYPE, "image/svg+xml; charset=utf-8")],
                    crate::state::dev_preview_svg(&media, &message),
                )
                    .into_response()
            }
        }
        Err(_) => (
            axum::http::StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "media not found",
        )
            .into_response(),
    }
}

async fn create_project(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(req): Json<ProjectRequest>,
) -> ApiResult<Json<Value>> {
    authorize(&state.config, &headers, "projects:write")?;
    let id = req
        .id
        .unwrap_or_else(|| req.name.to_lowercase().replace(' ', "_"));
    Ok(Json(state.upsert_project(id, req.name)))
}

async fn create_api_key(
    headers: HeaderMap,
    Path(path): Path<ProjectPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_project(&state.config, &headers, "projects:write", &path.project_id)?;
    let (plaintext, key_hash, key_id) = generate_api_key();
    let scopes: Vec<String> = [
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
    ]
    .into_iter()
    .map(String::from)
    .collect();
    let metadata = state.store_api_key_metadata(
        &path.project_id,
        None,
        &key_id,
        &key_hash,
        &scopes,
        "default",
    );
    register_project_api_key(
        key_hash,
        key_id,
        path.project_id.clone(),
        None,
        scopes.iter().cloned().collect::<HashSet<_>>(),
        false,
    );
    Ok(Json(json!({
        "project_id": path.project_id,
        "api_key": plaintext,
        "key": plaintext,
        "metadata": metadata,
        "scopes": scopes
    })))
}

async fn create_company(
    headers: HeaderMap,
    Path(path): Path<ProjectPath>,
    State(state): State<AppState>,
    Json(req): Json<CompanyRequest>,
) -> ApiResult<Json<Value>> {
    authorize_project(&state.config, &headers, "companies:write", &path.project_id)?;
    let id = req.id.unwrap_or_else(|| {
        req.external_company_id
            .clone()
            .unwrap_or_else(|| "company_dev".to_string())
    });
    Ok(Json(state.upsert_company(path.project_id, id, req.name)))
}

async fn get_company(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "companies:write",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.company(&path.project_id, &path.company_id)))
}

async fn create_channel_account(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
    Json(req): Json<ChannelAccountRequest>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "channels:write",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.create_channel(
        &path.project_id,
        &path.company_id,
        req.id,
        req.label,
        req.phone_e164,
    )))
}

async fn connect_channel(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyChannelPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "channels:write",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(
        state
            .connect_channel(&path.project_id, &path.company_id, &path.channel_id)
            .await?
    )))
}

async fn disconnect_channel(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyChannelPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "channels:write",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.disconnect_channel(&path.channel_id)))
}

async fn get_channel(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyChannelPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "channels:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.channel(&path.channel_id)))
}

async fn get_qr(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyChannelPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "channels:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(state.qr(&path.channel_id))))
}

async fn pair_code(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyChannelPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "channels:write",
        &path.project_id,
        &path.company_id,
    )?;
    Err(ApiError::NotSupported(format!(
        "pair-code is not exposed by the active whatsapp-rust adapter for channel {}",
        path.channel_id
    )))
}

async fn capabilities(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyChannelPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "channels:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(whatsapp::capabilities())))
}

async fn list_contacts(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "contacts:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.contacts(&path.project_id, &path.company_id),
        &query,
    )?))
}

async fn get_contact(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyContactPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "contacts:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(
        state
            .inspect_contact(&path.project_id, &path.company_id, &path.contact_id)
            .await?,
    ))
}

async fn get_contact_by_phone(
    headers: HeaderMap,
    Path((project_id, company_id, phone_e164)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "contacts:read",
        &project_id,
        &company_id,
    )?;
    Ok(Json(state.contact_by_phone(
        &project_id,
        &company_id,
        &phone_e164,
    )?))
}

async fn contact_media(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyContactPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "media:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.media_for_contact(&path.project_id, &path.company_id, &path.contact_id)?,
        &query,
    )?))
}

async fn contact_conversations(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyContactPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "contacts:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.contact_conversations(&path.project_id, &path.company_id, &path.contact_id)?,
        &query,
    )?))
}

async fn list_conversations(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "conversations:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.conversations(&path.project_id, &path.company_id),
        &query,
    )?))
}

async fn get_conversation(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "conversations:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(state.conversation(
        &path.project_id,
        &path.company_id,
        &path.conversation_id
    )?)))
}

async fn patch_conversation(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "conversations:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(
        json!({"conversation_id": path.conversation_id, "updated": body}),
    ))
}

async fn list_messages(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(state.list_messages_for_conversation(
        &path.project_id,
        &path.company_id,
        &path.conversation_id,
        query.after_seq,
        query.before_seq,
        query.limit()
    )?)))
}

async fn send_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    State(state): State<AppState>,
    Json(req): Json<SendMessageRequest>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "messages:send",
        &path.project_id,
        &path.company_id,
    )?;
    state.enforce_rate_limit(RateLimitScope {
        headers: &headers,
        api_key_id: &principal.api_key_id,
        project_id: Some(&path.project_id),
        company_id: Some(&path.company_id),
        family: "messages:send",
        resource_id: Some(&path.conversation_id),
        max_per_minute: state
            .config
            .rate_limits
            .send_message_per_minute_per_conversation,
    })?;
    state.enforce_rate_limit(RateLimitScope {
        headers: &headers,
        api_key_id: &principal.api_key_id,
        project_id: Some(&path.project_id),
        company_id: Some(&path.company_id),
        family: "messages:send:tenant",
        resource_id: None,
        max_per_minute: state.config.rate_limits.send_message_per_minute_per_channel,
    })?;
    let idempotency_key = idempotency_key(&headers)?;
    let outcome = state.prepare_send_message(
        &path.project_id,
        &path.company_id,
        &path.conversation_id,
        &idempotency_key,
        req,
    )?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "accepted": true,
            "command_id": outcome.message.id,
            "message": outcome.message,
            "status": "queued"
        })),
    ))
}

async fn search_messages(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:read",
        &path.project_id,
        &path.company_id,
    )?;
    let needle = query.q.clone().unwrap_or_default();
    let items = state.search_messages_for_project_conversation(
        &path.project_id,
        &path.company_id,
        &path.conversation_id,
        &needle,
        usize::MAX,
    )?;
    Ok(Json(paginated_items(items, &query)?))
}

async fn conversation_media(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "media:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.media_for_project_conversation(
            &path.project_id,
            &path.company_id,
            &path.conversation_id,
        )?,
        &query,
    )?))
}

async fn conversation_starred(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.starred_messages_for_project_conversation(
            &path.project_id,
            &path.company_id,
            &path.conversation_id,
        )?,
        &query,
    )?))
}

async fn mark_read(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:manage",
        &path.project_id,
        &path.company_id,
    )?;
    let conversation =
        state.conversation(&path.project_id, &path.company_id, &path.conversation_id)?;
    if !state.is_whatsapp_channel_active(&conversation.channel_account_id) {
        return Err(ApiError::NotSupported(format!(
            "mark-read requires an active WhatsApp provider channel {}; local-only read receipts are disabled",
            conversation.channel_account_id
        )));
    }
    let conversation =
        state.mark_read(&path.project_id, &path.company_id, &path.conversation_id)?;
    Ok(Json(
        json!({"marked_read": true, "conversation": conversation}),
    ))
}

async fn typing(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:manage",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!({"typing": true})))
}

async fn get_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(state.message_for_company(
        &path.project_id,
        &path.company_id,
        &path.message_id
    )?)))
}

async fn react_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:manage",
        &path.project_id,
        &path.company_id,
    )?;
    let emoji = body
        .get("emoji")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| Some("👍".to_string()));
    let message = state
        .react_to_message_for_company(&path.project_id, &path.company_id, &path.message_id, emoji)
        .await?;
    Ok(Json(json!(message)))
}

async fn delete_react_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:manage",
        &path.project_id,
        &path.company_id,
    )?;
    let message = state
        .react_to_message_for_company(&path.project_id, &path.company_id, &path.message_id, None)
        .await?;
    Ok(Json(json!(message)))
}

async fn pin_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state.message_for_company(&path.project_id, &path.company_id, &path.message_id)?;
    Err(ApiError::NotSupported(
        "pin is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn unpin_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state.message_for_company(&path.project_id, &path.company_id, &path.message_id)?;
    Err(ApiError::NotSupported(
        "pin is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn star_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state.message_for_company(&path.project_id, &path.company_id, &path.message_id)?;
    Err(ApiError::NotSupported(
        "star is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn unstar_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state.message_for_company(&path.project_id, &path.company_id, &path.message_id)?;
    Err(ApiError::NotSupported(
        "star is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn upload_outbound(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<Json<Value>> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "media:write",
        &path.project_id,
        &path.company_id,
    )?;
    state.enforce_rate_limit(RateLimitScope {
        headers: &headers,
        api_key_id: &principal.api_key_id,
        project_id: Some(&path.project_id),
        company_id: Some(&path.company_id),
        family: "media:upload",
        resource_id: None,
        max_per_minute: state
            .config
            .rate_limits
            .media_downloads_per_minute_per_channel,
    })?;
    let mut conversation_id = None;
    let mut media_type = None;
    let mut mime_type = None;
    let mut filename = None;
    let mut caption = None;
    let mut file_bytes = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| ApiError::BadRequest(format!("invalid multipart upload: {err}")))?
    {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "conversation_id" => {
                conversation_id = Some(
                    field
                        .text()
                        .await
                        .map_err(|err| {
                            ApiError::BadRequest(format!("invalid conversation_id field: {err}"))
                        })?
                        .trim()
                        .to_string(),
                );
            }
            "type" | "media_type" => {
                media_type = Some(
                    field
                        .text()
                        .await
                        .map_err(|err| {
                            ApiError::BadRequest(format!("invalid media type field: {err}"))
                        })?
                        .trim()
                        .to_string(),
                );
            }
            "mime_type" => {
                mime_type = Some(
                    field
                        .text()
                        .await
                        .map_err(|err| {
                            ApiError::BadRequest(format!("invalid mime_type field: {err}"))
                        })?
                        .trim()
                        .to_string(),
                );
            }
            "filename" => {
                filename = Some(
                    field
                        .text()
                        .await
                        .map_err(|err| {
                            ApiError::BadRequest(format!("invalid filename field: {err}"))
                        })?
                        .trim()
                        .to_string(),
                );
            }
            "caption" => {
                caption = Some(
                    field
                        .text()
                        .await
                        .map_err(|err| {
                            ApiError::BadRequest(format!("invalid caption field: {err}"))
                        })?
                        .trim()
                        .to_string(),
                );
            }
            "file" | "media" => {
                if filename.is_none() {
                    filename = field.file_name().map(str::to_string);
                }
                if mime_type.is_none() {
                    mime_type = field.content_type().map(str::to_string);
                }
                file_bytes = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|err| {
                            ApiError::BadRequest(format!("invalid upload file bytes: {err}"))
                        })?
                        .to_vec(),
                );
            }
            _ => {}
        }
    }
    let media = state
        .upload_outbound_media(
            &path.project_id,
            &path.company_id,
            OutboundMediaUpload {
                conversation_id: conversation_id.ok_or_else(|| {
                    ApiError::BadRequest("conversation_id is required".to_string())
                })?,
                media_type,
                mime_type,
                filename,
                caption,
                bytes: file_bytes
                    .ok_or_else(|| ApiError::BadRequest("file is required".to_string()))?,
            },
        )
        .await?;
    Ok(Json(json!(media)))
}

async fn get_media(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMediaPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "media:read",
        &path.project_id,
        &path.company_id,
    )?;
    state.enforce_rate_limit(RateLimitScope {
        headers: &headers,
        api_key_id: &principal.api_key_id,
        project_id: Some(&path.project_id),
        company_id: Some(&path.company_id),
        family: "media:download",
        resource_id: Some(&path.media_id),
        max_per_minute: state
            .config
            .rate_limits
            .media_downloads_per_minute_per_channel,
    })?;
    Ok(Json(json!(state.media_for_company(
        &path.project_id,
        &path.company_id,
        &path.media_id
    )?)))
}

async fn download_url(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMediaPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "media:read",
        &path.project_id,
        &path.company_id,
    )?;
    let media = state.media_for_company(&path.project_id, &path.company_id, &path.media_id)?;
    let url = media
        .object_key
        .as_deref()
        .and_then(|object_key| {
            presigned_r2_get_url(
                &state.config.r2,
                object_key,
                state.config.r2_presigned_url_ttl_seconds,
            )
            .ok()
            .flatten()
        })
        .or(media.public_url.clone())
        .or(media.thumbnail_url.clone())
        .or_else(|| {
            media
                .object_key
                .as_deref()
                .and_then(|object_key| state.config.public_object_url(object_key))
        })
        .unwrap_or_else(|| state.config.dev_media_url(&media.id));
    Ok(Json(
        json!({"media_id": path.media_id, "url": url, "ttl_seconds": state.config.r2_presigned_url_ttl_seconds}),
    ))
}

async fn save_media(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMediaPath>,
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "media:write",
        &path.project_id,
        &path.company_id,
    )?;
    let entity_type = body
        .get("entity_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let entity_id = body
        .get("entity_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let before = state.media_for_company(&path.project_id, &path.company_id, &path.media_id)?;
    let media = if let Some(source) = before.object_key.as_deref() {
        let destination = r2_object_key(R2ObjectKeyInput {
            base_prefix: &state.config.r2.base_prefix,
            class: "permanent",
            project_id: &before.project_id,
            company_id: &before.company_id,
            channel_id: "channel_dev",
            conversation_id: Some(&before.conversation_id),
            entity_type: Some(entity_type),
            entity_id: Some(entity_id),
            date: OffsetDateTime::now_utc().date(),
            media_id: &path.media_id,
            ext: "bin",
        });
        copy_r2_object(&state.config.r2, source, &destination)
            .await
            .map_err(ApiError::ProviderError)?;
        state.save_media_with_permanent_object_key_for_company(
            &path.project_id,
            &path.company_id,
            &path.media_id,
            entity_type,
            entity_id,
            destination,
        )?
    } else {
        state.save_media_for_company(
            &path.project_id,
            &path.company_id,
            &path.media_id,
            entity_type,
            entity_id,
        )?
    };
    Ok(Json(json!(media)))
}

async fn delete_media(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMediaPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "media:write",
        &path.project_id,
        &path.company_id,
    )?;
    let before = state.media_for_company(&path.project_id, &path.company_id, &path.media_id)?;
    if let Some(object_key) = before.object_key.as_deref() {
        delete_r2_object(&state.config.r2, object_key)
            .await
            .map_err(ApiError::ProviderError)?;
    }
    if let Some(object_key) = before.permanent_object_key.as_deref() {
        delete_r2_object(&state.config.r2, object_key)
            .await
            .map_err(ApiError::ProviderError)?;
    }
    let media =
        state.delete_media_for_company(&path.project_id, &path.company_id, &path.media_id)?;
    Ok(Json(
        json!({"media_id": path.media_id, "deleted": true, "media": media}),
    ))
}

async fn get_transcript(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "transcripts:read",
        &path.project_id,
        &path.company_id,
    )?;
    state.message_for_company(&path.project_id, &path.company_id, &path.message_id)?;
    Ok(Json(json!(state.transcript(&path.message_id)?)))
}

async fn transcribe_message(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyMessagePath>,
    State(state): State<AppState>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "transcripts:write",
        &path.project_id,
        &path.company_id,
    )?;
    state.enforce_rate_limit(RateLimitScope {
        headers: &headers,
        api_key_id: &principal.api_key_id,
        project_id: Some(&path.project_id),
        company_id: Some(&path.company_id),
        family: "stt",
        resource_id: Some(&path.message_id),
        max_per_minute: state.config.rate_limits.stt_per_minute_per_project,
    })?;
    state.message_for_company(&path.project_id, &path.company_id, &path.message_id)?;
    let transcript =
        state.request_transcript(&path.project_id, &path.company_id, &path.message_id)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "accepted": true,
            "command_id": transcript.id,
            "transcript": transcript,
            "status": "queued"
        })),
    ))
}

async fn list_groups(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "groups:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.groups(&path.project_id, &path.company_id),
        &query,
    )?))
}

async fn get_group(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "groups:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(
        state
            .inspect_group(&path.project_id, &path.company_id, &path.group_id)
            .await?,
    ))
}

async fn group_members(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "groups:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.group_members_for_group(&path.project_id, &path.company_id, &path.group_id)?,
        &query,
    )?))
}

async fn group_media(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "media:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.media_for_group(&path.project_id, &path.company_id, &path.group_id)?,
        &query,
    )?))
}

async fn group_starred(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(paginated_items(
        state.starred_messages_for_group(&path.project_id, &path.company_id, &path.group_id)?,
        &query,
    )?))
}

async fn group_search(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "messages:read",
        &path.project_id,
        &path.company_id,
    )?;
    let needle = query.q.clone().unwrap_or_default();
    Ok(Json(paginated_items(
        state.search_messages_for_group(
            &path.project_id,
            &path.company_id,
            &path.group_id,
            &needle,
            usize::MAX,
        )?,
        &query,
    )?))
}

async fn group_exit(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "groups:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state
        .inspect_group(&path.project_id, &path.company_id, &path.group_id)
        .await?;
    Err(ApiError::NotSupported(
        "group exit is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn add_group_member(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "groups:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state
        .inspect_group(&path.project_id, &path.company_id, &path.group_id)
        .await?;
    Err(ApiError::NotSupported(
        "group member add is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn remove_group_member(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupMemberPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "groups:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state
        .inspect_group(&path.project_id, &path.company_id, &path.group_id)
        .await?;
    Err(ApiError::NotSupported(
        "group member remove is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn promote_group_member(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupMemberPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "groups:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state
        .inspect_group(&path.project_id, &path.company_id, &path.group_id)
        .await?;
    Err(ApiError::NotSupported(
        "group member promote is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn demote_group_member(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupMemberPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "groups:manage",
        &path.project_id,
        &path.company_id,
    )?;
    state
        .inspect_group(&path.project_id, &path.company_id, &path.group_id)
        .await?;
    Err(ApiError::NotSupported(
        "group member demote is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn accept_join_request(
    headers: HeaderMap,
    Path(path): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let (project_id, company_id, group_id, _) = path;
    authorize_company(
        &state.config,
        &headers,
        "groups:manage",
        &project_id,
        &company_id,
    )?;
    state
        .inspect_group(&project_id, &company_id, &group_id)
        .await?;
    Err(ApiError::NotSupported(
        "group join request accept is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn reject_join_request(
    headers: HeaderMap,
    Path(path): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let (project_id, company_id, group_id, _) = path;
    authorize_company(
        &state.config,
        &headers,
        "groups:manage",
        &project_id,
        &company_id,
    )?;
    state
        .inspect_group(&project_id, &company_id, &group_id)
        .await?;
    Err(ApiError::NotSupported(
        "group join request reject is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn list_dirty(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "dirty:read",
        &path.project_id,
        &path.company_id,
    )?;
    let limit = query.limit();
    let consumer_id = query
        .consumer_id
        .unwrap_or_else(|| "dev_tester".to_string());
    Ok(Json(json!(DirtyListResponse {
        items: state.list_dirty(&path.project_id, &path.company_id, &consumer_id, limit)
    })))
}

async fn ack_dirty(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyConversationPath>,
    State(state): State<AppState>,
    Json(req): Json<DirtyAckRequest>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "dirty:ack",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.ack_dirty(
        &path.project_id,
        &path.company_id,
        &path.conversation_id,
        req,
    )?))
}

async fn list_callbacks(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "admin:*",
        &path.project_id,
        &path.company_id,
    )?;
    enforce_admin_rate_limit(
        &state,
        &headers,
        &principal.api_key_id,
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(
        json!({"items": state.list_callbacks(&path.project_id, &path.company_id)}),
    ))
}

async fn create_callback(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "admin:*",
        &path.project_id,
        &path.company_id,
    )?;
    enforce_admin_rate_limit(
        &state,
        &headers,
        &principal.api_key_id,
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.upsert_callback(
        &path.project_id,
        &path.company_id,
        None,
        body,
    )))
}

async fn update_callback(
    headers: HeaderMap,
    Path(path): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let (project_id, company_id, callback_id) = path;
    let principal =
        authorize_company(&state.config, &headers, "admin:*", &project_id, &company_id)?;
    enforce_admin_rate_limit(
        &state,
        &headers,
        &principal.api_key_id,
        &project_id,
        &company_id,
    )?;
    Ok(Json(state.upsert_callback(
        &project_id,
        &company_id,
        Some(&callback_id),
        body,
    )))
}

async fn delete_callback(
    headers: HeaderMap,
    Path(path): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let (project_id, company_id, callback_id) = path;
    let principal =
        authorize_company(&state.config, &headers, "admin:*", &project_id, &company_id)?;
    enforce_admin_rate_limit(
        &state,
        &headers,
        &principal.api_key_id,
        &project_id,
        &company_id,
    )?;
    Ok(Json(state.delete_callback(
        &project_id,
        &company_id,
        &callback_id,
    )?))
}

async fn privacy_export(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyContactPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "admin:*",
        &path.project_id,
        &path.company_id,
    )?;
    enforce_admin_rate_limit(
        &state,
        &headers,
        &principal.api_key_id,
        &path.project_id,
        &path.company_id,
    )?;
    let export =
        state.privacy_export_contact(&path.project_id, &path.company_id, &path.contact_id)?;
    state.audit_log(
        &path.project_id,
        &path.company_id,
        "privacy.export",
        "contact",
        Some(&path.contact_id),
        json!({"exported": true}),
    );
    Ok(Json(export))
}

async fn privacy_delete(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyContactPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "admin:*",
        &path.project_id,
        &path.company_id,
    )?;
    enforce_admin_rate_limit(
        &state,
        &headers,
        &principal.api_key_id,
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.anonymize_contact(
        &path.project_id,
        &path.company_id,
        &path.contact_id,
        true,
    )?))
}

async fn privacy_anonymize(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyContactPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let principal = authorize_company(
        &state.config,
        &headers,
        "admin:*",
        &path.project_id,
        &path.company_id,
    )?;
    enforce_admin_rate_limit(
        &state,
        &headers,
        &principal.api_key_id,
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.anonymize_contact(
        &path.project_id,
        &path.company_id,
        &path.contact_id,
        false,
    )?))
}

async fn dev_inbound_text(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
    Json(req): Json<SimulateInboundTextRequest>,
) -> ApiResult<Json<Value>> {
    ensure_dev(&state)?;
    authorize_company(
        &state.config,
        &headers,
        "dev:simulate",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(state.receive_inbound_text(
        &path.project_id,
        &path.company_id,
        req
    ))))
}

async fn dev_inbound_audio(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
    Json(mut req): Json<SimulateInboundMediaRequest>,
) -> ApiResult<Json<Value>> {
    ensure_dev(&state)?;
    authorize_company(
        &state.config,
        &headers,
        "dev:simulate",
        &path.project_id,
        &path.company_id,
    )?;
    req.media_type = Some("audio".to_string());
    req.mime_type = Some(req.mime_type.unwrap_or_else(|| "audio/ogg".to_string()));
    let (message, media, transcript) =
        state.receive_inbound_media(&path.project_id, &path.company_id, req);
    Ok(Json(
        json!({"message": message, "media": media, "transcript": transcript}),
    ))
}

async fn dev_inbound_image(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
    Json(mut req): Json<SimulateInboundMediaRequest>,
) -> ApiResult<Json<Value>> {
    ensure_dev(&state)?;
    authorize_company(
        &state.config,
        &headers,
        "dev:simulate",
        &path.project_id,
        &path.company_id,
    )?;
    req.media_type = Some(req.media_type.unwrap_or_else(|| "image".to_string()));
    req.mime_type = Some(req.mime_type.unwrap_or_else(|| "image/png".to_string()));
    let (message, media, transcript) =
        state.receive_inbound_media(&path.project_id, &path.company_id, req);
    Ok(Json(
        json!({"message": message, "media": media, "transcript": transcript}),
    ))
}

async fn dev_receipt(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
    Json(req): Json<ReceiptRequest>,
) -> ApiResult<Json<Value>> {
    ensure_dev(&state)?;
    authorize_company(
        &state.config,
        &headers,
        "dev:simulate",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(
        state.update_receipt(&req.message_id, &req.receipt_type)?
    )))
}

async fn dev_qr_rotate(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    ensure_dev(&state)?;
    authorize_company(
        &state.config,
        &headers,
        "dev:simulate",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(state.rotate_qr("channel_dev", "waiting_qr"))))
}

async fn dev_group_event(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    ensure_dev(&state)?;
    authorize_company(
        &state.config,
        &headers,
        "dev:simulate",
        &path.project_id,
        &path.company_id,
    )?;
    let group_id = body
        .get("group_id")
        .and_then(Value::as_str)
        .unwrap_or("120363000000000000@g.us")
        .to_string();
    let text = body
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("Mensagem de grupo simulada")
        .to_string();
    let from_phone_e164 = body
        .get("from_phone_e164")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| Some("+5511999999999".to_string()));
    let sender_name = body
        .get("sender_name")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            body.get("member_name")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    let profile_picture_url = body
        .get("profile_picture_url")
        .and_then(Value::as_str)
        .map(str::to_string);
    let channel_id = body
        .get("channel_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let message = state.receive_inbound_text(
        &path.project_id,
        &path.company_id,
        SimulateInboundTextRequest {
            conversation_id: Some(group_id.clone()),
            channel_id,
            from_phone_e164,
            sender_name,
            profile_picture_url,
            text,
        },
    );
    Ok(Json(
        json!({"event": "group.updated", "group_id": group_id, "message": message}),
    ))
}

async fn dev_reset(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyPath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    ensure_dev(&state)?;
    authorize_company(
        &state.config,
        &headers,
        "dev:simulate",
        &path.project_id,
        &path.company_id,
    )?;
    state.reset_dev();
    Ok(Json(json!({"reset": true})))
}

async fn websocket_handler(
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> ApiResult<Response> {
    let token = query
        .get("token")
        .cloned()
        .or_else(|| crate::security::bearer_token(&headers))
        .ok_or_else(|| ApiError::Unauthorized("missing websocket token".to_string()))?;
    let principal = authorize_token(&state.config, &token, "websocket:connect")?;
    Ok(ws.on_upgrade(move |socket| websocket_session(socket, state, principal)))
}

async fn websocket_session(mut socket: WebSocket, state: AppState, principal: Principal) {
    let mut events = state.subscribe_events();
    let mut subscribed_tenant: Option<(String, String)> = None;
    loop {
        tokio::select! {
            maybe_message = socket.recv() => {
                let Some(Ok(message)) = maybe_message else {
                    break;
                };
                match message {
                    WsMessage::Text(text) => {
                        let response = match serde_json::from_str::<SubscribeRequest>(&text) {
                            Ok(sub)
                                if sub.kind == "subscribe"
                                    && sub.topics.len()
                                        <= state.config.rate_limits.ws_subscriptions_per_connection
                                            as usize =>
                            {
                                match crate::security::enforce_principal_tenant(
                                    &principal,
                                    &sub.project_id,
                                    Some(&sub.company_id),
                                ) {
                                    Ok(()) => {
                                        state.record_websocket_subscription_metric();
                                        subscribed_tenant = Some((
                                            sub.project_id.clone(),
                                            sub.company_id.clone(),
                                        ));
                                        let events: Vec<_> = state
                                            .events()
                                            .into_iter()
                                            .filter(|event| {
                                                event_matches_tenant(
                                                    event,
                                                    &sub.project_id,
                                                    &sub.company_id,
                                                ) && principal_authorizes_event(&principal, event)
                                            })
                                            .collect();
                                        json!({
                                            "type": "subscribed",
                                            "project_id": sub.project_id,
                                            "company_id": sub.company_id,
                                            "topics": sub.topics,
                                            "events": events
                                        })
                                    }
                                    Err(err) => websocket_error(err),
                                }
                            }
                            Ok(_) => {
                                json!({"type": "error", "error": {"code": "bad_request", "message": "invalid subscribe request"}})
                            }
                            Err(err) => {
                                json!({"type": "error", "error": {"code": "bad_request", "message": err.to_string()}})
                            }
                        };
                        if socket
                            .send(WsMessage::Text(response.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }
            event = events.recv(), if subscribed_tenant.is_some() => {
                match event {
                    Ok(event) => {
                        let Some((project_id, company_id)) = subscribed_tenant.as_ref() else {
                            continue;
                        };
                        if !event_matches_tenant(&event, project_id, company_id)
                            || !principal_authorizes_event(&principal, &event)
                        {
                            continue;
                        }
                        let response = json!({
                            "type": event.event_type,
                            "event": event
                        });
                        if socket
                            .send(WsMessage::Text(response.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let response = json!({
                            "type": "events.lagged",
                            "skipped": skipped
                        });
                        let _ = socket.send(WsMessage::Text(response.to_string().into())).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

fn event_matches_tenant(
    event: &crate::models::CommonEvent,
    project_id: &str,
    company_id: &str,
) -> bool {
    event.project_id == project_id && event.company_id == company_id
}

fn principal_authorizes_event(principal: &Principal, event: &crate::models::CommonEvent) -> bool {
    crate::security::enforce_principal_tenant(principal, &event.project_id, Some(&event.company_id))
        .is_ok()
}

fn websocket_error(err: ApiError) -> Value {
    json!({
        "type": "error",
        "error": {
            "code": err.code(),
            "message": err.to_string()
        }
    })
}

fn enforce_admin_rate_limit(
    state: &AppState,
    headers: &HeaderMap,
    api_key_id: &str,
    project_id: &str,
    company_id: &str,
) -> ApiResult<()> {
    state.enforce_rate_limit(RateLimitScope {
        headers,
        api_key_id,
        project_id: Some(project_id),
        company_id: Some(company_id),
        family: "admin",
        resource_id: None,
        max_per_minute: state.config.rate_limits.admin_requests_per_minute,
    })
}

fn ensure_dev(state: &AppState) -> ApiResult<()> {
    if state.config.dev_mode && state.config.dev_simulation_enabled {
        Ok(())
    } else {
        Err(ApiError::Forbidden(
            "dev simulation endpoints disabled".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio::{
        net::{TcpListener, TcpStream},
        task::JoinHandle,
        time::{Duration, timeout},
    };
    use tokio_tungstenite::{
        MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as TungsteniteMessage,
    };
    use tower::ServiceExt;

    fn register_route_key(
        token: &str,
        project_id: &str,
        company_id: Option<&str>,
        scopes: &[&str],
    ) {
        register_project_api_key(
            crate::security::sha256_hex(token),
            format!("key_{token}"),
            project_id.to_string(),
            company_id.map(str::to_string),
            scopes.iter().map(|scope| (*scope).to_string()).collect(),
            false,
        );
    }

    async fn spawn_ws_server(state: AppState) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, build_router(state).into_make_service())
                .await
                .unwrap();
        });
        (format!("ws://{addr}/ws/v1"), server)
    }

    async fn read_ws_json(socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> Value {
        let message = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = match message {
            TungsteniteMessage::Text(text) => text.to_string(),
            other => panic!("expected websocket text message, got {other:?}"),
        };
        serde_json::from_str(&text).unwrap()
    }

    #[tokio::test]
    async fn outbound_requires_idempotency_key() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p/companies/c/conversations/conv/messages")
                    .header("Authorization", "Bearer dev_project_key")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"type":"text","text":"oi"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn error_request_id_matches_response_header() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/projects/p/companies/c/conversations")
                    .header("X-Request-Id", "req_test_123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let header = res
            .headers()
            .get("X-Request-Id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(header.as_deref(), Some("req_test_123"));
        assert_eq!(parsed["error"]["request_id"], "req_test_123");
    }

    #[tokio::test]
    async fn send_message_rate_limit_returns_standard_error() {
        let mut config = crate::config::AppConfig::from_env();
        config.rate_limits.send_message_per_minute_per_conversation = 1;
        config.rate_limits.send_message_per_minute_per_channel = 1;
        let app = build_router(AppState::new(config));

        for idem in ["rate-limit-1", "rate-limit-2"] {
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/projects/p/companies/c/conversations/conv/messages")
                        .header("Authorization", "Bearer dev_project_key")
                        .header("Idempotency-Key", idem)
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"type":"text","text":"oi"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            if idem == "rate-limit-1" {
                assert_eq!(res.status(), StatusCode::ACCEPTED);
            } else {
                assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
                let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
                let parsed: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(parsed["error"]["code"], "rate_limited");
                assert!(parsed["error"]["request_id"].as_str().is_some());
            }
        }
    }

    #[tokio::test]
    async fn openapi_lists_critical_registered_routes() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let spec: Value = serde_json::from_slice(&body).unwrap();
        let paths = spec["paths"].as_object().unwrap();

        for path in [
            "/metrics",
            "/v1/projects",
            "/v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/capabilities",
            "/v1/projects/{project_id}/companies/{company_id}/media/upload-outbound",
            "/v1/projects/{project_id}/companies/{company_id}/media/{media_id}/download-url",
            "/v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/transcribe",
            "/v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/exit",
            "/v1/projects/{project_id}/companies/{company_id}/dirty-conversations/{conversation_id}/ack",
            "/v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-audio",
        ] {
            assert!(paths.contains_key(path), "OpenAPI missing {path}");
        }
    }

    #[tokio::test]
    async fn company_scoped_key_cannot_read_other_tenant_conversations() {
        let token = "route_company_read_tenant_key";
        register_route_key(
            token,
            "project_a",
            Some("company_a"),
            &["conversations:read"],
        );
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));

        let forbidden = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/projects/project_b/companies/company_b/conversations")
                    .header("Authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let allowed = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/projects/project_a/companies/company_a/conversations")
                    .header("Authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn company_scoped_key_cannot_send_message_to_other_tenant() {
        let token = "route_company_send_tenant_key";
        register_route_key(token, "project_a", Some("company_a"), &["messages:send"]);
        let state = AppState::new(crate::config::AppConfig::from_env());
        let app = build_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/project_b/companies/company_b/conversations/conv/messages")
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Idempotency-Key", "cross-tenant-send")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"type":"text","text":"blocked"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(state.conversations("project_b", "company_b").is_empty());
    }

    #[tokio::test]
    async fn websocket_token_without_auth_fails() {
        let config = crate::config::AppConfig::from_env();
        let err = crate::security::authorize_token(&config, "bad_token", "websocket:connect")
            .unwrap_err();
        assert!(matches!(err, ApiError::Unauthorized(_)));
    }

    #[tokio::test]
    async fn websocket_tenant_key_rejects_cross_tenant_subscribe() {
        let token = "ws_cross_tenant_subscribe_key";
        register_route_key(
            token,
            "project_a",
            Some("company_a"),
            &["websocket:connect"],
        );
        let (url, server) =
            spawn_ws_server(AppState::new(crate::config::AppConfig::from_env())).await;
        let (mut socket, _) = connect_async(format!("{url}?token={token}")).await.unwrap();

        socket
            .send(TungsteniteMessage::Text(
                json!({
                    "type": "subscribe",
                    "project_id": "project_b",
                    "company_id": "company_b",
                    "topics": ["messages"]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();

        let response = read_ws_json(&mut socket).await;
        assert_eq!(response["type"], "error");
        assert_eq!(response["error"]["code"], "forbidden");

        server.abort();
    }

    #[tokio::test]
    async fn websocket_tenant_key_receives_only_authorized_snapshot_and_stream_events() {
        let token = "ws_authorized_events_key";
        register_route_key(
            token,
            "project_a",
            Some("company_a"),
            &["websocket:connect"],
        );
        let state = AppState::new(crate::config::AppConfig::from_env());
        state.push_event(eventbus::new_event(
            "message.received",
            "project_a",
            "company_a",
            None,
            Some("conv_a".to_string()),
            None,
            Some(1),
            json!({}),
        ));
        state.push_event(eventbus::new_event(
            "message.received",
            "project_b",
            "company_b",
            None,
            Some("conv_b".to_string()),
            None,
            Some(1),
            json!({}),
        ));
        let (url, server) = spawn_ws_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("{url}?token={token}")).await.unwrap();

        socket
            .send(TungsteniteMessage::Text(
                json!({
                    "type": "subscribe",
                    "project_id": "project_a",
                    "company_id": "company_a",
                    "topics": ["messages"]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();

        let snapshot = read_ws_json(&mut socket).await;
        assert_eq!(snapshot["type"], "subscribed");
        let events = snapshot["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["project_id"], "project_a");
        assert_eq!(events[0]["company_id"], "company_a");

        state.push_event(eventbus::new_event(
            "message.received",
            "project_b",
            "company_b",
            None,
            Some("conv_b_live".to_string()),
            None,
            Some(2),
            json!({}),
        ));
        state.push_event(eventbus::new_event(
            "message.received",
            "project_a",
            "company_a",
            None,
            Some("conv_a_live".to_string()),
            None,
            Some(2),
            json!({}),
        ));

        let live = read_ws_json(&mut socket).await;
        assert_eq!(live["event"]["project_id"], "project_a");
        assert_eq!(live["event"]["company_id"], "company_a");

        server.abort();
    }

    #[tokio::test]
    async fn pair_code_does_not_return_fake_code() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p/companies/c/channels/whatsapp/accounts/ch/pair-code")
                    .header("Authorization", "Bearer dev_project_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"]["code"], "not_supported");
    }

    #[tokio::test]
    async fn contact_and_group_get_routes_return_cached_inspection_json() {
        let state = AppState::new(crate::config::AppConfig::from_env());
        let contact_id = "5511999999999@s.whatsapp.net";
        let group_id = "120363000000000000@g.us";
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(contact_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente Rota".to_string()),
                profile_picture_url: Some("https://example.test/contact.jpg".to_string()),
                text: "oi".to_string(),
            },
        );
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(group_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511888888888".to_string()),
                sender_name: Some("Pessoa Grupo".to_string()),
                profile_picture_url: None,
                text: "grupo".to_string(),
            },
        );
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(group_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511777777777".to_string()),
                sender_name: Some("Outra Pessoa".to_string()),
                profile_picture_url: None,
                text: "grupo 2".to_string(),
            },
        );
        state.receive_inbound_text(
            "p",
            "c",
            SimulateInboundTextRequest {
                conversation_id: Some(group_id.to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511666666666".to_string()),
                sender_name: Some("Terceira Pessoa".to_string()),
                profile_picture_url: None,
                text: "grupo 3".to_string(),
            },
        );
        let app = build_router(state);

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/projects/p/companies/c/contacts/{contact_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let contact_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/projects/p/companies/c/contacts/{contact_id}"))
                    .header("Authorization", "Bearer dev_project_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(contact_response.status(), StatusCode::OK);
        let contact_body = to_bytes(contact_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let contact: Value = serde_json::from_slice(&contact_body).unwrap();
        assert_eq!(contact["id"], contact_id);
        assert_eq!(contact["display_name"], "Cliente Rota");
        assert_eq!(contact["phone_e164"], "+5511999999999");

        let group_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/projects/p/companies/c/groups/{group_id}"))
                    .header("Authorization", "Bearer dev_project_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(group_response.status(), StatusCode::OK);
        let group_body = to_bytes(group_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let group: Value = serde_json::from_slice(&group_body).unwrap();
        assert_eq!(group["id"], group_id);
        assert_eq!(group["members_count"], 3);
        assert_eq!(group["admins_count"], 0);

        let members_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/v1/projects/p/companies/c/groups/{group_id}/members?limit=2"
                    ))
                    .header("Authorization", "Bearer dev_project_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(members_response.status(), StatusCode::OK);
        let members_body = to_bytes(members_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let members: Value = serde_json::from_slice(&members_body).unwrap();
        assert_eq!(members["items"].as_array().unwrap().len(), 2);
        assert_eq!(members["total"], 3);
        assert_eq!(members["has_more"], true);
        assert_eq!(members["next_cursor"], "2");

        let next_members_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/v1/projects/p/companies/c/groups/{group_id}/members?limit=2&cursor=2"
                    ))
                    .header("Authorization", "Bearer dev_project_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(next_members_response.status(), StatusCode::OK);
        let next_members_body = to_bytes(next_members_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let next_members: Value = serde_json::from_slice(&next_members_body).unwrap();
        assert_eq!(next_members["items"].as_array().unwrap().len(), 1);
        assert_eq!(next_members["has_more"], false);
        assert_eq!(next_members["next_cursor"], Value::Null);
    }
}
