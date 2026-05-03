use std::collections::HashMap;

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
        ProjectCompanyCallbackPath, ProjectCompanyChannelPath, ProjectCompanyContactPath,
        ProjectCompanyContactPhonePath, ProjectCompanyConversationPath,
        ProjectCompanyGroupJoinRequestPath, ProjectCompanyGroupMemberPath, ProjectCompanyGroupPath,
        ProjectCompanyMediaPath, ProjectCompanyMessagePath, ProjectCompanyPath, ReadyCheck,
        ReadyResponse, ReceiptRequest, SendMessageRequest, SimulateInboundMediaRequest,
        SimulateInboundTextRequest, SubscribeRequest,
    },
    security::{
        Principal, actor_id, authorize, authorize_company, authorize_project, idempotency_key,
    },
    state::{AppState, OutboundMediaUpload, RateLimitScope},
    storage::presigned_r2_get_url,
    whatsapp,
};

const MAX_SEARCH_WINDOW: usize = 5_000;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/openapi.json", get(openapi_json))
        .route("/asyncapi.json", get(asyncapi_json))
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
        .route("/v1/companies", post(create_company))
        .route("/v1/companies/{company_id}", get(get_company))
        .route(
            "/v1/companies/{company_id}/channels/whatsapp/accounts",
            post(create_channel_account),
        )
        .route(
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/connect",
            post(connect_channel),
        )
        .route(
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/disconnect",
            post(disconnect_channel),
        )
        .route(
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}",
            get(get_channel),
        )
        .route(
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/qr",
            get(get_qr),
        )
        .route(
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/pair-code",
            post(pair_code),
        )
        .route(
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/capabilities",
            get(capabilities),
        )
        .route("/v1/companies/{company_id}/contacts", get(list_contacts))
        .route(
            "/v1/companies/{company_id}/contacts/by-phone/{phone_e164}",
            get(get_contact_by_phone),
        )
        .route(
            "/v1/companies/{company_id}/contacts/{contact_id}",
            get(get_contact),
        )
        .route(
            "/v1/companies/{company_id}/contacts/{contact_id}/media",
            get(contact_media),
        )
        .route(
            "/v1/companies/{company_id}/contacts/{contact_id}/conversations",
            get(contact_conversations),
        )
        .route(
            "/v1/companies/{company_id}/conversations",
            get(list_conversations),
        )
        .route(
            "/v1/companies/{company_id}/conversations/{conversation_id}",
            get(get_conversation).patch(patch_conversation),
        )
        .route(
            "/v1/companies/{company_id}/conversations/{conversation_id}/messages",
            get(list_messages).post(send_message),
        )
        .route(
            "/v1/companies/{company_id}/conversations/{conversation_id}/search",
            get(search_messages),
        )
        .route(
            "/v1/companies/{company_id}/conversations/{conversation_id}/media",
            get(conversation_media),
        )
        .route(
            "/v1/companies/{company_id}/conversations/{conversation_id}/starred",
            get(conversation_starred),
        )
        .route(
            "/v1/companies/{company_id}/conversations/{conversation_id}/mark-read",
            post(mark_read),
        )
        .route(
            "/v1/companies/{company_id}/conversations/{conversation_id}/typing",
            post(typing),
        )
        .route(
            "/v1/companies/{company_id}/messages/{message_id}",
            get(get_message),
        )
        .route(
            "/v1/companies/{company_id}/messages/{message_id}/react",
            post(react_message).delete(delete_react_message),
        )
        .route(
            "/v1/companies/{company_id}/messages/{message_id}/pin",
            post(pin_message).delete(unpin_message),
        )
        .route(
            "/v1/companies/{company_id}/messages/{message_id}/star",
            post(star_message).delete(unstar_message),
        )
        .route(
            "/v1/companies/{company_id}/media/upload-outbound",
            post(upload_outbound),
        )
        .route(
            "/v1/companies/{company_id}/media/{media_id}",
            get(get_media).delete(delete_media),
        )
        .route(
            "/v1/companies/{company_id}/media/{media_id}/download-url",
            get(download_url),
        )
        .route(
            "/v1/companies/{company_id}/media/{media_id}/save",
            post(save_media),
        )
        .route(
            "/v1/companies/{company_id}/messages/{message_id}/transcript",
            get(get_transcript),
        )
        .route(
            "/v1/companies/{company_id}/messages/{message_id}/transcribe",
            post(transcribe_message),
        )
        .route("/v1/companies/{company_id}/groups", get(list_groups))
        .route(
            "/v1/companies/{company_id}/groups/{group_id}",
            get(get_group),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/members",
            get(group_members).post(add_group_member),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/media",
            get(group_media),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/starred",
            get(group_starred),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/search",
            get(group_search),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/exit",
            post(group_exit),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}",
            delete(remove_group_member),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}/promote",
            post(promote_group_member),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}/demote",
            post(demote_group_member),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/accept",
            post(accept_join_request),
        )
        .route(
            "/v1/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/reject",
            post(reject_join_request),
        )
        .route(
            "/v1/companies/{company_id}/dirty-conversations",
            get(list_dirty),
        )
        .route(
            "/v1/companies/{company_id}/dirty-conversations/{conversation_id}/ack",
            post(ack_dirty),
        )
        .route(
            "/v1/companies/{company_id}/consumer-callbacks",
            get(list_callbacks).post(create_callback),
        )
        .route(
            "/v1/companies/{company_id}/consumer-callbacks/{callback_id}",
            patch(update_callback).delete(delete_callback),
        )
        .route(
            "/v1/companies/{company_id}/privacy/contacts/{contact_id}/export",
            post(privacy_export),
        )
        .route(
            "/v1/companies/{company_id}/privacy/contacts/{contact_id}",
            delete(privacy_delete),
        )
        .route(
            "/v1/companies/{company_id}/privacy/contacts/{contact_id}/anonymize",
            post(privacy_anonymize),
        )
        .route(
            "/v1/dev/companies/{company_id}/simulate/inbound-text",
            post(dev_inbound_text),
        )
        .route(
            "/v1/dev/companies/{company_id}/simulate/inbound-audio",
            post(dev_inbound_audio),
        )
        .route(
            "/v1/dev/companies/{company_id}/simulate/inbound-image",
            post(dev_inbound_image),
        )
        .route(
            "/v1/dev/companies/{company_id}/simulate/receipt",
            post(dev_receipt),
        )
        .route(
            "/v1/dev/companies/{company_id}/simulate/qr-rotate",
            post(dev_qr_rotate),
        )
        .route(
            "/v1/dev/companies/{company_id}/simulate/group-event",
            post(dev_group_event),
        )
        .route(
            "/v1/dev/companies/{company_id}/simulate/reset",
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
    let storage_check = state.storage_ready().await;
    let webhook_secret_check = state.webhook_secret_ready();
    let metadata_ok = match state.config.metadata_db {
        MetadataDbMode::InMemory => true,
        MetadataDbMode::Postgres => {
            state.config.database_url.as_deref().is_some() && metadata_check.is_ok()
        }
    };
    let ok = metadata_ok
        && session_dir_ok
        && event_bus_check.ok
        && storage_check.ok
        && webhook_secret_check.ok;
    Json(ReadyResponse {
        ok,
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
            storage_check,
            webhook_secret_check,
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
        "<!doctype html><title>RustZap API</title><h1>RustZap API</h1><a href=\"/openapi.json\">OpenAPI JSON</a><br><a href=\"/asyncapi.json\">AsyncAPI JSON</a>",
    )
}

async fn openapi_json() -> Json<Value> {
    let mut spec = json!({
        "openapi": "3.1.0",
        "info": {"title": "RustZap", "version": "0.1.0"},
        "components": {
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
            "/v1/companies/{company_id}/contacts": {
                "get": {
                    "summary": "List contacts",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/contacts/{contact_id}": {
                "get": {"summary": "Inspect contact"}
            },
            "/v1/companies/{company_id}/contacts/{contact_id}/media": {
                "get": {
                    "summary": "List contact media",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/contacts/{contact_id}/conversations": {
                "get": {
                    "summary": "List contact conversations",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/conversations": {
                "get": {
                    "summary": "List conversations",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/conversations/{conversation_id}/messages": {
                "get": {"summary": "Read messages by cursor"},
                "post": {"summary": "Send idempotent message"}
            },
            "/v1/companies/{company_id}/conversations/{conversation_id}/search": {
                "get": {
                    "summary": "Search conversation messages",
                    "parameters": [{"name": "q", "in": "query"}, {"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/conversations/{conversation_id}/media": {
                "get": {
                    "summary": "List conversation media",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/conversations/{conversation_id}/starred": {
                "get": {
                    "summary": "List starred conversation messages",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/groups": {
                "get": {
                    "summary": "List groups",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/groups/{group_id}": {
                "get": {"summary": "Inspect group"}
            },
            "/v1/companies/{company_id}/groups/{group_id}/members": {
                "get": {
                    "summary": "List group members",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/groups/{group_id}/media": {
                "get": {
                    "summary": "List group media",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/groups/{group_id}/starred": {
                "get": {
                    "summary": "List starred group messages",
                    "parameters": [{"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/groups/{group_id}/search": {
                "get": {
                    "summary": "Search group messages",
                    "parameters": [{"name": "q", "in": "query"}, {"name": "limit", "in": "query"}, {"name": "cursor", "in": "query"}],
                }
            },
            "/v1/companies/{company_id}/dirty-conversations": {
                "get": {"summary": "List compact dirty signals"}
            },
            "/v1/companies/{company_id}/consumer-callbacks": {
                "get": {"summary": "List persisted callbacks"},
                "post": {"summary": "Create persisted callback"}
            },
            "/v1/companies/{company_id}/privacy/contacts/{contact_id}/export": {
                "post": {"summary": "Export contact privacy data"}
            },
            "/v1/companies/{company_id}/privacy/contacts/{contact_id}": {
                "delete": {"summary": "Delete and redact contact privacy data"}
            },
            "/v1/companies/{company_id}/privacy/contacts/{contact_id}/anonymize": {
                "post": {"summary": "Anonymize contact privacy data"}
            }
        }
    });
    if let Some(paths) = spec.get_mut("paths").and_then(Value::as_object_mut) {
        add_openapi_paths(paths);
    }
    decorate_openapi_contract(&mut spec);
    Json(spec)
}

fn decorate_openapi_contract(spec: &mut Value) {
    add_openapi_schemas(spec);
    let Some(paths) = spec.get_mut("paths").and_then(Value::as_object_mut) else {
        return;
    };

    for methods in paths.values_mut().filter_map(Value::as_object_mut) {
        for operation in methods.values_mut().filter_map(Value::as_object_mut) {
            operation
                .entry("responses".to_string())
                .or_insert_with(|| json!({"200": ok_response("OK", None)}));
            if let Some(responses) = operation
                .get_mut("responses")
                .and_then(Value::as_object_mut)
            {
                responses
                    .entry("default".to_string())
                    .or_insert_with(|| error_response("Standard error envelope"));
            }
        }
    }

    if let Some(messages) = paths
        .get_mut("/v1/companies/{company_id}/conversations/{conversation_id}/messages")
        .and_then(Value::as_object_mut)
    {
        messages.insert(
            "get".to_string(),
            json!({
                "summary": "Read messages by conversation cursor",
                "parameters": [
                    path_parameter("company_id"),
                    path_parameter("conversation_id"),
                    query_parameter("after_seq", "integer", false, "Return messages after this conversation_seq"),
                    query_parameter("before_seq", "integer", false, "Return messages before this conversation_seq"),
                    query_parameter("limit", "integer", false, "Maximum number of messages to return")
                ],
                "responses": {
                    "200": ok_response("Messages page", Some("#/components/schemas/MessagesPage")),
                    "default": error_response("Standard error envelope")
                }
            }),
        );
        messages.insert(
            "post".to_string(),
            json!({
                "summary": "Send an idempotent message command",
                "parameters": [
                    path_parameter("company_id"),
                    path_parameter("conversation_id"),
                    idempotency_header_parameter()
                ],
                "requestBody": json_request_body("#/components/schemas/SendMessageRequest"),
                "responses": {
                    "202": ok_response("Command accepted", None),
                    "409": error_response("Idempotency conflict"),
                    "default": error_response("Standard error envelope")
                }
            }),
        );
    }

    if let Some(dirty) = paths
        .get_mut("/v1/companies/{company_id}/dirty-conversations")
        .and_then(Value::as_object_mut)
    {
        dirty.insert(
            "get".to_string(),
            json!({
                "summary": "List compact dirty conversation signals for one consumer",
                "parameters": [
                    path_parameter("company_id"),
                    query_parameter("consumer_id", "string", true, "Stable backend consumer identifier"),
                    query_parameter("limit", "integer", false, "Maximum number of dirty conversations to lease")
                ],
                "responses": {
                    "200": ok_response("Dirty conversation leases", Some("#/components/schemas/DirtyListResponse")),
                    "default": error_response("Standard error envelope")
                }
            }),
        );
    }

    if let Some(ack) = paths
        .get_mut("/v1/companies/{company_id}/dirty-conversations/{conversation_id}/ack")
        .and_then(Value::as_object_mut)
    {
        ack.insert(
            "post".to_string(),
            json!({
                "summary": "Acknowledge a leased dirty conversation cursor",
                "parameters": [
                    path_parameter("company_id"),
                    path_parameter("conversation_id")
                ],
                "requestBody": json_request_body("#/components/schemas/DirtyAckRequest"),
                "responses": {
                    "200": ok_response("Dirty ACK result", None),
                    "409": error_response("Lease conflict"),
                    "default": error_response("Standard error envelope")
                }
            }),
        );
    }

    paths.entry("/asyncapi.json".to_string()).or_insert_with(|| {
        json!({"get": {"summary": "AsyncAPI event contract", "responses": {"200": ok_response("AsyncAPI document", None), "default": error_response("Standard error envelope")}}})
    });
}

fn add_openapi_schemas(spec: &mut Value) {
    let Some(schemas) = spec
        .get_mut("components")
        .and_then(|components| components.get_mut("schemas"))
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for (name, schema) in [
        (
            "CommonEvent",
            json!({
                "type": "object",
                "required": ["event_id", "event_type", "project_id", "company_id", "trace_id", "correlation_id", "occurred_at", "payload"],
                "properties": common_event_properties()
            }),
        ),
        (
            "DirtyAckRequest",
            json!({
                "type": "object",
                "required": ["consumer_id", "processed_until_seq", "lease_token"],
                "properties": {
                    "consumer_id": {"type": "string"},
                    "processed_until_seq": {"type": "integer", "format": "int64"},
                    "lease_token": {"type": "string"}
                }
            }),
        ),
        (
            "DirtyConversationItem",
            json!({
                "type": "object",
                "required": ["conversation_id", "max_seq", "reason", "priority", "available_at", "lease_token", "locked_until"],
                "properties": {
                    "conversation_id": {"type": "string"},
                    "max_seq": {"type": "integer", "format": "int64"},
                    "reason": {"type": "string"},
                    "priority": {"type": "integer"},
                    "available_at": {"type": "string", "format": "date-time"},
                    "lease_token": {"type": "string"},
                    "locked_until": {"type": "string", "format": "date-time"}
                }
            }),
        ),
        (
            "DirtyListResponse",
            json!({
                "type": "object",
                "required": ["items"],
                "properties": {
                    "items": {"type": "array", "items": {"$ref": "#/components/schemas/DirtyConversationItem"}}
                }
            }),
        ),
        (
            "Message",
            json!({
                "type": "object",
                "required": ["id", "conversation_id", "conversation_seq", "direction", "message_type", "status", "delivery_state"],
                "properties": {
                    "id": {"type": "string"},
                    "conversation_id": {"type": "string"},
                    "conversation_seq": {"type": "integer", "format": "int64"},
                    "direction": {"type": "string"},
                    "message_type": {"type": "string"},
                    "text": {"type": ["string", "null"]},
                    "media_id": {"type": ["string", "null"]},
                    "status": {"type": "string"},
                    "delivery_state": {"type": "string", "enum": ["not_applicable", "pending", "sent", "delivered", "read", "failed"]}
                }
            }),
        ),
        (
            "MessagesPage",
            json!({
                "type": "object",
                "required": ["conversation_id", "has_more", "messages"],
                "properties": {
                    "conversation_id": {"type": "string"},
                    "from_seq": {"type": ["integer", "null"], "format": "int64"},
                    "to_seq": {"type": ["integer", "null"], "format": "int64"},
                    "has_more": {"type": "boolean"},
                    "messages": {"type": "array", "items": {"$ref": "#/components/schemas/Message"}}
                }
            }),
        ),
        (
            "SendMessageRequest",
            json!({
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": {"type": "string", "examples": ["text"]},
                    "text": {"type": ["string", "null"]},
                    "media_id": {"type": ["string", "null"]},
                    "caption": {"type": ["string", "null"]},
                    "filename": {"type": ["string", "null"]},
                    "quoted_message_id": {"type": ["string", "null"]},
                    "metadata": {"type": ["object", "null"]}
                }
            }),
        ),
    ] {
        schemas.insert(name.to_string(), schema);
    }
}

fn common_event_properties() -> Value {
    json!({
        "event_id": {"type": "string"},
        "event_type": {"type": "string"},
        "project_id": {"type": "string"},
        "company_id": {"type": "string"},
        "channel_id": {"type": ["string", "null"]},
        "conversation_id": {"type": ["string", "null"]},
        "message_id": {"type": ["string", "null"]},
        "conversation_seq": {"type": ["integer", "null"], "format": "int64"},
        "trace_id": {"type": "string"},
        "causation_id": {"type": ["string", "null"]},
        "correlation_id": {"type": "string"},
        "occurred_at": {"type": "string", "format": "date-time"},
        "produced_at": {"type": "string", "format": "date-time"},
        "payload": {"type": "object", "description": "Compact event-specific metadata. Raw media bytes, full transcripts, and full message history are not allowed."}
    })
}

fn path_parameter(name: &str) -> Value {
    json!({
        "name": name,
        "in": "path",
        "required": true,
        "schema": {"type": "string"}
    })
}

fn query_parameter(name: &str, kind: &str, required: bool, description: &str) -> Value {
    json!({
        "name": name,
        "in": "query",
        "required": required,
        "description": description,
        "schema": {"type": kind}
    })
}

fn idempotency_header_parameter() -> Value {
    json!({
        "name": "Idempotency-Key",
        "in": "header",
        "required": true,
        "description": "Stable command key. Replays with the same JSON body return the same command result; a different body returns 409.",
        "schema": {"type": "string"}
    })
}

fn json_request_body(schema_ref: &str) -> Value {
    json!({
        "required": true,
        "content": {
            "application/json": {
                "schema": {"$ref": schema_ref}
            }
        }
    })
}

fn ok_response(description: &str, schema_ref: Option<&str>) -> Value {
    match schema_ref {
        Some(schema_ref) => json!({
            "description": description,
            "content": {
                "application/json": {
                    "schema": {"$ref": schema_ref}
                }
            }
        }),
        None => json!({"description": description}),
    }
}

fn error_response(description: &str) -> Value {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}
            }
        }
    })
}

async fn asyncapi_json() -> Json<Value> {
    Json(json!({
        "asyncapi": "3.0.0",
        "info": {
            "title": "RustZap M2M Events",
            "version": "0.1.0",
            "description": "Compact Kafka/Redpanda and internal WebSocket notification contract. REST cursor reads remain the source of truth."
        },
        "servers": {
            "redpanda": {
                "host": "{brokers}",
                "protocol": "kafka",
                "description": "RustZap to backend-consumer durable signal bus"
            },
            "internalWebSocket": {
                "host": "{rustzap_host}",
                "pathname": "/ws/v1",
                "protocol": "wss",
                "description": "Development/internal diagnostics only; production browsers should use the consumer backend WebSocket."
            }
        },
        "channels": {
            "conversationDirty": {
                "address": "{topic_prefix}.conversation.dirty",
                "messages": {"conversation.dirty": {"$ref": "#/components/messages/conversation.dirty"}}
            },
            "messageReceipt": {
                "address": "{topic_prefix}.delivery.receipt",
                "messages": {"message.receipt": {"$ref": "#/components/messages/message.receipt"}}
            },
            "channelStatus": {
                "address": "{topic_prefix}.channel.status",
                "messages": {
                    "channel.status": {"$ref": "#/components/messages/channel.status"},
                    "channel.qr": {"$ref": "#/components/messages/channel.qr"}
                }
            },
            "groupEvent": {
                "address": "{topic_prefix}.group.event",
                "messages": {"group.updated": {"$ref": "#/components/messages/group.updated"}}
            },
            "mediaStored": {
                "address": "{topic_prefix}.media.stored",
                "messages": {"media.stored": {"$ref": "#/components/messages/media.stored"}}
            },
            "transcriptCompleted": {
                "address": "{topic_prefix}.audio.transcribed",
                "messages": {"transcript.completed": {"$ref": "#/components/messages/transcript.completed"}}
            }
        },
        "operations": {
            "consumeConversationDirty": {"action": "receive", "channel": {"$ref": "#/channels/conversationDirty"}},
            "consumeMessageReceipt": {"action": "receive", "channel": {"$ref": "#/channels/messageReceipt"}},
            "consumeChannelStatus": {"action": "receive", "channel": {"$ref": "#/channels/channelStatus"}},
            "consumeGroupEvent": {"action": "receive", "channel": {"$ref": "#/channels/groupEvent"}},
            "consumeMediaStored": {"action": "receive", "channel": {"$ref": "#/channels/mediaStored"}},
            "consumeTranscriptCompleted": {"action": "receive", "channel": {"$ref": "#/channels/transcriptCompleted"}}
        },
        "components": {
            "x-partition-keys": {
                "conversation": "{project_id}:{company_id}:{conversation_id}",
                "channel": "{project_id}:{company_id}:{channel_id}"
            },
            "schemas": {
                "CommonEvent": {
                    "type": "object",
                    "required": ["event_id", "event_type", "project_id", "company_id", "trace_id", "correlation_id", "occurred_at", "payload"],
                    "properties": common_event_properties()
                }
            },
            "messages": {
                "conversation.dirty": asyncapi_message("conversation.dirty", json!({"to_seq": 42, "reason": "new_message", "priority": 100})),
                "message.receipt": asyncapi_message("message.receipt", json!({"message_id": "msg_123", "receipt_type": "delivered", "delivery_state": "delivered"})),
                "channel.status": asyncapi_message("channel.status", json!({"status": "connected", "connected_at": "2026-05-02T00:00:00Z"})),
                "channel.qr": asyncapi_message("channel.qr", json!({"status": "waiting_qr"})),
                "group.updated": asyncapi_message("group.updated", json!({"group_id": "120363000000000000@g.us"})),
                "media.stored": asyncapi_message("media.stored", json!({"message_id": "msg_123", "media_id": "media_123", "storage_status": "temporary"})),
                "transcript.completed": asyncapi_message("transcript.completed", json!({"message_id": "msg_123", "media_id": "media_123", "transcript_id": "tr_123"}))
            }
        }
    }))
}

fn asyncapi_message(name: &str, payload_example: Value) -> Value {
    json!({
        "name": name,
        "payload": {"$ref": "#/components/schemas/CommonEvent"},
        "examples": [{
            "name": format!("{name} example"),
            "payload": {
                "event_id": "evt_00000000000000000000000000",
                "event_type": name,
                "project_id": "rustzap_internal",
                "company_id": "company_123",
                "channel_id": "channel_123",
                "conversation_id": "5511999999999",
                "message_id": "msg_123",
                "conversation_seq": 42,
                "trace_id": "trace_00000000000000000000000000",
                "causation_id": null,
                "correlation_id": "corr_00000000000000000000000000",
                "occurred_at": "2026-05-02T00:00:00Z",
                "produced_at": "2026-05-02T00:00:00Z",
                "payload": payload_example
            }
        }]
    })
}

fn add_openapi_paths(paths: &mut serde_json::Map<String, Value>) {
    for (path, methods) in [
        (
            "/metrics",
            json!({"get": {"summary": "Prometheus metrics"}}),
        ),
        ("/docs", json!({"get": {"summary": "API docs"}})),
        ("/debug/kafka", json!({"get": {"summary": "Kafka debug"}})),
        (
            "/debug/kafka/deadletters",
            json!({"get": {"summary": "List Kafka deadletters"}}),
        ),
        (
            "/debug/kafka/deadletters/{deadletter_id}/replay",
            json!({"post": {"summary": "Replay Kafka deadletter"}}),
        ),
        (
            "/debug/dirty",
            json!({"get": {"summary": "Debug dirty events"}}),
        ),
        (
            "/debug/channels",
            json!({"get": {"summary": "Debug channels"}}),
        ),
        (
            "/dev-media/{media_id}",
            json!({"get": {"summary": "Development media preview"}}),
        ),
        (
            "/v1/companies",
            json!({"post": {"summary": "Create company"}}),
        ),
        (
            "/v1/companies/{company_id}",
            json!({"get": {"summary": "Get company"}}),
        ),
        (
            "/v1/companies/{company_id}/channels/whatsapp/accounts",
            json!({"post": {"summary": "Create WhatsApp account"}}),
        ),
        (
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}",
            json!({"get": {"summary": "Get WhatsApp account"}}),
        ),
        (
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/connect",
            json!({"post": {"summary": "Connect WhatsApp account"}}),
        ),
        (
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/disconnect",
            json!({"post": {"summary": "Disconnect WhatsApp account"}}),
        ),
        (
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/qr",
            json!({"get": {"summary": "Get WhatsApp QR"}}),
        ),
        (
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/pair-code",
            json!({"post": {"summary": "Request pair code"}}),
        ),
        (
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/capabilities",
            json!({"get": {"summary": "Get channel capabilities"}}),
        ),
        (
            "/v1/companies/{company_id}/contacts/by-phone/{phone_e164}",
            json!({"get": {"summary": "Get contact by phone"}}),
        ),
        (
            "/v1/companies/{company_id}/conversations/{conversation_id}",
            json!({"get": {"summary": "Get conversation"}, "patch": {"summary": "Patch conversation"}}),
        ),
        (
            "/v1/companies/{company_id}/conversations/{conversation_id}/mark-read",
            json!({"post": {"summary": "Mark conversation read"}}),
        ),
        (
            "/v1/companies/{company_id}/conversations/{conversation_id}/typing",
            json!({"post": {"summary": "Send typing state"}}),
        ),
        (
            "/v1/companies/{company_id}/messages/{message_id}",
            json!({"get": {"summary": "Get message"}}),
        ),
        (
            "/v1/companies/{company_id}/messages/{message_id}/react",
            json!({"post": {"summary": "React to message"}, "delete": {"summary": "Delete reaction"}}),
        ),
        (
            "/v1/companies/{company_id}/messages/{message_id}/pin",
            json!({"post": {"summary": "Pin message"}, "delete": {"summary": "Unpin message"}}),
        ),
        (
            "/v1/companies/{company_id}/messages/{message_id}/star",
            json!({"post": {"summary": "Star message"}, "delete": {"summary": "Unstar message"}}),
        ),
        (
            "/v1/companies/{company_id}/media/upload-outbound",
            json!({"post": {"summary": "Upload outbound media"}}),
        ),
        (
            "/v1/companies/{company_id}/media/{media_id}",
            json!({"get": {"summary": "Get media"}, "delete": {"summary": "Delete media"}}),
        ),
        (
            "/v1/companies/{company_id}/media/{media_id}/download-url",
            json!({"get": {"summary": "Get media download URL"}}),
        ),
        (
            "/v1/companies/{company_id}/media/{media_id}/save",
            json!({"post": {"summary": "Save media permanently"}}),
        ),
        (
            "/v1/companies/{company_id}/messages/{message_id}/transcript",
            json!({"get": {"summary": "Get transcript"}}),
        ),
        (
            "/v1/companies/{company_id}/messages/{message_id}/transcribe",
            json!({"post": {"summary": "Request transcription"}}),
        ),
        (
            "/v1/companies/{company_id}/groups/{group_id}/exit",
            json!({"post": {"summary": "Exit group"}}),
        ),
        (
            "/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}",
            json!({"delete": {"summary": "Remove group member"}}),
        ),
        (
            "/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}/promote",
            json!({"post": {"summary": "Promote group member"}}),
        ),
        (
            "/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}/demote",
            json!({"post": {"summary": "Demote group member"}}),
        ),
        (
            "/v1/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/accept",
            json!({"post": {"summary": "Accept join request"}}),
        ),
        (
            "/v1/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/reject",
            json!({"post": {"summary": "Reject join request"}}),
        ),
        (
            "/v1/companies/{company_id}/dirty-conversations/{conversation_id}/ack",
            json!({"post": {"summary": "Ack dirty conversation"}}),
        ),
        (
            "/v1/companies/{company_id}/consumer-callbacks/{callback_id}",
            json!({"patch": {"summary": "Update callback"}, "delete": {"summary": "Delete callback"}}),
        ),
        (
            "/v1/dev/companies/{company_id}/simulate/inbound-text",
            json!({"post": {"summary": "Simulate inbound text"}}),
        ),
        (
            "/v1/dev/companies/{company_id}/simulate/inbound-audio",
            json!({"post": {"summary": "Simulate inbound audio"}}),
        ),
        (
            "/v1/dev/companies/{company_id}/simulate/inbound-image",
            json!({"post": {"summary": "Simulate inbound image"}}),
        ),
        (
            "/v1/dev/companies/{company_id}/simulate/receipt",
            json!({"post": {"summary": "Simulate receipt"}}),
        ),
        (
            "/v1/dev/companies/{company_id}/simulate/qr-rotate",
            json!({"post": {"summary": "Rotate dev QR"}}),
        ),
        (
            "/v1/dev/companies/{company_id}/simulate/group-event",
            json!({"post": {"summary": "Simulate group event"}}),
        ),
        (
            "/v1/dev/companies/{company_id}/simulate/reset",
            json!({"post": {"summary": "Reset dev state"}}),
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

fn bounded_search_limit(query: &PageQuery) -> ApiResult<usize> {
    let offset = page_offset(query)?;
    if offset >= MAX_SEARCH_WINDOW {
        return Err(ApiError::BadRequest(format!(
            "cursor must be below {MAX_SEARCH_WINDOW} for search"
        )));
    }
    Ok(offset
        .saturating_add(query.limit())
        .saturating_add(1)
        .min(MAX_SEARCH_WINDOW))
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
    if !state.config.dev_mode {
        return (
            axum::http::StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "dev media preview disabled",
        )
            .into_response();
    }
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
            if let Ok(Some((media, bytes))) = state.media_blob(&media_id).await {
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

async fn create_company(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(req): Json<CompanyRequest>,
) -> ApiResult<Json<Value>> {
    let project_id = crate::models::INTERNAL_PROJECT_ID;
    authorize_project(&state.config, &headers, "companies:write", project_id)?;
    let id = req.id.unwrap_or_else(|| {
        req.external_company_id
            .clone()
            .unwrap_or_else(|| "company_dev".to_string())
    });
    Ok(Json(state.upsert_company(
        project_id.to_string(),
        id,
        req.name,
    )))
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
    )?))
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
    Ok(Json(state.disconnect_channel_for_company(
        &path.project_id,
        &path.company_id,
        &path.channel_id,
    )?))
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
    Ok(Json(state.channel_for_company(
        &path.project_id,
        &path.company_id,
        &path.channel_id,
    )?))
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
    Ok(Json(json!(state.qr_for_company(
        &path.project_id,
        &path.company_id,
        &path.channel_id,
    )?)))
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
    Path(path): Path<ProjectCompanyContactPhonePath>,
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    authorize_company(
        &state.config,
        &headers,
        "contacts:read",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(state.contact_by_phone(
        &path.project_id,
        &path.company_id,
        &path.phone_e164,
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
        "conversations:write",
        &path.project_id,
        &path.company_id,
    )?;
    Ok(Json(json!(state.patch_conversation_metadata(
        &path.project_id,
        &path.company_id,
        &path.conversation_id,
        &body,
    )?)))
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
    let actor_id = actor_id(&headers);
    let outcome = state.prepare_send_message_with_actor(
        &path.project_id,
        &path.company_id,
        &path.conversation_id,
        &idempotency_key,
        req,
        actor_id,
    )?;
    let public_conversation_id = state.public_conversation_id_for_ref(
        &path.project_id,
        &path.company_id,
        &outcome.message.conversation_id,
    );
    let mut response_message = outcome.message.clone();
    response_message.conversation_id = public_conversation_id;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "accepted": true,
            "command_id": outcome.message.id,
            "message": response_message,
            "status": "queued",
            "delivery_state": outcome.message.delivery_state()
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
    let search_limit = bounded_search_limit(&query)?;
    let items = state.search_messages_for_project_conversation(
        &path.project_id,
        &path.company_id,
        &path.conversation_id,
        &needle,
        search_limit,
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
    let conversation =
        state.conversation(&path.project_id, &path.company_id, &path.conversation_id)?;
    Err(ApiError::NotSupported(format!(
        "typing indicators require an active WhatsApp provider channel {}; local-only typing success is disabled",
        conversation.channel_account_id
    )))
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
    let url = if state.config.storage_provider == crate::config::StorageProvider::R2 {
        media.object_key.as_deref().and_then(|object_key| {
            presigned_r2_get_url(
                &state.config.r2,
                object_key,
                state.config.r2_presigned_url_ttl_seconds,
            )
            .ok()
            .flatten()
        })
    } else {
        None
    }
    .or_else(|| {
        media
            .object_key
            .as_deref()
            .and_then(|object_key| state.config.public_object_url(object_key))
    })
    .or_else(|| {
        (state.config.dev_mode && media.object_key.is_some())
            .then(|| state.config.dev_media_url(&media.id))
    })
    .or(media.public_url.clone())
    .or(media.thumbnail_url.clone())
    .or_else(|| {
        state
            .config
            .dev_mode
            .then(|| state.config.dev_media_url(&media.id))
    })
    .ok_or_else(|| ApiError::NotSupported("download URL is not available".to_string()))?;
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
        state
            .copy_media_bytes(source, &destination, &before.mime_type)
            .await?;
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
        state.delete_media_bytes(object_key).await?;
    }
    if let Some(object_key) = before.permanent_object_key.as_deref() {
        state.delete_media_bytes(object_key).await?;
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
    let search_limit = bounded_search_limit(&query)?;
    Ok(Json(paginated_items(
        state.search_messages_for_group(
            &path.project_id,
            &path.company_id,
            &path.group_id,
            &needle,
            search_limit,
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
    Path(path): Path<ProjectCompanyGroupJoinRequestPath>,
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
        "group join request accept is not supported by the active WhatsApp adapter".to_string(),
    ))
}

async fn reject_join_request(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyGroupJoinRequestPath>,
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
        .as_deref()
        .map(str::trim)
        .filter(|consumer_id| !consumer_id.is_empty())
        .ok_or_else(|| {
            ApiError::BadRequest("consumer_id query parameter is required".to_string())
        })?;
    Ok(Json(json!(DirtyListResponse {
        items: state.list_dirty(&path.project_id, &path.company_id, consumer_id, limit)
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
    )?))
}

async fn update_callback(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyCallbackPath>,
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
        Some(&path.callback_id),
        body,
    )?))
}

async fn delete_callback(
    headers: HeaderMap,
    Path(path): Path<ProjectCompanyCallbackPath>,
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
    Ok(Json(state.delete_callback(
        &path.project_id,
        &path.company_id,
        &path.callback_id,
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
        state.try_receive_inbound_media(&path.project_id, &path.company_id, req)?;
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
        state.try_receive_inbound_media(&path.project_id, &path.company_id, req)?;
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
    Query(_query): Query<HashMap<String, String>>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> ApiResult<Response> {
    let principal = authorize(&state.config, &headers, "websocket:connect")?;
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
                    .uri("/v1/companies/c/conversations/conv/messages")
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
                    .uri("/v1/companies/c/messages/missing_message")
                    .header("X-Request-Id", "req_test_123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
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
                        .uri("/v1/companies/c/conversations/conv/messages")
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
    async fn m2m_company_route_sends_without_authorization_and_records_actor() {
        let state = AppState::new(crate::config::AppConfig::from_env());
        let app = build_router(state.clone());

        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/companies/company_m2m/conversations/conv_m2m/messages")
                    .header("Idempotency-Key", "m2m-send-actor")
                    .header("X-RustZap-Actor-Id", "ai-agent-7")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"type":"text","text":"oi"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"]["sent_by_external_user_id"], "ai-agent-7");
        assert_eq!(parsed["message"]["delivery_state"], "pending");
        assert_eq!(parsed["delivery_state"], "pending");
        assert_eq!(
            state.conversations("rustzap_internal", "company_m2m").len(),
            1
        );
    }

    #[tokio::test]
    async fn m2m_company_route_reads_without_authorization() {
        let state = AppState::new(crate::config::AppConfig::from_env());
        state.receive_inbound_text(
            "rustzap_internal",
            "company_m2m_read",
            SimulateInboundTextRequest {
                conversation_id: Some("5511999999999@s.whatsapp.net".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );
        let app = build_router(state);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/companies/company_m2m_read/conversations/5511999999999@s.whatsapp.net/messages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["messages"][0]["status"], "received");
        assert_eq!(parsed["messages"][0]["delivery_state"], "not_applicable");
    }

    #[tokio::test]
    async fn m2m_company_route_reads_with_numberonly_conversation_id() {
        let state = AppState::new(crate::config::AppConfig::from_env());
        state.receive_inbound_text(
            "rustzap_internal",
            "company_m2m_numberonly",
            SimulateInboundTextRequest {
                conversation_id: Some("5511999999999@s.whatsapp.net".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: Some("+5511999999999".to_string()),
                sender_name: Some("Cliente".to_string()),
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );
        let app = build_router(state);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(
                        "/v1/companies/company_m2m_numberonly/conversations/5511999999999/messages",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["conversation_id"], "5511999999999");
        assert_eq!(parsed["messages"][0]["conversation_id"], "5511999999999");
        assert_eq!(parsed["messages"][0]["status"], "received");
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
            "/v1/companies",
            "/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/capabilities",
            "/v1/companies/{company_id}/media/upload-outbound",
            "/v1/companies/{company_id}/media/{media_id}/download-url",
            "/v1/companies/{company_id}/messages/{message_id}/transcribe",
            "/v1/companies/{company_id}/groups/{group_id}/exit",
            "/v1/companies/{company_id}/dirty-conversations/{conversation_id}/ack",
            "/v1/dev/companies/{company_id}/simulate/inbound-audio",
        ] {
            assert!(paths.contains_key(path), "OpenAPI missing {path}");
        }
    }

    #[tokio::test]
    async fn openapi_documents_m2m_cursor_idempotency_and_error_contracts() {
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

        for schema in [
            "CommonEvent",
            "DirtyAckRequest",
            "DirtyConversationItem",
            "DirtyListResponse",
            "MessagesPage",
            "SendMessageRequest",
        ] {
            assert!(
                spec["components"]["schemas"].get(schema).is_some(),
                "OpenAPI missing schema {schema}"
            );
        }

        let messages =
            &spec["paths"]["/v1/companies/{company_id}/conversations/{conversation_id}/messages"];
        let get_parameters = messages["get"]["parameters"].as_array().unwrap();
        for name in ["after_seq", "before_seq", "limit"] {
            assert!(
                get_parameters
                    .iter()
                    .any(|parameter| parameter["name"] == name && parameter["in"] == "query"),
                "GET messages missing query parameter {name}"
            );
        }
        assert!(
            messages["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
                == "#/components/schemas/MessagesPage"
        );
        assert!(messages["get"]["responses"].get("default").is_some());

        let post_parameters = messages["post"]["parameters"].as_array().unwrap();
        assert!(post_parameters.iter().any(|parameter| {
            parameter["name"] == "Idempotency-Key"
                && parameter["in"] == "header"
                && parameter["required"] == true
        }));
        assert!(
            messages["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"]
                == "#/components/schemas/SendMessageRequest"
        );
        assert!(messages["post"]["responses"].get("202").is_some());
        assert!(messages["post"]["responses"].get("409").is_some());

        let dirty = &spec["paths"]["/v1/companies/{company_id}/dirty-conversations"];
        assert!(dirty["get"]["parameters"].as_array().unwrap().iter().any(
            |parameter| parameter["name"] == "consumer_id"
                && parameter["in"] == "query"
                && parameter["required"] == true
        ));
        assert!(
            dirty["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
                == "#/components/schemas/DirtyListResponse"
        );

        let ack = &spec["paths"]["/v1/companies/{company_id}/dirty-conversations/{conversation_id}/ack"]
            ["post"];
        assert!(
            ack["requestBody"]["content"]["application/json"]["schema"]["$ref"]
                == "#/components/schemas/DirtyAckRequest"
        );
    }

    #[tokio::test]
    async fn asyncapi_documents_compact_kafka_events_and_partition_keys() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/asyncapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let spec: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(spec["asyncapi"], "3.0.0");
        let common_event = &spec["components"]["schemas"]["CommonEvent"];
        for field in [
            "event_id",
            "event_type",
            "project_id",
            "company_id",
            "channel_id",
            "conversation_id",
            "message_id",
            "conversation_seq",
            "trace_id",
            "correlation_id",
            "occurred_at",
            "payload",
        ] {
            assert!(
                common_event["properties"].get(field).is_some(),
                "AsyncAPI CommonEvent missing {field}"
            );
        }

        for event_type in [
            "conversation.dirty",
            "message.receipt",
            "channel.status",
            "channel.qr",
            "group.updated",
            "media.stored",
            "transcript.completed",
        ] {
            assert!(
                spec["components"]["messages"].get(event_type).is_some(),
                "AsyncAPI missing message {event_type}"
            );
        }

        assert_eq!(
            spec["components"]["x-partition-keys"]["conversation"],
            "{project_id}:{company_id}:{conversation_id}"
        );
        assert_eq!(
            spec["components"]["x-partition-keys"]["channel"],
            "{project_id}:{company_id}:{channel_id}"
        );
    }

    #[tokio::test]
    async fn typing_route_returns_not_supported_instead_of_fake_success() {
        let state = AppState::new(crate::config::AppConfig::from_env());
        state.receive_inbound_text(
            "rustzap_internal",
            "company_typing",
            SimulateInboundTextRequest {
                conversation_id: Some("conv".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );
        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/companies/company_typing/conversations/conv/typing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"]["code"], "not_supported");
    }

    #[tokio::test]
    async fn patch_conversation_updates_local_metadata_instead_of_echoing_body() {
        let state = AppState::new(crate::config::AppConfig::from_env());
        state.receive_inbound_text(
            "rustzap_internal",
            "company_patch",
            SimulateInboundTextRequest {
                conversation_id: Some("conv".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                text: "oi".to_string(),
            },
        );
        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/v1/companies/company_patch/conversations/conv")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"is_archived":true,"is_muted":true,"control_mode":"autopilot"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["id"], "conv");
        assert_eq!(parsed["is_archived"], true);
        assert_eq!(parsed["is_muted"], true);
        assert_eq!(parsed["control_mode"], "autopilot");
        assert!(
            parsed.get("updated").is_none(),
            "route must not echo fake success"
        );
    }

    #[tokio::test]
    async fn dirty_list_requires_explicit_consumer_id() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/companies/company_dirty/dirty-conversations")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn search_routes_reject_cursor_outside_bounded_window() {
        let state = AppState::new(crate::config::AppConfig::from_env());
        state.receive_inbound_text(
            "rustzap_internal",
            "company_search",
            SimulateInboundTextRequest {
                conversation_id: Some("conv_search".to_string()),
                channel_id: Some("ch".to_string()),
                from_phone_e164: None,
                sender_name: None,
                profile_picture_url: None,
                text: "needle".to_string(),
            },
        );
        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/companies/company_search/conversations/conv_search/search?q=needle&cursor=5000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn channel_routes_do_not_cross_company_boundaries() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));

        let create_a = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/companies/company_a/channels/whatsapp/accounts")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":"shared_channel","label":"A"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_a.status(), StatusCode::OK);

        let read_b = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/companies/company_b/channels/whatsapp/accounts/shared_channel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(read_b.status(), StatusCode::NOT_FOUND);

        let create_b_same_id = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/companies/company_b/channels/whatsapp/accounts")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":"shared_channel","label":"B"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_b_same_id.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn callback_routes_do_not_cross_company_boundaries() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));

        let create_a = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/v1/companies/company_a/consumer-callbacks/shared_callback")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"url":"https://hooks.example.test/rustzap","enabled":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_a.status(), StatusCode::OK);

        let update_b = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/v1/companies/company_b/consumer-callbacks/shared_callback")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"url":"https://evil.example.test/rustzap","enabled":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(update_b.status(), StatusCode::NOT_FOUND);

        let delete_b = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/companies/company_b/consumer-callbacks/shared_callback")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_b.status(), StatusCode::NOT_FOUND);

        let list_a = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/companies/company_a/consumer-callbacks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_a.status(), StatusCode::OK);
        let body = to_bytes(list_a.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["items"][0]["id"], "shared_callback");
        assert_eq!(
            parsed["items"][0]["url"],
            "https://hooks.example.test/rustzap"
        );
    }

    #[tokio::test]
    async fn dev_media_preview_is_disabled_outside_dev_mode() {
        let mut config = crate::config::AppConfig::from_env();
        config.dev_mode = false;
        let state = AppState::new(config);
        let (_message, media, _transcript) = state
            .try_receive_inbound_media(
                crate::models::INTERNAL_PROJECT_ID,
                "company_media",
                SimulateInboundMediaRequest {
                    conversation_id: Some("conv_media".to_string()),
                    channel_id: Some("channel_media".to_string()),
                    from_phone_e164: None,
                    sender_name: None,
                    profile_picture_url: None,
                    media_type: Some("image".to_string()),
                    mime_type: Some("image/png".to_string()),
                    size_bytes: Some(512),
                    caption: None,
                    filename: Some("preview.png".to_string()),
                },
            )
            .unwrap();
        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/dev-media/{}", media.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn production_callbacks_reject_loopback_webhook_urls() {
        let mut config = crate::config::AppConfig::from_env();
        config.dev_mode = false;
        let app = build_router(AppState::new(config));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/companies/company_callback/consumer-callbacks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"url":"http://127.0.0.1:8080/internal","enabled":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn production_callbacks_keep_existing_url_when_patch_omits_url() {
        let mut config = crate::config::AppConfig::from_env();
        config.dev_mode = false;
        let app = build_router(AppState::new(config));

        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/v1/companies/company_callback/consumer-callbacks/callback_https")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"url":"https://hooks.example.test/rustzap","enabled":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::OK);

        let update = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/v1/companies/company_callback/consumer-callbacks/callback_https")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"enabled":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(update.status(), StatusCode::OK);
        let body = to_bytes(update.into_body(), usize::MAX).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["url"], "https://hooks.example.test/rustzap");
        assert_eq!(parsed["enabled"], false);
    }

    #[tokio::test]
    async fn company_context_is_trusted_for_reads() {
        let app = build_router(AppState::new(crate::config::AppConfig::from_env()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/companies/company_b/conversations")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn company_context_is_trusted_for_sends() {
        let state = AppState::new(crate::config::AppConfig::from_env());
        let app = build_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/companies/company_b/conversations/conv/messages")
                    .header("Idempotency-Key", "cross-tenant-send")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"type":"text","text":"blocked"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            state
                .conversations(crate::models::INTERNAL_PROJECT_ID, "company_b")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn websocket_token_authorization_is_internal_trust() {
        let config = crate::config::AppConfig::from_env();
        let principal =
            crate::security::authorize_token(&config, "bad_token", "websocket:connect").unwrap();
        assert!(crate::security::has_scope(&principal.scopes, "admin:*"));
    }

    #[tokio::test]
    async fn websocket_trusts_subscribe_tenant_context() {
        let (url, server) =
            spawn_ws_server(AppState::new(crate::config::AppConfig::from_env())).await;
        let (mut socket, _) = connect_async(url).await.unwrap();

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
        assert_eq!(response["type"], "subscribed");
        assert_eq!(response["project_id"], "project_b");
        assert_eq!(response["company_id"], "company_b");

        server.abort();
    }

    #[tokio::test]
    async fn websocket_receives_only_subscribed_company_snapshot_and_stream_events() {
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
        let (mut socket, _) = connect_async(url).await.unwrap();

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
                    .uri("/v1/companies/c/channels/whatsapp/accounts/ch/pair-code")
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
            crate::models::INTERNAL_PROJECT_ID,
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
            crate::models::INTERNAL_PROJECT_ID,
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
            crate::models::INTERNAL_PROJECT_ID,
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
            crate::models::INTERNAL_PROJECT_ID,
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

        let contact_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/companies/c/contacts/{contact_id}"))
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
        assert_eq!(contact["id"], "5511999999999");
        assert_eq!(contact["technical_id"], contact_id);
        assert_eq!(contact["phone_number"], "5511999999999");
        assert_eq!(contact["display_name"], "Cliente Rota");
        assert_eq!(contact["phone_e164"], "+5511999999999");

        let group_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/companies/c/groups/{group_id}"))
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
                    .uri(format!("/v1/companies/c/groups/{group_id}/members?limit=2"))
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
                        "/v1/companies/c/groups/{group_id}/members?limit=2&cursor=2"
                    ))
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
