# RustZap

RustZap is an internal M2M WhatsApp conversation gateway. It owns WhatsApp
channel state, conversations, messages, media metadata, audio transcripts,
dirty conversation state, callback delivery state, and compact event signals.

RustZap is not an end-user application. The SaaS/backend above RustZap owns
users, login, CRM data, business rules, AI decisions, browser WebSockets, and
per-user authorization. RustZap trusts that upper application to send the
correct company context.

The current public M2M REST contract is company-scoped:

```txt
/v1/companies/{company_id}/...
```

Older project-scoped examples in historical notes are not the current public
contract. RustZap may still store an internal project id such as
`rustzap_internal`, but external callers should use company-scoped paths.

## Source Of Truth

- Human onboarding and business rules: this `README.md`.
- REST contract: `GET /openapi.json`.
- Event contract: `GET /asyncapi.json`.
- Current implementation: `src/routes.rs`, `src/models.rs`, `src/state.rs`.
- Architecture notes: `docs/architecture.md`.
- Security notes: `docs/security.md`.
- Local development notes: `docs/local-dev.md`.

## Table Of Contents

- [Business Rules](#business-rules)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Contract Basics](#contract-basics)
- [Core Models](#core-models)
- [Endpoint Catalog](#endpoint-catalog)
- [Essential Workflows](#essential-workflows)
- [Events And Realtime](#events-and-realtime)
- [Dev Simulation](#dev-simulation)
- [Testing](#testing)
- [Operational Notes](#operational-notes)

## Business Rules

These rules are part of the product contract, not incidental implementation
details.

- RustZap is an internal M2M library/service boundary.
- The application above RustZap is trusted. If the caller says a request belongs
  to a company, RustZap accepts that context.
- `company_id` is the tenant boundary for WhatsApp sessions, channels, chats,
  messages, media, transcripts, dirty state, callbacks, events, and privacy
  operations.
- User identity is optional actor/audit metadata only. It must not partition
  WhatsApp state. The actor can be a human, automation, or AI.
- RustZap must not add end-user authentication, email verification, passwords,
  login screens, JWT sessions, or per-user authorization.
- The consumer backend owns CRM, business logic, AI orchestration, final
  authorization, and browser-facing contracts.
- REST cursor reads are the source of truth for message history and recovery.
  Kafka, WebSocket, webhook, and dirty signals are notifications.
- Events must stay compact. They may carry ids, cursors, sequence numbers, and
  small status payloads. They must not carry raw media bytes, full transcripts,
  or full message history.
- Public caller ids should use `phone_number` as digits only when a phone number
  is known, preferably including country code. WhatsApp JIDs, `@lid`, and raw
  technical aliases are internal/debug identifiers.
- For direct chats, RustZap normalizes known phone-number conversations to a
  public digit-only conversation id in responses where possible.
- For groups, the public id remains the WhatsApp group id, usually
  `120363...@g.us`.
- Provider commands that cannot be proven through the active WhatsApp adapter
  return `501 not_supported`. RustZap must not mutate local state to fake
  provider success.
- Object keys for media storage must not include phone numbers, names, CPF,
  original filenames, or raw WhatsApp JIDs. RustZap uses hashed path segments
  for conversation/entity identifiers.
- Production browsers should talk to the SaaS/backend above RustZap, not
  directly to RustZap.

## Architecture

```txt
Upper SaaS/backend
  -> trusted company context + optional actor context
  -> RustZap REST commands and reads
  -> WhatsApp adapter, metadata store, media store, STT, event bus
  -> compact signals back to consumer backend
  -> consumer backend REST/WebSocket to browser
```

Main responsibilities:

- **REST API:** company-scoped command/read surface.
- **State:** companies, channels, conversations, contacts, messages, media,
  transcripts, dirty conversations, callbacks, audit logs.
- **WhatsApp adapter:** QR/session/connectivity plus supported provider
  commands through `whatsapp-rust`.
- **Media:** local staging, local byte store for development, R2-compatible
  object keys for production.
- **Transcription:** pending/queued transcript lifecycle, with Groq STT as the
  production boundary and mocks/dev simulation in tests.
- **Events:** compact `CommonEvent` records, Kafka/Redpanda publishing,
  WebSocket diagnostics, webhooks, and dirty polling.
- **Recovery:** REST cursor reads and dirty leases let consumers recover after
  dropped signals, restarts, or deploys.

Production realtime shape:

```txt
RustZap
  -> Kafka/Redpanda compact signal
  -> consumer backend sync loop
  -> consumer backend cache + REST + WebSocket
  -> browser
```

Direct RustZap WebSocket is useful for development, diagnostics, and controlled
internal tools. It is not the recommended production browser fanout path.

## Quick Start

### Local Rust Run

```bash
cargo test
cargo run
```

Default local endpoints:

- API: `http://<LAN_IP>:8167`
- REST contract: `http://<LAN_IP>:8167/openapi.json`
- Event contract: `http://<LAN_IP>:8167/asyncapi.json`
- Basic docs page: `http://<LAN_IP>:8167/docs`

Health checks:

```bash
curl http://<LAN_IP>:8167/health
curl http://<LAN_IP>:8167/ready
```

### Podman Stack

```bash
cp .env.production.example .env.production
cp .env.secrets.example .env.secrets
$EDITOR .env.production .env.secrets
./scripts/deploy.sh production
```

This builds the RustZap image, starts RustZap, Postgres, and Redpanda, applies
migrations, waits for readiness, and prints service status.

Useful commands:

```bash
./scripts/podman-logs.sh
./scripts/podman-down.sh
```

To also start the Next dev tester:

```bash
RUSTZAP_START_DEV_TESTER=1 ./scripts/deploy.sh development
```

Services:

- RustZap API: `http://<LAN_IP>:8167`
- Dev tester UI: `http://<LAN_IP>:3167` when enabled
- Postgres and Redpanda run in the Podman network

## Configuration

Use `.env.development.example` for direct local in-memory development and
`.env.production.example` plus `.env.secrets.example` for the Podman production
shape.

Important configuration families:

- `RUSTZAP_DEV_MODE`: enables development mode behavior.
- `DEV_SIMULATION_ENABLED`: enables `/v1/dev/...` simulation endpoints when
  `RUSTZAP_DEV_MODE=true`.
- `METADATA_DB`: `in_memory` or `postgres`.
- `DATABASE_URL`: required for Postgres metadata mode.
- `WA_SESSION_SQLITE_DIR`: persistent local SQLite session directory for
  WhatsApp sessions.
- `EVENT_BUS`: `in_memory`, `postgres`, or `kafka`.
- `CONSUMER_SIGNAL_MODE`: polling, WebSocket, webhook, Kafka, or combined
  modes supported by config.
- `LOCAL_STORAGE_DIR`: local media byte storage path.
- `STORAGE_PROVIDER`: `local_fs` or `r2`.
- `R2_*`: endpoint, bucket, public URL, credentials, and base prefix.
- `WEBHOOK_DELIVERY_ENABLED`: enables webhook delivery attempts when the signal
  mode includes webhooks.
- `RUSTZAP_SECRET_MASTER_KEY`: required to encrypt callback secrets for webhook
  delivery.
- `GROQ_API_KEY` and `GROQ_STT_*`: production audio transcription settings.
- `RATE_LIMIT_*`: project, send-message, media, STT, admin, and WebSocket
  limits.
- `RUSTZAP_LOG_REDACT_MESSAGE_TEXT` and `RUSTZAP_LOG_REDACT_PHONE`: should stay
  enabled in production.

`WA_SESSION_ENCRYPT_AT_REST=true` is an operational statement. Configure host
volume/disk encryption or provider-supported SQLite encryption separately. The
code does not claim transparent SQLCipher encryption unless the selected
adapter stack supports it.

## Contract Basics

### Base URL And Scope

All business routes use the company-scoped base path:

```txt
/v1/companies/{company_id}
```

`company_id` is required and is the tenant boundary. RustZap currently trusts
internal M2M caller context; bearer tokens and scopes are documentation and
operational boundaries, not end-user authorization checks.

### Recommended Headers

```http
Content-Type: application/json
Idempotency-Key: stable-command-key
X-RustZap-Actor-Id: optional-human-or-automation-actor
x-request-id: optional-request-id-for-tracing
```

Use `Idempotency-Key` for write commands that enqueue or create externally
meaningful side effects, especially outbound sends. Replaying the same key with
the same body returns the same command result. Replaying the same key with a
different body returns `409 idempotency_conflict`.

`X-RustZap-Actor-Id` is audit metadata only. It is recorded on outbound
messages as `sent_by_external_user_id`; it does not partition state.

`x-request-id` is echoed on responses and included in error envelopes. If the
caller does not provide one, RustZap generates one.

### Standard Error Envelope

All API errors use:

```json
{
  "error": {
    "code": "bad_request",
    "message": "human readable detail",
    "details": {},
    "request_id": "req_..."
  }
}
```

Common codes:

- `bad_request`
- `unauthorized`
- `forbidden`
- `not_found`
- `conflict`
- `idempotency_conflict`
- `not_supported`
- `rate_limited`
- `payload_too_large`
- `provider_error`
- `internal_error`

### Pagination

Most collection endpoints support:

```txt
?limit=100&cursor=0
```

They return:

```json
{
  "items": [],
  "next_cursor": null,
  "has_more": false,
  "total": 0
}
```

Message reads use conversation sequence cursors:

```txt
?after_seq=536&before_seq=900&limit=100
```

Response:

```json
{
  "conversation_id": "5511999999999",
  "from_seq": 537,
  "to_seq": 541,
  "has_more": false,
  "messages": []
}
```

### Phone Numbers And IDs

- Public phone numbers are digits only, for example `5511999999999`.
- `+5511999999999`, `5511999999999`, and
  `5511999999999@s.whatsapp.net` can be resolved internally when enough state
  exists.
- Direct conversation responses prefer the digit-only phone number when known.
- Group ids remain WhatsApp group ids such as `120363000000000000@g.us`.
- Raw JIDs and LIDs may appear as debug/internal aliases, but callers should not
  require them as the public id when a phone number is known.

### Delivery State

`Message.status` preserves internal/provider status. `Message.delivery_state`
is the public normalized field.

| Direction/type | Status | delivery_state |
| --- | --- | --- |
| inbound | any | `not_applicable` |
| outbound system | any | `not_applicable` |
| outbound | `queued` | `pending` |
| outbound | `sent_to_whatsapp`, `server_ack` | `sent` |
| outbound | `delivered` | `delivered` |
| outbound | `read`, `played` | `read` |
| outbound | `failed` | `failed` |

### Unsupported Provider Commands

Capability-first behavior is intentional. If the active WhatsApp adapter cannot
prove a provider command, RustZap returns `501 not_supported`.

Current examples:

- `POST .../pair-code`
- pin/unpin
- star/unstar
- typing indicators
- mark-read when no active provider channel can perform it
- group admin commands such as exit, add/remove member, promote/demote, and
  join-request accept/reject

Read endpoints for groups, contacts, conversations, media, and transcripts are
still useful even when admin commands are not supported.

## Core Models

This section names the stable fields callers should expect. `/openapi.json`
contains the machine-readable subset.

### Company

Created through `POST /v1/companies`.

Important fields:

- `id`
- `project_id` (internal, usually `rustzap_internal`)
- `name`
- timestamps

### Channel Account

Created through `POST /v1/companies/{company_id}/channels/whatsapp/accounts`.

Important fields:

- `id`
- `project_id`
- `company_id`
- `provider`
- `phone_e164`
- `label`
- `status`
- `connected_at`
- `last_seen_at`
- timestamps

### Contact

Contacts are created and enriched from inbound/outbound WhatsApp activity and
best-effort provider profile inspection.

Important public fields:

- `id`: public contact id, preferably digit-only phone number when known
- `technical_id`: internal contact id/JID/LID
- `canonical_jid`
- `lid`
- `phone_e164`
- `phone_number`
- `push_name`
- `display_name`
- `profile_picture_media_id`
- `business_description`
- `avatar_url`
- `profile_picture_url`
- `first_contact_at`
- `last_contact_at`

Profile picture basics:

- Use `GET /v1/companies/{company_id}/contacts/{contact_id}` to inspect a
  contact.
- If a connected channel can refresh the WhatsApp profile, RustZap updates
  `profile_picture_url`, `avatar_url`, `business_description`, and related
  fields best-effort.
- If no provider profile picture is known, profile fields may be `null`.
- Use `GET /v1/companies/{company_id}/contacts/by-phone/{phone_e164}` when the
  caller knows only the phone number.

### Conversation

Important fields:

- `id`
- `project_id`
- `company_id`
- `channel_account_id`
- `type`: `direct` or `group`
- `contact_id`
- `group_id`
- `display_name`
- `display_phone`
- `phone_number`
- `avatar_url`
- `profile_picture_url`
- `last_seq`
- `last_message_at`
- `unread_count`
- `is_archived`
- `is_muted`
- `is_pinned`
- `control_mode`
- timestamps

Patchable fields:

- `is_archived`
- `is_muted`
- `is_pinned`
- `control_mode`

### Message

Important fields:

- `id`
- `project_id`
- `company_id`
- `conversation_id`
- `channel_account_id`
- `conversation_seq`
- `wa_message_id`
- `direction`
- `sender_contact_id`
- `sender_display_name`
- `message_type`
- `text`
- `media_id`
- `media_url`
- `thumbnail_url`
- `mime_type`
- `file_name`
- `quoted_message_id`
- `status`
- `delivery_state`
- `error_message`
- `is_starred`
- `is_pinned`
- `reaction`
- `sent_by_source`
- `sent_by_external_user_id`
- timestamps

Common message types:

- `text`
- `image`
- `video`
- `audio`
- `document`
- `sticker`
- `reaction`
- `location`
- `contact_card`
- `system`

### MediaObject

Important fields:

- `id`
- `project_id`
- `company_id`
- `conversation_id`
- `message_id`
- `media_type`
- `mime_type`
- `original_filename`
- `size_bytes`
- `sha256`
- `storage_status`
- `bucket`
- `object_key`
- `permanent_object_key`
- `public_url`
- `thumbnail_url`
- `width`
- `height`
- `duration_seconds`
- `expires_at`
- `saved_at`
- timestamps

Storage statuses:

- `temp`: available as temporary media.
- `quarantine`: retained but considered large/suspicious enough for slower
  handling.
- `permanent`: saved to a permanent entity path.
- `deleted`: metadata was deleted/redacted.
- `rejected`: metadata exists but bytes were not stored.

### Transcript

Important fields:

- `id`
- `project_id`
- `company_id`
- `message_id`
- `media_id`
- `provider`
- `model`
- `language`
- `text`
- `raw_response_json`
- `status`
- `error_message`
- timestamps

Typical statuses:

- `pending`
- `queued`
- `completed`
- `failed`
- skipped states when transcription is not applicable or not available

### DirtyConversationItem

Used by backend consumers that poll for recoverable work.

Fields:

- `conversation_id`
- `max_seq`
- `reason`
- `priority`
- `available_at`
- `lease_token`
- `locked_until`

ACK body:

```json
{
  "consumer_id": "consumer-backend-name",
  "processed_until_seq": 541,
  "lease_token": "lease_..."
}
```

### CommonEvent

Compact event shape used by Kafka, WebSocket, webhook, and internal event
streams.

Fields:

- `event_id`
- `event_type`
- `project_id`
- `company_id`
- `channel_id`
- `conversation_id`
- `message_id`
- `conversation_seq`
- `trace_id`
- `causation_id`
- `correlation_id`
- `occurred_at`
- `produced_at`
- `payload`

## Endpoint Catalog

All endpoints return the standard error envelope on failures.

### Health, Docs, Metrics, Debug

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | Liveness |
| GET | `/ready` | Readiness and dependency checks |
| GET | `/metrics` | Prometheus metrics |
| GET | `/openapi.json` | REST contract |
| GET | `/asyncapi.json` | Event contract |
| GET | `/docs` | Minimal docs HTML |
| GET | `/debug/kafka` | Kafka/event-bus debug snapshot |
| GET | `/debug/kafka/deadletters` | List Kafka deadletters |
| POST | `/debug/kafka/deadletters/{deadletter_id}/replay` | Replay one deadletter |
| GET | `/debug/dirty` | Debug dirty/event records |
| GET | `/debug/channels` | Debug channel/session config |
| GET | `/dev-media/{media_id}` | Development media preview/download |

### Companies

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/companies` | Create/upsert company in the internal project |
| GET | `/v1/companies/{company_id}` | Read company |

Create company:

```json
{
  "id": "company_123",
  "external_company_id": "optional-external-id",
  "name": "Acme Inc"
}
```

If `id` is omitted, RustZap uses `external_company_id`; if both are omitted,
development defaults may apply.

### WhatsApp Channels

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/companies/{company_id}/channels/whatsapp/accounts` | Create channel account |
| GET | `/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}` | Read channel account |
| POST | `/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/connect` | Connect/start WhatsApp channel |
| POST | `/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/disconnect` | Disconnect channel |
| GET | `/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/qr` | Read QR state |
| POST | `/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/pair-code` | Request pair code; currently `not_supported` |
| GET | `/v1/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/capabilities` | Read provider capabilities |

Create channel body:

```json
{
  "id": "channel_main",
  "label": "Main WhatsApp",
  "phone_e164": "+5511999999999"
}
```

QR response shape:

```json
{
  "channel_id": "channel_main",
  "status": "waiting_qr",
  "qr_code_text": "qr payload or null",
  "expires_at": "2026-05-03T12:00:00Z"
}
```

Capabilities response:

```json
{
  "provider": "whatsapp-rust",
  "features": {
    "send_text": {"supported": true},
    "pin_message": {
      "supported": false,
      "reason": "pin is not available in the active adapter"
    },
    "mark_read": {
      "supported": true,
      "guaranteed": false
    }
  }
}
```

### Contacts

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/companies/{company_id}/contacts` | List contacts |
| GET | `/v1/companies/{company_id}/contacts/by-phone/{phone_e164}` | Read contact by phone |
| GET | `/v1/companies/{company_id}/contacts/{contact_id}` | Inspect contact and best-effort refresh profile |
| GET | `/v1/companies/{company_id}/contacts/{contact_id}/media` | List media related to contact |
| GET | `/v1/companies/{company_id}/contacts/{contact_id}/conversations` | List direct/group conversations related to contact |

List endpoints support `limit` and `cursor`.

### Conversations

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/companies/{company_id}/conversations` | List conversations |
| GET | `/v1/companies/{company_id}/conversations/{conversation_id}` | Read one conversation |
| PATCH | `/v1/companies/{company_id}/conversations/{conversation_id}` | Patch local metadata |
| GET | `/v1/companies/{company_id}/conversations/{conversation_id}/messages` | Read messages by cursor |
| POST | `/v1/companies/{company_id}/conversations/{conversation_id}/messages` | Send idempotent message |
| GET | `/v1/companies/{company_id}/conversations/{conversation_id}/search` | Search messages with `q` |
| GET | `/v1/companies/{company_id}/conversations/{conversation_id}/media` | List conversation media |
| GET | `/v1/companies/{company_id}/conversations/{conversation_id}/starred` | List starred messages |
| POST | `/v1/companies/{company_id}/conversations/{conversation_id}/mark-read` | Provider mark-read; may be `not_supported` |
| POST | `/v1/companies/{company_id}/conversations/{conversation_id}/typing` | Provider typing indicator; currently `not_supported` |

Patch example:

```json
{
  "is_archived": false,
  "is_muted": false,
  "is_pinned": true,
  "control_mode": "autopilot"
}
```

Only the listed fields are accepted.

### Messages

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/companies/{company_id}/messages/{message_id}` | Read one message |
| POST | `/v1/companies/{company_id}/messages/{message_id}/react` | Send reaction through provider |
| DELETE | `/v1/companies/{company_id}/messages/{message_id}/react` | Remove reaction through provider |
| POST | `/v1/companies/{company_id}/messages/{message_id}/pin` | Pin through provider; currently `not_supported` |
| DELETE | `/v1/companies/{company_id}/messages/{message_id}/pin` | Unpin through provider; currently `not_supported` |
| POST | `/v1/companies/{company_id}/messages/{message_id}/star` | Star through provider; currently `not_supported` |
| DELETE | `/v1/companies/{company_id}/messages/{message_id}/star` | Unstar through provider; currently `not_supported` |

Send text:

```bash
curl -X POST \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: send-company-123-5511999999999-0001' \
  -H 'X-RustZap-Actor-Id: ai_agent_1' \
  http://<LAN_IP>:8167/v1/companies/company_123/conversations/5511999999999/messages \
  -d '{
    "type": "text",
    "text": "Oi, tudo bem?",
    "quoted_message_id": null,
    "metadata": {
      "source": "tetoz_ai",
      "mode": "autopilot"
    }
  }'
```

Accepted response:

```json
{
  "accepted": true,
  "command_id": "msg_...",
  "message": {
    "id": "msg_...",
    "conversation_id": "5511999999999",
    "status": "queued",
    "delivery_state": "pending",
    "sent_by_external_user_id": "ai_agent_1"
  },
  "status": "queued",
  "delivery_state": "pending"
}
```

React body:

```json
{
  "emoji": "+1"
}
```

If `emoji` is omitted, the route defaults to thumbs-up.

### Media

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/companies/{company_id}/media/upload-outbound` | Upload outbound media bytes |
| GET | `/v1/companies/{company_id}/media/{media_id}` | Read media metadata |
| DELETE | `/v1/companies/{company_id}/media/{media_id}` | Delete media bytes and metadata |
| GET | `/v1/companies/{company_id}/media/{media_id}/download-url` | Get R2/public/dev URL |
| POST | `/v1/companies/{company_id}/media/{media_id}/save` | Save media permanently for an entity |

Outbound upload is `multipart/form-data` and requires `Idempotency-Key`.
Retrying the same key with the same body returns the same media; retrying with a
different body returns an idempotency conflict.

Fields:

- `conversation_id`: required.
- `type` or `media_type`: optional, one of `image`, `audio`, `document`.
- `mime_type`: optional if multipart file content type is present.
- `filename`: optional.
- `caption`: optional.
- `file` or `media`: required file field.

Example:

```bash
curl -X POST \
  -H 'Idempotency-Key: upload-media-0001' \
  -F 'conversation_id=5511999999999' \
  -F 'type=document' \
  -F 'caption=Segue o documento.' \
  -F 'file=@contrato.pdf;type=application/pdf' \
  http://<LAN_IP>:8167/v1/companies/company_123/media/upload-outbound
```

Send uploaded media:

```bash
curl -X POST \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: send-media-0001' \
  http://<LAN_IP>:8167/v1/companies/company_123/conversations/5511999999999/messages \
  -d '{
    "type": "document",
    "media_id": "media_...",
    "caption": "Segue o documento.",
    "filename": "contrato.pdf"
  }'
```

Save permanently:

```json
{
  "entity_type": "lead",
  "entity_id": "lead_123"
}
```

The permanent object key hashes entity identifiers. The request body may include
additional fields, but the current implementation uses `entity_type` and
`entity_id`.

### Transcripts

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/companies/{company_id}/messages/{message_id}/transcript` | Read transcript |
| POST | `/v1/companies/{company_id}/messages/{message_id}/transcribe` | Queue/request transcription |

Inbound audio simulation and real inbound audio media create a pending
transcript request when the audio is accepted for storage. Production workers
complete the transcript through the configured STT provider.

Request transcription:

```bash
curl -X POST \
  http://<LAN_IP>:8167/v1/companies/company_123/messages/msg_123/transcribe
```

Accepted response:

```json
{
  "accepted": true,
  "command_id": "transcript_...",
  "status": "queued",
  "transcript": {
    "id": "transcript_...",
    "message_id": "msg_123",
    "media_id": "media_123",
    "provider": "groq",
    "status": "queued",
    "text": null
  }
}
```

Read transcript:

```bash
curl http://<LAN_IP>:8167/v1/companies/company_123/messages/msg_123/transcript
```

### Groups

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/companies/{company_id}/groups` | List groups |
| GET | `/v1/companies/{company_id}/groups/{group_id}` | Inspect group and best-effort refresh profile |
| GET | `/v1/companies/{company_id}/groups/{group_id}/members` | List members |
| POST | `/v1/companies/{company_id}/groups/{group_id}/members` | Add member; currently `not_supported` |
| GET | `/v1/companies/{company_id}/groups/{group_id}/media` | List group media |
| GET | `/v1/companies/{company_id}/groups/{group_id}/starred` | List starred group messages |
| GET | `/v1/companies/{company_id}/groups/{group_id}/search` | Search group messages with `q` |
| POST | `/v1/companies/{company_id}/groups/{group_id}/exit` | Exit group; currently `not_supported` |
| DELETE | `/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}` | Remove member; currently `not_supported` |
| POST | `/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}/promote` | Promote member; currently `not_supported` |
| POST | `/v1/companies/{company_id}/groups/{group_id}/members/{contact_id}/demote` | Demote member; currently `not_supported` |
| POST | `/v1/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/accept` | Accept join request; currently `not_supported` |
| POST | `/v1/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/reject` | Reject join request; currently `not_supported` |

Group member public shape:

```json
{
  "group_id": "120363000000000000@g.us",
  "contact_id": "5511999999999",
  "technical_contact_id": "5511999999999@s.whatsapp.net",
  "wa_jid": "5511999999999@s.whatsapp.net",
  "phone_e164": "+5511999999999",
  "phone_number": "5511999999999",
  "name": "Maria",
  "display_name": "Maria",
  "role": "member",
  "is_admin": false
}
```

Group reads are cached/local plus best-effort provider refresh when the channel
is connected. Admin mutations return `not_supported` unless the active adapter
implements them.

### Dirty Conversations

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/companies/{company_id}/dirty-conversations?consumer_id={id}&limit=100` | Lease dirty conversations |
| POST | `/v1/companies/{company_id}/dirty-conversations/{conversation_id}/ack` | Acknowledge leased work |

List response:

```json
{
  "items": [
    {
      "conversation_id": "5511999999999@s.whatsapp.net",
      "max_seq": 541,
      "reason": "new_message",
      "priority": 100,
      "available_at": "2026-05-03T12:00:00Z",
      "lease_token": "lease_...",
      "locked_until": "2026-05-03T12:01:00Z"
    }
  ]
}
```

ACK:

```json
{
  "consumer_id": "tetoz-ai-worker",
  "processed_until_seq": 541,
  "lease_token": "lease_..."
}
```

If `max_seq` advanced while the consumer was processing, RustZap keeps the
conversation dirty and returns:

```json
{
  "acked": true,
  "remaining_dirty": true,
  "current_max_seq": 542
}
```

### Consumer Callbacks

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/companies/{company_id}/consumer-callbacks` | List callbacks |
| POST | `/v1/companies/{company_id}/consumer-callbacks` | Create callback |
| PATCH | `/v1/companies/{company_id}/consumer-callbacks/{callback_id}` | Update callback |
| DELETE | `/v1/companies/{company_id}/consumer-callbacks/{callback_id}` | Delete callback |

Create/update body:

```json
{
  "url": "https://consumer.example.com/rustzap/webhook",
  "secret": "whsec_plaintext_only_on_write",
  "enabled": true,
  "events": ["conversation.dirty", "message.received"],
  "max_batch_size": 100,
  "timeout_seconds": 10
}
```

Secrets are stored encrypted when `RUSTZAP_SECRET_MASTER_KEY` is configured.
Read responses are sanitized and do not expose plaintext secrets.

Webhook signatures use:

```txt
HMAC-SHA256(secret, timestamp + "." + raw_body)
```

Header names are configurable. Defaults include:

- `X-RustZap-Signature`
- `X-RustZap-Timestamp`
- `X-RustZap-Event-Id`

### Privacy

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/companies/{company_id}/privacy/contacts/{contact_id}/export` | Export contact data |
| DELETE | `/v1/companies/{company_id}/privacy/contacts/{contact_id}` | Delete/anonymize contact and message text |
| POST | `/v1/companies/{company_id}/privacy/contacts/{contact_id}/anonymize` | Anonymize contact without deleting message text |

Export includes contact, related conversations, messages, media metadata, and
transcripts. Delete/anonymize redacts contact and related display fields and
records an audit entry.

### WebSocket

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/ws/v1` | Internal diagnostic event WebSocket |

Subscribe frame:

```json
{
  "type": "subscribe",
  "project_id": "rustzap_internal",
  "company_id": "company_123",
  "topics": [
    "channel.*",
    "conversation.dirty",
    "message.*",
    "media.*",
    "transcript.*",
    "group.*"
  ]
}
```

Subscription response:

```json
{
  "type": "subscribed",
  "project_id": "rustzap_internal",
  "company_id": "company_123",
  "topics": ["message.*"],
  "events": []
}
```

Stream frame:

```json
{
  "type": "conversation.dirty",
  "event": {
    "event_id": "evt_...",
    "event_type": "conversation.dirty",
    "company_id": "company_123",
    "conversation_id": "5511999999999@s.whatsapp.net",
    "conversation_seq": 541,
    "payload": {
      "to_seq": 541,
      "reason": "new_message",
      "priority": 100
    }
  }
}
```

If the socket lags, RustZap can emit:

```json
{
  "type": "events.lagged",
  "skipped": 42
}
```

Clients must then recover through REST cursor reads.

### Dev Simulation

Enabled only when both are true:

```env
RUSTZAP_DEV_MODE=true
DEV_SIMULATION_ENABLED=true
```

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/dev/companies/{company_id}/simulate/inbound-text` | Create inbound text |
| POST | `/v1/dev/companies/{company_id}/simulate/inbound-audio` | Create inbound audio, media, and pending transcript |
| POST | `/v1/dev/companies/{company_id}/simulate/inbound-image` | Create inbound image/document-like media |
| POST | `/v1/dev/companies/{company_id}/simulate/receipt` | Update a message receipt/status |
| POST | `/v1/dev/companies/{company_id}/simulate/qr-rotate` | Generate dev QR state |
| POST | `/v1/dev/companies/{company_id}/simulate/group-event` | Create group event/message |
| POST | `/v1/dev/companies/{company_id}/simulate/reset` | Reset in-memory dev state |

Inbound text body:

```json
{
  "conversation_id": "5511999999999@s.whatsapp.net",
  "channel_id": "channel_dev",
  "from_phone_e164": "+5511999999999",
  "sender_name": "Maria",
  "profile_picture_url": "https://example.com/avatar.jpg",
  "text": "Oi"
}
```

Inbound media body:

```json
{
  "conversation_id": "5511999999999@s.whatsapp.net",
  "channel_id": "channel_dev",
  "from_phone_e164": "+5511999999999",
  "sender_name": "Maria",
  "media_type": "image",
  "mime_type": "image/png",
  "size_bytes": 1024,
  "filename": "foto.png",
  "caption": "Foto"
}
```

Receipt body:

```json
{
  "message_id": "msg_...",
  "receipt_type": "delivered"
}
```

Group event body:

```json
{
  "group_id": "120363000000000000@g.us",
  "channel_id": "channel_dev",
  "from_phone_e164": "+5511999999999",
  "member_name": "Maria",
  "text": "Mensagem de grupo"
}
```

## Essential Workflows

### 1. Create A Company Context

```bash
curl -X POST \
  -H 'content-type: application/json' \
  http://<LAN_IP>:8167/v1/companies \
  -d '{"id":"company_123","name":"Acme Inc"}'
```

Then use `company_123` in all tenant-scoped calls.

### 2. Create A Channel And Read QR

```bash
curl -X POST \
  -H 'content-type: application/json' \
  http://<LAN_IP>:8167/v1/companies/company_123/channels/whatsapp/accounts \
  -d '{"id":"channel_main","label":"Main WhatsApp","phone_e164":"+5511999999999"}'
```

Connect:

```bash
curl -X POST \
  http://<LAN_IP>:8167/v1/companies/company_123/channels/whatsapp/accounts/channel_main/connect
```

Poll QR:

```bash
curl \
  http://<LAN_IP>:8167/v1/companies/company_123/channels/whatsapp/accounts/channel_main/qr
```

Show `qr_code_text` to the operator while `status` is `waiting_qr`. QR secrets
expire; poll again for the latest state.

### 3. Simulate Inbound Text Locally

```bash
curl -X POST \
  -H 'content-type: application/json' \
  http://<LAN_IP>:8167/v1/dev/companies/company_123/simulate/inbound-text \
  -d '{
    "conversation_id":"5511999999999@s.whatsapp.net",
    "from_phone_e164":"+5511999999999",
    "sender_name":"Maria",
    "text":"Oi"
  }'
```

Read the conversation:

```bash
curl \
  'http://<LAN_IP>:8167/v1/companies/company_123/conversations/5511999999999/messages?after_seq=0&limit=100'
```

### 4. Get A User Profile Picture Or Contact Profile

If you know the phone:

```bash
curl \
  http://<LAN_IP>:8167/v1/companies/company_123/contacts/by-phone/5511999999999
```

If you know the contact id:

```bash
curl \
  http://<LAN_IP>:8167/v1/companies/company_123/contacts/5511999999999
```

Use these fields:

- `profile_picture_url`: provider profile picture URL when known.
- `avatar_url`: UI-friendly avatar URL when known.
- `profile_picture_media_id`: local media id when RustZap has a stored profile
  media object.
- `business_description`: WhatsApp business profile text when known.
- `display_name` and `push_name`: best known names from WhatsApp events/profile.

Profile refresh is best-effort. If the channel is disconnected or the provider
does not return a picture, these fields can be `null`.

### 5. Send A Text Message Safely

```bash
curl -X POST \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: company_123-5511999999999-send-0001' \
  -H 'X-RustZap-Actor-Id: user_or_ai_123' \
  http://<LAN_IP>:8167/v1/companies/company_123/conversations/5511999999999/messages \
  -d '{"type":"text","text":"Oi, tudo bem?"}'
```

Rules:

- Use a stable idempotency key per logical send.
- Retry the same body with the same key on network failures.
- Do not reuse the same key with a different body.
- Treat `202 accepted` and `delivery_state=pending` as queued, not delivered.
- Watch later receipts/events or reread the message for delivery updates.

### 6. Upload And Send Media

Upload:

```bash
curl -X POST \
  -H 'Idempotency-Key: company_123-upload-doc-0001' \
  -F 'conversation_id=5511999999999' \
  -F 'type=document' \
  -F 'caption=Contrato' \
  -F 'file=@contrato.pdf;type=application/pdf' \
  http://<LAN_IP>:8167/v1/companies/company_123/media/upload-outbound
```

Send:

```bash
curl -X POST \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: company_123-send-doc-0001' \
  http://<LAN_IP>:8167/v1/companies/company_123/conversations/5511999999999/messages \
  -d '{"type":"document","media_id":"media_...","caption":"Contrato","filename":"contrato.pdf"}'
```

Get a download URL:

```bash
curl \
  http://<LAN_IP>:8167/v1/companies/company_123/media/media_123/download-url
```

Save media permanently to a consumer entity:

```bash
curl -X POST \
  -H 'content-type: application/json' \
  http://<LAN_IP>:8167/v1/companies/company_123/media/media_123/save \
  -d '{"entity_type":"lead","entity_id":"lead_123"}'
```

### 7. Transcribe Audio

Simulate inbound audio:

```bash
curl -X POST \
  -H 'content-type: application/json' \
  http://<LAN_IP>:8167/v1/dev/companies/company_123/simulate/inbound-audio \
  -d '{
    "conversation_id":"5511999999999@s.whatsapp.net",
    "from_phone_e164":"+5511999999999",
    "mime_type":"audio/ogg",
    "size_bytes":4096,
    "filename":"voice.ogg"
  }'
```

The response includes:

```json
{
  "message": {"id": "msg_...", "message_type": "audio"},
  "media": {"id": "media_...", "storage_status": "temp"},
  "transcript": {"id": "transcript_...", "status": "pending"}
}
```

If a transcript does not yet exist or needs retry:

```bash
curl -X POST \
  http://<LAN_IP>:8167/v1/companies/company_123/messages/msg_123/transcribe
```

Read transcript:

```bash
curl \
  http://<LAN_IP>:8167/v1/companies/company_123/messages/msg_123/transcript
```

Production transcription requires Groq configuration. Development and tests can
use mocked/dev flows.

### 8. Work With Groups

List groups:

```bash
curl \
  'http://<LAN_IP>:8167/v1/companies/company_123/groups?limit=100'
```

Inspect one group:

```bash
curl \
  'http://<LAN_IP>:8167/v1/companies/company_123/groups/120363000000000000%40g.us'
```

List members:

```bash
curl \
  'http://<LAN_IP>:8167/v1/companies/company_123/groups/120363000000000000%40g.us/members?limit=100'
```

Search group messages:

```bash
curl \
  'http://<LAN_IP>:8167/v1/companies/company_123/groups/120363000000000000%40g.us/search?q=contrato&limit=50'
```

List group media:

```bash
curl \
  'http://<LAN_IP>:8167/v1/companies/company_123/groups/120363000000000000%40g.us/media?limit=50'
```

Current group admin commands are documented but return `not_supported` with the
active adapter. Use `GET .../capabilities` before exposing group admin actions
in a consumer UI.

### 9. Process New Messages With Dirty Polling

Consumer loop:

1. Lease dirty conversations.
2. For each item, read messages after the consumer's last processed seq.
3. Process and persist consumer-side state.
4. ACK with the lease token and highest processed sequence.
5. If RustZap returns `remaining_dirty=true`, lease and process again.

Lease:

```bash
curl \
  'http://<LAN_IP>:8167/v1/companies/company_123/dirty-conversations?consumer_id=tetoz-ai&limit=100'
```

Read cursor:

```bash
curl \
  'http://<LAN_IP>:8167/v1/companies/company_123/conversations/5511999999999/messages?after_seq=540&limit=500'
```

ACK:

```bash
curl -X POST \
  -H 'content-type: application/json' \
  http://<LAN_IP>:8167/v1/companies/company_123/dirty-conversations/5511999999999/ack \
  -d '{
    "consumer_id":"tetoz-ai",
    "processed_until_seq":541,
    "lease_token":"lease_..."
  }'
```

### 10. Consume Kafka But Recover With REST

Kafka event signals are compact. The consumer backend should:

1. Consume compact `CommonEvent` records.
2. Deduplicate by `event_id`.
3. For `conversation.dirty`, read its local `last_seen_seq`.
4. Fetch REST message deltas with `after_seq`.
5. Persist/cache messages.
6. Advance local cursor only after successful persistence.
7. Commit Kafka offset only after successful processing or durable
   deduplication.
8. Notify browsers through the consumer backend's own WebSocket.

Browsers should not receive full RustZap event payloads directly in production.

## Events And Realtime

The event contract is exposed at `/asyncapi.json`.

Important event types include:

- `conversation.dirty`
- `message.received`
- `message.queued`
- `message.sent`
- `message.failed`
- `message.receipt`
- `message.reaction`
- `conversation.updated`
- `media.stored`
- `media.deleted`
- `audio.transcription.requested`
- `transcript.completed`
- `group.updated`
- `channel.status`
- `channel.qr`

Partitioning intent:

- Conversation events partition by company/channel/conversation where possible.
- Channel events partition by company/channel.
- Event payloads are small and do not include raw bytes.

Kafka debug endpoints:

```bash
curl http://<LAN_IP>:8167/debug/kafka
curl http://<LAN_IP>:8167/debug/kafka/deadletters
curl -X POST http://<LAN_IP>:8167/debug/kafka/deadletters/<deadletter_id>/replay
```

Webhook delivery:

- Uses persisted consumer callback definitions.
- Delivers compact event batches.
- Retries with configured backoff.
- Records delivery attempts.
- Does not replace REST cursor reads for recovery.

## Dev Simulation

Dev simulation is the fastest way to validate UIs and consumer backends without
a real WhatsApp device.

Enable:

```env
RUSTZAP_DEV_MODE=true
DEV_SIMULATION_ENABLED=true
```

Example end-to-end local chat:

```bash
curl -X POST \
  -H 'content-type: application/json' \
  http://<LAN_IP>:8167/v1/dev/companies/company_dev/simulate/inbound-text \
  -d '{"conversation_id":"conv_dev","text":"Oi"}'

curl \
  'http://<LAN_IP>:8167/v1/companies/company_dev/conversations/conv_dev/messages?after_seq=0'
```

Dev media preview:

```bash
curl http://<LAN_IP>:8167/dev-media/media_123
```

Dev reset:

```bash
curl -X POST \
  http://<LAN_IP>:8167/v1/dev/companies/company_dev/simulate/reset
```

## Testing

Run the default suite:

```bash
cargo test
```

Run current M2M contract tests:

```bash
cargo test current_contract_tests
```

Run feature-gated compilation and ignored real integration tests:

```bash
cargo test --features external-integrations
```

Run real Kafka/Postgres integration tests with containers:

```bash
./scripts/test-kafka.sh
```

For already running services:

```bash
KAFKA_TEST_BROKERS=127.0.0.1:9092 \
KAFKA_TEST_DATABASE_URL=postgres://rustzap:rustzap@127.0.0.1:5432/rustzap \
  cargo test --features external-integrations --test kafka_integration -- --ignored --nocapture
```

## Operational Notes

- Do not expose RustZap directly to browsers in production.
- Put authentication, authorization, tenancy choice, and user session handling
  in the upper application.
- Keep `company_id` stable. Changing it creates a different tenant boundary.
- Use high-cardinality actor ids only as audit metadata; do not use them to
  shard WhatsApp state.
- Use `Idempotency-Key` on outbound sends and other side-effectful commands.
- Use REST cursor reads to recover after any dropped signal.
- Keep media bytes out of events and WebSocket frames.
- Keep logs redacted for message text and phone numbers in production.
- Monitor `/ready`, `/metrics`, Kafka deadletters, webhook delivery attempts,
  STT failures, and dirty backlog.
- Configure persistent volumes for Postgres, Redpanda, WhatsApp session SQLite,
  and local media storage when not using external managed services.
- Use `/openapi.json` and `/asyncapi.json` in CI or consumer SDK generation to
  detect contract drift.
