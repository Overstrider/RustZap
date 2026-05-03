# RustZap Architecture

RustZap is an internal M2M WhatsApp conversation library/service boundary. It owns raw conversation history, media metadata, transcript storage, dirty conversation state, and idempotent send commands. The application above RustZap supplies trusted `company_id` and optional actor context and owns CRM, business rules, AI decisions, and end-user authentication.

The current implementation uses capability-first adapters. Development flows run through mock WhatsApp, local state, simulation endpoints, and internal tools so the API can validate QR, chat, media, receipts, reactions, groups, dirty polling, and WebSocket contracts without a live WhatsApp device. Provider commands that the active adapter cannot prove are returned as `not_supported`; local state is not mutated to fake provider success.

Production boundaries are explicit:

- Metadata DB: Postgres migrations in `migrations/`.
- WhatsApp session state: local SQLite path under `WA_SESSION_SQLITE_DIR`.
- Event bus: `EVENT_BUS=kafka` contract with Redpanda in Podman; events never carry raw media bytes. In Postgres-backed Kafka mode, events are written to `event_outbox` transactionally and drained to Kafka with retry/DLQ metadata instead of relying on process memory.
- Media: local staging plus R2-compatible object keys built without phone/name/PII.
- STT: Groq boundary, mocked in tests/dev simulation.
- Consumer signals: polling is always recoverable; WebSocket/webhook/Kafka are compact notification channels only. Dirty ACK is tracked per registered consumer so one consumer cannot clear another consumer's pending work.
- Tenant context: `company_id` partitions WhatsApp state. Actor/user identity is audit metadata only and never partitions chats, messages, media, dirty state, callbacks, or events.

The production realtime path is `RustZap -> Kafka/Redpanda -> consumer backend -> browser`. The consumer backend reads details by REST cursor (`after_seq`, `limit`) and exposes its own REST/WebSocket UI contract. RustZap's direct WebSocket is for development, diagnostics, and controlled internal tools, not for browser fanout in production.

RustZap events are compact signals. They may include identifiers such as `conversation_id`, `message_id`, `media_id`, `conversation_seq`, `to_seq`, `trace_id`, and `correlation_id`; they must not carry media bytes, full transcripts, or full message history. The event contract is exposed at `/asyncapi.json`, while the REST command/read contract is exposed at `/openapi.json`.

`WA_SESSION_ENCRYPT_AT_REST=true` means host volume/disk encryption or provider-supported SQLite encryption must be configured operationally. This code does not claim transparent SQLCipher encryption for `whatsapp-rust` unless the selected adapter stack supports it.
