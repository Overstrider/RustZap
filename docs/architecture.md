# RustZap Architecture

RustZap is a backend-only WhatsApp conversation gateway. It owns raw conversation history, media metadata, transcript storage, dirty conversation state, and idempotent send commands. SaaS consumers own CRM, business rules, and AI decisions.

The current implementation uses capability-first adapters. Development flows run through mock WhatsApp, local state, and simulation endpoints so the API and `dev-tester/` can validate QR, chat, media, receipts, reactions, groups, dirty polling, and WebSocket contracts without a live WhatsApp device. Provider commands that the active adapter cannot prove are returned as `not_supported`; local state is not mutated to fake provider success.

Production boundaries are explicit:

- Metadata DB: Postgres migrations in `migrations/`.
- WhatsApp session state: local SQLite path under `WA_SESSION_SQLITE_DIR`.
- Event bus: `EVENT_BUS=kafka` contract with Redpanda in Podman; events never carry raw media bytes. In Postgres-backed Kafka mode, events are written to `event_outbox` transactionally and drained to Kafka with retry/DLQ metadata instead of relying on process memory.
- Media: local staging plus R2-compatible object keys built without phone/name/PII.
- STT: Groq boundary, mocked in tests/dev simulation.
- Consumer signals: polling is always recoverable; WebSocket/webhook/Kafka are compact notification channels only. Dirty ACK is tracked per registered consumer so one consumer cannot clear another consumer's pending work.
- API keys: generated plaintext is shown once; only hashes are retained for validation. Fixed bearer tokens are development fallback only.

`WA_SESSION_ENCRYPT_AT_REST=true` means host volume/disk encryption or provider-supported SQLite encryption must be configured operationally. This code does not claim transparent SQLCipher encryption for `whatsapp-rust` unless the selected adapter stack supports it.
