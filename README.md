# RustZap

RustZap is a Rust 2024 WhatsApp conversation gateway for SaaS consumers. It exposes REST APIs, compact event contracts, internal diagnostic WebSocket signals, dirty conversation cursors, media metadata, and dev simulation endpoints.

## Quick Start

```bash
cargo test
cargo run
```

API defaults:

- API: `http://<LAN_IP>:8167`
- REST contract: `http://<LAN_IP>:8167/openapi.json`
- Event contract: `http://<LAN_IP>:8167/asyncapi.json`
- Project token: `dev_project_key`
- Admin token: `dev_admin_key`

## Podman

```bash
cp .env.production.example .env.production
cp .env.secrets.example .env.secrets
$EDITOR .env.production .env.secrets
./scripts/deploy.sh production
```

This builds the RustZap image, starts RustZap, Postgres, and Redpanda as Podman containers, applies migrations, waits for readiness, and prints the service status.

- API: `http://<LAN_IP>:8167`

## Consumer Backend

Production browsers should connect to the SaaS/backend above RustZap, not to RustZap directly. RustZap emits compact Kafka/Redpanda signals and remains the REST source of truth for cursor reads and idempotent commands. See `examples/whatsapp-web-shared/` for the consumer backend realtime contract.

## Important Constraints

- No `aws-sdk-s3`.
- WhatsApp session SQLite lives under `WA_SESSION_SQLITE_DIR` on a persistent Podman volume.
- Cloudflare D1 is not used for WhatsApp session or RustZap core metadata.
- Kafka/Redpanda events use compact metadata only, never raw media bytes; Postgres-backed Kafka mode uses a durable `event_outbox`, `event_inbox`, and persistent DLQ/replay path.
- WebSocket/webhook/Kafka signals are notifications; REST cursor reads remain the source of truth.
