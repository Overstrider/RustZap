# RustZap

RustZap is a Rust 2024 WhatsApp conversation gateway for SaaS consumers. It exposes REST and WebSocket APIs, stores conversation state, tracks dirty conversations for cursor-based processing, manages media metadata, and provides dev simulation endpoints plus a local Next.js tester.

## Quick Start

```bash
cargo test
cargo run
```

API defaults:

- API: `http://<LAN_IP>:8167`
- Project token: `dev_project_key`
- Admin token: `dev_admin_key`

## Podman

```bash
cp .env.production.example .env.production
cp .env.secrets.example .env.secrets
$EDITOR .env.production .env.secrets
./scripts/deploy.sh production
```

This builds the RustZap image, starts RustZap, Postgres, Redpanda, and the dev tester frontend as Podman containers, applies migrations, waits for readiness, and prints the service status.

- API: `http://<LAN_IP>:8167`
- Dev tester: `http://<LAN_IP>:3167`

## Dev Tester

```bash
cd dev-tester
npm install
npm run dev
```

Open `http://<LAN_IP>:3167`.

## Important Constraints

- No `aws-sdk-s3`.
- WhatsApp session SQLite lives under `WA_SESSION_SQLITE_DIR` on a persistent Podman volume.
- Cloudflare D1 is not used for WhatsApp session or RustZap core metadata.
- Kafka/Redpanda events use compact metadata only, never raw media bytes; Postgres-backed Kafka mode uses a durable `event_outbox`, `event_inbox`, and persistent DLQ/replay path.
- WebSocket/webhook/Kafka signals are notifications; REST cursor reads remain the source of truth.
