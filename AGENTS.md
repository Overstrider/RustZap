# RustZap Agent Guide

This file is the operational guide for agents working in this repository. Use
it to preserve RustZap's M2M contract, tenant boundary, security posture, and
local development rules.

## Project Model

RustZap is an internal M2M WhatsApp conversation library/service boundary, not
an autonomous internet-facing application. The SaaS/backend above RustZap owns
users, login, CRM data, business rules, AI decisions, browser WebSockets, and
per-user authorization.

RustZap trusts the application above it. If the caller says a request is for a
company, RustZap accepts that context.

`company_id` is the required tenant boundary for WhatsApp sessions, channel
accounts, chats, messages, media, transcripts, dirty state, callbacks, events,
and privacy operations. User identity is optional actor/audit metadata only; it
must not partition WhatsApp state. The actor can be a human, automation, or AI.

Do not add end-user authentication, email verification, passwords, login
screens, JWT sessions, or per-user authorization to RustZap. Example apps such
as `whatsapp-web-shared` should simulate the upper application by selecting or
injecting company and optional actor context before calling RustZap.

## Source Of Truth

- Human onboarding, business rules, workflows, and endpoint catalog:
  `README.md`.
- REST contract: `GET /openapi.json`.
- Event contract: `GET /asyncapi.json`.
- Route, model, and state implementation: `src/routes.rs`, `src/models.rs`,
  `src/state.rs`.
- Local development details: `docs/local-dev.md`.
- Security and privacy details: `docs/security.md`.
- Architecture and production boundaries: `docs/architecture.md`.

When documentation and code disagree, inspect the current implementation and
contract tests before changing behavior. Update docs and generated contracts
together when a public contract intentionally changes.

## Public Contract Rules

The current public M2M REST base path is:

```txt
/v1/companies/{company_id}/...
```

Do not reintroduce `/v1/projects/{project_id}/...` as the public contract.
RustZap may still store internal project ids such as `rustzap_internal`, but
external callers should use company-scoped paths.

Public caller/contact identifiers should use `phone_number` as digits only when
a phone number is known, preferably including country code. WhatsApp JIDs,
`@lid`, and raw technical aliases are internal/debug identifiers and must not
be required as the public M2M id when a phone number is known.

Expose minimal, clean M2M contracts for another backend to consume with
`company_id` and optional actor context. Prefer normalized public fields such
as `delivery_state` instead of forcing callers to interpret internal provider
states.

Provider commands that the active WhatsApp adapter cannot prove must return
`501 not_supported`. Do not mutate local state to fake provider success for
unsupported commands.

Events are compact recovery signals. They may carry ids, cursors, sequence
numbers, trace/correlation ids, and small status payloads. They must never
carry raw media bytes, full message history, or full transcripts. Consumers
recover detail through REST cursor reads.

## Security And Privacy

- Do not log secrets, raw provider payloads, unnecessary phone numbers, or
  message text in production.
- Keep `RUSTZAP_LOG_REDACT_MESSAGE_TEXT=true` and
  `RUSTZAP_LOG_REDACT_PHONE=true` in production-shaped configs.
- Media object keys must not contain phone numbers, names, CPF, original
  filenames, or raw WhatsApp JIDs.
- Webhook signatures use `HMAC-SHA256(secret, timestamp + "." + raw_body)`.
- Write commands that create externally meaningful side effects should use
  `Idempotency-Key`.
- Privacy export/anonymize/delete flows must remain company-scoped and must
  record redacted audit entries.
- For staging or production, do not deploy, mutate services, run migrations,
  restart processes, or execute operational commands unless the user explicitly
  grants permission for that action.

## Local Execution And Ports

For local development, do not tell the user to run commands. Run required
installs, builds, tests, servers, and smoke checks yourself, then report the
result and exact local URL.

Never run demos, frontends, backends, or public/local URLs on conventional
ports such as `3000`, `5000`, `5173`, `8000`, `8080`, or other common
framework defaults. Use high, nonstandard ports, verify the port is free with
`ss` before starting services, and report the exact URL and port used.

The checked-in docs may show canonical RustZap service examples on `8167` or
`3167`. For ad hoc agent-run services, still choose a high nonstandard free
port unless the user explicitly asks to use the documented local stack.

## Development Commands

Use the smallest command that verifies the work, then broaden when the blast
radius requires it.

```bash
cargo fmt --check
cargo test
cargo test current_contract_tests
cargo test --features external-integrations
./scripts/test-kafka.sh
sh -n scripts/*.sh
```

Preferred verification:

- Documentation-only changes: run `git diff --check -- AGENTS.md` and review
  against `README.md`, `docs/security.md`, and `docs/local-dev.md`.
- Public REST or event contract changes: run `cargo fmt --check` and
  `cargo test current_contract_tests`; update `/openapi.json` or
  `/asyncapi.json` generation paths as needed.
- Shared implementation changes: run `cargo fmt --check` and `cargo test`.
- Kafka, Postgres, R2, webhook, STT, or other external integration changes:
  run `cargo test --features external-integrations` and, when real broker/DB
  behavior matters, `./scripts/test-kafka.sh`.
- Shell script changes: run `sh -n scripts/*.sh` plus the focused smoke path
  for the changed script.

## Implementation Guardrails

- Keep changes scoped to the requested behavior. Preserve unrelated user work
  in the working tree.
- Keep company scoping on sessions, channels, chats, messages, media,
  transcripts, dirty state, callbacks, events, and privacy operations.
- Treat actor/user fields as audit metadata only.
- Use structured route/model/state helpers instead of ad hoc string parsing
  when RustZap already has a helper.
- Keep REST cursor reads as the source of truth for recovery. Kafka,
  WebSocket, webhook, and dirty signals are notifications.
- Keep direct RustZap WebSocket usage for development, diagnostics, and
  controlled internal tools. Production browser fanout belongs to the consumer
  backend.
- Do not broaden public API shapes or error envelopes casually. Public errors
  should use the standard envelope and stable error codes.
