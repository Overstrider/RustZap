# Security Notes

- RustZap is an internal M2M boundary. The caller supplies trusted company context; RustZap does not perform end-user auth, email verification, bearer-token auth, or per-user authorization.
- `company_id` is the tenant boundary. Optional actor/user identity is audit metadata only.
- Write commands use `Idempotency-Key` where required.
- Standard error envelope prevents ad hoc error shapes.
- R2 object keys never include phone numbers, names, CPF, or original filenames.
- Logs should keep `RUSTZAP_LOG_REDACT_MESSAGE_TEXT=true` and `RUSTZAP_LOG_REDACT_PHONE=true` in production.
- WebSocket and HTTP routes use the trusted company context sent by the caller.
- Webhook signatures use `HMAC-SHA256(secret, timestamp + "." + raw_body)`.
- Callback CRUD is persisted in RustZap metadata state and audited; delivery attempts are designed for at-least-once retry without replacing cursor polling as recovery.
- Privacy export/anonymize/delete operates on contact, conversation, message, media metadata, and transcript records, then records redacted audit entries.
- Provider-facing commands that are not implemented by the active WhatsApp adapter return `not_supported` instead of mutating local state.
