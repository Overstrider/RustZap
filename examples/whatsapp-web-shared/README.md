# whatsapp-web-shared Consumer Contract

This example documents the production shape for an application backend that sits
above RustZap. It intentionally does not connect browsers directly to RustZap.

```txt
RustZap
  -> Kafka/Redpanda compact signal
  -> consumer backend cache/sync loop
  -> consumer backend REST + WebSocket
  -> browser
```

RustZap is the source of truth for WhatsApp history, media metadata, transcripts,
dirty state, and idempotent commands. The consumer backend owns users, CRM,
business rules, AI decisions, and browser fanout.

## Backend Sync Loop

The backend keeps `last_seen_seq` per `company_id + conversation_id`.

1. Consume compact RustZap events from Kafka/Redpanda.
2. Deduplicate by `event_id`.
3. For `conversation.dirty`, read the local `last_seen_seq`.
4. Fetch `GET /v1/companies/{company_id}/conversations/{conversation_id}/messages?after_seq={last_seen_seq}&limit=500`.
5. Persist/cache the returned messages and advance `last_seen_seq` to the highest `conversation_seq`.
6. Commit the Kafka offset only after the signal was persisted or safely deduplicated.
7. Emit compact backend WebSocket frames to browsers.

If Kafka is not enabled in local development, the same loop can lease dirty work
through:

```txt
GET  /v1/companies/{company_id}/dirty-conversations?consumer_id=whatsapp-web-shared&limit=100
POST /v1/companies/{company_id}/dirty-conversations/{conversation_id}/ack
```

The ACK body must include `consumer_id`, `lease_token`, and
`processed_until_seq`. RustZap keeps the conversation dirty if `max_seq` advanced
while the backend was processing.

## Browser Realtime Contract

The backend WebSocket emits UI-level notifications only:

```json
{"type":"conversation.updated","conversation_id":"5511999999999","to_seq":42}
{"type":"message.delta_available","conversation_id":"5511999999999","from_seq":41,"to_seq":42}
{"type":"receipt.updated","conversation_id":"5511999999999","message_id":"msg_123","delivery_state":"delivered"}
{"type":"channel.status","channel_id":"channel_123","status":"connected"}
{"type":"group.participants.updated","group_id":"120363000000000000@g.us"}
```

Browsers do not receive media bytes, full transcripts, or full message history
over WebSocket. When the active conversation receives `message.delta_available`,
the browser fetches the delta from the consumer backend REST API. If the
conversation is not open, the browser updates only list metadata and badges.

## Fanout Defaults

- Batch/debounce repeated frames per conversation for a short window.
- Keep one bounded queue per socket.
- Coalesce repeated `conversation.updated` and `message.delta_available` frames
  by conversation.
- Send heartbeat/ping frames and require reconnect with REST resync.
- Drop stale socket frames before allowing unbounded memory growth.

## Metrics

The consumer backend should expose at least:

- Kafka consumer lag.
- Events received, processed, and deduplicated.
- Active WebSocket clients.
- Frames sent per second.
- Average and maximum socket queue depth.
- RustZap event to browser notification latency.
