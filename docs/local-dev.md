# Local Development

## Backend

```bash
cargo test
cargo run
```

Health:

```bash
curl http://<LAN_IP>:8167/health
curl http://<LAN_IP>:8167/ready
```

Local routes trust the internal caller context. Use company-scoped paths such as `/v1/companies/{company_id}/...`; optional actor metadata can be sent with `X-RustZap-Actor-Id`.

## Podman

```bash
cp .env.production.example .env.production
cp .env.secrets.example .env.secrets
$EDITOR .env.production .env.secrets
./scripts/deploy.sh production
./scripts/podman-logs.sh
./scripts/podman-down.sh
```

`production` starts backend, Postgres, and Redpanda only. To also start the
Next dev tester, run:

```bash
RUSTZAP_START_DEV_TESTER=1 ./scripts/deploy.sh development
```

Services:

- RustZap API: `http://<LAN_IP>:8167`
- Metadata DB: Postgres in the same Podman network
- Event bus: Redpanda in the same Podman network
- Dev tester UI: `http://<LAN_IP>:3167` when `RUSTZAP_START_DEV_TESTER=1`

For local in-memory development without Postgres persistence, copy `.env.development.example` to `.env.development` and run the binary directly with those variables loaded.

## Kafka

Production containers build RustZap with `external-integrations`, so `EVENT_BUS=kafka` uses `rdkafka` against Redpanda/Kafka. The app creates required topics when `KAFKA_ENABLE_AUTO_CREATE_TOPICS=true`, persists events to the Postgres `event_outbox` in the same transaction as the metadata snapshot, drains that outbox to Kafka, publishes compact `CommonEvent` JSON with the conversation/channel partition key, and exposes status at:

```bash
curl http://<LAN_IP>:8167/debug/kafka
curl http://<LAN_IP>:8167/debug/kafka/deadletters
curl -X POST http://<LAN_IP>:8167/debug/kafka/deadletters/<deadletter_id>/replay
```

The default Kafka compression is `none` so the local binary and Redpanda test image work without extra native compression features. Use another `KAFKA_COMPRESSION_TYPE` only after confirming the deployed `librdkafka` build supports it.

Run the ignored real-broker integration tests with Redpanda and Postgres containers:

```bash
./scripts/test-kafka.sh
```

For already running services:

```bash
KAFKA_TEST_BROKERS=127.0.0.1:9092 \
KAFKA_TEST_DATABASE_URL=postgres://rustzap:rustzap@127.0.0.1:5432/rustzap \
  cargo test --features external-integrations --test kafka_integration -- --ignored --nocapture
```

## Dev Simulation

```bash
curl -X POST \
  -H 'content-type: application/json' \
  http://<LAN_IP>:8167/v1/dev/companies/company_dev/simulate/inbound-text \
  -d '{"conversation_id":"conv_dev","text":"Oi"}'
```

Then read:

```bash
curl 'http://<LAN_IP>:8167/v1/companies/company_dev/conversations/conv_dev/messages?after_seq=0'
```
