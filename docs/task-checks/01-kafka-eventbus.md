# 01 - Kafka Event Bus

## Status
Implementado no runtime principal com broker real, outbox duravel transacional, inbox idempotente e DLQ persistente.

## Evidencia Atual
- `podman-compose.yml` sobe Redpanda e expõe Kafka em `9092`.
- `.env.example` e `.env.production.example` declaram variaveis Kafka.
- `Containerfile` compila com `--features external-integrations`.
- `src/eventbus.rs` cria topicos, serializa eventos compactos, publica via `rdkafka`, valida readiness real, aplica partition key por conversa/canal, drena `event_outbox` em background, calcula retry/backoff, publica/persiste DLQ e fornece helper de consumer com commit somente depois de processamento duravel.
- `src/db.rs` persiste snapshot e outbox na mesma transacao, faz claim de outbox com lock/lease, marca published/failed/deadlettered, guarda `event_inbox` por consumer group e offset, lista deadletters e reenvia DLQ para o outbox.
- `src/state.rs` publica todos os eventos internos no event bus alem do broadcast local; em modo Postgres+Kafka, a publicacao real sai do outbox transacional, nao de uma fila em memoria. O handler real do WhatsApp enfileira `whatsapp.raw.inbound`, e a persistencia/normalizacao ocorre no worker Kafka.
- `src/workers.rs` inicia consumers internos no proprio monolito para `raw-inbound`, `outbound-send` e `audio-transcription`, todos com inbox idempotente, commit seguro e DLQ persistida em erro.
- Rotas de envio e transcricao agora aceitam comando assincrono (`202 Accepted`) e deixam o dispatch/STT para os workers Kafka quando `EVENT_BUS=kafka`.
- `src/routes.rs` usa readiness real do event bus e expõe `/debug/kafka`, `/debug/kafka/deadletters` e `POST /debug/kafka/deadletters/{deadletter_id}/replay`.
- `migrations/202604280001_kafka_eventbus.sql` cria tabelas operacionais de outbox, inbox e deadletters com indices de claim, replay e offsets.
- `tests/kafka_integration.rs` cobre publicacao em broker real e fluxo AppState -> Postgres outbox -> Redpanda quando `KAFKA_TEST_BROKERS` e `KAFKA_TEST_DATABASE_URL` estao definidos.
- `scripts/test-kafka.sh` sobe Redpanda e Postgres reais via Podman e executa os testes ignorados de integracao.

## Limite Atual
- Kafka esta real e duravel: broker, producer, topicos, outbox, inbox, retry, DLQ, readiness, debug, replay e workers internos.
- O caminho principal esta Kafka-first para eventos reais do WhatsApp, envio outbound e transcricao de audio. WebSocket e dirty signals continuam acoplados ao `AppState`, mas agora recebem os eventos produzidos pelos workers. Download/decrypt de midia inbound real ainda depende de expor metadados/bytes pelo adapter `whatsapp-rust`.

## Plano De Implementacao
1. Criar modulo de event bus com `EventBusHandle` e implementacoes locais/Kafka.
2. Ler `KAFKA_BROKERS`, `KAFKA_TOPIC_PREFIX`, producer config, retry/deadletter suffixes e consumer group em `AppConfig`.
3. Implementar serializacao de `CommonEvent` para JSON, usando `event_id`, `event_type`, `trace_id`, `correlation_id` e `occurred_at`.
4. Implementar escolha de topic por `event_type`.
5. Implementar partition key:
   - Eventos de conversa: `{project_id}:{company_id}:{conversation_id}`.
   - Eventos de canal: `{project_id}:{company_id}:{channel_id}`.
6. Publicar eventos via event bus alem do broadcast websocket local.
7. Persistir eventos no `event_outbox` na mesma transacao do snapshot Postgres.
8. Drenar o outbox em background com claim/lease e publish real no Kafka.
9. Implementar helper de consumer com inbox idempotente e commit seguro apenas depois de processamento.
10. Implementar retry com `retry_count`, `first_failed_at`, `last_failed_at`, `next_attempt_at`, source offset e `error_code`.
11. Implementar deadletter consultavel e replay admin.
12. Atualizar `/ready` para checar conexao Kafka quando `EVENT_BUS=kafka`.

## Criterios De Aceite
- `EVENT_BUS=kafka` publica eventos compactos no Redpanda.
- Eventos de uma mesma conversa usam a mesma partition key.
- Falhas de publicacao/codec geram retry e depois deadletter persistente.
- Eventos persistidos no Postgres entram no outbox transacional antes de serem publicados.
- Consumers podem registrar inbox idempotente e so commitar offset apos sucesso, duplicata ou falha duravel.
- Deadletters podem ser consultadas e reenfileiradas por endpoint admin.
- `/ready` falha se Kafka estiver indisponivel em modo Kafka.
- Nenhum evento Kafka contem bytes de arquivo ou payload bruto de midia.

## Testes
- `cargo test`
- `cargo test --features external-integrations`
- `cargo test eventbus`
- Teste unitario de topic mapping por `event_type`.
- Teste unitario de partition key por conversa/canal.
- Teste de serializacao garantindo ausencia de campo `bytes`.
- Teste de readiness com Kafka mock indisponivel.
- Teste de retry/deadletter/backoff com source offset.
- Teste de decisao de commit de consumer.
- Teste de modo outbox duravel sem publicacao direta em memoria.
- `./scripts/test-kafka.sh` para testes ignorados contra Redpanda e Postgres reais.
