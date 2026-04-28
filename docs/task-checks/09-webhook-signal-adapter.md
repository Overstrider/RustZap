# 09 - Webhook Signal Adapter

## Status
Pendente.

## Evidencia Atual
- Migrations criam `consumer_callbacks` e `webhook_delivery_attempts`.
- `src/routes.rs` tem endpoints de callbacks, mas eles retornam dados fixos/em memoria.
- `security::webhook_signature` ja calcula HMAC-SHA256.
- Nao ha worker de entrega webhook.

## O Que Falta
- CRUD real de callbacks persistido.
- Worker de entrega at-least-once.
- Assinatura HMAC nos headers configurados.
- Batching, timeout, retry e deadletter/registro de tentativas.
- Deduplicacao por `event_id` no contrato externo.

## Plano De Implementacao
1. Expandir `AppConfig` com todas as variaveis `WEBHOOK_*`.
2. Implementar repository para `consumer_callbacks`.
3. Substituir endpoints stub por CRUD real.
4. Criar `ConsumerSignalAdapter` com modos `polling`, `websocket`, `webhook`, `kafka`, combinacoes com polling.
5. Criar worker webhook que consome eventos compactos.
6. Enviar batch ate `WEBHOOK_MAX_BATCH_SIZE`.
7. Assinar `timestamp + "." + raw_body` usando secret do callback.
8. Registrar cada tentativa em `webhook_delivery_attempts`.
9. Implementar backoff ate `WEBHOOK_MAX_RETRIES`.
10. Garantir que falha de webhook nao impede leitura REST por cursor.

## Criterios De Aceite
- Callback criado aparece em GET e pode ser alterado/desativado.
- Webhook recebe somente eventos compactos.
- Headers de assinatura/timestamp/event_id sao enviados.
- Falhas geram tentativas com backoff e registro consultavel.
- Polling continua sendo fonte de recuperacao.

## Testes
- Teste de assinatura HMAC.
- Teste CRUD de callback.
- Teste de entrega com servidor HTTP mock.
- Teste de retry/backoff.
- Teste de callback desativado nao recebe evento.
