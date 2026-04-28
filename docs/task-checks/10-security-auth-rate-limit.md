# 10 - Security, Auth E Rate Limit

## Status
Pendente.

## Evidencia Atual
- `src/security.rs` aceita tokens fixos de env e monta scopes em memoria.
- Migration `project_api_keys` tem `key_hash`, `scopes`, `revoked_at`.
- Nao ha uso real da tabela de API keys no authorization path.
- Variaveis de rate limit existem nos envs, mas nao ha limiter real.

## O Que Falta
- API keys persistidas com hash, nao texto puro.
- Validacao por `project_id`, `company_id`, status, scopes e `revoked_at`.
- Rate limit por project, company, channel, api_key, IP e familia de endpoint.
- Redacao de logs por ambiente/policy.
- Criptografia/secret management coerente para tokens sensiveis.

## Plano De Implementacao
1. Criar repository para `project_api_keys`.
2. Gerar API key apenas uma vez e persistir hash seguro.
3. No auth middleware, buscar key por hash e validar status/revocation.
4. Validar tenant path contra escopo da key.
5. Implementar scopes granulares conforme `task.md`.
6. Criar rate limiter compartilhado em memoria para MVP, com interface para Redis/futuro se necessario.
7. Aplicar limites por familia: admin, send message, media upload, STT, websocket subscription.
8. Adicionar request id/correlation id nos logs.
9. Redigir texto de mensagens e telefones quando flags exigirem.

## Criterios De Aceite
- Nenhuma API key nova e armazenada em texto puro.
- Key revogada retorna `unauthorized` ou `forbidden`.
- Scope ausente retorna `forbidden`.
- Rate limit excedido retorna `rate_limited`.
- Logs de producao nao imprimem texto completo de mensagens por padrao.

## Testes
- Teste de hash/lookup de API key.
- Teste de key revogada.
- Teste de scope por endpoint.
- Teste de tenant mismatch.
- Teste de rate limit por canal/conversa.
- Teste de redacao de telefone/texto.
