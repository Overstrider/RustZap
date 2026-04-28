# 11 - API Contract, OpenAPI E Pagination

## Status
Pendente.

## Evidencia Atual
- `src/routes.rs` expõe muitas rotas da especificacao.
- `/openapi.json` existe, mas e manual e incompleto.
- `PageQuery` aceita `limit`, `cursor`, `direction`, `before_seq`, `after_seq`, `order`.
- A paginacao de varias listagens e offset simples; algumas rotas sao stubs.

## O Que Falta
- OpenAPI completa para todos os endpoints, schemas e erros.
- Paginacao consistente em todas as listagens.
- Cursor estavel e nao apenas offset quando a ordem puder mudar.
- Identificar e substituir rotas stub ou retornar `not_supported`.
- Padronizar erro em todos os caminhos.

## Plano De Implementacao
1. Escolher gerador OpenAPI compativel com Axum e Rust 2024, como `utoipa` ou `aide`.
2. Definir schemas Rust para requests/responses principais.
3. Documentar auth, scopes, erros e headers.
4. Substituir `openapi_json` manual por spec gerada ou arquivo mantido por testes.
5. Criar helper unico de pagination response.
6. Padronizar `limit default=50`, `limit max=500`.
7. Implementar cursor estavel baseado em seq/time/id onde aplicavel.
8. Revisar rotas stub: ou implementar comportamento real ou retornar `not_supported`.
9. Adicionar testes de contrato para paths obrigatorios.

## Criterios De Aceite
- `/openapi.json` lista todos os endpoints obrigatorios.
- Schemas de erro usam `{ error: { code, message, details, request_id } }`.
- Toda listagem aceita os parametros definidos e respeita max limit.
- Rotas nao implementadas nao fingem sucesso.

## Testes
- Teste snapshot ou validação estrutural de `/openapi.json`.
- Teste de `limit` default e max.
- Teste de cursor em mensagens por `after_seq`.
- Teste de erro padrao em endpoint protegido.
- Teste de rota nao suportada retornando `not_supported`.
