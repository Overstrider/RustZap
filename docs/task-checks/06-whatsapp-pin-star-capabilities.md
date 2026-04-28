# 06 - WhatsApp Pin/Star/Capabilities

## Status
Pendente.

## Evidencia Atual
- `src/whatsapp.rs` declara `pin_message` como nao suportado.
- Rotas `pin`, `star` e deletes existem em `src/routes.rs`.
- `src/state.rs` altera `is_pinned` e `is_starred` apenas localmente.

## O Que Falta
- Capability-first estrito.
- Chamar provider real para pin/star quando suportado.
- Retornar `not_supported` quando nao suportado.
- Evitar que a API finja sucesso de operacao nao suportada.

## Plano De Implementacao
1. Verificar suporte real de pin/star no `whatsapp-rust`.
2. Atualizar `CapabilitiesResponse` com razoes precisas.
3. Criar metodos `pin_message` e `star_message` no adapter, se suportado.
4. Alterar rotas para consultar capabilities antes de mutar estado local.
5. Quando suportado, chamar provider e atualizar estado local apenas apos sucesso.
6. Quando nao suportado, retornar `ApiError::NotSupported`.
7. Garantir idempotencia: repetir pin/star no mesmo estado nao deve falhar.

## Criterios De Aceite
- API nao retorna sucesso falso para pin/star.
- `/capabilities` reflete exatamente suporte atual.
- Dev tester pode ocultar/desabilitar controles nao suportados.
- Estado local permanece consistente apos falha do provider.

## Testes
- Teste `pin_message` nao suportado retorna `not_supported`.
- Teste `star_message` com provider mock.
- Teste de operacao idempotente repetida.
- Teste de erro provider nao altera estado local.
