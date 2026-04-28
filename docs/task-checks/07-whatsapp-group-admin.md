# 07 - WhatsApp Group Admin

## Status
Pendente.

## Evidencia Atual
- Rotas de grupo existem em `src/routes.rs`.
- Leitura de metadata de grupo usa `client.groups().get_metadata`.
- Comandos de sair/adicionar/remover/promover/demover/accept/reject retornam JSON fixo.

## O Que Falta
- Validar se o canal conectado e admin antes de comandos admin.
- Chamar APIs reais de grupo do provider.
- Atualizar metadata/membros apos sucesso.
- Retornar `not_supported` para acoes sem suporte.
- Registrar audit log de operacoes de grupo.

## Plano De Implementacao
1. Mapear APIs reais de grupo disponiveis no `whatsapp-rust`.
2. Criar metodos de adapter: `exit_group`, `add_member`, `remove_member`, `promote_member`, `demote_member`, `accept_join_request`, `reject_join_request`.
3. Antes de comandos admin, carregar metadata e checar papel do proprio canal.
4. Validar escopo `groups:manage`.
5. Chamar provider real e tratar erros como `provider_error` ou `not_supported`.
6. Atualizar `groups` e `group_members` apos sucesso ou forcar refresh.
7. Emitir eventos compactos `group.member.*` e `group.updated`.
8. Registrar audit log.

## Criterios De Aceite
- Acoes admin falham com `forbidden` quando canal nao e admin.
- Acoes nao suportadas retornam `not_supported`.
- Estado local de membros reflete a operacao apos sucesso.
- Eventos de grupo nao carregam payload pesado.

## Testes
- Testes com provider mock para cada comando.
- Teste canal nao admin.
- Teste escopo ausente.
- Teste refresh de membros apos promote/demote.
