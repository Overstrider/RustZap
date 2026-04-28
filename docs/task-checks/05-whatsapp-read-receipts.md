# 05 - WhatsApp Read Receipts

## Status
Pendente.

## Evidencia Atual
- `src/state.rs` implementa `mark_read` alterando apenas `unread_count` local.
- Receipts recebidos do WhatsApp sao mapeados e aplicados por `wa_message_id`.
- Nao ha envio real de read receipt ao provider.

## O Que Falta
- Enviar read/mark-read real via `whatsapp-rust` quando suportado.
- Aplicar status local de forma idempotente.
- Respeitar capabilities e limitações do WhatsApp.
- Nao prometer read receipt garantido quando provider/contato nao entregar.

## Plano De Implementacao
1. Confirmar metodo de read receipt no `whatsapp-rust`.
2. Adicionar capability `mark_read` com `guaranteed: false` quando aplicavel.
3. Criar metodo `mark_read` no `WhatsappManager`.
4. No endpoint, carregar conversa e mensagens inbound pendentes.
5. Chamar provider com IDs do WhatsApp quando o canal estiver conectado.
6. Se provider nao suportar, retornar `not_supported` ou aplicar apenas estado local se isso for explicitamente definido como fallback dev.
7. Persistir receipt local idempotente em `message_receipts`.
8. Emitir evento `message.receipt` compacto.

## Criterios De Aceite
- `mark-read` chama provider real quando suportado.
- Repetir `mark-read` nao duplica receipts.
- Endpoint nao garante check azul quando WhatsApp nao entregar.
- Conversa fica com `unread_count=0` apenas depois da operacao local aceita.

## Testes
- Teste com provider mock validando chamada de read receipt.
- Teste idempotente de receipt duplicado.
- Teste `not_supported`.
- Teste de conversa inexistente.
