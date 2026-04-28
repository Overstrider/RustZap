# 04 - WhatsApp Pair Code

## Status
Pendente.

## Evidencia Atual
- Endpoint existe em `src/routes.rs`.
- A resposta atual retorna `pair_code: "123-456"` fixo.
- `src/whatsapp.rs` nao expoe chamada real de pair-code.

## O Que Falta
- Verificar se `whatsapp-rust` suporta pair-code para a versao atual.
- Chamar provider real quando suportado.
- Retornar erro padrao `not_supported` quando nao suportado.
- Refletir suporte no endpoint `/capabilities`.

## Plano De Implementacao
1. Confirmar na API local do crate `whatsapp-rust` qual metodo existe para pair-code.
2. Adicionar metodo `request_pair_code` em `WhatsappManager`.
3. Fazer o endpoint validar canal, escopo e status antes de chamar provider.
4. Se provider nao suportar, retornar `ApiError::NotSupported`.
5. Atualizar `capabilities()` com feature `pair_code`.
6. Remover o valor fixo `123-456`.

## Criterios De Aceite
- Endpoint nunca retorna codigo falso.
- Quando suportado, retorna codigo real e expiracao.
- Quando nao suportado, retorna erro padronizado com code `not_supported`.
- Dev tester consegue se adaptar via capabilities.

## Testes
- Teste unitario de endpoint retornando `not_supported` quando adapter nao suporta.
- Teste com provider mock retornando pair-code.
- Teste de auth/scope `channels:write`.
