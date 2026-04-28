# 13 - Privacy, LGPD E Retention

## Status
Pendente.

## Evidencia Atual
- Migration `companies` tem campos de retencao e privacidade.
- Rotas privacy existem em `src/routes.rs`, mas retornam respostas fixas.
- `audit_logs` existe na migration.
- Nao ha job real de retencao.

## O Que Falta
- Export real de dados tecnicos e mensagens do contato.
- Delete/anonymize real com tratamento de PII.
- Audit log para operacoes de privacidade.
- Retencao por company para mensagens, midia e transcripts.
- Politica para logs de seguranca que nao podem ser apagados integralmente.

## Plano De Implementacao
1. Criar repository de company privacy settings.
2. Implementar `privacy/contacts/{contact_id}/export` coletando contato, conversas, mensagens, media metadata e transcripts permitidos.
3. Implementar delete/anonymize removendo ou redigindo PII em contatos, mensagens, media metadata e transcripts.
4. Preservar logs de seguranca quando necessario, removendo PII quando possivel.
5. Registrar cada operacao em `audit_logs`.
6. Criar job de retencao por company:
   - mensagens acima de `message_retention_days`;
   - media temp acima de `media_temp_retention_days`;
   - transcripts acima de `transcript_retention_days`.
7. Respeitar `allow_transcript_storage` e `allow_message_text_in_logs`.
8. Documentar comportamento em `docs/security.md`.

## Criterios De Aceite
- Export retorna dados do contato de forma estruturada.
- Anonymize remove telefone/nome/identificadores pessoais do contato e referencias derivadas.
- Delete remove ou redige dados conforme politica.
- Audit log registra ator, recurso e resultado.
- Retention job respeita configuracao por company.

## Testes
- Teste de export de contato com mensagens e media.
- Teste de anonymize removendo PII.
- Teste de delete mantendo audit log redigido.
- Teste de retention por company.
- Teste de `allow_transcript_storage=false`.
