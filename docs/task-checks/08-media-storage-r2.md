# 08 - Media Storage R2

## Status
Pendente.

## Evidencia Atual
- `src/media.rs` classifica limites e gera object keys.
- `src/storage.rs` faz upload basico via `rusty-s3` e `reqwest`.
- `download-url` em `src/routes.rs` retorna URL publica/dev, nao presigned URL real.
- Nao ha cleanup diario implementado.

## O Que Falta
- Staging local em `MEDIA_LOCAL_TEMP_DIR`.
- Sniff de magic bytes quando `MEDIA_SNIFF_MAGIC_BYTES=true`.
- Presigned GET URL com TTL curto.
- Copia/move para path permanente em `media/save`.
- Delete real de midia temporaria/quarantine expirada.
- Limpeza de staging local e objetos parciais.
- AV scan plugavel para producao.

## Plano De Implementacao
1. Expandir config de media local, TTL e AV scan.
2. Criar `MediaStorage` com implementacoes `LocalFsStorage` e `R2Storage`.
3. Gravar uploads/downloads primeiro no staging local.
4. Validar tamanho, MIME informado e magic bytes.
5. Calcular `sha256` antes/durante upload.
6. Implementar `put_object`, `presigned_get`, `copy_object` e `delete_object` para R2.
7. Implementar `media/save` copiando para path permanente e registrando auditoria.
8. Criar job diario de cleanup de temp/quarantine/outbound-temp expirados.
9. Garantir que rejeicao apaga staging e objeto parcial.

## Criterios De Aceite
- `download-url` retorna presigned URL real com TTL.
- `media/save` cria `permanent_object_key`.
- Midia rejeitada nao permanece no staging nem no R2.
- Cleanup remove objetos expirados.
- Paths nao contem telefone, nome ou PII.

## Testes
- Teste de object key sem PII.
- Teste de presigned URL.
- Teste de save permanente com storage mock.
- Teste de cleanup expirado.
- Teste de magic bytes divergente do MIME informado.
