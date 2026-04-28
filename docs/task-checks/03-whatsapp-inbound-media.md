# 03 - WhatsApp Inbound Media

## Status
Pendente.

## Evidencia Atual
- `src/whatsapp.rs` mapeia eventos de mensagem recebida.
- `src/state.rs` tem `receive_inbound_media`, mas ele e fluxo de simulacao/dev.
- Nao ha download/decrypt real de midia recebida do WhatsApp antes de salvar metadata.

## O Que Falta
- Detectar mensagens de imagem, audio, voice note, video e documento no evento real.
- Baixar/decriptar bytes da midia usando APIs do `whatsapp-rust`/`wacore`.
- Passar midia pelo pipeline real de validacao, staging, storage e metadata.
- Publicar eventos `media.download.requested`, `media.stored` e `audio.transcription.requested`.
- Nao colocar bytes de midia em Kafka/WebSocket.

## Plano De Implementacao
1. Expandir `map_message_event` para extrair metadados de midia do protocolo WhatsApp.
2. Criar um tipo interno `InboundMediaDescriptor` com IDs, tipo, mime, filename, tamanho e referencias de download.
3. No handler de mensagem real, salvar mensagem primeiro e publicar request de download de midia.
4. Criar worker de media download com acesso ao client ativo do canal.
5. Baixar/decriptar bytes para `MEDIA_LOCAL_TEMP_DIR`.
6. Calcular `sha256` durante ou logo apos o download.
7. Classificar temp/quarantine/rejected conforme limites.
8. Salvar em R2/local storage e criar `media_objects`.
9. Associar `media_id` a mensagem.
10. Se for audio e permitido, publicar request de STT.

## Criterios De Aceite
- Mensagem inbound com imagem cria `message` e `media_object`.
- Mensagem inbound com audio cria `media_object` e request de STT.
- Midia rejeitada nao permanece no staging e nao fica parcialmente no R2.
- Eventos externos seguem payload compacto.
- Conversa fica dirty depois da midia ser persistida.

## Testes
- `cargo test inbound_media`
- Teste com WhatsApp media downloader mock.
- Teste de imagem temp.
- Teste de audio pedindo STT.
- Teste de arquivo acima de `MEDIA_REJECT_THRESHOLD_MB`.
- Teste garantindo que eventos nao contem bytes.
