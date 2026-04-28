# 02 - Groq Speech To Text

## Status
Pendente.

## Evidencia Atual
- `.env.example` declara variaveis `GROQ_*` e `FFMPEG_PATH`.
- `src/transcription.rs` retorna uma transcricao simulada com provider `groq`.
- `src/state.rs` cria transcript mock para audio recebido por endpoint dev.

## O Que Falta
- Cliente HTTP real para Groq Speech to Text.
- Respeitar `GROQ_STT_MODEL`, `GROQ_STT_LANGUAGE`, `GROQ_STT_RESPONSE_FORMAT`, timeout e max MB.
- Preprocessamento com ffmpeg quando habilitado.
- Estados `pending`, `processing`, `completed`, `failed`, `skipped_size_limit`, `skipped_unsupported_type`.
- Worker assincrono acionado por evento `audio.transcription.requested`.
- Evento `audio.transcribed` e `transcript.completed`.

## Plano De Implementacao
1. Expandir `AppConfig` com todas as variaveis Groq e ffmpeg.
2. Criar `GroqSttClient` em `src/transcription.rs` com `reqwest`.
3. Implementar upload multipart para endpoint de audio da Groq.
4. Validar tamanho do audio contra `GROQ_STT_MAX_AUDIO_MB`.
5. Validar mime/type para transcrever apenas audio suportado.
6. Criar fluxo de transcript:
   - Inserir `pending`.
   - Atualizar para `processing`.
   - Chamar Groq.
   - Salvar `completed` com texto e `raw_response_json`, ou `failed`.
7. Se `GROQ_STT_ENABLE_PREPROCESSING=true`, executar ffmpeg em staging local antes do upload.
8. Marcar conversation dirty depois de transcript concluido.
9. Manter modo dev/mock somente quando explicitamente configurado.

## Criterios De Aceite
- Audio abaixo do limite e com MIME suportado gera transcript real via Groq.
- Audio acima do limite recebe status `skipped_size_limit`.
- Falha HTTP/timeout vira transcript `failed` sem derrubar worker.
- Transcript concluido emite evento compacto e marca dirty.
- Groq nao e usado para CRM, classificacao ou resposta inteligente.

## Testes
- `cargo test transcription`
- Teste com Groq client mock retornando verbose JSON.
- Teste de timeout.
- Teste de audio acima do limite.
- Teste de MIME nao suportado.
- Teste de dirty/evento apos transcript concluido.
