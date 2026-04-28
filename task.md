# RustZap - Especificacao de implementacao v1.3

Data: 2026-04-26

Este documento deve ser usado como prompt tecnico para um code agent implementar o RustZap e, no mesmo repositorio, um tester local em Next.js.

Mudancas principais da v1.2 e revisao final v1.3:

- O tester Next.js nao deve ser gerado separadamente por ChatGPT. O proprio code agent deve criar a pasta `dev-tester/` dentro do repositorio.
- O RustZap deve rodar em Podman desde o inicio.
- A sessao do `whatsapp-rust` deve usar SQLite local em volume persistente do Podman, nao Cloudflare D1.
- O Cloudflare D1 pode ser citado como o banco SQLite serverless da Cloudflare, mas nao deve ser usado para sessao WhatsApp nem como banco principal do RustZap neste desenho.
- O R2 deve ser acessado com crate Rust leve, preferencialmente `rusty_s3`, com cliente HTTP proprio como `reqwest` ou `hyper`. Nao usar `aws-sdk-s3`.
- Usar Rust edition 2024 e dependencias recentes. O code agent deve pesquisar as versoes atuais das crates no momento da implementacao.
- Kafka deve ser previsto como event bus interno de alta performance. O monolito continua sendo monolito, mas usa Kafka para absorver pico, replay e paralelismo.

## 1. Objetivo do RustZap

RustZap e um backend only de WhatsApp para multiplos SaaS consumidores.

Ele deve ser um Conversation Gateway, nao uma IA de negocio.

O RustZap deve:

- Conectar contas WhatsApp usando `whatsapp-rust`.
- Ler mensagens recebidas.
- Enviar mensagens.
- Reagir a mensagens com emoji.
- Controlar status de entrega e leitura, incluindo ack, enviado, entregue, lido, reproduzido quando suportado.
- Fixar mensagem quando suportado.
- Estrelar mensagem quando suportado.
- Expor detalhes de usuario, historico de midia, numero, nome, descricao e primeiro contato.
- Pesquisar mensagens dentro de uma conversa individual.
- Interagir com grupos de WhatsApp.
- Expor detalhes de grupos, membros, admins, midias, descricao, busca, mensagens estreladas e sair do grupo.
- Se o numero conectado for admin, permitir adicionar, aceitar, remover e alterar cargo de membros quando suportado.
- Baixar audios, imagens e documentos recebidos.
- Salvar midia temporaria no R2.
- Transcrever audio usando Groq Speech to Text.
- Disponibilizar APIs e WebSocket para que a IA do SaaS consumidor leia, entenda e interaja com o WhatsApp.
- Emitir sinais compactos de conversa alterada.
- Permitir leitura por cursor e `conversation_seq`.
- Permitir comando idempotente para envio de mensagem.
- Ter um tester local em Next.js dentro do mesmo repositorio para validar a experiencia durante o desenvolvimento.

O RustZap nao deve:

- Decidir etapa de CRM.
- Taguear lead por conta propria.
- Gerar resposta inteligente usando IA propria.
- Recomendar imovel.
- Saber regras especificas do TETOZ ou de outro SaaS.
- Escrever diretamente no banco do SaaS consumidor.
- Depender de Cloudflare Workers, D1 ou Queues para funcionar.

A IA unica fica no SaaS consumidor, por exemplo TETOZ. O RustZap fornece canal, historico, midia, transcricao e comandos.

## 2. Decisao arquitetural principal

A arquitetura correta e:

```txt
WhatsApp cliente
  -> whatsapp-rust
  -> RustZap
  -> storage de conversa/midia/transcricao
  -> sinal compacto para SaaS consumidor
  -> SaaS consumidor busca delta por cursor
  -> IA do SaaS decide
  -> SaaS envia comando idempotente para RustZap
  -> RustZap envia mensagem pelo WhatsApp
```

O RustZap deve ser dono da conversa bruta.

O SaaS consumidor deve ser dono da IA, CRM e regras de negocio.

## 3. Monolito com Kafka opcional desde o inicio

RustZap sera um monolito. Mesmo assim, ele pode usar Kafka internamente.

Kafka entra entre o ingestor de eventos WhatsApp e os workers internos.

```txt
whatsapp-rust event handler
  -> normalizer
  -> Kafka topic
  -> persistence worker
  -> media worker
  -> transcription worker
  -> dirty signal worker
  -> websocket broadcaster
  -> REST/gRPC API
```

Isso nao transforma RustZap em microservicos. Os consumers podem ser tasks dentro do proprio processo RustZap ou processos separados no futuro.

### 3.1 Quando usar Kafka

Usar Kafka se `EVENT_BUS=kafka`.

Caso contrario, permitir modo simples:

```txt
EVENT_BUS=postgres
```

ou:

```txt
EVENT_BUS=in_memory
```

Modo recomendado para producao com performance:

```txt
EVENT_BUS=kafka
```

### 3.2 Ganhos do Kafka

Kafka da ganhos em:

- Absorcao de picos: se chegam muitas mensagens ao mesmo tempo, Kafka segura o backlog.
- Backpressure: workers processam no ritmo que conseguem.
- Paralelismo: consumer groups e particoes permitem varios workers.
- Ordem por conversa: usando partition key baseada em `conversation_id`, manter ordem dos eventos daquela conversa.
- Replay: se der bug em transcricao ou persistencia, pode reprocessar eventos.
- Separacao de prioridades: topicos diferentes para autopilot, background, midia e status.
- Durabilidade: evento nao some se o processo RustZap reiniciar.
- Evolucao futura: o monolito pode virar multiplos processos sem mudar contratos.

### 3.3 Topicos Kafka recomendados

Usar prefixo configuravel:

```env
KAFKA_TOPIC_PREFIX=rustzap
```

Topicos:

```txt
rustzap.raw.inbound
rustzap.message.persisted
rustzap.media.download.requested
rustzap.media.stored
rustzap.audio.transcription.requested
rustzap.audio.transcribed
rustzap.conversation.dirty
rustzap.delivery.receipt
rustzap.channel.status
rustzap.group.event
rustzap.websocket.event
rustzap.deadletter
```

### 3.4 Partition key

Para eventos de conversa:

```txt
partition_key = {project_id}:{company_id}:{conversation_id}
```

Para eventos de canal:

```txt
partition_key = {project_id}:{company_id}:{channel_id}
```

Assim, mensagens da mesma conversa tendem a ser processadas na ordem.

### 3.5 Compactacao de dirty conversation

`rustzap.conversation.dirty` deve poder ser compactado logicamente por chave:

```txt
key = {project_id}:{company_id}:{conversation_id}
value = { max_seq, reason, priority, updated_at }
```

Mesmo que cheguem 100 mensagens na mesma conversa, o estado final e:

```txt
conv_123 dirty ate seq 541
```

Nao criar um job infinito por mensagem.

## 4. Banco de dados, SQLite e Cloudflare D1

### 4.1 Regra principal

O SQLite do `whatsapp-rust` deve ficar local na VPS, em volume persistente do Podman.

Nao usar Cloudflare D1 para sessao WhatsApp.

Motivo:

- `whatsapp-rust` usa storage SQLite como backend local de sessao e estado do protocolo.
- Esse estado deve ficar perto do processo RustZap.
- D1 nao e um arquivo SQLite local montavel no container.
- D1 e uma database serverless da Cloudflare com semantica SQL de SQLite, acessada via Workers e HTTP API.
- D1 nao substitui um arquivo SQLite usado por uma lib dentro de uma VPS.

### 4.2 O que fica em SQLite

SQLite local deve guardar apenas estado de protocolo/sessao do WhatsApp quando o adapter do `whatsapp-rust` usar SQLite.

Exemplo:

```txt
/data/rustzap/wa-sessions/{project_id}/{company_id}/{channel_id}/session.sqlite
```

Volume Podman:

```txt
./data/wa-sessions:/data/rustzap/wa-sessions:Z
```

### 4.3 O que fica no banco principal do RustZap

O RustZap precisa de banco proprio para metadados de negocio tecnico:

- projects
- companies
- api keys
- channel accounts
- contacts
- groups
- group members
- conversations
- messages
- media metadata
- transcripts
- receipts
- dirty conversation state
- idempotency keys
- audit logs
- consumer cursors

Recomendacao para producao:

```txt
Postgres
```

Pode ser Postgres em Podman na mesma VPS no inicio, ou Postgres externo.

Para MVP/dev, pode haver fallback SQLite para o banco de metadados, mas a implementacao deve prever que producao usa Postgres.

### 4.4 Variaveis de banco

```env
RUSTZAP_METADATA_DB=postgres
DATABASE_URL=postgres://rustzap:rustzap@postgres:5432/rustzap
DATABASE_MAX_CONNECTIONS=50
DATABASE_MIN_CONNECTIONS=5
```

Fallback dev:

```env
RUSTZAP_METADATA_DB=sqlite
RUSTZAP_SQLITE_PATH=/data/rustzap/app/rustzap.sqlite
```

Sessao WhatsApp:

```env
WA_SESSION_STORAGE=sqlite
WA_SESSION_SQLITE_DIR=/data/rustzap/wa-sessions
WA_SESSION_ENCRYPT_AT_REST=true
```

Nao usar:

```env
CLOUDFLARE_D1_DATABASE_ID=...
```

para o core do RustZap.

## 5. Podman

RustZap deve ser implementado pensando em execucao via Podman.

O code agent deve criar:

```txt
Containerfile
podman-compose.yml
.env.example
scripts/podman-up.sh
scripts/podman-down.sh
scripts/podman-logs.sh
scripts/migrate.sh
```

### 5.1 Servicos no podman-compose

Minimo:

```txt
rustzap
postgres
redpanda ou kafka
```

Preferencia pratica:

```txt
redpanda
```

Redpanda pode ser usado como Kafka-compatible broker para desenvolvimento e producao inicial, pois reduz complexidade operacional. Ainda assim, a aplicacao deve falar protocolo Kafka, nao API proprietaria.

### 5.2 Volumes obrigatorios

```txt
./data/rustzap:/data/rustzap:Z
./data/wa-sessions:/data/rustzap/wa-sessions:Z
./data/logs:/var/log/rustzap:Z
```

Se usar Postgres local:

```txt
./data/postgres:/var/lib/postgresql/data:Z
```

Se usar Redpanda/Kafka local:

```txt
./data/redpanda:/var/lib/redpanda/data:Z
```

### 5.3 Portas

```txt
RustZap API: 8080
RustZap WebSocket: 8080 no mesmo servidor HTTP
Postgres: 5432, apenas rede interna se possivel
Kafka/Redpanda: 9092, apenas rede interna se possivel
Dev tester Next.js: 3005
```

### 5.4 Containerfile

Requisitos:

- Multi-stage build.
- Rust stable mais recente.
- `edition = "2024"` no Cargo.toml.
- Instalar `ffmpeg` na imagem final.
- Rodar como usuario nao root.
- Criar diretorios `/data/rustzap`, `/data/rustzap/wa-sessions`, `/var/log/rustzap`.
- Healthcheck chamando `/ready`.

## 6. Rust 2024 e dependencias

O projeto deve usar Rust 2024.

Cargo.toml:

```toml
[package]
edition = "2024"
```

Criar `rust-toolchain.toml`:

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

O code agent deve pesquisar e usar as versoes recentes e estaveis das crates no momento da implementacao.

Crates esperadas, mas o agente deve validar:

```txt
axum
tokio
tower
tower-http
serde
serde_json
tracing
tracing-subscriber
sqlx ou diesel, preferencia sqlx
rusqlite se necessario para SQLite proprio
whatsapp-rust
whatsapp-rust-sqlite-storage
rusty_s3
reqwest ou hyper
rdkafka ou crate Kafka atual equivalente
uuid
time ou chrono
jsonwebtoken ou alternativa atual
argon2 ou blake3/hmac para hashing e assinatura conforme uso
secrecy
thiserror
anyhow
validator
utoipa ou aide para OpenAPI, se o agente decidir gerar docs
```

Nao fixar versoes antigas sem motivo. O code agent deve investigar compatibilidade entre Rust 2024, whatsapp-rust, storage SQLite, Kafka e Axum.

## 7. R2 e storage de midia

### 7.1 Regra obrigatoria

Nao usar `aws-sdk-s3`.

Usar preferencialmente:

```txt
rusty_s3
```

com cliente HTTP proprio:

```txt
reqwest
```

ou:

```txt
hyper
```

Motivo: reduzir dependencia pesada, evitar problemas recorrentes do AWS SDK com R2, facilitar signing e request control.

`rusty_s3` e Sans-IO. Ele assina e monta requests S3. O RustZap escolhe o cliente HTTP para enviar.

### 7.2 Variaveis R2

```env
STORAGE_PROVIDER=r2
R2_ACCOUNT_ID=xxx
R2_ENDPOINT=https://<account_id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=xxx
R2_SECRET_ACCESS_KEY=xxx
R2_BUCKET=rustzap-prod
R2_REGION=auto
R2_BASE_PREFIX=rustzap
R2_PRESIGNED_URL_TTL_SECONDS=900
R2_UPLOAD_TIMEOUT_SECONDS=120
R2_DOWNLOAD_TIMEOUT_SECONDS=120
```

Fallback dev opcional:

```env
STORAGE_PROVIDER=local_fs
LOCAL_STORAGE_DIR=/data/rustzap/local-storage
```

### 7.3 Path recomendado no R2

Nao usar telefone, nome, CPF ou identificador pessoal no path.

Temporario:

```txt
rustzap/temp/project={project_id}/company={company_id}/channel={channel_id}/conversation={conversation_id}/date={yyyy-mm-dd}/media={media_id}.{ext}
```

Quarentena para midia grande com delecao rapida:

```txt
rustzap/quarantine/project={project_id}/company={company_id}/channel={channel_id}/conversation={conversation_id}/date={yyyy-mm-dd}/media={media_id}.{ext}
```

Permanente:

```txt
rustzap/permanent/project={project_id}/company={company_id}/entity={entity_type}/entity_id={entity_id}/date={yyyy-mm-dd}/media={media_id}.{ext}
```

Outbound temporario:

```txt
rustzap/outbound-temp/project={project_id}/company={company_id}/channel={channel_id}/date={yyyy-mm-dd}/upload={upload_id}.{ext}
```

### 7.4 Retencao

Configurar lifecycle no R2 quando possivel:

```txt
rustzap/temp/ -> delete after 30 days
rustzap/quarantine/ -> delete after 1 day
rustzap/outbound-temp/ -> delete after 1 day
```

RustZap tambem deve rodar cron diario para fallback de limpeza:

```env
MEDIA_CLEANUP_CRON=0 3 * * *
MEDIA_TEMP_RETENTION_DAYS=30
MEDIA_QUICK_DELETE_AFTER_HOURS=24
```

### 7.5 Limites de midia

```env
MEDIA_TEMP_RETENTION_DAYS=30
MEDIA_QUICK_DELETE_THRESHOLD_MB=25
MEDIA_QUICK_DELETE_AFTER_HOURS=24
MEDIA_REJECT_THRESHOLD_MB=100
MEDIA_ALLOW_MANUAL_SAVE_BEFORE_QUICK_DELETE=true
MEDIA_ALLOWED_MIME_PREFIXES=image/,audio/,video/,application/pdf,application/msword,application/vnd.openxmlformats-officedocument
```

Comportamento:

- Ate `MEDIA_QUICK_DELETE_THRESHOLD_MB`: salvar em temp por 30 dias.
- Maior que `MEDIA_QUICK_DELETE_THRESHOLD_MB` e menor ou igual a `MEDIA_REJECT_THRESHOLD_MB`: salvar em quarantine e apagar rapidamente.
- Maior que `MEDIA_REJECT_THRESHOLD_MB`: rejeitar. Se o download ja tiver ocorrido por detalhe tecnico, deletar imediatamente.
- Audio acima de `GROQ_STT_MAX_AUDIO_MB`: salvar se permitido, mas nao transcrever.

## 8. Groq Speech to Text

RustZap usa Groq apenas para speech to text.

Nao usar Groq para decidir CRM, gerar resposta ou fazer classificacao de lead.

```env
GROQ_API_KEY=gsk_xxx
GROQ_STT_MODEL=whisper-large-v3-turbo
GROQ_STT_LANGUAGE=pt
GROQ_STT_RESPONSE_FORMAT=verbose_json
GROQ_STT_TIMEOUT_SECONDS=60
GROQ_STT_MAX_AUDIO_MB=25
GROQ_STT_ENABLE_PREPROCESSING=true
FFMPEG_PATH=/usr/bin/ffmpeg
```

Fluxo:

```txt
audio recebido
  -> baixar midia
  -> salvar no R2 ou local dev
  -> verificar tamanho e mime
  -> converter com ffmpeg se necessario
  -> enviar para Groq STT
  -> salvar transcript
  -> emitir evento audio.transcribed
  -> marcar conversa dirty
```

## 9. Variaveis de ambiente completas

`.env.example` deve conter:

```env
# App
RUSTZAP_ENV=development
RUSTZAP_BIND_ADDR=0.0.0.0:8080
RUSTZAP_PUBLIC_BASE_URL=http://localhost:8080
RUSTZAP_LOG_LEVEL=info
RUSTZAP_TIMEZONE=America/Sao_Paulo
RUSTZAP_DEV_MODE=true

# Auth e seguranca
RUSTZAP_ADMIN_API_KEY=dev_admin_key
RUSTZAP_INTERNAL_JWT_SECRET=dev_jwt_secret_change_me
RUSTZAP_WEBHOOK_SIGNING_SECRET=dev_webhook_secret_change_me
RUSTZAP_SECRETS_MASTER_KEY=base64-32-bytes-or-more
RUSTZAP_IDEMPOTENCY_TTL_HOURS=72

# Banco principal
RUSTZAP_METADATA_DB=postgres
DATABASE_URL=postgres://rustzap:rustzap@postgres:5432/rustzap
DATABASE_MAX_CONNECTIONS=50
DATABASE_MIN_CONNECTIONS=5

# Sessao WhatsApp
WA_SESSION_STORAGE=sqlite
WA_SESSION_SQLITE_DIR=/data/rustzap/wa-sessions
WA_SESSION_ENCRYPT_AT_REST=true
WA_QR_REFRESH_SECONDS=20
WA_CONNECT_TIMEOUT_SECONDS=60
WA_RECONNECT_MAX_BACKOFF_SECONDS=120

# Kafka
EVENT_BUS=kafka
KAFKA_BROKERS=redpanda:9092
KAFKA_CLIENT_ID=rustzap
KAFKA_TOPIC_PREFIX=rustzap
KAFKA_CONSUMER_GROUP=rustzap-main
KAFKA_ENABLE_AUTO_CREATE_TOPICS=true
KAFKA_DEFAULT_PARTITIONS=24
KAFKA_DEFAULT_REPLICATION_FACTOR=1
KAFKA_MESSAGE_RETENTION_HOURS=168
KAFKA_DEADLETTER_RETENTION_HOURS=168

# Dirty queue e debounce
DIRTY_LEASE_SECONDS=60
DIRTY_DEFAULT_DEBOUNCE_SECONDS=5
DIRTY_AUTOPILOT_DEBOUNCE_SECONDS=3
DIRTY_BACKGROUND_DEBOUNCE_SECONDS=30

# WebSocket
RUSTZAP_SIGNAL_MODE=websocket_and_kafka
RUSTZAP_WS_MAX_CONNECTIONS=10000
RUSTZAP_WS_PING_INTERVAL_SECONDS=30
RUSTZAP_WS_EVENT_LOG_RETENTION_HOURS=72

# Groq
GROQ_API_KEY=
GROQ_STT_MODEL=whisper-large-v3-turbo
GROQ_STT_LANGUAGE=pt
GROQ_STT_RESPONSE_FORMAT=verbose_json
GROQ_STT_TIMEOUT_SECONDS=60
GROQ_STT_MAX_AUDIO_MB=25
GROQ_STT_ENABLE_PREPROCESSING=true
FFMPEG_PATH=/usr/bin/ffmpeg

# Storage
STORAGE_PROVIDER=r2
R2_ACCOUNT_ID=
R2_ENDPOINT=https://<account_id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET=rustzap-dev
R2_REGION=auto
R2_BASE_PREFIX=rustzap
R2_PRESIGNED_URL_TTL_SECONDS=900
R2_UPLOAD_TIMEOUT_SECONDS=120
R2_DOWNLOAD_TIMEOUT_SECONDS=120
LOCAL_STORAGE_DIR=/data/rustzap/local-storage

# Midia
MEDIA_TEMP_RETENTION_DAYS=30
MEDIA_QUICK_DELETE_THRESHOLD_MB=25
MEDIA_QUICK_DELETE_AFTER_HOURS=24
MEDIA_REJECT_THRESHOLD_MB=100
MEDIA_ALLOW_MANUAL_SAVE_BEFORE_QUICK_DELETE=true
MEDIA_CLEANUP_CRON=0 3 * * *
MEDIA_ALLOWED_MIME_PREFIXES=image/,audio/,video/,application/pdf,application/msword,application/vnd.openxmlformats-officedocument

# Rate limit
RATE_LIMIT_REQUESTS_PER_MINUTE_PER_PROJECT=6000
RATE_LIMIT_SEND_MESSAGE_PER_MINUTE_PER_CHANNEL=120
RATE_LIMIT_MEDIA_DOWNLOADS_PER_MINUTE_PER_CHANNEL=300
RATE_LIMIT_STT_PER_MINUTE_PER_PROJECT=120

# Dev tester
DEV_TESTER_ALLOWED_ORIGIN=http://localhost:3005
DEV_SIMULATION_ENABLED=true
```

## 10. Modelo de dados minimo

O code agent deve criar migrations para Postgres e, se implementar fallback dev, SQLite.

### 10.1 projects

```txt
id
name
status
created_at
updated_at
```

### 10.2 project_api_keys

```txt
id
project_id
name
key_hash
scopes
last_used_at
created_at
revoked_at
```

### 10.3 companies

```txt
id
project_id
external_company_id
name
status
created_at
updated_at
```

### 10.4 channel_accounts

```txt
id
project_id
company_id
provider
wa_jid
phone_e164
label
status
connected_at
last_seen_at
created_at
updated_at
```

### 10.5 contacts

```txt
id
project_id
company_id
channel_account_id
wa_jid
phone_e164
push_name
display_name
profile_picture_media_id
business_description
first_contact_at
last_contact_at
created_at
updated_at
```

### 10.6 groups

```txt
id
project_id
company_id
channel_account_id
wa_jid
subject
description
owner_jid
created_at_wa
last_message_at
created_at
updated_at
```

### 10.7 group_members

```txt
group_id
contact_id
wa_jid
phone_e164
name
role
is_admin
joined_at
updated_at
```

Roles:

```txt
owner
admin
member
unknown
```

### 10.8 conversations

```txt
id
project_id
company_id
channel_account_id
type
contact_id
group_id
last_seq
last_message_at
unread_count
is_archived
is_muted
is_pinned
control_mode
created_at
updated_at
```

Conversation type:

```txt
direct
group
```

Control mode:

```txt
manual
copilot
background
autopilot
human_takeover
```

### 10.9 messages

```txt
id
project_id
company_id
conversation_id
channel_account_id
conversation_seq
wa_message_id
direction
sender_contact_id
message_type
text
quoted_message_id
status
is_starred
is_pinned
sent_by_source
sent_by_external_user_id
created_at_wa
created_at
updated_at
```

Message type:

```txt
text
image
video
audio
voice_note
document
sticker
reaction
location
contact_card
system
```

Status:

```txt
received
queued
sent_to_whatsapp
server_ack
delivered
read
played
failed
```

### 10.10 message_receipts

```txt
id
message_id
receipt_type
participant_jid
created_at_wa
created_at
```

Receipt type:

```txt
server_ack
delivered
read
played
```

### 10.11 media_objects

```txt
id
project_id
company_id
conversation_id
message_id
media_type
mime_type
original_filename
size_bytes
sha256
width
height
duration_seconds
storage_status
bucket
object_key
permanent_object_key
expires_at
saved_at
created_at
updated_at
```

Storage status:

```txt
temp
quarantine
permanent
deleted
rejected
```

### 10.12 transcripts

```txt
id
project_id
company_id
message_id
media_id
provider
model
language
text
raw_response_json
status
error_message
created_at
updated_at
```

Status:

```txt
pending
processing
completed
failed
skipped_size_limit
skipped_unsupported_type
```

### 10.13 dirty_conversations

```txt
id
project_id
company_id
conversation_id
max_seq
reason
priority
available_at
locked_until
lease_owner
created_at
updated_at
```

Unique key:

```txt
unique(project_id, company_id, conversation_id)
```

### 10.14 consumer_processing_state

```txt
id
project_id
company_id
consumer_id
conversation_id
last_processed_seq
last_processed_at
created_at
updated_at
```

### 10.15 idempotency_keys

```txt
id
project_id
company_id
key
request_hash
response_json
status
expires_at
created_at
updated_at
```

### 10.16 audit_logs

```txt
id
project_id
company_id
actor_type
actor_id
action
resource_type
resource_id
request_json
response_json
ip_address
created_at
```

## 11. REST API

Base path:

```txt
/v1/projects/{project_id}/companies/{company_id}
```

Auth:

```http
Authorization: Bearer <project_api_key>
```

Headers recomendados:

```http
X-RustZap-Project-Id: tetoz
X-RustZap-Company-Id: company_123
Idempotency-Key: unique-key-for-write-commands
```

### 11.1 Health

```http
GET /health
GET /ready
```

### 11.2 Projects e companies

Admin only:

```http
POST /v1/projects
POST /v1/projects/{project_id}/api-keys
POST /v1/projects/{project_id}/companies
GET /v1/projects/{project_id}/companies/{company_id}
```

### 11.3 Channel accounts

```http
POST /v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts
POST /v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/connect
POST /v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/disconnect
GET  /v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}
GET  /v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/qr
POST /v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/pair-code
GET  /v1/projects/{project_id}/companies/{company_id}/channels/whatsapp/accounts/{channel_id}/capabilities
```

Capabilities deve informar se a lib atual suporta:

```txt
send_text
send_media
send_reaction
pin_message
star_message
mark_read
groups_read
groups_manage
group_invite_accept
group_member_promote
group_member_demote
```

### 11.4 Contacts

```http
GET /v1/projects/{project_id}/companies/{company_id}/contacts
GET /v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}
GET /v1/projects/{project_id}/companies/{company_id}/contacts/by-phone/{phone_e164}
GET /v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}/media
GET /v1/projects/{project_id}/companies/{company_id}/contacts/{contact_id}/conversations
```

Contato deve expor:

```txt
numero
nome
push_name
display_name
descricao/business_description
primeiro contato
ultimo contato
historico de midia paginado
conversas relacionadas
```

### 11.5 Conversations

```http
GET /v1/projects/{project_id}/companies/{company_id}/conversations
GET /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}
PATCH /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}
GET /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/messages
GET /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/search
GET /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/media
GET /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/starred
POST /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/mark-read
POST /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/typing
```

Leitura por cursor:

```http
GET /messages?after_seq=536&limit=100
```

Resposta:

```json
{
  "conversation_id": "conv_123",
  "from_seq": 537,
  "to_seq": 541,
  "has_more": false,
  "messages": []
}
```

### 11.6 Messages

```http
POST /v1/projects/{project_id}/companies/{company_id}/conversations/{conversation_id}/messages
GET  /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}
POST /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/react
DELETE /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/react
POST /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/pin
DELETE /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/pin
POST /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/star
DELETE /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/star
```

Envio de texto:

```json
{
  "type": "text",
  "text": "Oi, tudo bem?",
  "quoted_message_id": null,
  "metadata": {
    "source": "tetoz_ai",
    "mode": "autopilot",
    "external_user_id": "user_123"
  }
}
```

Envio de midia:

```json
{
  "type": "document",
  "media_id": "media_123",
  "caption": "Segue o documento.",
  "filename": "contrato.pdf"
}
```

Todo POST de envio deve exigir `Idempotency-Key`.

### 11.7 Media

```http
GET  /v1/projects/{project_id}/companies/{company_id}/media/{media_id}
GET  /v1/projects/{project_id}/companies/{company_id}/media/{media_id}/download-url
POST /v1/projects/{project_id}/companies/{company_id}/media/{media_id}/save
DELETE /v1/projects/{project_id}/companies/{company_id}/media/{media_id}
POST /v1/projects/{project_id}/companies/{company_id}/media/upload-outbound
```

Salvar em definitivo:

```json
{
  "entity_type": "lead",
  "entity_id": "lead_123",
  "folder": "documentos_do_lead",
  "filename": "rg_maria.pdf",
  "metadata": {
    "source": "crm_ui",
    "saved_by_user_id": "user_789"
  }
}
```

### 11.8 Transcripts

```http
GET  /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/transcript
POST /v1/projects/{project_id}/companies/{company_id}/messages/{message_id}/transcribe
```

### 11.9 Groups

```http
GET    /v1/projects/{project_id}/companies/{company_id}/groups
GET    /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}
GET    /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members
GET    /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/media
GET    /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/starred
GET    /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/search
POST   /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/exit
POST   /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members
DELETE /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}
POST   /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}/promote
POST   /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/members/{contact_id}/demote
POST   /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/accept
POST   /v1/projects/{project_id}/companies/{company_id}/groups/{group_id}/join-requests/{request_id}/reject
```

Todos os endpoints de admin de grupo devem checar se o canal conectado tem permissao admin no grupo.

### 11.10 Dirty conversations

```http
GET  /v1/projects/{project_id}/companies/{company_id}/dirty-conversations
POST /v1/projects/{project_id}/companies/{company_id}/dirty-conversations/{conversation_id}/ack
```

Query:

```txt
?consumer_id=tetoz_ai&limit=100&mode=autopilot
```

Resposta:

```json
{
  "items": [
    {
      "conversation_id": "conv_123",
      "max_seq": 541,
      "reason": "new_message",
      "priority": 100,
      "available_at": "2026-04-26T18:00:00Z"
    }
  ]
}
```

Ack:

```json
{
  "consumer_id": "tetoz_ai",
  "processed_until_seq": 541
}
```

### 11.11 Dev simulation endpoints

Dev only. Ativar apenas se:

```env
RUSTZAP_DEV_MODE=true
DEV_SIMULATION_ENABLED=true
```

Endpoints:

```http
POST /v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-text
POST /v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-audio
POST /v1/dev/projects/{project_id}/companies/{company_id}/simulate/inbound-image
POST /v1/dev/projects/{project_id}/companies/{company_id}/simulate/receipt
POST /v1/dev/projects/{project_id}/companies/{company_id}/simulate/qr-rotate
POST /v1/dev/projects/{project_id}/companies/{company_id}/simulate/group-event
POST /v1/dev/projects/{project_id}/companies/{company_id}/simulate/reset
```

Esses endpoints existem para o `dev-tester/` validar UI e fluxos sem depender de um WhatsApp real.

## 12. WebSocket

Endpoint:

```txt
/ws/v1
```

Auth:

```http
Authorization: Bearer <project_api_key>
```

Ou token curto:

```txt
/ws/v1?token=short_lived_ws_token
```

### 12.1 Subscribe

Cliente envia:

```json
{
  "type": "subscribe",
  "project_id": "tetoz",
  "company_id": "company_123",
  "topics": [
    "channel.*",
    "conversation.dirty",
    "message.*",
    "media.*",
    "transcript.*",
    "group.*"
  ]
}
```

### 12.2 Eventos emitidos

```txt
channel.qr
channel.status
conversation.dirty
message.received
message.sent
message.receipt
message.reaction
message.pinned
message.starred
media.stored
media.deleted
transcript.completed
group.updated
group.member.added
group.member.removed
group.member.promoted
group.member.demoted
```

### 12.3 Evento conversation.dirty

Payload pequeno:

```json
{
  "type": "conversation.dirty",
  "event_seq": 1002,
  "project_id": "tetoz",
  "company_id": "company_123",
  "conversation_id": "conv_789",
  "to_seq": 541,
  "reason": "new_message",
  "priority": 100
}
```

O WebSocket nao deve ser a fonte da verdade. Se cair, o SaaS consumidor recupera usando REST por cursor.

## 13. Fluxos essenciais

### 13.1 Inbound texto

```txt
WhatsApp envia evento
  -> whatsapp-rust recebe
  -> RustZap normaliza
  -> gera conversation_seq
  -> salva mensagem
  -> publica Kafka message.persisted
  -> marca conversation.dirty
  -> emite WebSocket conversation.dirty
```

### 13.2 Inbound audio

```txt
WhatsApp envia audio
  -> baixar media
  -> validar tamanho
  -> salvar R2 temp ou quarantine
  -> publicar audio.transcription.requested
  -> worker transcreve via Groq
  -> salvar transcript
  -> marcar conversation.dirty
```

### 13.3 SaaS consumidor processando IA

```txt
SaaS recebe conversation.dirty
  -> consulta consumer_processing_state
  -> GET messages?after_seq=last_processed_seq
  -> monta contexto proprio de CRM
  -> IA do SaaS decide
  -> se for responder, POST messages com Idempotency-Key
  -> se processou, ACK dirty conversation
```

### 13.4 Outbound mensagem

```txt
SaaS chama POST /messages
  -> RustZap valida idempotency
  -> salva outbound queued
  -> envia via whatsapp-rust
  -> atualiza status sent_to_whatsapp/server_ack
  -> emite receipts depois
```

## 14. Dev tester em Next.js

O code agent deve criar a pasta:

```txt
dev-tester/
```

Nao gerar essa pasta fora do repositorio. O tester deve ser criado pelo mesmo code agent que cria RustZap para garantir compatibilidade entre endpoints, payloads e WebSocket.

### 14.1 Stack do dev tester

Usar:

```txt
Next.js mais recente
TypeScript
App Router
React
Tailwind opcional
pnpm ou npm, escolher e documentar
```

### 14.2 Configuracao

Arquivo:

```txt
dev-tester/.env.local.example
```

Conteudo:

```env
NEXT_PUBLIC_RUSTZAP_MOCK=true
RUSTZAP_BASE_URL=http://localhost:8080
RUSTZAP_PROJECT_ID=tetoz
RUSTZAP_COMPANY_ID=company_dev
RUSTZAP_PROJECT_API_KEY=dev_project_key
NEXT_PUBLIC_RUSTZAP_WS_URL=ws://localhost:8080/ws/v1
```

### 14.3 Modos

O tester deve suportar:

```txt
mock mode
real localhost mode
```

Mock mode simula tudo dentro do browser/app Next.

Real localhost mode chama RustZap em `localhost:8080`.

### 14.4 Telas obrigatorias

Criar telas simples, mas funcionais:

```txt
/dashboard
/channel
/chat
/groups
/media
/events
```

### 14.5 Funcionalidades obrigatorias do tester

O tester deve permitir validar:

- Criar project e company dev.
- Criar channel WhatsApp dev.
- Iniciar connect.
- Ver QR code atual.
- QR code rotacionando a cada `WA_QR_REFRESH_SECONDS`.
- Status do canal mudando: disconnected, waiting_qr, connecting, connected.
- Lista de conversas.
- Abrir conversa.
- Receber texto simulado.
- Receber audio simulado.
- Ver transcript do audio.
- Receber imagem/documento simulado.
- Enviar mensagem outbound.
- Ver status de mensagem: queued, sent, delivered, read.
- Reagir a mensagem com emoji.
- Fixar e desafixar mensagem.
- Estrelar e desestrelar mensagem.
- Marcar conversa como lida.
- Ver media de contato/conversa.
- Clicar em salvar midia permanente.
- Pesquisar mensagens dentro da conversa.
- Ver grupos.
- Ver membros do grupo.
- Ver quem e admin.
- Simular evento de entrada/saida de membro.
- Sair de grupo em modo real se suportado.
- Ver stream de eventos WebSocket em uma tela `events`.
- Reconectar WebSocket automaticamente.
- Mostrar latencia media dos eventos.

### 14.6 QR code

A tela de QR deve mostrar claramente:

```txt
qr_code_text
expires_at
contador regressivo
status da conexao
```

O code agent deve implementar rotacao rapida no modo dev, pois problemas de QR sao muito visuais e so aparecem bem usando interface.

### 14.7 Proxy interno do Next

Para evitar CORS em dev, o tester pode ter route handlers:

```txt
dev-tester/app/api/rustzap/[...path]/route.ts
```

Esses handlers repassam chamadas para `RUSTZAP_BASE_URL`.

## 15. Cloudflare alem do R2

Obrigatorio:

```txt
R2
```

Recomendado:

```txt
Cloudflare DNS/proxy
Cloudflare WAF
Cloudflare Rate Limiting
Cloudflare Tunnel
Cloudflare Access/Zero Trust para endpoints admin e ambiente dev
```

Nao necessario agora:

```txt
Cloudflare D1
Cloudflare Queues
Durable Objects
Workers
```

Motivo:

- O monolito roda na VPS/Podman.
- Kafka/Redpanda ja cobre fila/event bus.
- D1 nao e o SQLite local necessario para sessao do whatsapp-rust.
- Durable Objects faz mais sentido para WebSocket stateful na borda, mas o RustZap pode manter WebSocket no proprio servidor.

## 16. Observabilidade

Implementar:

```txt
structured logs com tracing
request id
correlation id
project_id/company_id nos logs
metricas por endpoint
metricas por channel
metricas por topico Kafka
metricas de STT
metricas de upload/download R2
metricas de WebSocket
metricas de dirty queue
```

Endpoints:

```http
GET /metrics
GET /debug/kafka
GET /debug/dirty
GET /debug/channels
```

`/debug/*` apenas admin/dev.

## 17. Regras de seguranca

- API keys devem ser armazenadas com hash, nao texto puro.
- Secrets devem ser criptografados ou armazenados fora do banco quando possivel.
- Nao expor R2 object key publico sem presigned URL.
- Nunca colocar telefone, nome ou dados pessoais no path do R2.
- Endpoints admin precisam de scope `admin`.
- Endpoints de grupos precisam de `groups:read` ou `groups:manage`.
- Endpoints de envio precisam de `messages:send`.
- Todo comando de escrita deve aceitar `Idempotency-Key`.
- WebSocket precisa de auth e controle de escopo.
- Logs nao devem conter texto completo de mensagens por padrao em producao.

## 18. Ordem sugerida de implementacao

1. Criar workspace Rust com edition 2024.
2. Criar API Axum com health e auth.
3. Criar migrations Postgres.
4. Criar Podman setup com Postgres e Redpanda/Kafka.
5. Criar storage R2 usando `rusty_s3` e cliente HTTP proprio.
6. Criar session storage SQLite local do whatsapp-rust em volume.
7. Criar channel account e QR flow.
8. Criar message ingestion e persistencia.
9. Criar conversation_seq por conversa.
10. Criar dirty conversation state.
11. Criar WebSocket event broadcaster.
12. Criar endpoints de leitura por cursor.
13. Criar envio idempotente de mensagem.
14. Criar media download/upload e salvar permanente.
15. Criar Groq STT.
16. Criar endpoints de contato e busca.
17. Criar endpoints de grupo.
18. Criar dev simulation endpoints.
19. Criar `dev-tester/` Next.js.
20. Criar testes unitarios e de integracao.
21. Criar documentacao de operacao.

## 19. Criterios de aceite

O projeto so esta aceitavel se:

- Roda com Podman usando `.env`.
- Sobe RustZap, Postgres e Kafka/Redpanda.
- Usa Rust edition 2024.
- Nao usa `aws-sdk-s3`.
- Usa `rusty_s3` ou justificativa tecnica forte para alternativa Rust leve.
- Usa SQLite local persistente para sessao do whatsapp-rust.
- Nao usa Cloudflare D1 como sessao WhatsApp.
- Expõe QR code e eventos de rotacao.
- Recebe e salva mensagens.
- Envia mensagens com idempotencia.
- Gera `conversation_seq` monotonicamente por conversa.
- Permite leitura por cursor.
- Marca conversation.dirty sem empurrar payload pesado.
- Salva midia em R2 temp/quarantine/permanent.
- Limpa midia temporaria por cron.
- Transcreve audio com Groq STT.
- Disponibiliza WebSocket de eventos.
- Tem `dev-tester/` criado pelo code agent.
- O dev tester valida QR, chat, media, receipts, reacao, pin, star, grupos e eventos.
- Tem logs e metricas basicas.
- Tem docs de uso local e producao.

## 20. Fontes tecnicas para o code agent conferir

O code agent deve conferir a documentacao mais recente antes de escolher versoes e detalhes finais:

```txt
Rust 2024 Edition Guide:
https://doc.rust-lang.org/edition-guide/rust-2024/index.html

whatsapp-rust:
https://github.com/oxidezap/whatsapp-rust

whatsapp-rust SQLite storage:
https://docs.rs/whatsapp-rust-sqlite-storage

Cloudflare D1:
https://developers.cloudflare.com/d1/

Cloudflare R2:
https://developers.cloudflare.com/r2/

rusty_s3:
https://docs.rs/rusty-s3

Kafka design:
https://kafka.apache.org/documentation/

Podman docs:
https://docs.podman.io/

Groq Speech to Text:
https://console.groq.com/docs/speech-to-text
```


## 21. Revisao final v1.3 - ajustes obrigatorios antes de codar

Esta secao complementa a especificacao anterior. O code agent deve tratar estes pontos como requisitos de implementacao, nao como ideias opcionais, exceto onde estiver explicitamente marcado como opcional.

### 21.1 Contrato de sinal para o SaaS consumidor

O RustZap nao deve depender de um unico meio de entrega de sinal. Implementar o conceito de `consumer signal adapter`.

Variavel principal:

```env
CONSUMER_SIGNAL_MODE=polling
```

Valores aceitos:

```txt
polling
websocket
webhook
kafka
websocket_and_polling
webhook_and_polling
```

Regra:

```txt
O RustZap sempre persiste a mensagem e o dirty state.
O sinal externo e apenas uma notificacao compacta.
Se o sinal falhar, o SaaS consumidor deve conseguir recuperar usando REST por cursor.
```

O sinal externo nunca deve carregar payload pesado de mensagem, midia ou transcript completo. Enviar apenas:

```json
{
  "event_id": "evt_...",
  "type": "conversation.dirty",
  "project_id": "tetoz",
  "company_id": "company_123",
  "channel_id": "channel_123",
  "conversation_id": "conv_123",
  "to_seq": 541,
  "reason": "new_message",
  "priority": 100,
  "occurred_at": "2026-04-26T18:00:00Z"
}
```

### 21.2 Webhook compacto opcional

Mesmo que WebSocket e polling sejam suficientes no inicio, prever webhook compacto porque outros SaaS consumidores podem preferir callback HTTP.

Endpoints de configuracao:

```http
POST /v1/projects/{project_id}/companies/{company_id}/consumer-callbacks
GET  /v1/projects/{project_id}/companies/{company_id}/consumer-callbacks
PATCH /v1/projects/{project_id}/companies/{company_id}/consumer-callbacks/{callback_id}
DELETE /v1/projects/{project_id}/companies/{company_id}/consumer-callbacks/{callback_id}
```

Campos:

```txt
id
project_id
company_id
url
secret_ref ou encrypted_secret
enabled
events
max_batch_size
timeout_seconds
created_at
updated_at
```

Variaveis:

```env
WEBHOOK_DELIVERY_ENABLED=false
WEBHOOK_TIMEOUT_SECONDS=10
WEBHOOK_MAX_BATCH_SIZE=100
WEBHOOK_MAX_RETRIES=8
WEBHOOK_RETRY_BASE_SECONDS=5
WEBHOOK_RETRY_MAX_SECONDS=3600
WEBHOOK_SIGNING_HEADER=X-RustZap-Signature
WEBHOOK_TIMESTAMP_HEADER=X-RustZap-Timestamp
WEBHOOK_EVENT_ID_HEADER=X-RustZap-Event-Id
```

Assinatura recomendada:

```txt
HMAC-SHA256(secret, timestamp + "." + raw_body)
```

Semantica:

```txt
at-least-once delivery
SaaS consumidor precisa deduplicar por event_id
RustZap precisa registrar tentativas e erros de delivery
Webhook nao substitui REST por cursor
```

### 21.3 Modelo padrao de evento interno

Todo evento interno, seja Kafka, WebSocket, webhook ou log, deve seguir uma estrutura comum.

```json
{
  "event_id": "evt_uuidv7",
  "event_type": "message.received",
  "project_id": "tetoz",
  "company_id": "company_123",
  "channel_id": "channel_123",
  "conversation_id": "conv_123",
  "message_id": "msg_123",
  "conversation_seq": 541,
  "trace_id": "trace_...",
  "causation_id": "evt_original",
  "correlation_id": "corr_...",
  "occurred_at": "2026-04-26T18:00:00Z",
  "produced_at": "2026-04-26T18:00:01Z",
  "payload": {}
}
```

Gerar `event_id` estavel por publicacao. Usar UUIDv7 ou ULID para facilitar ordenacao temporal.

### 21.4 Semantica de entrega e deduplicacao

Assumir `at-least-once` em tudo.

Isso significa:

```txt
Um evento pode chegar mais de uma vez.
Um comando pode ser reexecutado por retry.
Um worker pode cair depois de processar e antes de confirmar.
```

Requisitos:

- Inbound deve deduplicar por `channel_account_id + wa_message_id + direction`.
- Em grupo, se necessario, incluir `participant_jid` na chave de deduplicacao.
- Outbound deve deduplicar por `project_id + company_id + idempotency_key`.
- Se a mesma `Idempotency-Key` for reenviada com o mesmo body, retornar a resposta anterior.
- Se a mesma `Idempotency-Key` for reenviada com body diferente, retornar `409 Conflict`.
- Persistencia de mensagem deve ter unique index contra duplicacao.
- Aplicacao de receipt deve ser idempotente.
- Aplicacao de reaction/pin/star deve ser idempotente quando a API permitir.

### 21.5 Ack correto de dirty conversations

O endpoint de dirty conversations precisa evitar race condition.

Ao listar dirty conversations, retornar um `lease_token` ou `ack_token`:

```json
{
  "conversation_id": "conv_123",
  "max_seq": 541,
  "lease_token": "lease_abc",
  "locked_until": "2026-04-26T18:01:00Z"
}
```

O ACK deve conter:

```json
{
  "consumer_id": "tetoz_ai",
  "processed_until_seq": 541,
  "lease_token": "lease_abc"
}
```

Regra de ACK:

```txt
Se processed_until_seq >= max_seq atual, limpar dirty para esse consumer.
Se max_seq atual aumentou enquanto o consumidor processava, nao limpar. Atualizar consumer_processing_state e manter dirty para o restante.
Se lease_token estiver vencido ou errado, retornar 409 ou 423.
```

A tabela `consumer_processing_state` deve ser por consumidor, porque diferentes consumidores podem processar a mesma conversa para finalidades diferentes.

Exemplos:

```txt
tetoz_ai_autopilot
tetoz_ai_background
tetoz_crm_sync
dev_tester
```

### 21.6 Geracao de `conversation_seq`

`conversation_seq` deve ser monotonicamente crescente dentro de cada conversa.

Requisitos:

- Garantir atomicidade em transacao.
- Criar unique index `unique(conversation_id, conversation_seq)`.
- Criar unique index para mensagem externa quando disponivel.
- Evitar race quando duas mensagens chegam quase ao mesmo tempo na mesma conversa.

Implementacoes aceitas:

```txt
1. Lock transacional na linha da conversa e incremento de last_seq.
2. Postgres advisory lock por conversation_id.
3. Tabela separada conversation_sequences com update atomic returning.
```

O code agent deve escolher a melhor solucao para Postgres e documentar.

### 21.7 Indices minimos de banco

Adicionar indices alem dos campos ja listados.

Obrigatorios:

```txt
messages(project_id, company_id, conversation_id, conversation_seq)
messages(channel_account_id, wa_message_id)
messages(project_id, company_id, created_at_wa)
messages(project_id, company_id, message_type)
conversations(project_id, company_id, last_message_at)
contacts(project_id, company_id, phone_e164)
groups(project_id, company_id, wa_jid)
media_objects(project_id, company_id, message_id)
media_objects(storage_status, expires_at)
dirty_conversations(project_id, company_id, priority, available_at)
idempotency_keys(project_id, company_id, key)
```

Para busca textual:

```txt
Postgres full-text search com configuracao adequada para portugues quando possivel.
Opcionalmente pg_trgm para busca aproximada.
```

### 21.8 Paginacao, ordenacao e schema de erro

Todos os endpoints de listagem devem aceitar:

```txt
limit
cursor
direction
before_seq
after_seq
order
```

Limites:

```txt
limit default = 50
limit max = 500
```

Schema de erro padrao:

```json
{
  "error": {
    "code": "not_supported",
    "message": "Feature not supported by current WhatsApp provider",
    "details": {},
    "request_id": "req_..."
  }
}
```

Codigos esperados:

```txt
bad_request
unauthorized
forbidden
not_found
conflict
idempotency_conflict
not_supported
rate_limited
payload_too_large
provider_error
internal_error
```

### 21.9 Capability-first para WhatsApp

Varias features de WhatsApp podem depender da lib, do tipo de conta, do estado da sessao ou de mudancas do protocolo.

Regra:

```txt
Nunca fingir suporte.
Se nao suportar, retornar not_supported.
A UI do SaaS consumidor deve conseguir se adaptar via /capabilities.
```

`/capabilities` deve retornar:

```json
{
  "provider": "whatsapp-rust",
  "features": {
    "send_text": { "supported": true },
    "send_media": { "supported": true },
    "send_reaction": { "supported": true },
    "pin_message": { "supported": false, "reason": "provider_not_supported" },
    "star_message": { "supported": true },
    "mark_read": { "supported": true },
    "read_receipts": { "supported": true, "guaranteed": false },
    "groups_manage": { "supported": true, "requires_admin": true }
  }
}
```

Observacao importante:

```txt
Checks, double-checks, read receipts e played receipts devem refletir o que o WhatsApp/provider entregar. Nao garantir check azul se o contato desativou confirmacao ou se o provider nao receber esse evento.
```

### 21.10 Scopes e permissoes de API

Definir scopes granulares.

```txt
admin:*
projects:write
companies:write
channels:read
channels:write
contacts:read
conversations:read
messages:read
messages:send
messages:manage
media:read
media:write
transcripts:read
transcripts:write
groups:read
groups:manage
dirty:read
dirty:ack
websocket:connect
dev:simulate
```

API keys podem ser:

```txt
project-scoped
company-scoped
admin global
```

Toda rota deve validar:

```txt
project_id
company_id
scope
status da key
revoked_at
rate limit
```

### 21.11 Rate limit por camada

Rate limit precisa existir por:

```txt
project_id
company_id
channel_id
api_key_id
ip_address
endpoint family
```

Envio de mensagem precisa de rate limit independente:

```txt
messages:send por channel_id
messages:send por conversation_id
media upload por project_id
STT por project_id
admin endpoints por api_key_id
```

Adicionar variaveis:

```env
RATE_LIMIT_SEND_MESSAGE_PER_MINUTE_PER_CONVERSATION=20
RATE_LIMIT_ADMIN_REQUESTS_PER_MINUTE=300
RATE_LIMIT_WS_SUBSCRIPTIONS_PER_CONNECTION=1000
```

### 21.12 Media: staging local, validacao real e seguranca

Antes de enviar para R2, usar staging local temporario:

```env
MEDIA_LOCAL_TEMP_DIR=/data/rustzap/tmp-media
MEDIA_LOCAL_TEMP_MAX_GB=20
MEDIA_SNIFF_MAGIC_BYTES=true
MEDIA_ENABLE_VIRUS_SCAN=false
MEDIA_VIRUS_SCAN_PROVIDER=none
```

Regras:

- Nao confiar apenas em MIME informado pelo WhatsApp.
- Calcular `sha256` antes ou durante upload.
- Se o arquivo for rejeitado, deletar staging local e qualquer objeto R2 parcial.
- Kafka nunca deve carregar bytes de midia, apenas metadados e IDs.
- Presigned URL deve ter TTL curto.
- `media/save` deve copiar para path permanente e registrar auditoria.
- Manter o objeto temporario ate expirar, salvo se a implementacao decidir mover/copy-delete com seguranca.

Para producao, prever AV scan plugavel, principalmente para documentos baixados por usuarios.

### 21.13 Privacidade, LGPD e retencao por company

Adicionar configuracao por company:

```txt
message_retention_days
media_temp_retention_days
media_permanent_retention_policy
transcript_retention_days
allow_message_text_in_logs
allow_transcript_storage
```

Adicionar endpoints de privacidade:

```http
POST   /v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}/export
DELETE /v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}
POST   /v1/projects/{project_id}/companies/{company_id}/privacy/contacts/{contact_id}/anonymize
```

Comportamento esperado:

```txt
exportar dados tecnicos e mensagens do contato quando permitido
apagar ou anonimizar dados pessoais quando solicitado
registrar audit log da operacao
nao apagar logs de seguranca quando houver obrigacao legitima de retencao, mas remover PII quando possivel
```

### 21.14 Criptografia e secrets

`WA_SESSION_ENCRYPT_AT_REST=true` precisa ser implementavel ou documentado corretamente.

Opcoes aceitas:

```txt
1. Criptografia de disco/volume no host.
2. Criptografia de secrets e tokens, com SQLite protegido por permissao de arquivo.
3. SQLite encryption se a crate/adapter suportar de forma segura.
```

Nao prometer criptografia do arquivo SQLite se a stack escolhida nao suportar.

Adicionar:

```env
RUSTZAP_SECRET_ENCRYPTION_PROVIDER=local_master_key
RUSTZAP_SECRET_KEY_ROTATION_ENABLED=false
RUSTZAP_LOG_REDACT_MESSAGE_TEXT=true
RUSTZAP_LOG_REDACT_PHONE=true
```

### 21.15 Kafka em producao

Kafka/Redpanda deve ser configurado com cuidado.

Variaveis adicionais:

```env
KAFKA_COMPRESSION_TYPE=zstd
KAFKA_PRODUCER_LINGER_MS=5
KAFKA_PRODUCER_BATCH_SIZE_BYTES=65536
KAFKA_MESSAGE_MAX_BYTES=1048576
KAFKA_CONSUMER_MAX_POLL_RECORDS=500
KAFKA_RETRY_TOPIC_SUFFIX=retry
KAFKA_DEADLETTER_TOPIC_SUFFIX=deadletter
```

Regras:

- Nao publicar midia bruta no Kafka.
- Em dev, replication factor 1 e aceitavel.
- Em producao multi-node, replication factor recomendado deve ser maior que 1.
- Topicos de retry devem conter `retry_count`, `first_failed_at`, `last_failed_at` e `error_code`.
- Deadletter deve ser consultavel por endpoint admin.

### 21.16 Health, readiness e graceful shutdown

`/health` deve ser liveness simples:

```txt
processo esta de pe
```

`/ready` deve validar:

```txt
banco principal
Kafka/Redpanda, se EVENT_BUS=kafka
storage local/R2 basico
migrations aplicadas
```

Graceful shutdown:

```txt
parar de aceitar novas conexoes
fechar WebSockets com codigo apropriado
aguardar requests em andamento
finalizar consumers Kafka com commit seguro
liberar leases de dirty conversations
fechar conexoes do whatsapp-rust corretamente quando aplicavel
```

### 21.17 Dev tester nao entra na imagem de producao

O `dev-tester/` deve existir no mesmo repositorio, mas nao deve ir na imagem final de producao do RustZap.

Podman deve ter profiles ou compose services separados:

```txt
rustzap
postgres
redpanda
dev-tester
```

O dev tester deve poder rodar com:

```bash
cd dev-tester
npm install
npm run dev
```

ou via Podman profile dev, se o code agent implementar.

### 21.18 OpenAPI e colecao de teste

Gerar documentacao de API para facilitar integracao com o TETOZ.

Requisitos:

```txt
OpenAPI JSON em /openapi.json
Swagger ou Scalar opcional em /docs apenas dev/admin
colecao Bruno, Insomnia ou Postman opcional no repo
```

Se usar `utoipa`, `aide` ou alternativa, o code agent deve escolher a que melhor combina com Axum e Rust 2024.

### 21.19 Estrutura de repo sugerida

O code agent pode ajustar, mas deve mirar algo proximo de:

```txt
rustzap/
  Cargo.toml
  rust-toolchain.toml
  Containerfile
  podman-compose.yml
  .env.example
  crates/
    rustzap-api/
    rustzap-core/
    rustzap-whatsapp/
    rustzap-storage/
    rustzap-eventbus/
    rustzap-media/
    rustzap-transcription/
  migrations/
  scripts/
  dev-tester/
  docs/
```

Se preferir um crate unico no inicio, ainda assim organizar modulos internos com fronteiras claras.

### 21.20 Testes obrigatorios

Adicionar testes unitarios e de integracao para:

```txt
idempotency key
conversation_seq concorrente
dirty ack com max_seq novo
media limit/reject/quarantine
R2 object key builder sem PII
webhook signature
API scopes
message cursor pagination
Kafka event serialization
Groq STT mocked
WebSocket subscribe/auth
```

O code agent deve mockar WhatsApp, R2, Groq e Kafka quando necessario para testes rapidos.

### 21.21 Criterios de aceite adicionais da v1.3

Adicionar aos criterios anteriores:

- Possui adapter de sinal configuravel: polling, WebSocket e webhook compacto pelo menos previsto.
- Eventos possuem schema padrao com `event_id`, `trace_id` e `correlation_id`.
- Dirty ACK nao perde mensagens novas por race condition.
- `conversation_seq` e seguro sob concorrencia.
- API retorna erro padronizado.
- Endpoints de listagem tem paginacao consistente.
- Capability map retorna `not_supported` quando necessario.
- Kafka nao transporta midia bruta.
- Dev tester nao e empacotado na imagem final de producao.
- Existem testes para idempotencia, cursor, dirty ack e media limits.
