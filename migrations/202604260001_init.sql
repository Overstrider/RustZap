CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE projects (
  id text PRIMARY KEY,
  name text NOT NULL,
  status text NOT NULL DEFAULT 'active',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE project_api_keys (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id text NOT NULL REFERENCES projects(id),
  company_id text,
  name text NOT NULL,
  key_hash text NOT NULL,
  scopes text[] NOT NULL,
  last_used_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz
);

CREATE TABLE companies (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  external_company_id text,
  name text NOT NULL,
  status text NOT NULL DEFAULT 'active',
  message_retention_days integer NOT NULL DEFAULT 365,
  media_temp_retention_days integer NOT NULL DEFAULT 30,
  media_permanent_retention_policy text NOT NULL DEFAULT 'manual',
  transcript_retention_days integer NOT NULL DEFAULT 365,
  allow_message_text_in_logs boolean NOT NULL DEFAULT false,
  allow_transcript_storage boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(project_id, external_company_id)
);

CREATE TABLE channel_accounts (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  provider text NOT NULL,
  wa_jid text,
  phone_e164 text,
  label text,
  status text NOT NULL DEFAULT 'disconnected',
  connected_at timestamptz,
  last_seen_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE contacts (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  channel_account_id text NOT NULL REFERENCES channel_accounts(id),
  wa_jid text,
  phone_e164 text,
  push_name text,
  display_name text,
  profile_picture_media_id text,
  business_description text,
  first_contact_at timestamptz,
  last_contact_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE groups (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  channel_account_id text NOT NULL REFERENCES channel_accounts(id),
  wa_jid text NOT NULL,
  subject text,
  description text,
  owner_jid text,
  created_at_wa timestamptz,
  last_message_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE group_members (
  group_id text NOT NULL REFERENCES groups(id),
  contact_id text NOT NULL REFERENCES contacts(id),
  wa_jid text,
  phone_e164 text,
  name text,
  role text NOT NULL DEFAULT 'unknown',
  is_admin boolean NOT NULL DEFAULT false,
  joined_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(group_id, contact_id)
);

CREATE TABLE conversations (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  channel_account_id text NOT NULL REFERENCES channel_accounts(id),
  type text NOT NULL,
  contact_id text REFERENCES contacts(id),
  group_id text REFERENCES groups(id),
  last_seq bigint NOT NULL DEFAULT 0,
  last_message_at timestamptz,
  unread_count bigint NOT NULL DEFAULT 0,
  is_archived boolean NOT NULL DEFAULT false,
  is_muted boolean NOT NULL DEFAULT false,
  is_pinned boolean NOT NULL DEFAULT false,
  control_mode text NOT NULL DEFAULT 'manual',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE messages (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  conversation_id text NOT NULL REFERENCES conversations(id),
  channel_account_id text NOT NULL REFERENCES channel_accounts(id),
  conversation_seq bigint NOT NULL,
  wa_message_id text,
  participant_jid text,
  direction text NOT NULL,
  sender_contact_id text REFERENCES contacts(id),
  message_type text NOT NULL,
  text text,
  text_search tsvector GENERATED ALWAYS AS (to_tsvector('portuguese', coalesce(text, ''))) STORED,
  quoted_message_id text,
  status text NOT NULL,
  is_starred boolean NOT NULL DEFAULT false,
  is_pinned boolean NOT NULL DEFAULT false,
  sent_by_source text,
  sent_by_external_user_id text,
  created_at_wa timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(conversation_id, conversation_seq)
);

CREATE TABLE message_receipts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  message_id text NOT NULL REFERENCES messages(id),
  receipt_type text NOT NULL,
  participant_jid text,
  created_at_wa timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(message_id, receipt_type, participant_jid)
);

CREATE TABLE media_objects (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  conversation_id text REFERENCES conversations(id),
  message_id text REFERENCES messages(id),
  media_type text NOT NULL,
  mime_type text NOT NULL,
  original_filename text,
  size_bytes bigint NOT NULL,
  sha256 text NOT NULL,
  width integer,
  height integer,
  duration_seconds numeric,
  storage_status text NOT NULL,
  bucket text,
  object_key text,
  permanent_object_key text,
  expires_at timestamptz,
  saved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE transcripts (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  message_id text NOT NULL REFERENCES messages(id),
  media_id text REFERENCES media_objects(id),
  provider text NOT NULL,
  model text NOT NULL,
  language text NOT NULL,
  text text,
  raw_response_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE dirty_conversations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  conversation_id text NOT NULL REFERENCES conversations(id),
  max_seq bigint NOT NULL,
  reason text NOT NULL,
  priority integer NOT NULL DEFAULT 100,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_until timestamptz,
  lease_owner text,
  lease_token text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(project_id, company_id, conversation_id)
);

CREATE TABLE consumer_processing_state (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  consumer_id text NOT NULL,
  conversation_id text NOT NULL REFERENCES conversations(id),
  last_processed_seq bigint NOT NULL DEFAULT 0,
  last_processed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(project_id, company_id, consumer_id, conversation_id)
);

CREATE TABLE idempotency_keys (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  key text NOT NULL,
  request_hash text NOT NULL,
  response_json jsonb,
  status text NOT NULL,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(project_id, company_id, key)
);

CREATE TABLE audit_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  actor_type text NOT NULL,
  actor_id text,
  action text NOT NULL,
  resource_type text NOT NULL,
  resource_id text,
  request_json jsonb,
  response_json jsonb,
  ip_address inet,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE consumer_callbacks (
  id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  url text NOT NULL,
  encrypted_secret text,
  enabled boolean NOT NULL DEFAULT true,
  events text[] NOT NULL DEFAULT ARRAY['conversation.dirty'],
  max_batch_size integer NOT NULL DEFAULT 100,
  timeout_seconds integer NOT NULL DEFAULT 10,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE webhook_delivery_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  callback_id text NOT NULL REFERENCES consumer_callbacks(id),
  event_id text NOT NULL,
  attempt integer NOT NULL,
  status text NOT NULL,
  http_status integer,
  error_message text,
  first_failed_at timestamptz,
  last_failed_at timestamptz,
  next_retry_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_messages_conversation_seq ON messages(project_id, company_id, conversation_id, conversation_seq);
CREATE INDEX idx_messages_channel_wa ON messages(channel_account_id, wa_message_id);
CREATE UNIQUE INDEX idx_messages_external_dedupe ON messages(channel_account_id, wa_message_id, direction, coalesce(participant_jid, '')) WHERE wa_message_id IS NOT NULL;
CREATE UNIQUE INDEX idx_receipts_idempotent ON message_receipts(message_id, receipt_type, coalesce(participant_jid, ''));
CREATE INDEX idx_messages_created_at_wa ON messages(project_id, company_id, created_at_wa);
CREATE INDEX idx_messages_type ON messages(project_id, company_id, message_type);
CREATE INDEX idx_messages_text_search ON messages USING gin(text_search);
CREATE INDEX idx_conversations_last_message ON conversations(project_id, company_id, last_message_at);
CREATE INDEX idx_contacts_phone ON contacts(project_id, company_id, phone_e164);
CREATE INDEX idx_groups_wa_jid ON groups(project_id, company_id, wa_jid);
CREATE INDEX idx_media_message ON media_objects(project_id, company_id, message_id);
CREATE INDEX idx_media_expiry ON media_objects(storage_status, expires_at);
CREATE INDEX idx_dirty_priority ON dirty_conversations(project_id, company_id, priority, available_at);
CREATE INDEX idx_idempotency_key ON idempotency_keys(project_id, company_id, key);
