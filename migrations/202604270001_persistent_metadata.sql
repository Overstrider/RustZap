CREATE TABLE IF NOT EXISTS metadata_snapshots (
  id text PRIMARY KEY,
  state_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE conversations ADD COLUMN IF NOT EXISTS display_name text;
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS display_phone text;
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS avatar_url text;
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS profile_picture_url text;

ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender_display_name text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_id text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_url text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS thumbnail_url text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS mime_type text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS error_message text;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS reaction text;

ALTER TABLE media_objects ADD COLUMN IF NOT EXISTS public_url text;
ALTER TABLE media_objects ADD COLUMN IF NOT EXISTS thumbnail_url text;

ALTER TABLE idempotency_keys ADD COLUMN IF NOT EXISTS message_id text;

CREATE TABLE IF NOT EXISTS dirty_conversation_leases (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id text NOT NULL REFERENCES projects(id),
  company_id text NOT NULL REFERENCES companies(id),
  consumer_id text NOT NULL,
  conversation_id text NOT NULL REFERENCES conversations(id),
  lease_token text NOT NULL,
  locked_until timestamptz NOT NULL,
  max_seq bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(project_id, company_id, consumer_id, conversation_id)
);

CREATE INDEX IF NOT EXISTS idx_metadata_snapshots_updated ON metadata_snapshots(updated_at);
CREATE INDEX IF NOT EXISTS idx_dirty_leases_expiry ON dirty_conversation_leases(project_id, company_id, consumer_id, locked_until);
CREATE INDEX IF NOT EXISTS idx_messages_media_id ON messages(project_id, company_id, media_id);
