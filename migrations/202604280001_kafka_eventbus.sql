CREATE TABLE IF NOT EXISTS event_outbox (
  event_id text PRIMARY KEY,
  event_type text NOT NULL,
  topic text NOT NULL,
  partition_key text NOT NULL,
  payload_json jsonb NOT NULL,
  headers_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'queued',
  attempt_count integer NOT NULL DEFAULT 0,
  kafka_partition integer,
  kafka_offset bigint,
  locked_by text,
  locked_until timestamptz,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error text,
  available_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS event_inbox (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id text NOT NULL,
  consumer_group text NOT NULL,
  topic text NOT NULL,
  partition integer NOT NULL,
  offset_value bigint NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  payload_json jsonb NOT NULL,
  UNIQUE(consumer_group, event_id),
  UNIQUE(consumer_group, topic, partition, offset_value)
);

CREATE TABLE IF NOT EXISTS kafka_deadletters (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id text NOT NULL,
  event_type text NOT NULL,
  source_topic text,
  original_topic text,
  original_partition integer,
  original_offset bigint,
  partition_key text NOT NULL,
  retry_count integer NOT NULL,
  error_code text NOT NULL,
  payload_json jsonb NOT NULL,
  headers_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  first_failed_at timestamptz NOT NULL,
  last_failed_at timestamptz NOT NULL,
  replayed_at timestamptz,
  replayed_by text,
  resolved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_status_available
  ON event_outbox(status, next_attempt_at, available_at);

CREATE INDEX IF NOT EXISTS idx_event_outbox_lock
  ON event_outbox(locked_until)
  WHERE locked_until IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_outbox_topic_created
  ON event_outbox(topic, created_at);

CREATE INDEX IF NOT EXISTS idx_event_inbox_processed
  ON event_inbox(consumer_group, processed_at);

CREATE INDEX IF NOT EXISTS idx_kafka_deadletters_created
  ON kafka_deadletters(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_kafka_deadletters_event
  ON kafka_deadletters(event_id);
