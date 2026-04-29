use std::{path::Path, time::Duration as StdDuration};

use anyhow::{Context, bail};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx_core::{query::query, query_scalar::query_scalar, row::Row};
use sqlx_postgres::{PgPool, PgPoolOptions, Postgres};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    config::{AppConfig, KafkaConfig},
    eventbus::{partition_key, topic_for_event},
    models::{CommonEvent, MediaObject, Message, Transcript, WebhookDeliveryAttempt},
    state::{
        ContactRecord, DirtyLeaseRecord, DirtyRecord, GroupMemberRecord, GroupRecord,
        PersistedStore,
    },
};

#[derive(Clone)]
pub struct MetadataDb {
    pool: PgPool,
    kafka: KafkaConfig,
}

#[derive(Debug, Clone)]
pub struct OutboxRecord {
    pub event_id: String,
    pub topic: String,
    pub partition_key: String,
    pub payload_json: Value,
    pub attempt_count: i32,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct KafkaDeadLetterRecord {
    pub id: String,
    pub event_id: String,
    pub event_type: String,
    pub source_topic: Option<String>,
    pub original_topic: Option<String>,
    pub original_partition: Option<i32>,
    pub original_offset: Option<i64>,
    pub partition_key: String,
    pub retry_count: i32,
    pub error_code: String,
    pub payload_json: Value,
    pub first_failed_at: String,
    pub last_failed_at: String,
    pub replayed_at: Option<String>,
    pub replayed_by: Option<String>,
    pub resolved_at: Option<String>,
    pub created_at: String,
}

impl MetadataDb {
    pub async fn connect(config: &AppConfig) -> anyhow::Result<Self> {
        let Some(database_url) = config.database_url.as_deref() else {
            bail!("DATABASE_URL is required when RUSTZAP_METADATA_DB=postgres");
        };
        let pool = PgPoolOptions::new()
            .min_connections(config.database_min_connections)
            .max_connections(config.database_max_connections)
            .acquire_timeout(StdDuration::from_secs(10))
            .connect(database_url)
            .await
            .context("failed to connect to metadata Postgres")?;
        Ok(Self {
            pool,
            kafka: config.kafka.clone(),
        })
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        let migrator = sqlx_core::migrate::Migrator::new(Path::new("./migrations"))
            .await
            .context("failed to load migrations")?;
        migrator
            .run(&self.pool)
            .await
            .context("failed to apply metadata migrations")
    }

    pub async fn check_ready(&self) -> anyhow::Result<()> {
        query_scalar::<Postgres, i64>("SELECT 1::BIGINT")
            .fetch_one(&self.pool)
            .await
            .context("metadata Postgres readiness query failed")?;
        Ok(())
    }

    pub async fn insert_event_outbox(
        &self,
        event: &CommonEvent,
        topic: &str,
        partition_key: &str,
    ) -> anyhow::Result<()> {
        let payload_json = serde_json::to_value(event)?;
        query::<Postgres>(
            r#"
            INSERT INTO event_outbox (
              event_id, event_type, topic, partition_key, payload_json, status, available_at, next_attempt_at
            )
            VALUES ($1, $2, $3, $4, $5, 'queued', now(), now())
            ON CONFLICT (event_id) DO UPDATE
              SET event_type = EXCLUDED.event_type,
                  topic = EXCLUDED.topic,
                  partition_key = EXCLUDED.partition_key,
                  payload_json = EXCLUDED.payload_json,
                  status = CASE
                    WHEN event_outbox.status = 'published' THEN event_outbox.status
                    ELSE EXCLUDED.status
                  END,
                  updated_at = now()
            "#,
        )
        .bind(&event.event_id)
        .bind(&event.event_type)
        .bind(topic)
        .bind(partition_key)
        .bind(payload_json)
        .execute(&self.pool)
        .await
        .context("failed to insert Kafka outbox event")?;
        Ok(())
    }

    pub async fn claim_event_outbox_batch(
        &self,
        worker_id: &str,
        limit: i64,
        lock_seconds: i64,
    ) -> anyhow::Result<Vec<OutboxRecord>> {
        let rows = query::<Postgres>(
            r#"
            WITH next_events AS (
              SELECT event_id
                FROM event_outbox
               WHERE (
                      (status IN ('queued', 'failed') AND next_attempt_at <= now())
                   OR (status = 'publishing' AND locked_until < now())
                 )
                 AND (locked_until IS NULL OR locked_until < now())
               ORDER BY available_at, created_at
               LIMIT $1
               FOR UPDATE SKIP LOCKED
            )
            UPDATE event_outbox outbox
               SET status = 'publishing',
                   locked_by = $2,
                   locked_until = now() + ($3::text || ' seconds')::interval,
                   updated_at = now()
              FROM next_events
             WHERE outbox.event_id = next_events.event_id
            RETURNING outbox.event_id,
                      outbox.topic,
                      outbox.partition_key,
                      outbox.payload_json,
                      outbox.attempt_count
            "#,
        )
        .bind(limit)
        .bind(worker_id)
        .bind(lock_seconds.max(1))
        .fetch_all(&self.pool)
        .await
        .context("failed to claim Kafka outbox events")?;

        rows.into_iter()
            .map(|row| {
                Ok(OutboxRecord {
                    event_id: row.try_get("event_id")?,
                    topic: row.try_get("topic")?,
                    partition_key: row.try_get("partition_key")?,
                    payload_json: row.try_get("payload_json")?,
                    attempt_count: row.try_get("attempt_count")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx_core::Error>>()
            .context("failed to decode Kafka outbox rows")
    }

    pub(crate) async fn mark_event_outbox_published(
        &self,
        event_id: &str,
        partition: i32,
        offset: i64,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            UPDATE event_outbox
               SET status = 'published',
                   attempt_count = attempt_count + 1,
                   kafka_partition = $2,
                   kafka_offset = $3,
                   last_error = NULL,
                   published_at = now(),
                   updated_at = now()
             WHERE event_id = $1
            "#,
        )
        .bind(event_id)
        .bind(partition)
        .bind(offset)
        .execute(&self.pool)
        .await
        .context("failed to mark Kafka outbox event published")?;
        Ok(())
    }

    pub(crate) async fn mark_event_outbox_failed(
        &self,
        event_id: &str,
        error: &str,
        retry_after_seconds: i64,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            UPDATE event_outbox
               SET status = 'failed',
                   attempt_count = attempt_count + 1,
                   last_error = $2,
                   locked_by = NULL,
                   locked_until = NULL,
                   next_attempt_at = now() + ($3::text || ' seconds')::interval,
                   updated_at = now()
             WHERE event_id = $1
            "#,
        )
        .bind(event_id)
        .bind(error)
        .bind(retry_after_seconds.max(1))
        .execute(&self.pool)
        .await
        .context("failed to mark Kafka outbox event failed")?;
        Ok(())
    }

    pub(crate) async fn mark_event_outbox_deadlettered(
        &self,
        event_id: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            UPDATE event_outbox
               SET status = 'deadlettered',
                   attempt_count = attempt_count + 1,
                   last_error = $2,
                   locked_by = NULL,
                   locked_until = NULL,
                   updated_at = now()
             WHERE event_id = $1
            "#,
        )
        .bind(event_id)
        .bind(error)
        .execute(&self.pool)
        .await
        .context("failed to mark Kafka outbox event deadlettered")?;
        Ok(())
    }

    pub(crate) async fn outbox_backlog_count(&self) -> anyhow::Result<i64> {
        query_scalar::<Postgres, i64>(
            r#"
            SELECT count(*)::BIGINT
              FROM event_outbox
             WHERE status IN ('queued', 'failed', 'publishing')
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to count Kafka outbox backlog")
    }

    pub(crate) async fn insert_kafka_deadletter(
        &self,
        envelope: &crate::eventbus::DeadLetterEnvelope,
        source_topic: Option<&str>,
        partition_key: &str,
    ) -> anyhow::Result<()> {
        let payload_json = serde_json::to_value(&envelope.event)?;
        let first_failed_at = OffsetDateTime::parse(&envelope.first_failed_at, &Rfc3339)
            .context("failed to parse deadletter first_failed_at")?;
        let last_failed_at = OffsetDateTime::parse(&envelope.last_failed_at, &Rfc3339)
            .context("failed to parse deadletter last_failed_at")?;
        query::<Postgres>(
            r#"
            INSERT INTO kafka_deadletters (
              event_id, event_type, source_topic, original_topic, original_partition,
              original_offset, partition_key, retry_count, error_code, payload_json,
              first_failed_at, last_failed_at
            )
            VALUES ($1, $2, $3, $3, -1, -1, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(&envelope.event.event_id)
        .bind(&envelope.event.event_type)
        .bind(source_topic)
        .bind(partition_key)
        .bind(envelope.retry_count as i32)
        .bind(&envelope.error_code)
        .bind(payload_json)
        .bind(first_failed_at)
        .bind(last_failed_at)
        .execute(&self.pool)
        .await
        .context("failed to insert Kafka deadletter")?;
        Ok(())
    }

    pub async fn event_inbox_processed(
        &self,
        consumer_group: &str,
        event: &CommonEvent,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> anyhow::Result<bool> {
        query_scalar::<Postgres, bool>(
            r#"
            SELECT EXISTS (
              SELECT 1
                FROM event_inbox
               WHERE consumer_group = $1
                 AND (
                      event_id = $2
                   OR (topic = $3 AND partition = $4 AND offset_value = $5)
                 )
            )
            "#,
        )
        .bind(consumer_group)
        .bind(&event.event_id)
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .fetch_one(&self.pool)
        .await
        .context("failed to check Kafka inbox event")
    }

    pub async fn mark_event_inbox_processed(
        &self,
        consumer_group: &str,
        event: &CommonEvent,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> anyhow::Result<()> {
        let payload_json = serde_json::to_value(event)?;
        query::<Postgres>(
            r#"
            INSERT INTO event_inbox (
              event_id, consumer_group, topic, partition, offset_value, payload_json
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (consumer_group, event_id) DO UPDATE SET
              topic = EXCLUDED.topic,
              partition = EXCLUDED.partition,
              offset_value = EXCLUDED.offset_value,
              payload_json = EXCLUDED.payload_json,
              processed_at = now()
            "#,
        )
        .bind(&event.event_id)
        .bind(consumer_group)
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .bind(payload_json)
        .execute(&self.pool)
        .await
        .context("failed to mark Kafka inbox event processed")?;
        Ok(())
    }

    pub async fn list_kafka_deadletters(
        &self,
        limit: i64,
        offset: i64,
    ) -> anyhow::Result<Vec<KafkaDeadLetterRecord>> {
        let rows = query::<Postgres>(
            r#"
            SELECT id::text,
                   event_id,
                   event_type,
                   source_topic,
                   original_topic,
                   original_partition,
                   original_offset,
                   partition_key,
                   retry_count,
                   error_code,
                   payload_json,
                   first_failed_at,
                   last_failed_at,
                   replayed_at,
                   replayed_by,
                   resolved_at,
                   created_at
              FROM kafka_deadletters
             ORDER BY created_at DESC
             LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit.clamp(1, 500))
        .bind(offset.max(0))
        .fetch_all(&self.pool)
        .await
        .context("failed to list Kafka deadletters")?;

        rows.into_iter()
            .map(|row| {
                Ok(KafkaDeadLetterRecord {
                    id: row.try_get("id")?,
                    event_id: row.try_get("event_id")?,
                    event_type: row.try_get("event_type")?,
                    source_topic: row.try_get("source_topic")?,
                    original_topic: row.try_get("original_topic")?,
                    original_partition: row.try_get("original_partition")?,
                    original_offset: row.try_get("original_offset")?,
                    partition_key: row.try_get("partition_key")?,
                    retry_count: row.try_get("retry_count")?,
                    error_code: row.try_get("error_code")?,
                    payload_json: row.try_get("payload_json")?,
                    first_failed_at: ts(row.try_get("first_failed_at")?),
                    last_failed_at: ts(row.try_get("last_failed_at")?),
                    replayed_at: row
                        .try_get::<Option<OffsetDateTime>, _>("replayed_at")?
                        .map(ts),
                    replayed_by: row.try_get("replayed_by")?,
                    resolved_at: row
                        .try_get::<Option<OffsetDateTime>, _>("resolved_at")?
                        .map(ts),
                    created_at: ts(row.try_get("created_at")?),
                })
            })
            .collect::<Result<Vec<_>, sqlx_core::Error>>()
            .context("failed to decode Kafka deadletter rows")
    }

    pub async fn replay_kafka_deadletter(
        &self,
        deadletter_id: &str,
        replayed_by: &str,
    ) -> anyhow::Result<Option<String>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin replay transaction")?;
        let row = query::<Postgres>(
            r#"
            SELECT payload_json
              FROM kafka_deadletters
             WHERE id = $1::uuid
               AND replayed_at IS NULL
             FOR UPDATE
            "#,
        )
        .bind(deadletter_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to load Kafka deadletter for replay")?;
        let Some(row) = row else {
            tx.rollback()
                .await
                .context("failed to rollback empty replay transaction")?;
            return Ok(None);
        };
        let payload_json: Value = row.try_get("payload_json")?;
        let event: CommonEvent = serde_json::from_value(payload_json.clone())
            .context("failed to decode Kafka deadletter payload")?;
        let topic = topic_for_event(&self.kafka, &event.event_type);
        let partition_key = partition_key(&event);
        query::<Postgres>(
            r#"
            INSERT INTO event_outbox (
              event_id, event_type, topic, partition_key, payload_json, status, available_at, next_attempt_at
            )
            VALUES ($1, $2, $3, $4, $5, 'queued', now(), now())
            ON CONFLICT (event_id) DO UPDATE
              SET status = 'queued',
                  topic = EXCLUDED.topic,
                  partition_key = EXCLUDED.partition_key,
                  payload_json = EXCLUDED.payload_json,
                  next_attempt_at = now(),
                  locked_by = NULL,
                  locked_until = NULL,
                  updated_at = now()
            "#,
        )
        .bind(&event.event_id)
        .bind(&event.event_type)
        .bind(topic)
        .bind(partition_key)
        .bind(payload_json)
        .execute(&mut *tx)
        .await
        .context("failed to enqueue replayed Kafka deadletter")?;
        query::<Postgres>(
            r#"
            UPDATE kafka_deadletters
               SET replayed_at = now(),
                   replayed_by = $2
             WHERE id = $1::uuid
            "#,
        )
        .bind(deadletter_id)
        .bind(replayed_by)
        .execute(&mut *tx)
        .await
        .context("failed to mark Kafka deadletter replayed")?;
        tx.commit()
            .await
            .context("failed to commit Kafka deadletter replay")?;
        Ok(Some(event.event_id))
    }

    pub async fn load_snapshot<T>(&self) -> anyhow::Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let row = query::<Postgres>("SELECT state_json FROM metadata_snapshots WHERE id = 'main'")
            .fetch_optional(&self.pool)
            .await
            .context("failed to load metadata snapshot")?;
        let Some(row) = row else {
            return Ok(None);
        };
        let value: Value = row.try_get("state_json")?;
        serde_json::from_value(value)
            .map(Some)
            .context("failed to deserialize metadata snapshot")
    }

    pub(crate) async fn persist_store(&self, store: &PersistedStore) -> anyhow::Result<()> {
        self.persist_snapshot_and_outbox(store).await?;
        self.persist_relational(store).await?;
        Ok(())
    }

    async fn persist_snapshot_and_outbox(&self, store: &PersistedStore) -> anyhow::Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin metadata snapshot/outbox transaction")?;
        let state_json = serde_json::to_value(store)?;
        query::<Postgres>(
            r#"
            INSERT INTO metadata_snapshots (id, state_json, updated_at)
            VALUES ('main', $1, now())
            ON CONFLICT (id) DO UPDATE
              SET state_json = EXCLUDED.state_json,
                  updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(state_json)
        .execute(&mut *tx)
        .await
        .context("failed to persist metadata snapshot transactionally")?;

        for event in &store.events {
            let topic = topic_for_event(&self.kafka, &event.event_type);
            let partition_key = partition_key(event);
            let payload_json = serde_json::to_value(event)?;
            query::<Postgres>(
                r#"
                INSERT INTO event_outbox (
                  event_id, event_type, topic, partition_key, payload_json, status, available_at, next_attempt_at
                )
                VALUES ($1, $2, $3, $4, $5, 'queued', now(), now())
                ON CONFLICT (event_id) DO NOTHING
                "#,
            )
            .bind(&event.event_id)
            .bind(&event.event_type)
            .bind(topic)
            .bind(partition_key)
            .bind(payload_json)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("failed to enqueue outbox event {}", event.event_id))?;
        }

        tx.commit()
            .await
            .context("failed to commit metadata snapshot/outbox transaction")
    }

    async fn persist_relational(&self, store: &PersistedStore) -> anyhow::Result<()> {
        self.persist_projects(store).await?;
        self.persist_companies(store).await?;
        self.persist_channels(store).await?;
        self.persist_contacts(store).await?;
        self.persist_groups(store).await?;
        self.persist_group_members(store).await?;
        self.persist_conversations(store).await?;
        self.persist_messages(store).await?;
        self.persist_media(store).await?;
        self.persist_transcripts(store).await?;
        self.persist_dirty(store).await?;
        self.persist_dirty_leases(store).await?;
        self.persist_consumer_state(store).await?;
        self.persist_idempotency(store).await?;
        self.persist_callbacks(store).await?;
        self.persist_webhook_delivery_attempts(store).await?;
        Ok(())
    }

    async fn persist_projects(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for (id, value) in &store.projects {
            let name = value_str(value, "name").unwrap_or_else(|| id.clone());
            let status = value_str(value, "status").unwrap_or_else(|| "active".to_string());
            self.upsert_project(id, &name, &status).await?;
        }
        for ((project_id, _), _) in &store.companies {
            self.upsert_project(project_id, project_id, "active")
                .await?;
        }
        for value in store.channels.values() {
            if let Some(project_id) = value_str(value, "project_id") {
                self.upsert_project(&project_id, &project_id, "active")
                    .await?;
            }
        }
        for conversation in store.conversations.values() {
            self.upsert_project(&conversation.project_id, &conversation.project_id, "active")
                .await?;
        }
        for message in store.messages_by_id.values() {
            self.upsert_project(&message.project_id, &message.project_id, "active")
                .await?;
        }
        for media in store.media.values() {
            self.upsert_project(&media.project_id, &media.project_id, "active")
                .await?;
        }
        for transcript in store.transcripts.values() {
            self.upsert_project(&transcript.project_id, &transcript.project_id, "active")
                .await?;
        }
        for ((project_id, _, _), _) in &store.dirty {
            self.upsert_project(project_id, project_id, "active")
                .await?;
        }
        for ((project_id, _, _, _), _) in &store.consumer_state {
            self.upsert_project(project_id, project_id, "active")
                .await?;
        }
        for ((project_id, _, _), _) in &store.idempotency {
            self.upsert_project(project_id, project_id, "active")
                .await?;
        }
        Ok(())
    }

    async fn persist_companies(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for ((project_id, company_id), value) in &store.companies {
            let name = value_str(value, "name").unwrap_or_else(|| company_id.clone());
            let status = value_str(value, "status").unwrap_or_else(|| "active".to_string());
            self.upsert_company(project_id, company_id, &name, &status)
                .await?;
        }
        for value in store.channels.values() {
            if let (Some(project_id), Some(company_id)) = (
                value_str(value, "project_id"),
                value_str(value, "company_id"),
            ) {
                self.upsert_company(&project_id, &company_id, &company_id, "active")
                    .await?;
            }
        }
        for conversation in store.conversations.values() {
            self.upsert_company(
                &conversation.project_id,
                &conversation.company_id,
                &conversation.company_id,
                "active",
            )
            .await?;
        }
        for message in store.messages_by_id.values() {
            self.upsert_company(
                &message.project_id,
                &message.company_id,
                &message.company_id,
                "active",
            )
            .await?;
        }
        for ((project_id, company_id, _), _) in &store.dirty {
            self.upsert_company(project_id, company_id, company_id, "active")
                .await?;
        }
        for ((project_id, company_id, _, _), _) in &store.consumer_state {
            self.upsert_company(project_id, company_id, company_id, "active")
                .await?;
        }
        for ((project_id, company_id, _), _) in &store.idempotency {
            self.upsert_company(project_id, company_id, company_id, "active")
                .await?;
        }
        Ok(())
    }

    async fn persist_channels(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for (id, value) in &store.channels {
            let project_id = value_str(value, "project_id").unwrap_or_else(|| "default".into());
            let company_id = value_str(value, "company_id").unwrap_or_else(|| "default".into());
            let provider =
                value_str(value, "provider").unwrap_or_else(|| "whatsapp-rust".to_string());
            let phone_e164 = value_str(value, "phone_e164");
            let label = value_str(value, "label");
            let status = value_str(value, "status").unwrap_or_else(|| "disconnected".to_string());
            let connected_at = value_time(value, "connected_at");
            self.upsert_project(&project_id, &project_id, "active")
                .await?;
            self.upsert_company(&project_id, &company_id, &company_id, "active")
                .await?;
            self.upsert_channel(
                id,
                &project_id,
                &company_id,
                &provider,
                phone_e164.as_deref(),
                label.as_deref(),
                &status,
                connected_at,
            )
            .await?;
        }
        for conversation in store.conversations.values() {
            let status = channel_status_for_id(store, &conversation.channel_account_id);
            self.upsert_channel(
                &conversation.channel_account_id,
                &conversation.project_id,
                &conversation.company_id,
                "whatsapp-rust",
                None,
                Some("WhatsApp"),
                &status,
                channel_connected_at_for_id(store, &conversation.channel_account_id),
            )
            .await?;
        }
        for message in store.messages_by_id.values() {
            let status = channel_status_for_id(store, &message.channel_account_id);
            self.upsert_channel(
                &message.channel_account_id,
                &message.project_id,
                &message.company_id,
                "whatsapp-rust",
                None,
                Some("WhatsApp"),
                &status,
                channel_connected_at_for_id(store, &message.channel_account_id),
            )
            .await?;
        }
        Ok(())
    }

    async fn persist_contacts(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for contact in store.contacts.values() {
            let channel_id =
                channel_for_project_company(store, &contact.project_id, &contact.company_id);
            let status = channel_status_for_id(store, &channel_id);
            self.upsert_channel(
                &channel_id,
                &contact.project_id,
                &contact.company_id,
                "whatsapp-rust",
                None,
                Some("WhatsApp"),
                &status,
                channel_connected_at_for_id(store, &channel_id),
            )
            .await?;
            self.upsert_contact(contact, &channel_id).await?;
        }
        for conversation in store.conversations.values() {
            if let Some(contact_id) = conversation.contact_id.as_deref() {
                self.upsert_minimal_contact(
                    &conversation.project_id,
                    &conversation.company_id,
                    &conversation.channel_account_id,
                    contact_id,
                    conversation.display_name.as_deref(),
                    conversation.display_phone.as_deref(),
                )
                .await?;
            }
        }
        for message in store.messages_by_id.values() {
            if let Some(contact_id) = message.sender_contact_id.as_deref() {
                self.upsert_minimal_contact(
                    &message.project_id,
                    &message.company_id,
                    &message.channel_account_id,
                    contact_id,
                    message.sender_display_name.as_deref(),
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn persist_groups(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for (group_id, group) in &store.groups {
            let (project_id, company_id, channel_id) = group_context(store, group_id);
            self.upsert_group(group_id, group, &project_id, &company_id, &channel_id)
                .await?;
        }
        for conversation in store.conversations.values() {
            if let Some(group_id) = conversation.group_id.as_deref() {
                self.upsert_group(
                    group_id,
                    &GroupRecord {
                        wa_jid: Some(group_id.to_string()),
                        subject: conversation.display_name.clone(),
                        description: None,
                        owner_jid: None,
                        subject_owner_jid: None,
                        profile_picture_media_id: None,
                        avatar_url: conversation.avatar_url.clone(),
                        profile_picture_url: conversation.profile_picture_url.clone(),
                        created_at_wa: None,
                        members_count: None,
                        admins_count: None,
                    },
                    &conversation.project_id,
                    &conversation.company_id,
                    &conversation.channel_account_id,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn persist_group_members(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for (group_id, members) in &store.group_members {
            let (project_id, company_id, channel_id) = group_context(store, group_id);
            let group = store
                .groups
                .get(group_id)
                .cloned()
                .unwrap_or_else(|| GroupRecord {
                    wa_jid: Some(group_id.clone()),
                    subject: Some(group_id.clone()),
                    description: None,
                    owner_jid: None,
                    subject_owner_jid: None,
                    profile_picture_media_id: None,
                    avatar_url: None,
                    profile_picture_url: None,
                    created_at_wa: None,
                    members_count: None,
                    admins_count: None,
                });
            self.upsert_group(group_id, &group, &project_id, &company_id, &channel_id)
                .await?;
            for member in members.values() {
                self.upsert_minimal_contact(
                    &project_id,
                    &company_id,
                    &channel_id,
                    &member.contact_id,
                    Some(&member.display_name),
                    member.phone_e164.as_deref(),
                )
                .await?;
                self.upsert_group_member(member).await?;
            }
        }
        Ok(())
    }

    async fn persist_conversations(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for conversation in store.conversations.values() {
            query::<Postgres>(
                r#"
                INSERT INTO conversations (
                  id, project_id, company_id, channel_account_id, type, contact_id, group_id,
                  display_name, display_phone, avatar_url, profile_picture_url,
                  last_seq, last_message_at, unread_count, is_archived, is_muted, is_pinned,
                  control_mode, created_at, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
                ON CONFLICT (id) DO UPDATE SET
                  project_id = EXCLUDED.project_id,
                  company_id = EXCLUDED.company_id,
                  channel_account_id = EXCLUDED.channel_account_id,
                  type = EXCLUDED.type,
                  contact_id = EXCLUDED.contact_id,
                  group_id = EXCLUDED.group_id,
                  display_name = EXCLUDED.display_name,
                  display_phone = EXCLUDED.display_phone,
                  avatar_url = EXCLUDED.avatar_url,
                  profile_picture_url = EXCLUDED.profile_picture_url,
                  last_seq = EXCLUDED.last_seq,
                  last_message_at = EXCLUDED.last_message_at,
                  unread_count = EXCLUDED.unread_count,
                  is_archived = EXCLUDED.is_archived,
                  is_muted = EXCLUDED.is_muted,
                  is_pinned = EXCLUDED.is_pinned,
                  control_mode = EXCLUDED.control_mode,
                  updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(&conversation.id)
            .bind(&conversation.project_id)
            .bind(&conversation.company_id)
            .bind(&conversation.channel_account_id)
            .bind(&conversation.conversation_type)
            .bind(&conversation.contact_id)
            .bind(&conversation.group_id)
            .bind(&conversation.display_name)
            .bind(&conversation.display_phone)
            .bind(&conversation.avatar_url)
            .bind(&conversation.profile_picture_url)
            .bind(conversation.last_seq)
            .bind(conversation.last_message_at)
            .bind(conversation.unread_count)
            .bind(conversation.is_archived)
            .bind(conversation.is_muted)
            .bind(conversation.is_pinned)
            .bind(&conversation.control_mode)
            .bind(conversation.created_at)
            .bind(conversation.updated_at)
            .execute(&self.pool)
            .await
            .with_context(|| format!("failed to persist conversation {}", conversation.id))?;
        }
        Ok(())
    }

    async fn persist_messages(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for message in store.messages_by_id.values() {
            self.persist_message(message).await?;
        }
        Ok(())
    }

    async fn persist_message(&self, message: &Message) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO messages (
              id, project_id, company_id, conversation_id, channel_account_id,
              conversation_seq, wa_message_id, direction, sender_contact_id, sender_display_name,
              message_type, text, media_id, media_url, thumbnail_url, mime_type, file_name,
              quoted_message_id, status, error_message, is_starred, is_pinned, reaction,
              sent_by_source, sent_by_external_user_id, created_at_wa, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)
            ON CONFLICT (id) DO UPDATE SET
              conversation_seq = EXCLUDED.conversation_seq,
              wa_message_id = EXCLUDED.wa_message_id,
              sender_contact_id = EXCLUDED.sender_contact_id,
              sender_display_name = EXCLUDED.sender_display_name,
              text = EXCLUDED.text,
              media_id = EXCLUDED.media_id,
              media_url = EXCLUDED.media_url,
              thumbnail_url = EXCLUDED.thumbnail_url,
              mime_type = EXCLUDED.mime_type,
              file_name = EXCLUDED.file_name,
              quoted_message_id = EXCLUDED.quoted_message_id,
              status = EXCLUDED.status,
              error_message = EXCLUDED.error_message,
              is_starred = EXCLUDED.is_starred,
              is_pinned = EXCLUDED.is_pinned,
              reaction = EXCLUDED.reaction,
              sent_by_source = EXCLUDED.sent_by_source,
              sent_by_external_user_id = EXCLUDED.sent_by_external_user_id,
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&message.id)
        .bind(&message.project_id)
        .bind(&message.company_id)
        .bind(&message.conversation_id)
        .bind(&message.channel_account_id)
        .bind(message.conversation_seq)
        .bind(&message.wa_message_id)
        .bind(&message.direction)
        .bind(&message.sender_contact_id)
        .bind(&message.sender_display_name)
        .bind(&message.message_type)
        .bind(&message.text)
        .bind(&message.media_id)
        .bind(&message.media_url)
        .bind(&message.thumbnail_url)
        .bind(&message.mime_type)
        .bind(&message.file_name)
        .bind(&message.quoted_message_id)
        .bind(&message.status)
        .bind(&message.error_message)
        .bind(message.is_starred)
        .bind(message.is_pinned)
        .bind(&message.reaction)
        .bind(&message.sent_by_source)
        .bind(&message.sent_by_external_user_id)
        .bind(message.created_at_wa)
        .bind(message.created_at)
        .bind(message.updated_at)
        .execute(&self.pool)
        .await
        .with_context(|| format!("failed to persist message {}", message.id))?;
        Ok(())
    }

    async fn persist_media(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for media in store.media.values() {
            self.persist_media_object(media).await?;
        }
        Ok(())
    }

    async fn persist_media_object(&self, media: &MediaObject) -> anyhow::Result<()> {
        let size_bytes = i64::try_from(media.size_bytes).unwrap_or(i64::MAX);
        query::<Postgres>(
            r#"
            INSERT INTO media_objects (
              id, project_id, company_id, conversation_id, message_id, media_type, mime_type,
              original_filename, size_bytes, sha256, storage_status, bucket, object_key,
              permanent_object_key, public_url, thumbnail_url, expires_at, saved_at, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
            ON CONFLICT (id) DO UPDATE SET
              conversation_id = EXCLUDED.conversation_id,
              message_id = EXCLUDED.message_id,
              media_type = EXCLUDED.media_type,
              mime_type = EXCLUDED.mime_type,
              original_filename = EXCLUDED.original_filename,
              size_bytes = EXCLUDED.size_bytes,
              sha256 = EXCLUDED.sha256,
              storage_status = EXCLUDED.storage_status,
              bucket = EXCLUDED.bucket,
              object_key = EXCLUDED.object_key,
              permanent_object_key = EXCLUDED.permanent_object_key,
              public_url = EXCLUDED.public_url,
              thumbnail_url = EXCLUDED.thumbnail_url,
              expires_at = EXCLUDED.expires_at,
              saved_at = EXCLUDED.saved_at,
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&media.id)
        .bind(&media.project_id)
        .bind(&media.company_id)
        .bind(&media.conversation_id)
        .bind(&media.message_id)
        .bind(&media.media_type)
        .bind(&media.mime_type)
        .bind(&media.original_filename)
        .bind(size_bytes)
        .bind(&media.sha256)
        .bind(&media.storage_status)
        .bind(&media.bucket)
        .bind(&media.object_key)
        .bind(&media.permanent_object_key)
        .bind(&media.public_url)
        .bind(&media.thumbnail_url)
        .bind(media.expires_at)
        .bind(media.saved_at)
        .bind(media.created_at)
        .bind(media.updated_at)
        .execute(&self.pool)
        .await
        .with_context(|| format!("failed to persist media {}", media.id))?;
        Ok(())
    }

    async fn persist_transcripts(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for transcript in store.transcripts.values() {
            self.persist_transcript(transcript).await?;
        }
        Ok(())
    }

    async fn persist_transcript(&self, transcript: &Transcript) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO transcripts (
              id, project_id, company_id, message_id, media_id, provider, model, language,
              text, raw_response_json, status, error_message, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (id) DO UPDATE SET
              media_id = EXCLUDED.media_id,
              text = EXCLUDED.text,
              raw_response_json = EXCLUDED.raw_response_json,
              status = EXCLUDED.status,
              error_message = EXCLUDED.error_message,
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&transcript.id)
        .bind(&transcript.project_id)
        .bind(&transcript.company_id)
        .bind(&transcript.message_id)
        .bind(&transcript.media_id)
        .bind(&transcript.provider)
        .bind(&transcript.model)
        .bind(&transcript.language)
        .bind(&transcript.text)
        .bind(&transcript.raw_response_json)
        .bind(&transcript.status)
        .bind(&transcript.error_message)
        .bind(transcript.created_at)
        .bind(transcript.updated_at)
        .execute(&self.pool)
        .await
        .with_context(|| format!("failed to persist transcript {}", transcript.id))?;
        Ok(())
    }

    async fn persist_dirty(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for ((project_id, company_id, _conversation_id), dirty) in &store.dirty {
            self.persist_dirty_record(project_id, company_id, dirty)
                .await?;
        }
        Ok(())
    }

    async fn persist_dirty_record(
        &self,
        project_id: &str,
        company_id: &str,
        dirty: &DirtyRecord,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO dirty_conversations (
              project_id, company_id, conversation_id, max_seq, reason, priority,
              available_at, locked_until, lease_token, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
            ON CONFLICT (project_id, company_id, conversation_id) DO UPDATE SET
              max_seq = EXCLUDED.max_seq,
              reason = EXCLUDED.reason,
              priority = EXCLUDED.priority,
              available_at = EXCLUDED.available_at,
              locked_until = EXCLUDED.locked_until,
              lease_token = EXCLUDED.lease_token,
              updated_at = now()
            "#,
        )
        .bind(project_id)
        .bind(company_id)
        .bind(&dirty.conversation_id)
        .bind(dirty.max_seq)
        .bind(&dirty.reason)
        .bind(dirty.priority)
        .bind(dirty.available_at)
        .bind(dirty.locked_until)
        .bind(&dirty.lease_token)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "failed to persist dirty conversation {}",
                dirty.conversation_id
            )
        })?;
        Ok(())
    }

    async fn persist_dirty_leases(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for ((project_id, company_id, consumer_id, conversation_id), lease) in &store.dirty_leases {
            self.persist_dirty_lease(project_id, company_id, consumer_id, conversation_id, lease)
                .await?;
        }
        Ok(())
    }

    async fn persist_dirty_lease(
        &self,
        project_id: &str,
        company_id: &str,
        consumer_id: &str,
        conversation_id: &str,
        lease: &DirtyLeaseRecord,
    ) -> anyhow::Result<()> {
        sqlx_core::query::query::<Postgres>(
            r#"
            INSERT INTO dirty_conversation_leases (
              project_id, company_id, consumer_id, conversation_id,
              lease_token, locked_until, max_seq, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,now())
            ON CONFLICT (project_id, company_id, consumer_id, conversation_id) DO UPDATE SET
              lease_token = EXCLUDED.lease_token,
              locked_until = EXCLUDED.locked_until,
              max_seq = EXCLUDED.max_seq,
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(project_id)
        .bind(company_id)
        .bind(consumer_id)
        .bind(conversation_id)
        .bind(&lease.lease_token)
        .bind(lease.locked_until)
        .bind(lease.max_seq)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!("failed to persist dirty lease {consumer_id}/{conversation_id}")
        })?;
        Ok(())
    }

    async fn persist_consumer_state(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for ((project_id, company_id, consumer_id, conversation_id), last_processed_seq) in
            &store.consumer_state
        {
            query::<Postgres>(
                r#"
                INSERT INTO consumer_processing_state (
                  project_id, company_id, consumer_id, conversation_id,
                  last_processed_seq, last_processed_at, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,now(),now())
                ON CONFLICT (project_id, company_id, consumer_id, conversation_id) DO UPDATE SET
                  last_processed_seq = EXCLUDED.last_processed_seq,
                  last_processed_at = EXCLUDED.last_processed_at,
                  updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(project_id)
            .bind(company_id)
            .bind(consumer_id)
            .bind(conversation_id)
            .bind(*last_processed_seq)
            .execute(&self.pool)
            .await
            .with_context(|| format!("failed to persist consumer state {consumer_id}"))?;
        }
        Ok(())
    }

    async fn persist_idempotency(&self, store: &PersistedStore) -> anyhow::Result<()> {
        let expires_at = OffsetDateTime::now_utc() + Duration::hours(72);
        for ((project_id, company_id, key), record) in &store.idempotency {
            query::<Postgres>(
                r#"
                INSERT INTO idempotency_keys (
                  project_id, company_id, key, request_hash, response_json,
                  message_id, status, expires_at, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,'completed',$7,now())
                ON CONFLICT (project_id, company_id, key) DO UPDATE SET
                  request_hash = EXCLUDED.request_hash,
                  response_json = EXCLUDED.response_json,
                  message_id = EXCLUDED.message_id,
                  status = EXCLUDED.status,
                  expires_at = EXCLUDED.expires_at,
                  updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(project_id)
            .bind(company_id)
            .bind(key)
            .bind(&record.request_hash)
            .bind(json!({ "message_id": record.message_id }))
            .bind(&record.message_id)
            .bind(expires_at)
            .execute(&self.pool)
            .await
            .with_context(|| format!("failed to persist idempotency key {key}"))?;
        }
        Ok(())
    }

    async fn persist_callbacks(&self, store: &PersistedStore) -> anyhow::Result<()> {
        for callback in store.callbacks.values() {
            let id = value_str(callback, "id").unwrap_or_else(|| "callback_unknown".to_string());
            let project_id =
                value_str(callback, "project_id").unwrap_or_else(|| "default".to_string());
            let company_id =
                value_str(callback, "company_id").unwrap_or_else(|| "default".to_string());
            self.upsert_project(&project_id, &project_id, "active")
                .await?;
            self.upsert_company(&project_id, &company_id, &company_id, "active")
                .await?;
            let events: Vec<String> = callback
                .get("events")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_else(|| vec!["conversation.dirty".to_string()]);
            query::<Postgres>(
                r#"
                INSERT INTO consumer_callbacks (
                  id, project_id, company_id, url, encrypted_secret, enabled,
                  events, max_batch_size, timeout_seconds, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
                ON CONFLICT (id) DO UPDATE SET
                  url = EXCLUDED.url,
                  encrypted_secret = EXCLUDED.encrypted_secret,
                  enabled = EXCLUDED.enabled,
                  events = EXCLUDED.events,
                  max_batch_size = EXCLUDED.max_batch_size,
                  timeout_seconds = EXCLUDED.timeout_seconds,
                  updated_at = now()
                "#,
            )
            .bind(&id)
            .bind(&project_id)
            .bind(&company_id)
            .bind(
                value_str(callback, "url")
                    .unwrap_or_else(|| "http://localhost/webhook".to_string()),
            )
            .bind(value_str(callback, "secret"))
            .bind(callback["enabled"].as_bool().unwrap_or(true))
            .bind(events)
            .bind(callback["max_batch_size"].as_i64().unwrap_or(100) as i32)
            .bind(callback["timeout_seconds"].as_i64().unwrap_or(10) as i32)
            .execute(&self.pool)
            .await
            .with_context(|| format!("failed to persist consumer callback {id}"))?;
        }
        Ok(())
    }

    async fn persist_webhook_delivery_attempts(
        &self,
        store: &PersistedStore,
    ) -> anyhow::Result<()> {
        for attempt in &store.webhook_delivery_attempts {
            self.persist_webhook_delivery_attempt(attempt).await?;
        }
        Ok(())
    }

    async fn persist_webhook_delivery_attempt(
        &self,
        attempt: &WebhookDeliveryAttempt,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO webhook_delivery_attempts (
              id, callback_id, event_id, attempt, status, http_status,
              error_message, first_failed_at, last_failed_at, next_retry_at, created_at
            )
            VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&attempt.id)
        .bind(&attempt.callback_id)
        .bind(&attempt.event_id)
        .bind(i32::try_from(attempt.attempt).unwrap_or(i32::MAX))
        .bind(&attempt.status)
        .bind(attempt.http_status.map(i32::from))
        .bind(&attempt.error_message)
        .bind(attempt.first_failed_at)
        .bind(attempt.last_failed_at)
        .bind(attempt.next_retry_at)
        .bind(attempt.created_at)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "failed to persist webhook attempt {}/{}",
                attempt.callback_id, attempt.event_id
            )
        })?;
        Ok(())
    }

    async fn upsert_project(&self, id: &str, name: &str, status: &str) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO projects (id, name, status, updated_at)
            VALUES ($1,$2,$3,now())
            ON CONFLICT (id) DO UPDATE SET
              name = EXCLUDED.name,
              status = EXCLUDED.status,
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(id)
        .bind(name)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn upsert_company(
        &self,
        project_id: &str,
        company_id: &str,
        name: &str,
        status: &str,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO companies (id, project_id, name, status, updated_at)
            VALUES ($1,$2,$3,$4,now())
            ON CONFLICT (id) DO UPDATE SET
              project_id = EXCLUDED.project_id,
              name = EXCLUDED.name,
              status = EXCLUDED.status,
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(company_id)
        .bind(project_id)
        .bind(name)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn upsert_channel(
        &self,
        id: &str,
        project_id: &str,
        company_id: &str,
        provider: &str,
        phone_e164: Option<&str>,
        label: Option<&str>,
        status: &str,
        connected_at: Option<OffsetDateTime>,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO channel_accounts (
              id, project_id, company_id, provider, phone_e164, label, status, connected_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())
            ON CONFLICT (id) DO UPDATE SET
              project_id = EXCLUDED.project_id,
              company_id = EXCLUDED.company_id,
              provider = EXCLUDED.provider,
              phone_e164 = COALESCE(EXCLUDED.phone_e164, channel_accounts.phone_e164),
              label = COALESCE(EXCLUDED.label, channel_accounts.label),
              status = EXCLUDED.status,
              connected_at = CASE
                WHEN EXCLUDED.status = 'connected'
                  THEN COALESCE(EXCLUDED.connected_at, channel_accounts.connected_at)
                ELSE NULL
              END,
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(id)
        .bind(project_id)
        .bind(company_id)
        .bind(provider)
        .bind(phone_e164)
        .bind(label)
        .bind(status)
        .bind(connected_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn upsert_contact(
        &self,
        contact: &ContactRecord,
        channel_id: &str,
    ) -> anyhow::Result<()> {
        let channel_id = contact.channel_account_id.as_deref().unwrap_or(channel_id);
        query::<Postgres>(
            r#"
            INSERT INTO contacts (
              id, project_id, company_id, channel_account_id, wa_jid, phone_e164,
              lid, push_name, display_name, profile_picture_media_id, business_description,
              avatar_url, profile_picture_url, first_contact_at, last_contact_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now())
            ON CONFLICT (id) DO UPDATE SET
              channel_account_id = EXCLUDED.channel_account_id,
              wa_jid = COALESCE(EXCLUDED.wa_jid, contacts.wa_jid),
              lid = COALESCE(EXCLUDED.lid, contacts.lid),
              phone_e164 = COALESCE(EXCLUDED.phone_e164, contacts.phone_e164),
              push_name = COALESCE(EXCLUDED.push_name, contacts.push_name),
              display_name = EXCLUDED.display_name,
              profile_picture_media_id = COALESCE(EXCLUDED.profile_picture_media_id, contacts.profile_picture_media_id),
              business_description = COALESCE(EXCLUDED.business_description, contacts.business_description),
              avatar_url = COALESCE(EXCLUDED.avatar_url, contacts.avatar_url),
              profile_picture_url = COALESCE(EXCLUDED.profile_picture_url, contacts.profile_picture_url),
              last_contact_at = EXCLUDED.last_contact_at,
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&contact.id)
        .bind(&contact.project_id)
        .bind(&contact.company_id)
        .bind(channel_id)
        .bind(contact.canonical_jid.as_ref().or(contact.lid.as_ref()))
        .bind(&contact.phone_e164)
        .bind(&contact.lid)
        .bind(&contact.push_name)
        .bind(&contact.display_name)
        .bind(&contact.profile_picture_media_id)
        .bind(&contact.business_description)
        .bind(&contact.avatar_url)
        .bind(&contact.profile_picture_url)
        .bind(contact.first_contact_at)
        .bind(contact.last_contact_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn upsert_minimal_contact(
        &self,
        project_id: &str,
        company_id: &str,
        channel_id: &str,
        contact_id: &str,
        display_name: Option<&str>,
        phone_e164: Option<&str>,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO contacts (
              id, project_id, company_id, channel_account_id, wa_jid, phone_e164,
              display_name, first_contact_at, last_contact_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,now(),now(),now())
            ON CONFLICT (id) DO UPDATE SET
              display_name = COALESCE(EXCLUDED.display_name, contacts.display_name),
              phone_e164 = COALESCE(EXCLUDED.phone_e164, contacts.phone_e164),
              last_contact_at = now(),
              updated_at = now()
            "#,
        )
        .bind(contact_id)
        .bind(project_id)
        .bind(company_id)
        .bind(channel_id)
        .bind(contact_id)
        .bind(phone_e164)
        .bind(display_name.unwrap_or(contact_id))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn upsert_group(
        &self,
        group_id: &str,
        group: &GroupRecord,
        project_id: &str,
        company_id: &str,
        channel_id: &str,
    ) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO groups (
              id, project_id, company_id, channel_account_id, wa_jid, subject, description,
              owner_jid, subject_owner_jid, created_at_wa, profile_picture_media_id,
              avatar_url, profile_picture_url, members_count, admins_count, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now())
            ON CONFLICT (id) DO UPDATE SET
              channel_account_id = EXCLUDED.channel_account_id,
              wa_jid = EXCLUDED.wa_jid,
              subject = COALESCE(EXCLUDED.subject, groups.subject),
              description = COALESCE(EXCLUDED.description, groups.description),
              owner_jid = COALESCE(EXCLUDED.owner_jid, groups.owner_jid),
              subject_owner_jid = COALESCE(EXCLUDED.subject_owner_jid, groups.subject_owner_jid),
              created_at_wa = COALESCE(EXCLUDED.created_at_wa, groups.created_at_wa),
              profile_picture_media_id = COALESCE(EXCLUDED.profile_picture_media_id, groups.profile_picture_media_id),
              avatar_url = COALESCE(EXCLUDED.avatar_url, groups.avatar_url),
              profile_picture_url = COALESCE(EXCLUDED.profile_picture_url, groups.profile_picture_url),
              members_count = COALESCE(EXCLUDED.members_count, groups.members_count),
              admins_count = COALESCE(EXCLUDED.admins_count, groups.admins_count),
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(group_id)
        .bind(project_id)
        .bind(company_id)
        .bind(channel_id)
        .bind(group.wa_jid.as_deref().unwrap_or(group_id))
        .bind(&group.subject)
        .bind(&group.description)
        .bind(&group.owner_jid)
        .bind(&group.subject_owner_jid)
        .bind(group.created_at_wa)
        .bind(&group.profile_picture_media_id)
        .bind(&group.avatar_url)
        .bind(&group.profile_picture_url)
        .bind(group.members_count.and_then(|value| i32::try_from(value).ok()))
        .bind(group.admins_count.and_then(|value| i32::try_from(value).ok()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn upsert_group_member(&self, member: &GroupMemberRecord) -> anyhow::Result<()> {
        query::<Postgres>(
            r#"
            INSERT INTO group_members (
              group_id, contact_id, wa_jid, phone_e164, name, role, is_admin, joined_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (group_id, contact_id) DO UPDATE SET
              wa_jid = COALESCE(EXCLUDED.wa_jid, group_members.wa_jid),
              phone_e164 = COALESCE(EXCLUDED.phone_e164, group_members.phone_e164),
              name = COALESCE(EXCLUDED.name, group_members.name),
              role = EXCLUDED.role,
              is_admin = EXCLUDED.is_admin,
              joined_at = COALESCE(group_members.joined_at, EXCLUDED.joined_at),
              updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&member.group_id)
        .bind(&member.contact_id)
        .bind(member.wa_jid.as_deref().unwrap_or(&member.contact_id))
        .bind(&member.phone_e164)
        .bind(&member.display_name)
        .bind(&member.role)
        .bind(member.is_admin)
        .bind(member.joined_at)
        .bind(member.updated_at)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "failed to persist group member {}/{}",
                member.group_id, member.contact_id
            )
        })?;
        Ok(())
    }
}

fn value_str(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn value_time(value: &Value, key: &str) -> Option<OffsetDateTime> {
    value_str(value, key).and_then(|value| OffsetDateTime::parse(&value, &Rfc3339).ok())
}

fn ts(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).expect("RFC3339 format works")
}

fn channel_status_for_id(store: &PersistedStore, channel_id: &str) -> String {
    store
        .channels
        .get(channel_id)
        .and_then(|channel| value_str(channel, "status"))
        .unwrap_or_else(|| "disconnected".to_string())
}

fn channel_connected_at_for_id(store: &PersistedStore, channel_id: &str) -> Option<OffsetDateTime> {
    store
        .channels
        .get(channel_id)
        .and_then(|channel| value_time(channel, "connected_at"))
}

fn channel_for_project_company(
    store: &PersistedStore,
    project_id: &str,
    company_id: &str,
) -> String {
    store
        .channels
        .iter()
        .find_map(|(id, value)| {
            (value.get("project_id").and_then(Value::as_str) == Some(project_id)
                && value.get("company_id").and_then(Value::as_str) == Some(company_id))
            .then(|| id.clone())
        })
        .or_else(|| {
            store
                .conversations
                .values()
                .find(|conversation| {
                    conversation.project_id == project_id && conversation.company_id == company_id
                })
                .map(|conversation| conversation.channel_account_id.clone())
        })
        .unwrap_or_else(|| "channel_dev".to_string())
}

fn group_context(store: &PersistedStore, group_id: &str) -> (String, String, String) {
    store
        .conversations
        .values()
        .find(|conversation| conversation.group_id.as_deref() == Some(group_id))
        .map(|conversation| {
            (
                conversation.project_id.clone(),
                conversation.company_id.clone(),
                conversation.channel_account_id.clone(),
            )
        })
        .unwrap_or_else(|| {
            (
                "default".to_string(),
                "default".to_string(),
                "channel_dev".to_string(),
            )
        })
}
