#![cfg(feature = "external-integrations")]

use std::{env, sync::LazyLock, time::Duration};

use anyhow::{Context, bail};
use rdkafka::{
    ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message,
};
use rustzap::{
    AppState,
    config::{AppConfig, KafkaConfig},
    db::MetadataDb,
    eventbus::{
        KafkaEventBus, decode_event_from_kafka, dirty_signal, partition_key, topic_for_event,
    },
    models::{SendMessageRequest, SimulateInboundTextRequest},
};
use tokio::time::{Instant, sleep, timeout};
use uuid::Uuid;

static ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

fn test_config(brokers: String) -> KafkaConfig {
    KafkaConfig {
        brokers: Some(brokers),
        client_id: "rustzap-kafka-integration".to_string(),
        topic_prefix: format!("rustzap_test_{}", Uuid::now_v7().simple()),
        consumer_group: format!("rustzap-test-{}", Uuid::now_v7().simple()),
        retry_topic_suffix: "retry".to_string(),
        deadletter_topic_suffix: "deadletter".to_string(),
        message_max_bytes: 1_048_576,
        enable_auto_create_topics: true,
        default_partitions: 3,
        default_replication_factor: 1,
        message_retention_hours: 1,
        deadletter_retention_hours: 1,
        compression_type: "none".to_string(),
        producer_linger_ms: 0,
        producer_batch_size_bytes: 16_384,
        consumer_max_poll_records: 100,
        retry_max_attempts: 3,
        request_timeout_ms: 10_000,
    }
}

#[tokio::test]
#[ignore]
async fn kafka_event_bus_publishes_compact_event_to_real_broker() -> anyhow::Result<()> {
    let _env_guard = ENV_LOCK.lock().await;
    let brokers = match env::var("KAFKA_TEST_BROKERS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!("KAFKA_TEST_BROKERS not set; skipping real Kafka integration test");
            return Ok(());
        }
    };
    let config = test_config(brokers.clone());
    let bus = KafkaEventBus::connect(config.clone()).await?;
    bus.ensure_topics().await?;
    let health = bus.health().await?;
    assert!(health.ok, "{}", health.detail);

    let event = dirty_signal("p", "c", "ch", "conv", 9, "integration", 10);
    let expected_key = partition_key(&event);
    let ack = bus.publish(&event).await?;
    assert_eq!(
        ack.topic,
        format!("{}.conversation.dirty", config.topic_prefix)
    );
    assert_eq!(ack.key, expected_key);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("{}-reader", config.consumer_group))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .create()
        .context("failed to create Kafka integration consumer")?;
    consumer.subscribe(&[&ack.topic])?;

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Ok(message) = timeout(Duration::from_secs(1), consumer.recv()).await {
            let message = message?;
            if message.key() != Some(expected_key.as_bytes()) {
                continue;
            }
            let payload = message.payload().context("Kafka message payload missing")?;
            let decoded = decode_event_from_kafka(payload)?;
            if decoded.event_id == event.event_id {
                assert_eq!(decoded.event_type, "conversation.dirty");
                assert_eq!(decoded.conversation_id.as_deref(), Some("conv"));
                assert!(!payload.windows(5).any(|window| window == br#"bytes"#));
                return Ok(());
            }
        }
    }

    bail!("published Kafka event was not consumed before timeout");
}

#[tokio::test]
#[ignore]
async fn appstate_postgres_outbox_publishes_transactional_events_to_real_broker()
-> anyhow::Result<()> {
    let _env_guard = ENV_LOCK.lock().await;
    let brokers = match env::var("KAFKA_TEST_BROKERS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!("KAFKA_TEST_BROKERS not set; skipping real Kafka outbox integration test");
            return Ok(());
        }
    };
    let database_url = match env::var("KAFKA_TEST_DATABASE_URL") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!(
                "KAFKA_TEST_DATABASE_URL not set; skipping real Kafka outbox integration test"
            );
            return Ok(());
        }
    };
    let topic_prefix = format!("rustzap_outbox_{}", Uuid::now_v7().simple());
    set_env("RUSTZAP_METADATA_DB", "postgres");
    set_env("DATABASE_URL", &database_url);
    set_env("EVENT_BUS", "kafka");
    set_env("KAFKA_BROKERS", &brokers);
    set_env("KAFKA_TOPIC_PREFIX", &topic_prefix);
    set_env("KAFKA_CONSUMER_GROUP", "rustzap-outbox-test");
    set_env("KAFKA_ENABLE_AUTO_CREATE_TOPICS", "true");
    set_env("KAFKA_DEFAULT_PARTITIONS", "3");
    set_env("KAFKA_DEFAULT_REPLICATION_FACTOR", "1");
    set_env("KAFKA_COMPRESSION_TYPE", "none");
    set_env("KAFKA_REQUEST_TIMEOUT_MS", "10000");
    set_env(
        "WA_SESSION_SQLITE_DIR",
        &format!("/tmp/rustzap-wa-{}", Uuid::now_v7().simple()),
    );

    let state = AppState::from_config(AppConfig::from_env()).await?;
    let topic = format!("{topic_prefix}.message.persisted");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set(
            "group.id",
            format!("rustzap-outbox-reader-{}", Uuid::now_v7().simple()),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .create()
        .context("failed to create Kafka outbox integration consumer")?;
    consumer.subscribe(&[&topic])?;

    let message = state.receive_inbound_text(
        "project_outbox",
        "company_outbox",
        SimulateInboundTextRequest {
            conversation_id: Some("conv_outbox".to_string()),
            channel_id: Some("channel_outbox".to_string()),
            from_phone_e164: Some("+15550001111".to_string()),
            sender_name: Some("Outbox Tester".to_string()),
            profile_picture_url: None,
            text: "hello from transactional outbox".to_string(),
        },
    );

    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(message_result) = timeout(Duration::from_secs(1), consumer.recv()).await {
            let kafka_message = message_result?;
            let payload = kafka_message
                .payload()
                .context("Kafka outbox message payload missing")?;
            let decoded = decode_event_from_kafka(payload)?;
            if decoded.message_id.as_deref() == Some(message.id.as_str()) {
                assert_eq!(decoded.event_type, "message.received");
                assert_eq!(decoded.conversation_id.as_deref(), Some("conv_outbox"));
                assert_eq!(
                    kafka_message.key(),
                    Some(b"project_outbox:company_outbox:conv_outbox".as_slice())
                );
                return Ok(());
            }
        }
    }

    bail!("transactional outbox event was not published before timeout");
}

#[tokio::test]
#[ignore]
async fn appstate_kafka_worker_consumes_outbound_send_request_and_marks_failed_when_channel_disconnected()
-> anyhow::Result<()> {
    let _env_guard = ENV_LOCK.lock().await;
    let brokers = match env::var("KAFKA_TEST_BROKERS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!("KAFKA_TEST_BROKERS not set; skipping real Kafka worker integration test");
            return Ok(());
        }
    };
    let database_url = match env::var("KAFKA_TEST_DATABASE_URL") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!(
                "KAFKA_TEST_DATABASE_URL not set; skipping real Kafka worker integration test"
            );
            return Ok(());
        }
    };
    let topic_prefix = format!("rustzap_worker_{}", Uuid::now_v7().simple());
    set_env("RUSTZAP_METADATA_DB", "postgres");
    set_env("DATABASE_URL", &database_url);
    set_env("EVENT_BUS", "kafka");
    set_env("KAFKA_BROKERS", &brokers);
    set_env("KAFKA_TOPIC_PREFIX", &topic_prefix);
    set_env(
        "KAFKA_CONSUMER_GROUP",
        &format!("rustzap-worker-test-{}", Uuid::now_v7().simple()),
    );
    set_env("KAFKA_ENABLE_AUTO_CREATE_TOPICS", "true");
    set_env("KAFKA_DEFAULT_PARTITIONS", "3");
    set_env("KAFKA_DEFAULT_REPLICATION_FACTOR", "1");
    set_env("KAFKA_COMPRESSION_TYPE", "none");
    set_env("KAFKA_REQUEST_TIMEOUT_MS", "10000");
    set_env(
        "WA_SESSION_SQLITE_DIR",
        &format!("/tmp/rustzap-wa-{}", Uuid::now_v7().simple()),
    );

    let state = AppState::from_config(AppConfig::from_env()).await?;
    let conversation_id = format!("{}@s.whatsapp.net", Uuid::now_v7().simple());
    let outcome = state.prepare_send_message(
        "project_worker",
        "company_worker",
        &conversation_id,
        &format!("idem-{}", Uuid::now_v7().simple()),
        SendMessageRequest {
            message_type: "text".to_string(),
            text: Some("worker integration dispatch".to_string()),
            media_id: None,
            caption: None,
            filename: None,
            quoted_message_id: None,
            metadata: None,
        },
    )?;
    assert_eq!(outcome.message.status, "queued");

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let message =
            state.message_for_company("project_worker", "company_worker", &outcome.message.id)?;
        if message.status == "failed"
            && message
                .error_message
                .as_deref()
                .is_some_and(|error| error.contains("not connected"))
        {
            return Ok(());
        }
        sleep(Duration::from_millis(250)).await;
    }

    bail!("Kafka outbound worker did not process queued send before timeout");
}

#[tokio::test]
#[ignore]
async fn metadata_db_reclaims_stale_publishing_outbox_events() -> anyhow::Result<()> {
    let _env_guard = ENV_LOCK.lock().await;
    let brokers = match env::var("KAFKA_TEST_BROKERS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };
    let database_url = match env::var("KAFKA_TEST_DATABASE_URL") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };
    let topic_prefix = format!("rustzap_outbox_reclaim_{}", Uuid::now_v7().simple());
    set_env("RUSTZAP_METADATA_DB", "postgres");
    set_env("DATABASE_URL", &database_url);
    set_env("EVENT_BUS", "kafka");
    set_env("KAFKA_BROKERS", &brokers);
    set_env("KAFKA_TOPIC_PREFIX", &topic_prefix);

    let config = AppConfig::from_env();
    let db = MetadataDb::connect(&config).await?;
    db.migrate().await?;
    let event = dirty_signal("p_reclaim", "c_reclaim", "ch", "conv", 1, "test", 10);
    db.insert_event_outbox(
        &event,
        &topic_for_event(&config.kafka, &event.event_type),
        &partition_key(&event),
    )
    .await?;

    let first = db.claim_event_outbox_batch("worker-a", 1, 1).await?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].event_id, event.event_id);
    sleep(Duration::from_millis(1200)).await;

    let reclaimed = db.claim_event_outbox_batch("worker-b", 1, 1).await?;
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].event_id, event.event_id);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn metadata_db_marks_inbox_only_after_processing() -> anyhow::Result<()> {
    let _env_guard = ENV_LOCK.lock().await;
    let brokers = match env::var("KAFKA_TEST_BROKERS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };
    let database_url = match env::var("KAFKA_TEST_DATABASE_URL") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };
    let topic_prefix = format!("rustzap_inbox_{}", Uuid::now_v7().simple());
    set_env("RUSTZAP_METADATA_DB", "postgres");
    set_env("DATABASE_URL", &database_url);
    set_env("EVENT_BUS", "kafka");
    set_env("KAFKA_BROKERS", &brokers);
    set_env("KAFKA_TOPIC_PREFIX", &topic_prefix);

    let config = AppConfig::from_env();
    let db = MetadataDb::connect(&config).await?;
    db.migrate().await?;
    let event = dirty_signal("p_inbox", "c_inbox", "ch", "conv", 1, "test", 10);
    let topic = topic_for_event(&config.kafka, &event.event_type);
    assert!(
        !db.event_inbox_processed("cg-inbox", &event, &topic, 0, 42)
            .await?
    );

    db.mark_event_inbox_processed("cg-inbox", &event, &topic, 0, 42)
        .await?;
    assert!(
        db.event_inbox_processed("cg-inbox", &event, &topic, 0, 42)
            .await?
    );
    let same_offset_different_event =
        dirty_signal("p_inbox", "c_inbox", "ch", "conv", 2, "test", 10);
    assert!(
        db.event_inbox_processed("cg-inbox", &same_offset_different_event, &topic, 0, 42)
            .await?
    );
    Ok(())
}

fn set_env(key: &str, value: &str) {
    unsafe {
        env::set_var(key, value);
    }
}
