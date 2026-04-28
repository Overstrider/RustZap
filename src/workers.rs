use crate::state::AppState;

#[cfg(feature = "external-integrations")]
mod kafka {
    use std::sync::Arc;

    use rdkafka::{
        ClientConfig,
        consumer::{Consumer, StreamConsumer},
    };
    use tokio::time::{Duration, sleep};

    use crate::{
        config::{EventBusMode, MetadataDbMode},
        db::MetadataDb,
        eventbus::{
            ConsumerHandlerStatus, EventBusError, KafkaConsumerMessage,
            consume_one_message_with_inbox, deadletter_envelope, partition_key, topic_for_event,
        },
        state::AppState,
    };

    pub fn spawn(state: AppState) {
        if state.config.event_bus != EventBusMode::Kafka
            || state.config.metadata_db != MetadataDbMode::Postgres
        {
            return;
        }
        let Some(db) = state.metadata_db_handle() else {
            return;
        };

        spawn_worker(
            state.clone(),
            db.clone(),
            "raw-inbound",
            vec![topic_for_event(&state.config.kafka, "whatsapp.raw.inbound")],
        );
        spawn_worker(
            state.clone(),
            db.clone(),
            "outbound-send",
            vec![topic_for_event(
                &state.config.kafka,
                "outbound.send.requested",
            )],
        );
        spawn_worker(
            state.clone(),
            db,
            "audio-transcription",
            vec![topic_for_event(
                &state.config.kafka,
                "audio.transcription.requested",
            )],
        );
    }

    fn spawn_worker(
        state: AppState,
        db: Arc<MetadataDb>,
        worker_name: &'static str,
        topics: Vec<String>,
    ) {
        tokio::spawn(async move {
            let group_id = format!("{}.{}", state.config.kafka.consumer_group, worker_name);
            let consumer: StreamConsumer = match ClientConfig::new()
                .set(
                    "bootstrap.servers",
                    state.config.kafka.brokers.as_deref().unwrap_or_default(),
                )
                .set("group.id", &group_id)
                .set(
                    "client.id",
                    format!("{}-{worker_name}", state.config.kafka.client_id),
                )
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .set("enable.partition.eof", "false")
                .create()
            {
                Ok(consumer) => consumer,
                Err(err) => {
                    tracing::error!(worker_name, error = %err, "failed to create Kafka worker consumer");
                    return;
                }
            };
            let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
            if let Err(err) = consumer.subscribe(&topic_refs) {
                tracing::error!(worker_name, error = %err, "failed to subscribe Kafka worker");
                return;
            }

            loop {
                let state = state.clone();
                let db_for_inbox = db.clone();
                let db_for_handler = db.clone();
                let result = consume_one_message_with_inbox(
                    &consumer,
                    &db_for_inbox,
                    &group_id,
                    |message| async move {
                        handle_message(state, db_for_handler, worker_name, message).await
                    },
                )
                .await;
                if let Err(err) = result {
                    tracing::warn!(worker_name, error = %err, "Kafka worker poll failed");
                    sleep(Duration::from_millis(500)).await;
                }
            }
        });
    }

    async fn handle_message(
        state: AppState,
        db: Arc<MetadataDb>,
        worker_name: &'static str,
        message: KafkaConsumerMessage,
    ) -> Result<ConsumerHandlerStatus, EventBusError> {
        let event = message.event;
        let result = match event.event_type.as_str() {
            "whatsapp.raw.inbound" => state.process_whatsapp_raw_event(&event).await.map(|_| ()),
            "outbound.send.requested" => state
                .process_outbound_send_request(&event)
                .await
                .map(|_| ()),
            "audio.transcription.requested" => state
                .process_transcription_request(&event)
                .await
                .map(|_| ()),
            _ => Ok(()),
        };

        match result {
            Ok(()) => Ok(ConsumerHandlerStatus::Processed),
            Err(err) => {
                let error = err.to_string();
                let envelope = deadletter_envelope(event, 1, &error);
                if let Err(insert_err) = db
                    .insert_kafka_deadletter(
                        &envelope,
                        Some(&message.source.topic),
                        &partition_key(&envelope.event),
                    )
                    .await
                {
                    tracing::error!(
                        worker_name,
                        error = %insert_err,
                        "failed to persist Kafka worker deadletter"
                    );
                    return Err(EventBusError::Kafka(insert_err.to_string()));
                }
                tracing::warn!(worker_name, error, "Kafka worker sent event to deadletter");
                Ok(ConsumerHandlerStatus::DeadLettered)
            }
        }
    }
}

pub fn spawn_kafka_workers(state: AppState) {
    #[cfg(feature = "external-integrations")]
    kafka::spawn(state);

    #[cfg(not(feature = "external-integrations"))]
    let _ = state;
}
