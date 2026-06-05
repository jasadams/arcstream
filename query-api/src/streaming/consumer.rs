use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use std::collections::HashMap;
use tokio::sync::broadcast;
use futures_util::StreamExt;

use super::types::{FlatProfileUpdate, ProfileUpdateMessage};

const FLUSH_INTERVAL_MS: u64 = 100;

pub async fn run(
    sender: broadcast::Sender<ProfileUpdateMessage>,
    brokers: &str,
    group_id: &str,
    topic: &str,
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .set("fetch.wait.max.ms", "500")
        .set("fetch.min.bytes", "1024")
        .set("queued.max.messages.kbytes", "4096")
        .set("fetch.message.max.bytes", "524288")
        .create()
        .expect("failed to create Kafka consumer");

    consumer
        .subscribe(&[topic])
        .expect("failed to subscribe to topic");

    eprintln!("Profile update consumer started on topic: {topic}");

    let mut stream = consumer.stream();
    let mut pending: HashMap<String, ProfileUpdateMessage> = HashMap::new();
    let mut last_flush = tokio::time::Instant::now();

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                if sender.receiver_count() > 0 {
                    if let Some(payload) = msg.payload() {
                        if let Ok(flat) = serde_json::from_slice::<FlatProfileUpdate>(payload) {
                            let update = flat.into_message();
                            pending.insert(update.canonical_id.clone(), update);
                        }
                    }

                    if last_flush.elapsed().as_millis() >= FLUSH_INTERVAL_MS as u128 {
                        for (_, update) in pending.drain() {
                            let _ = sender.send(update);
                        }
                        last_flush = tokio::time::Instant::now();
                    }
                }
                if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                    eprintln!("Failed to commit offset: {e}");
                }
            }
            Err(e) => {
                eprintln!("Kafka consumer error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
