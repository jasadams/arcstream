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

    loop {
        if sender.receiver_count() == 0 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        }

        match tokio::time::timeout(
            std::time::Duration::from_millis(FLUSH_INTERVAL_MS),
            stream.next(),
        ).await {
            Ok(Some(Ok(msg))) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(flat) = serde_json::from_slice::<FlatProfileUpdate>(payload) {
                        let update = flat.into_message();
                        pending.insert(update.canonical_id.clone(), update);
                    }
                }
                let _ = consumer.commit_message(&msg, CommitMode::Async);

                if last_flush.elapsed().as_millis() >= FLUSH_INTERVAL_MS as u128 {
                    for (_, update) in pending.drain() {
                        let _ = sender.send(update);
                    }
                    last_flush = tokio::time::Instant::now();
                    tokio::task::yield_now().await;
                }
            }
            Ok(Some(Err(e))) => {
                eprintln!("Kafka consumer error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Ok(None) => break,
            Err(_) => {
                for (_, update) in pending.drain() {
                    let _ = sender.send(update);
                }
                last_flush = tokio::time::Instant::now();
            }
        }
    }
}
