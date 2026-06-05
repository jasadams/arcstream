use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use std::collections::VecDeque;
use tokio::sync::broadcast;
use futures_util::StreamExt;

use super::types::LiveEventMessage;

const FLUSH_INTERVAL_MS: u64 = 100;
const MAX_EVENTS_PER_FLUSH: usize = 5;

pub async fn run(
    sender: broadcast::Sender<LiveEventMessage>,
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
        .set("queued.max.messages.kbytes", "65536")
        .set("fetch.message.max.bytes", "1048576")
        .create()
        .expect("failed to create Kafka consumer");

    consumer
        .subscribe(&[topic])
        .expect("failed to subscribe to topic");

    eprintln!("Event stream consumer started on topic: {topic}");

    let mut stream = consumer.stream();
    let mut pending: VecDeque<LiveEventMessage> = VecDeque::new();
    let mut last_flush = tokio::time::Instant::now();

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                if sender.receiver_count() > 0 {
                    if let Some(payload) = msg.payload() {
                        if let Ok(event) = serde_json::from_slice::<LiveEventMessage>(payload) {
                            if pending.len() < MAX_EVENTS_PER_FLUSH {
                                pending.push_back(event);
                            }
                        }
                    }

                    if last_flush.elapsed().as_millis() >= FLUSH_INTERVAL_MS as u128 {
                        for event in pending.drain(..) {
                            let _ = sender.send(event);
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
