use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::sync::broadcast;
use futures_util::StreamExt;

use super::types::{FlatProfileUpdate, ProfileUpdateMessage};

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
        .set("queued.max.messages.kbytes", "65536")
        .set("fetch.message.max.bytes", "1048576")
        .create()
        .expect("failed to create Kafka consumer");

    consumer
        .subscribe(&[topic])
        .expect("failed to subscribe to topic");

    eprintln!("Profile update consumer started on topic: {topic}");

    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                if sender.receiver_count() > 0 {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<FlatProfileUpdate>(payload) {
                            Ok(flat) => {
                                let _ = sender.send(flat.into_message());
                            }
                            Err(e) => {
                                eprintln!("Failed to deserialize profile update: {e}");
                            }
                        }
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
