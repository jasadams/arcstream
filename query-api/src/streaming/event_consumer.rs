use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::sync::broadcast;
use futures_util::StreamExt;

use super::types::LiveEventMessage;

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
        .create()
        .expect("failed to create Kafka consumer");

    consumer
        .subscribe(&[topic])
        .expect("failed to subscribe to topic");

    eprintln!("Event stream consumer started on topic: {topic}");

    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<LiveEventMessage>(payload) {
                        Ok(event) => {
                            let _ = sender.send(event);
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize event: {e}");
                        }
                    }
                }
                if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                    eprintln!("Failed to commit offset: {e}");
                }
            }
            Err(e) => {
                eprintln!("Kafka consumer error: {e}");
            }
        }
    }
}
