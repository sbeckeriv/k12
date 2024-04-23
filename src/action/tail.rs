use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};

use crate::common::{print_message, FormatConfig};
pub async fn tail(consumer: StreamConsumer, format_config: FormatConfig, topics: Vec<&str>) {
    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");
    loop {
        match consumer.recv().await {
            Err(e) => eprint!("Kafka error: {}", e),
            Ok(m) => {
                print_message(&m, &format_config);
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}
