use clap::ArgMatches;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};

use crate::common::{print_message, FormatHint, Verbosity};
pub async fn tail(
    consumer: StreamConsumer,
    verbosity: Verbosity,
    topics: Vec<&str>,
    matches: &ArgMatches<'static>,
) {
    let format_hint: Option<FormatHint> =
        matches.value_of("format-hint").map(|format| format.into());
    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");
    loop {
        match consumer.recv().await {
            Err(e) => eprint!("Kafka error: {}", e),
            Ok(m) => {
                print_message(&m, &verbosity, &format_hint);
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}
