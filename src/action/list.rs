use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;

use crate::common::Verbosity;

pub fn list(consumer: BaseConsumer, timeout: Duration, verbosity: Verbosity) {
    let metadata = consumer
        .fetch_metadata(None, timeout)
        .unwrap_or_else(|err| {
            eprintln!("Failed to fetch metadata: {}", err);
            std::process::exit(1);
        });

    if verbosity >= Verbosity::Loud {
        println!("Cluster information:");
        println!("  Broker count: {}", metadata.brokers().len());
        println!("  Topics count: {}", metadata.topics().len());
        println!("  Metadata broker name: {}", metadata.orig_broker_name());
        println!("  Metadata broker id: {}\n", metadata.orig_broker_id());
    }

    println!("\nTopics:");
    for topic in metadata.topics() {
        print!("  {}", topic.name());

        if topic.error().is_some() {
            print!(" Err: {:?}", topic.error());
        }
        println!("");
        if verbosity >= Verbosity::Loud {
            for partition in topic.partitions() {
                println!(
                    "    Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                    partition.id(),
                    partition.leader(),
                    partition.replicas(),
                    partition.isr(),
                    partition.error()
                );
            }
        }
    }

    std::process::exit(0);
}
