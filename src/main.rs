use clap::value_t;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::BaseConsumer;
use rdkafka::util::get_rdkafka_version;
use std::env;
use std::time::Duration;

mod action;
mod cli;
mod common;

use common::{kafka_debug_from_int, Verbosity};

#[tokio::main]
async fn main() {
    let matches = cli::app().get_matches();
    let verbosity = Verbosity::from(matches.occurrences_of("verbose") as u8);
    let debug_level = kafka_debug_from_int(matches.occurrences_of("debug") as u8);
    let group = matches.value_of("group");
    let kafka_client_id = matches.value_of("client-id").map(ToString::to_string);
    let brokers = matches
        .value_of("brokers")
        .expect("Brokers in kaf)ka format");
    let timeout = value_t!(matches, "timeout", u64).unwrap();
    let timeout = Duration::from_millis(timeout);

    let user_id = env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    let kafka_client_id: String = kafka_client_id
        .or_else(|| {
            Some(
                user_id
                    .chars()
                    .map(|c| match c {
                        'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => c.to_string(),
                        _ => format!("_{}", c as u8),
                    })
                    .collect(),
            )
        })
        .unwrap();
    let group = group
        .map(ToString::to_string)
        .or_else(|| Some(format!("{kafka_client_id}_group")))
        .expect("Group id set");
    if verbosity >= Verbosity::TooMuch {
        let (version_n, version_s) = get_rdkafka_version();
        println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
    }
    match matches.subcommand() {
        ("list", Some(_)) => {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("group.id", group)
                .set("client.id", kafka_client_id)
                .set("bootstrap.servers", brokers)
                .set_log_level(debug_level)
                .create()
                .unwrap_or_else(|err| {
                    eprintln!(
                        "Could not create consumer from broker list {} : {}",
                        brokers, err
                    );
                    std::process::exit(1);
                });
            action::list(consumer, timeout, verbosity);
        }
        ("read", Some(matches)) => {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("group.id", group)
                .set("client.id", kafka_client_id)
                .set("bootstrap.servers", brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", format!("{}", timeout.as_millis()))
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.offset.store", "false")
                .set_log_level(debug_level)
                .create()
                .unwrap_or_else(|err| {
                    eprintln!(
                        "Could not create consumer from broker list {} : {}",
                        brokers, err
                    );
                    std::process::exit(1);
                });

            action::read(consumer, verbosity, timeout, &matches);
        }
        ("tail", Some(match_list)) => {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", group)
                .set("client.id", kafka_client_id)
                .set("bootstrap.servers", brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", format!("{}", timeout.as_millis()))
                .set("enable.auto.commit", "false")
                .set_log_level(debug_level)
                .create()
                .unwrap_or_else(|err| {
                    eprintln!(
                        "Could not create consumer from broker list {} : {}",
                        brokers, err
                    );
                    std::process::exit(1);
                });
            let topics: Vec<&str> = match_list
                .values_of("topic")
                .or_else(|| {
                    eprintln!("No topic provided.",);
                    std::process::exit(1);
                })
                .expect("topics")
                .collect();
            action::tail(consumer, verbosity, topics, &matches).await;
        }
        _ => unreachable!(),
    };
}
