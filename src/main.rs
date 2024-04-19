use clap::value_t;
use common::Verbosity;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::BaseConsumer;
use rdkafka::util::get_rdkafka_version;
use std::env;
use std::time::Duration;

mod action;
mod cli;
mod common;

#[tokio::main]
async fn main() {
    let matches = cli::app().get_matches();
    let verbosity = Verbosity::from(matches.occurrences_of("verbose") as u8);
    let group = matches.value_of("group").expect("group id for client");
    let brokers = matches
        .value_of("brokers")
        .expect("Brokers in kafka format");
    let timeout = value_t!(matches, "timeout", u64).unwrap();
    let timeout = Duration::from_millis(timeout);

    let user_id = env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    let kafka_client_id: String = user_id
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => c.to_string(),
            _ => format!("_{}", c as u8),
        })
        .collect();

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
                .create()
                .unwrap_or_else(|err| {
                    eprintln!(
                        "Could not create consumer from broker list {} : {}",
                        brokers, err
                    );
                    std::process::exit(1);
                });

            action::read(consumer, verbosity, timeout, matches);
        }
        ("tail", Some(match_list)) => {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", group)
                .set("client.id", kafka_client_id)
                .set("bootstrap.servers", brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", format!("{}", timeout.as_millis()))
                .set("enable.auto.commit", "false")
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
                .map(|t| t)
                .collect();
            action::tail(consumer, verbosity, topics).await;
        }
        _ => unreachable!(),
    };
}
