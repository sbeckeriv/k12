use clap::value_t;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::BaseConsumer;
use rdkafka::producer::FutureProducer;
use rdkafka::util::get_rdkafka_version;
use std::env;
use std::io::Read;
use std::time::Duration;

mod action;
mod cli;
mod common;

use common::{kafka_debug_from_int, Format, FormatConfig, FormatHint, Verbosity};

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
    let format_hint: Option<FormatHint> =
        matches.value_of("format-hint").map(|format| format.into());

    let format: Format = matches
        .value_of("format")
        .map(|format| format.into())
        .unwrap_or_else(|| Format::Json);

    let format_config = FormatConfig {
        format_hint,
        verbosity,
        format,
    };

    match matches.subcommand() {
        ("write", Some(matches)) => {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("message.timeout.ms", "5000")
                .set("message.timeout.ms", format!("{}", timeout.as_millis()))
                .create()
                .expect("Producer creation error");
            let topic = matches.value_of("topic").unwrap_or_else(|| {
                eprintln!("topic is required");
                std::process::exit(1);
            });
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .expect("could not read stdin");
            let buf = buf.strip_suffix("\n").expect("could not read stdin");
            action::produce(producer, topic, &buf).await;
        }
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
            action::list(consumer, timeout, format_config.verbosity);
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

            action::read(consumer, format_config, timeout, &matches);
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
            action::tail(consumer, format_config, topics).await;
        }
        _ => unreachable!(),
    };
}
