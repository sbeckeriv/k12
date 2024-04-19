use std::any::Any;
use std::env;
use std::fmt::Debug;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use chrono_english::{parse_date_string, Dialect};
use clap::{value_t, App, Arg, ArgMatches, SubCommand};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::{get_rdkafka_version, Timeout};
use rdkafka::Offset;
use serde_json::json;

fn app() -> App<'static, 'static> {
    App::new("k2")
        .version("1.0")
        .author("Your Name")
        .about("A command-line application with subcommands and optional parameters")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092")
                .global(true),
        )
        .arg(
            Arg::with_name("verbose")
                .global(true)
                .short("v")
                .multiple(true)
                .help("Increase verbosity level"),
        )
        .arg(
            Arg::with_name("timeout")
                .global(true)
                .long("timeout")
                .help("Metadata fetch timeout in milliseconds")
                .takes_value(true)
                .default_value("10000"),
        )
        .subcommand(SubCommand::with_name("list").about("List items"))
        .subcommand(
            SubCommand::with_name("read")
                .about("Read an item")
                .arg(
                    Arg::with_name("topic")
                        .long("topic")
                        .multiple(true)
                        .help("Only fetch the metadata of the specified topic")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("start")
                        .long("start")
                        .value_name("DATETIME")
                        .help("Start datetime (YYYY-MM-DDTHH:MM:SS+ZZ:ZZ)")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("end")
                        .long("end")
                        .value_name("DATETIME")
                        .help("End datetime (YYYY-MM-DDTHH:MM:SS+ZZ:ZZ)")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("offset")
                        .long("offset")
                        .value_name("NUMBER")
                        .help("Offset (non-zero number)")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("start-offset")
                        .long("start-offset")
                        .value_name("OFFSET")
                        .help("Start offset (e.g., '1 hour ago', '2 days later')")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("end-offset")
                        .long("end-offset")
                        .value_name("OFFSET")
                        .help("End offset (e.g., '30 minutes ago', '4 months from now')")
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("tail").about("Tail items").arg(
                Arg::with_name("topic")
                    .long("topic")
                    .multiple(true)
                    .help("tail the specified topic")
                    .takes_value(true),
            ),
        )
}

fn parse_datetime(datetime_str: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(datetime_str)
        .map(|datetime| datetime.with_timezone(&Utc))
        .map_err(|err| format!("{}", err))
}
#[derive(Debug, PartialEq, Eq, PartialOrd)]
enum Verbosity {
    Silent,
    Soft,
    Loud,
    TooMuch,
}

impl From<u8> for Verbosity {
    fn from(level: u8) -> Self {
        match level {
            0 => Verbosity::Silent,
            1 => Verbosity::Soft,
            2 => Verbosity::Loud,
            _ => Verbosity::TooMuch,
        }
    }
}
#[tokio::main]
async fn main() {
    let matches = app().get_matches();
    let verbosity = Verbosity::from(matches.occurrences_of("verbose") as u8);
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
                .set("bootstrap.servers", brokers)
                .create()
                .unwrap_or_else(|err| {
                    eprintln!(
                        "Could not create consumer from broker list {} : {}",
                        brokers, err
                    );
                    std::process::exit(1);
                });
            list(consumer, timeout, verbosity);
        }
        ("read", Some(matches)) => {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("group.id", "example")
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

            read(consumer, verbosity, timeout, matches);
        }
        ("tail", Some(match_list)) => {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", "example")
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
            tail(consumer, verbosity, timeout, topics).await;
        }
        _ => unreachable!(),
    };
}
fn read(
    consumer: BaseConsumer,
    verbosity: Verbosity,
    timeout: Duration,
    matches: &ArgMatches<'static>,
) {
    let topic = matches.value_of("topic").unwrap_or_else(|| {
        eprintln!("topic is required");
        std::process::exit(1);
    });

    consumer
        .subscribe(&vec![topic])
        .expect("Can't subscribe to specified topics");

    let now = Utc::now();
    let start_datetime = matches.value_of("start").map(|start| {
        parse_datetime(start).unwrap_or_else(|err| {
            eprintln!("Invalid start datetime: {}", err);
            std::process::exit(1);
        })
    });
    let end_datetime = matches.value_of("end").map(|end| {
        parse_datetime(end).unwrap_or_else(|err| {
            eprintln!("Invalid end datetime: {}", err);
            std::process::exit(1);
        })
    });

    let offset = matches.value_of("offset").map(|offset| {
        offset.parse::<i64>().unwrap_or_else(|err| {
            eprintln!("Invalid offset: {}", err);
            std::process::exit(1);
        })
    });

    let start_offset = matches.value_of("start-offset").map(|offset| {
        parse_date_string(offset, now, Dialect::Us).unwrap_or_else(|err| {
            eprintln!("Invalid start offset: {}", err);
            std::process::exit(1);
        })
    });

    let end_offset = matches.value_of("end-offset").map(|offset| {
        parse_date_string(offset, now, Dialect::Us).unwrap_or_else(|err| {
            eprintln!("Invalid end offset: {}", err);
            std::process::exit(1);
        })
    });
    match (
        start_datetime,
        end_datetime,
        offset,
        start_offset,
        end_offset,
    ) {
        (Some(_), None, _, Some(_), None) => {
            eprintln!("Invalid set of params: Only start-offset or start can be set");
            std::process::exit(1);
        }

        (None, Some(_), _, None, Some(_)) => {
            eprintln!("Invalid set of params: Only end-offset or end can be set");
            std::process::exit(1);
        }
        (Some(_), Some(_), None, None, None)
        | (Some(_), None, None, None, None)
        | (None, None, Some(_), None, None)
        | (None, None, None, Some(_), None)
        | (None, None, None, Some(_), Some(_)) => {
            //these are the only valid cases
        }
        _ => {
            eprintln!("Invalid set of params");
            std::process::exit(1);
        }
    }

    let end_time = end_datetime
        .or(end_offset)
        .or(Some(now))
        .expect("end_time should always be set")
        .timestamp_millis();
    let start_time = start_datetime.or(start_offset);
    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .expect("metatdata could not be loaded");

    let metadata_topics = metadata.topics();
    let meta_topic = &metadata_topics[0];
    let partitions = meta_topic.partitions();
    if partitions.len() == 0 {
        eprintln!("No partitions found for {topic}.");
        std::process::exit(1);
    }

    let mut tpl = TopicPartitionList::with_capacity(partitions.len());
    for partition in partitions {
        tpl.add_partition(&topic, partition.id());
    }

    let tpl = if let Some(start_time) = start_time {
        tpl.set_all_offsets(Offset::Offset(start_time.timestamp_millis()))
            .expect("cannot set time offsets");

        let mut tpl = consumer.offsets_for_times(tpl, timeout).expect("msg");
        tpl.set_all_offsets(Offset::Offset(start_time.timestamp_millis()))
            .expect("cannot set time offsets");
        consumer
            .offsets_for_times(tpl, Timeout::Never)
            .expect("offsets_for_times failed to set")
    } else if let Some(offset) = offset {
        tpl.set_all_offsets(Offset::OffsetTail(offset))
            .expect("set all offsets error");
        tpl
    } else {
        unreachable!("Offset or start date should have been set! ")
    };

    consumer.assign(&tpl).expect("msg");
    consumer
        .seek_partitions(tpl, timeout)
        .unwrap_or_else(|err| {
            eprintln!("error with seek partition: {:?}", err);
            std::process::exit(1);
        });
    let watermarks = consumer
        .fetch_watermarks(topic, partitions.first().expect("a parition").id(), timeout)
        .unwrap_or_else(|err| {
            eprintln!("error partition watermark: {:?}", err);
            std::process::exit(1);
        });

    let mut message_read = false;
    loop {
        let message = consumer.poll(timeout);
        if let Some(m) = message {
            match m {
                Err(e) => eprint!("Kafka error: {}", e),
                Ok(m) => {
                    message_read = true;
                    if m.timestamp()
                        .to_millis()
                        .map(|t| t > end_time)
                        .unwrap_or(false)
                    {
                        std::process::exit(0);
                    }
                    print_message(&m, &verbosity);
                    //consumer.commit_message(&m, CommitMode::Async).unwrap();
                    let consumed_offset = m.offset();
                    if consumed_offset >= watermarks.1 - 1 {
                        break;
                    }
                }
            }
        } else {
            // only exit if nothing read we might still be waiting for data.
            if !message_read {
                eprintln!("Polling timed out no messages read.");
                std::process::exit(1);
            }
        }
    }
    // dont bother with destructors.
    std::process::exit(0);
}

fn print_message(m: &BorrowedMessage, verbosity: &Verbosity) {
    let payload = match m.payload_view::<str>() {
        None => "",
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            eprintln!("Error while deserializing message payload: {:?}", e);
            ""
        }
    };

    match verbosity {
        Verbosity::TooMuch => println!(
            "key:'{:?}', topic:'{}', partition:{}, offset:{}, timestamp:{}, payload:{payload}",
            m.key(),
            m.topic(),
            m.partition(),
            m.offset(),
            m.timestamp().to_millis().unwrap_or_default()
        ),
        _ => {
            let json = json!({
                "timestamp": m.timestamp().to_millis().unwrap_or_default(),
                "topic": m.topic(),
                "message": payload
            });
            println!("{json}");
        }
    }

    if verbosity == &Verbosity::TooMuch {
        if let Some(headers) = m.headers() {
            for header in headers.iter() {
                println!("  Header {:#?}: {:?}", header.key, header.value);
            }
        }
    }
}

async fn tail(
    consumer: StreamConsumer,
    verbosity: Verbosity,
    timeout: Duration,
    topics: Vec<&str>,
) {
    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");
    loop {
        match consumer.recv().await {
            Err(e) => eprint!("Kafka error: {}", e),
            Ok(m) => {
                print_message(&m, &verbosity);
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

fn list(consumer: BaseConsumer, timeout: Duration, verbosity: Verbosity) {
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
