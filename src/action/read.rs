use chrono::{DateTime, Utc};
use chrono_english::{parse_date_string, Dialect};
use clap::ArgMatches;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::Timeout;
use rdkafka::Offset;
use std::time::Duration;

use crate::common::{print_message, FormatHint, Verbosity};

fn parse_datetime(datetime_str: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(datetime_str)
        .map(|datetime| datetime.with_timezone(&Utc))
        .map_err(|err| format!("{}", err))
}

pub fn read(
    consumer: BaseConsumer,
    verbosity: Verbosity,
    timeout: Duration,
    matches: &ArgMatches<'static>,
) {
    let topic = matches.value_of("topic").unwrap_or_else(|| {
        eprintln!("topic is required");
        std::process::exit(1);
    });
    let format_hint: Option<FormatHint> =
        matches.value_of("format-hint").map(|format| format.into());
    consumer
        .subscribe(&[topic])
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
    if partitions.is_empty() {
        eprintln!("No partitions found for {topic}.");
        std::process::exit(1);
    }

    let mut tpl = TopicPartitionList::with_capacity(partitions.len());
    for partition in partitions {
        tpl.add_partition(topic, partition.id());
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
                    print_message(&m, &verbosity, &format_hint);
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
