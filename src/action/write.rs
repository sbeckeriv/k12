use rdkafka::producer::{FutureProducer, FutureRecord};
use std::{process::exit, time::Duration};

pub async fn produce(producer: FutureProducer, topic_name: &str, message: &str) {
    let record: FutureRecord<'_, str, str> = FutureRecord::to(topic_name).payload(message);
    let delivery_status = producer.send(record, Duration::from_secs(0)).await;
    match delivery_status {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Could not write message:{:?}, error:{}", e.1, e.0);
            exit(1)
        }
    }
}
