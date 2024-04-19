use rdkafka::message::{BorrowedMessage, Headers, Message};
use serde_json::json;

#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub enum Verbosity {
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

pub fn print_message(m: &BorrowedMessage, verbosity: &Verbosity) {
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
