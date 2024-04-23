use rdkafka::{
    config::RDKafkaLogLevel,
    message::{BorrowedMessage, Headers, Message},
};
use serde_json::json;

#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub enum FormatHint {
    Json,
    None,
}
impl From<&str> for FormatHint {
    fn from(hint: &str) -> Self {
        match hint {
            "json" => FormatHint::Json,
            _ => FormatHint::None,
        }
    }
}

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

pub fn print_message(m: &BorrowedMessage, verbosity: &Verbosity, format_hint: &Option<FormatHint>) {
    let payload = match m.payload_view::<str>() {
        None => "",
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            eprintln!("Error while deserializing message payload: {:?}", e);
            ""
        }
    };

    let payload = match format_hint.as_ref().unwrap_or(&FormatHint::None) {
        FormatHint::Json => serde_json::from_str(payload).unwrap_or_else(|_e| {
            match verbosity {
                Verbosity::Silent => {}
                _ => {
                    eprintln!("Error parsing json string: {payload}");
                }
            };
            serde_json::Value::Null
        }),
        _ => serde_json::Value::String(payload.into()),
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

pub fn kafka_debug_from_int(level: u8) -> RDKafkaLogLevel {
    match level {
        0 => RDKafkaLogLevel::Emerg,
        1 => RDKafkaLogLevel::Alert,
        2 => RDKafkaLogLevel::Critical,
        3 => RDKafkaLogLevel::Error,
        4 => RDKafkaLogLevel::Warning,
        5 => RDKafkaLogLevel::Notice,
        6 => RDKafkaLogLevel::Info,
        _ => RDKafkaLogLevel::Debug,
    }
}
