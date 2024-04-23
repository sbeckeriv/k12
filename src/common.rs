use rdkafka::{
    config::RDKafkaLogLevel,
    message::{BorrowedHeaders, BorrowedMessage, Headers, Message},
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

#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub enum Format {
    Json,
    Raw,
}
impl From<&str> for Format {
    fn from(value: &str) -> Self {
        match value {
            "json" => Self::Json,
            _ => Self::Raw,
        }
    }
}
pub struct FormatConfig {
    pub verbosity: Verbosity,
    pub format_hint: Option<FormatHint>,
    pub format: Format,
}

pub fn print_message(m: &BorrowedMessage, format_config: &FormatConfig) {
    let payload = match m.payload_view::<str>() {
        None => "",
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            eprintln!("Error while deserializing message payload: {:?}", e);
            ""
        }
    };

    let display = DataDisplay {
        key: m.key(),
        topic: m.topic(),
        partition: m.partition(),
        offset: m.offset(),
        timestamp: m.timestamp().to_millis().unwrap_or_default(),
        headers: m.headers(),
        payload,
        format_config,
    };
    display.print()
}

struct DataDisplay<'a> {
    key: Option<&'a [u8]>,
    topic: &'a str,
    partition: i32,
    offset: i64,
    timestamp: i64,
    payload: &'a str,
    headers: Option<&'a BorrowedHeaders>,
    format_config: &'a FormatConfig,
}
impl<'a> DataDisplay<'a> {
    pub fn print(&self) {
        match self.format_config.format {
            Format::Json => self.as_json(),
            Format::Raw => self.as_raw(),
        }
    }
    pub fn as_raw(&self) {
        match self.format_config.verbosity {
            Verbosity::TooMuch => {
                println!(
                    "key:'{:?}', topic:'{}', partition:{}, offset:{}, timestamp:{}, payload:{}",
                    self.key, self.topic, self.partition, self.offset, self.timestamp, self.payload
                );

                if let Some(headers) = self.headers {
                    for header in headers.iter() {
                        println!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
            }
            Verbosity::Silent => println!("{}", self.payload),
            Verbosity::Soft => println!("{} - {}", self.offset, self.payload),
            Verbosity::Loud => println!("{} - {} - {}", self.topic, self.offset, self.payload),
        }
    }
    pub fn as_json(&self) {
        let payload = match self
            .format_config
            .format_hint
            .as_ref()
            .unwrap_or(&FormatHint::None)
        {
            FormatHint::Json => serde_json::from_str(self.payload).unwrap_or_else(|_e| {
                match self.format_config.verbosity {
                    Verbosity::Silent => {}
                    _ => {
                        eprintln!("Error parsing json string: {}", self.payload);
                    }
                };
                serde_json::Value::Null
            }),
            _ => serde_json::Value::String(self.payload.into()),
        };

        let json = json!({
            "timestamp": self.timestamp,
            "topic": self.topic,
            "message": payload
        });
        println!("{json}");
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
