use clap::{App, Arg, SubCommand};

pub fn app() -> App<'static, 'static> {
    App::new("k2")
        .version("1.0")
        .author("Your Name")
        .about("A command-line application with subcommands and optional parameters")
        .arg(
            Arg::with_name("group")
                .short("g")
                .long("group")
                .help("Group id for client")
                .takes_value(true)
                .default_value("example")
                .global(true),
        )
        .arg(
            Arg::with_name("client_id")
                .short("cid")
                .long("client_id")
                .help("Client id to use. current user name or unknown is used by default.")
                .takes_value(true)
                .global(true),
        )
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
                .help("kafka timeout in milliseconds")
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
