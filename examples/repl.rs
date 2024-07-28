use std::collections::HashMap;

use clap::{ArgAction, ArgMatches, Parser, Subcommand};
use reedline_repl_rs::{CallBackMap, Repl, Result};

#[derive(Parser, Debug)]
#[command(name = "MyApp", version = "0.1.0", about = "My very cool app")]
pub struct MyApp {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Say {
        #[command(subcommand)]
        command: SayCommands,
    },
}

#[derive(Debug, Subcommand)]
pub enum SayCommands {
    Hello {
        #[arg(required = true)]
        who: String,
        uppercase: String,
    },
    Goodbye {
        #[arg(long, action(ArgAction::SetTrue))]
        spanish: bool,
    },
}

fn say<T>(args: ArgMatches, _context: &mut T) -> Result<Option<String>> {
    match args.subcommand() {
        Some(("hello", args)) => Ok(Some(format!(
            "Hello, {}!",
            args.get_one::<String>("who").unwrap()
        ))),
        Some(("goodbye", args)) => {
            let spanish = args.get_flag("spanish");
            Ok(Some(format!(
                "Goodbye, {}!",
                if spanish { "adios" } else { "bye" }
            )))
        }
        _ => panic!("Unknown subcommand: {:?}", args.subcommand_name()),
    }
}

fn main() -> Result<()> {
    let mut callbacks: CallBackMap<(), reedline_repl_rs::Error> = HashMap::new();

    callbacks.insert("say".to_string(), say);

    let mut repl = Repl::new(())
        .with_banner("Welcome to MyApp")
        .with_derived::<MyApp>(callbacks);

    repl.run()
}
