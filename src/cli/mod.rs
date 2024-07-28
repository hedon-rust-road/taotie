mod connect;

pub use self::connect::connect;

use clap::Parser;
use connect::ConnectOpts;

type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;

#[derive(Debug, Parser)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a database and register it to Taotie"
    )]
    Connect(ConnectOpts),
}
