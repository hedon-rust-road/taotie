use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(help = "The SQL Query")]
    pub query: String,
}

pub fn sql(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let query = args
        .get_one::<String>("query")
        .expect("expect sql query")
        .to_string();

    let cmd = SqlOpts::new(query).into();
    ctx.send(cmd);

    Ok(None)
}

impl From<SqlOpts> for ReplCommand {
    fn from(value: SqlOpts) -> Self {
        ReplCommand::Sql(value)
    }
}

impl SqlOpts {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}
