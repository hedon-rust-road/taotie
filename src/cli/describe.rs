use clap::{ArgMatches, Parser};

use crate::ReplContext;

use super::{ReplCommand, ReplResult};

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(help = "The name of the dataset")]
    pub name: String,
}

pub fn describe(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let cmd = DescribeOpts::new(name).into();
    ctx.send(cmd);
    Ok(None)
}

impl From<DescribeOpts> for ReplCommand {
    fn from(value: DescribeOpts) -> Self {
        ReplCommand::Describe(value)
    }
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}
