use std::path::PathBuf;

use clap::{Parser, ValueHint};

#[derive(Debug, Parser)]
#[command(version = toolbox::version!(), long_version = toolbox::long_version!())]
pub(crate) struct Args {
    /// Arguments for your app.
    #[clap(long, value_hint = ValueHint::Url, default_value = "tcp://127.0.0.1:1337")]
    pub(crate) important_value: String,
    /// Generate completions for provided shell.
    #[arg(long, value_name = "SHELL")]
    pub(crate) completions: Option<clap_complete::Shell>,

    /// If provided, will write hourly log files to this directory.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub(crate) logs: Option<PathBuf>,
}
