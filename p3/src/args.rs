use clap::{Parser, ValueHint};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Debug, Parser)]
#[command(version = toolbox::version!(), long_version = toolbox::long_version!())]
pub(crate) struct Args {
    /// P3 QUIC server address for regular packets
    #[clap(long, default_value = "127.0.0.1:4819")]
    pub(crate) p3_addr: SocketAddr,

    /// P3 QUIC server address for MEV packets
    #[clap(long, default_value = "127.0.0.1:4820")]
    pub(crate) p3_mev_addr: SocketAddr,

    /// RPC URL to connect to Solana validator for chain state
    #[clap(long, value_hint = ValueHint::Url, default_value = "http://127.0.0.1:8899")]
    pub(crate) rpc_url: String,

    /// Identity keypair file path
    #[clap(long, value_hint = ValueHint::FilePath)]
    pub(crate) identity_keypair: Option<PathBuf>,

    /// Block engine Grpc server address
    #[arg(long, default_value = "127.0.0.1:5999")]
    pub(crate) grpc_bind_ip: SocketAddr,

    /// Generate completions for provided shell.
    #[arg(long, value_name = "SHELL")]
    pub(crate) completions: Option<clap_complete::Shell>,

    /// If provided, will write hourly log files to this directory.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub(crate) logs: Option<PathBuf>,
}
