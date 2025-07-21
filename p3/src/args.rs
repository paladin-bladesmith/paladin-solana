use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, ValueHint};
use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Parser)]
#[command(version = toolbox::version!(), long_version = toolbox::long_version!())]
pub(crate) struct Args {
    /// P3 QUIC server address for regular packets
    #[clap(long, default_value = "127.0.0.1:4819")]
    pub(crate) p3_addr: SocketAddr,

    /// P3 QUIC server address for MEV packets
    #[clap(long, default_value = "127.0.0.1:4820")]
    pub(crate) p3_mev_addr: SocketAddr,

    /// RPC servers as a space-separated list. Shall be same position as websocket equivalent below
    #[arg(long, value_delimiter = ' ', default_value = "http://127.0.0.1:8899")]
    pub(crate) rpc_servers: Vec<String>,

    /// Websocket servers as a space-separated list. Shall be same position as RPC equivalent above
    #[arg(long, value_delimiter = ' ', default_value = "ws://127.0.0.1:8900")]
    pub(crate) websocket_servers: Vec<String>,

    /// Identity keypair file path
    #[clap(long, value_hint = ValueHint::FilePath)]
    pub(crate) identity_keypair: Option<PathBuf>,

    /// Block engine Grpc server address
    #[arg(long, default_value = "127.0.0.1:5999")]
    pub(crate) grpc_bind_ip: SocketAddr,

    /// How long it takes to miss a slot for the system to be considered unhealthy
    #[arg(long, default_value_t = 10)]
    pub(crate) missing_slot_unhealthy_secs: u64,

    /// Validators allowed to authenticate and connect to the relayer, comma separated.
    /// If null then all validators on the leader schedule shall be permitted.
    #[arg(long, value_delimiter = ',')]
    pub(crate) allowed_validators: Option<Vec<Pubkey>>,

    /// The private key used to sign tokens by this server.
    #[arg(long)]
    pub(crate) signing_key_pem_path: Option<PathBuf>,

    /// The public key used to verify tokens by this and other services.
    #[arg(long)]
    pub(crate) verifying_key_pem_path: Option<PathBuf>,

    /// Specifies how long access_tokens are valid for, expressed in seconds.
    #[arg(long, default_value_t = 1_800)]
    pub(crate) access_token_ttl_secs: u64,

    /// Specifies how long refresh_tokens are valid for, expressed in seconds.
    #[arg(long, default_value_t = 180_000)]
    pub(crate) refresh_token_ttl_secs: u64,

    /// Specifies how long challenges are valid for, expressed in seconds.
    #[arg(long, default_value_t = 1_800)]
    pub(crate) challenge_ttl_secs: u64,

    /// The interval at which challenges are checked for expiration.
    #[arg(long, default_value_t = 180)]
    pub(crate) challenge_expiration_sleep_interval_secs: u64,

    /// Generate completions for provided shell.
    #[arg(long, value_name = "SHELL")]
    pub(crate) completions: Option<clap_complete::Shell>,

    /// If provided, will write hourly log files to this directory.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub(crate) logs: Option<PathBuf>,
}
