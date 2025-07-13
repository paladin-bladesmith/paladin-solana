mod args;
mod p3_quic;

use {
    crossbeam_channel::{unbounded, Receiver, Sender},
    solana_perf::packet::PacketBatch,
    solana_rpc_client::rpc_client::RpcClient,
    solana_sdk::{
        signature::{read_keypair_file, Keypair},
        signer::Signer,
    },
    std::sync::{atomic::AtomicBool, Arc},
    tonic::transport::Server,
    tracing::{error, info, warn},
};

#[tokio::main]
async fn main() {
    use {
        clap::{CommandFactory, Parser},
        tokio::signal::unix::SignalKind,
        tracing::{error, info},
    };

    // Parse .env if it exists (and before args in case args want to read
    // environment).
    match dotenvy::dotenv() {
        Ok(_) | Err(dotenvy::Error::Io(_)) => {}
        Err(err) => panic!("Failed to parse .env file; err={err}"),
    }

    // Parse command-line arguments.
    let args = args::Args::parse();

    // If user is requesting completions, return them and exit.
    if let Some(shell) = args.completions {
        clap_complete::generate(
            shell,
            &mut args::Args::command(),
            "p3",
            &mut std::io::stdout(),
        );

        return;
    }

    // Setup tracing.
    let _log_guard = toolbox::tracing::setup_tracing("p3", args.logs.as_deref());

    // Log build information (as soon as possible).
    toolbox::log_build_info!();

    // Setup standard panic handling.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        error!(?panic_info, "Application panic");

        default_panic(panic_info);
    }));

    // Load the identity keypair
    let keypair = if let Some(keypair_path) = args.identity_keypair {
        match read_keypair_file(&keypair_path) {
            Ok(keypair) => Arc::new(keypair),
            Err(e) => {
                error!(
                    "Failed to read identity keypair from {:?}: {}",
                    keypair_path, e
                );
                return;
            }
        }
    } else {
        info!("No identity keypair provided, generating a new one");
        Arc::new(Keypair::new())
    };

    info!("P3 Identity: {}", keypair.pubkey());

    // Create RPC client
    let rpc_client = Arc::new(RpcClient::new(args.rpc_url.clone()));

    // Test RPC connection
    match rpc_client.get_health() {
        Ok(_) => info!("Successfully connected to RPC at {}", args.rpc_url),
        Err(e) => {
            warn!(
                "Failed to connect to RPC at {}: {}. Continuing anyway...",
                args.rpc_url, e
            );
        }
    }

    // Create packet forwarding channel
    let (packet_tx, packet_rx): (Sender<PacketBatch>, Receiver<PacketBatch>) = unbounded();

    // Setup exit signal
    let exit = Arc::new(AtomicBool::new(false));

    info!(
        "Starting P3 QUIC servers on {} and {}",
        args.p3_addr, args.p3_mev_addr
    );
    let packet_rx = Arc::new(packet_rx);
    let (p3_handle, _key_updaters) = p3_quic::P3Quic::spawn(
        exit.clone(),
        packet_tx,
        rpc_client.clone(),
        &keypair,
        (args.p3_addr, args.p3_mev_addr),
    );

    // Create cancellation token
    let cxl = tokio_util::sync::CancellationToken::new();
    let cxl_child = cxl.clone();
    let mut handle = tokio::spawn(async move { cxl_child.cancelled().await });

    // Wait for server exit or SIGTERM/SIGINT.
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            res.expect("Failed to register SIGINT hook");

            info!("SIGINT caught, stopping server");
            cxl.cancel();

            handle.await.unwrap();
        }
        _ = sigterm.recv() => info!("SIGTERM caught, stopping server"),
        _ = sigint.recv() => info!("SIGINT caught, stopping server"),
        res = &mut handle => {
            res.unwrap();
        }
    }
}
