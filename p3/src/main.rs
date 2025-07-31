mod args;
mod block_engine;
mod convert;
mod p3_quic;
mod rpc;

use {
    args::Args,
    block_engine::{
        auth_interceptor::AuthInterceptor,
        auth_service::{AuthServiceImpl, ValidatorAuther},
        block_engine::BlockEngineImpl,
    },
    clap::{CommandFactory, Parser},
    crossbeam_channel::bounded,
    jito_protos::proto::{
        auth::auth_service_server::AuthServiceServer,
        block_engine::block_engine_validator_server::BlockEngineValidatorServer,
    },
    jwt::{AlgorithmType, PKeyWithDigest},
    openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa},
    rpc::load_balancer::LoadBalancer,
    solana_sdk::{
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair},
        signer::Signer,
    },
    std::{
        sync::{atomic::AtomicBool, Arc},
        time::Duration,
    },
    tonic::transport::Server,
    tracing::{error, info, warn},
};

impl ValidatorAuther for () {
    fn is_authorized(&self, _pubkey: &Pubkey) -> bool {
        true
    }
}

#[tokio::main]
async fn main() {
    // Parse .env if it exists (and before args in case args want to read
    // environment).
    match dotenvy::dotenv() {
        Ok(_) | Err(dotenvy::Error::Io(_)) => {}
        Err(err) => panic!("Failed to parse .env file; err={err}"),
    }

    // Parse command-line arguments.
    let args = Args::parse();

    // If user is requesting completions, return them and exit.
    if let Some(shell) = args.completions {
        clap_complete::generate(shell, &mut Args::command(), "p3", &mut std::io::stdout());

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

    // Supporting IPV6 addresses is a DOS vector since they are cheap and there's a much larger amount of them.
    // The DOS is specifically with regards to the challenges queue filling up and starving other legitimate
    // challenge requests.
    assert!(args.grpc_bind_ip.is_ipv4(), "must bind to IPv4 address");

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

    solana_metrics::set_host_id(format!(
        "{}_{}",
        hostname::get().unwrap().to_str().unwrap(), // hostname should follow RFC1123
        keypair.pubkey()
    ));

    // Setup RPC load balancer for slot updates
    let servers = args
        .rpc_servers
        .into_iter()
        .zip(args.websocket_servers)
        .collect::<Vec<_>>();

    let exit = Arc::new(AtomicBool::new(false));

    let rpc_load_balancer = LoadBalancer::new(&servers, &exit);

    let rpc_load_balancer = Arc::new(rpc_load_balancer);

    // Create packet forwarding channel - broadcast so all validators get all packets
    let (p3_packet_tx, p3_packet_rx) = bounded(LoadBalancer::SLOT_QUEUE_CAPACITY);

    let (p3_handle, _key_updaters) = p3_quic::P3Quic::spawn(
        exit.clone(),
        p3_packet_tx,
        rpc_load_balancer.clone().rpc_client().clone(),
        &keypair,
        (args.p3_addr, args.p3_mev_addr),
    );

    // Generate server keypairs.
    let private_key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let public_key = PKey::public_key_from_der(&private_key.public_key_to_der().unwrap()).unwrap();
    let signing_key = PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: private_key,
    };
    let verifying_key = Arc::new(PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: public_key,
    });

    // Create BlockEngine service
    let (block_engine_svc, block_engine_handle) =
        BlockEngineImpl::new(p3_packet_rx, keypair.pubkey(), exit.clone());

    let auth_svc = AuthServiceImpl::new(
        (),
        signing_key,
        verifying_key.clone(),
        Duration::from_secs(args.access_token_ttl_secs),
        Duration::from_secs(args.refresh_token_ttl_secs),
        Duration::from_secs(args.challenge_ttl_secs),
        Duration::from_secs(args.challenge_expiration_sleep_interval_secs),
        &exit,
    );

    let server_addr = args.grpc_bind_ip;

    Server::builder()
        .add_service(BlockEngineValidatorServer::with_interceptor(
            block_engine_svc,
            AuthInterceptor::new(verifying_key.clone(), AlgorithmType::Rs256),
        ))
        .add_service(AuthServiceServer::new(auth_svc))
        .serve_with_shutdown(server_addr, shutdown_signal(exit.clone()))
        .await
        .expect("serve BlockEngine server");

    if let Err(e) = p3_handle.join() {
        error!("P3 QUIC server panicked: {:?}", e);
    }

    if let Err(e) = block_engine_handle.join() {
        error!("Block engine thread panicked: {:?}", e);
    }
}

pub async fn shutdown_signal(exit: Arc<AtomicBool>) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    exit.store(true, std::sync::atomic::Ordering::Relaxed);
    warn!("signal received, starting graceful shutdown");
}
