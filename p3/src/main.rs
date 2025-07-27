mod args;
mod block_engine;
mod convert;
mod p3_quic;
mod rpc;

use crate::block_engine::LeaderScheduleCacheUpdater;
use args::Args;
use block_engine::health_manager::HealthManager;
use block_engine::{
    auth_interceptor::AuthInterceptor,
    auth_service::{AuthServiceImpl, ValidatorAuther},
    block_engine::BlockEngineImpl,
    schedule_cache::LeaderScheduleUpdatingHandle,
};
use clap::{CommandFactory, Parser};
use crossbeam_channel::bounded;
use jito_protos::{
    proto::auth::auth_service_server::AuthServiceServer,
    proto::block_engine::block_engine_validator_server::BlockEngineValidatorServer,
};
use jwt::{AlgorithmType, PKeyWithDigest};
use openssl::{hash::MessageDigest, pkey::PKey};
use rpc::load_balancer::LoadBalancer;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{
    signature::{read_keypair_file, Keypair},
    signer::Signer,
};
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;
use std::{collections::HashSet, fs};
use tonic::transport::Server;
use tracing::{error, info, warn};

enum ValidatorStore {
    #[allow(dead_code)]
    LeaderSchedule(LeaderScheduleUpdatingHandle),
    #[allow(dead_code)]
    UserDefined(HashSet<Pubkey>),
}

struct ValidatorAutherImpl {
    #[allow(dead_code)]
    store: ValidatorStore,
}

impl ValidatorAuther for ValidatorAutherImpl {
    fn is_authorized(&self, _pubkey: &Pubkey) -> bool {
        // match &self.store {
        //     ValidatorStore::LeaderSchedule(cache) => cache.is_scheduled_validator(pubkey),
        //     ValidatorStore::UserDefined(pubkeys) => pubkeys.contains(pubkey),
        // }
        // Allow everyone as the grpc server is private and will run locally behind a firewall
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

    // Read auth keys if provided
    let signing_key_path = args
        .signing_key_pem_path
        .expect("signing_key_pem_path is required but not provided");
    let verifying_key_path = args
        .verifying_key_pem_path
        .expect("verifying_key_pem_path is required but not provided");

    let priv_key = fs::read(&signing_key_path)
        .unwrap_or_else(|_| panic!("Failed to read signing key file: {:?}", signing_key_path));
    let signing_key = PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::private_key_from_pem(&priv_key).unwrap(),
    };

    let key = fs::read(&verifying_key_path).unwrap_or_else(|_| {
        panic!(
            "Failed to read verifying key file: {:?}",
            verifying_key_path
        )
    });
    let verifying_key = Arc::new(PKeyWithDigest {
        digest: MessageDigest::sha256(),
        key: PKey::public_key_from_pem(&key).unwrap(),
    });

    // Setup RPC load balancer for slot updates
    let servers = args
        .rpc_servers
        .into_iter()
        .zip(args.websocket_servers)
        .collect::<Vec<_>>();

    let exit = Arc::new(AtomicBool::new(false));

    let (rpc_load_balancer, slot_receiver) = LoadBalancer::new(&servers, &exit);

    let rpc_load_balancer = Arc::new(rpc_load_balancer);

    let leader_cache = LeaderScheduleCacheUpdater::new(&rpc_load_balancer.clone(), &exit);

    let validator_store = match args.allowed_validators {
        Some(pubkeys) => ValidatorStore::UserDefined(HashSet::from_iter(pubkeys)),
        None => ValidatorStore::LeaderSchedule(leader_cache.handle()),
    };

    // Setup health manager
    let (downstream_slot_sender, downstream_slot_receiver) =
        bounded::<u64>(LoadBalancer::SLOT_QUEUE_CAPACITY);
    let health_manager = HealthManager::new(
        slot_receiver,
        downstream_slot_sender,
        Duration::from_secs(args.missing_slot_unhealthy_secs),
        exit.clone(),
    );

    // Create packet forwarding channel - broadcast so all validators get all packets
    let (p3_packet_tx, p3_packet_rx) = bounded(LoadBalancer::SLOT_QUEUE_CAPACITY);

    let (p3_handle, _key_updaters) = p3_quic::P3Quic::spawn(
        exit.clone(),
        p3_packet_tx,
        rpc_load_balancer.clone().rpc_client().clone(),
        &keypair,
        (args.p3_addr, args.p3_mev_addr),
    );

    // Create BlockEngine service
    let (block_engine_svc, block_engine_handle) = BlockEngineImpl::new(
        downstream_slot_receiver,
        p3_packet_rx,
        health_manager.handle(),
        keypair.pubkey(),
        exit.clone(),
    );

    let auth_svc = AuthServiceImpl::new(
        ValidatorAutherImpl {
            store: validator_store,
        },
        signing_key,
        verifying_key.clone(),
        Duration::from_secs(args.access_token_ttl_secs),
        Duration::from_secs(args.refresh_token_ttl_secs),
        Duration::from_secs(args.challenge_ttl_secs),
        Duration::from_secs(args.challenge_expiration_sleep_interval_secs),
        &exit,
        health_manager.handle(),
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

    // Wait for leader schedule cache thread to finish
    if let Err(e) = leader_cache.join() {
        error!("Leader schedule cache thread panicked: {:?}", e);
    }

    // Wait for health manager thread to finish
    if let Err(e) = health_manager.join() {
        error!("Health manager thread panicked: {:?}", e);
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
