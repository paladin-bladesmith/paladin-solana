//! Maintains a connection to the Block Engine.
//!
//! The Block Engine is responsible for the following:
//! - Acts as a system that sends high profit bundles and transactions to a validator.
//! - Sends transactions and bundles to the validator.

use {
    crate::{
        banking_trace::BankingPacketSender,
        packet_bundle::PacketBundle,
        proto_packet_to_packet,
        proxy::{
            auth::{generate_auth_tokens, maybe_refresh_auth_tokens, AuthInterceptor},
            ProxyError,
        },
    },
    crossbeam_channel::Sender,
    itertools::Either,
    jito_protos::proto::{
        auth::{auth_service_client::AuthServiceClient, Token},
        block_engine::{
            self, block_engine_validator_client::BlockEngineValidatorClient,
            BlockBuilderFeeInfoRequest,
        },
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_perf::packet::{BytesPacket, PacketBatch},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        ops::AddAssign,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    tokio::{
        task::{self, JoinSet},
        time::{interval, sleep, timeout},
    },
    tonic::{
        codegen::InterceptedService,
        transport::{Channel, Endpoint},
        Status, Streaming,
    },
};

const CONNECTION_TIMEOUT_S: u64 = 10;
const CONNECTION_BACKOFF_S: u64 = 5;

#[derive(Default)]
struct BlockEngineStageStats {
    num_bundles: u64,
    num_bundle_packets: u64,
    num_packets: u64,
    num_empty_packets: u64,
}

impl BlockEngineStageStats {
    pub(crate) fn report_with_url(&self, url: &str, is_primary: bool) {
        datapoint_info!(
            "block_engine_stage-stats",
            ("url", url, String),
            ("is_primary", is_primary, bool),
            ("num_bundles", self.num_bundles, i64),
            ("num_bundle_packets", self.num_bundle_packets, i64),
            ("num_packets", self.num_packets, i64),
            ("num_empty_packets", self.num_empty_packets, i64)
        );
    }
}

pub struct BlockBuilderFeeInfo {
    pub block_builder: Pubkey,
    pub block_builder_commission: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockEngineConfig {
    /// Block Engine URL
    pub block_engine_url: String,

    /// If set then it will be assumed the backend verified packets so signature verification will be bypassed in the validator.
    pub trust_packets: bool,
}

pub struct BlockEngineStage {
    t_hdls: Vec<JoinHandle<()>>,
}

impl BlockEngineStage {
    pub fn new(
        block_engine_config: Arc<Mutex<BlockEngineConfig>>,
        secondary_urls: Vec<String>,
        // Channel that bundles get piped through.
        bundle_tx: Sender<Vec<PacketBundle>>,
        // The keypair stored here is used to sign auth challenges.
        cluster_info: Arc<ClusterInfo>,
        // Channel that non-trusted packets get piped through.
        packet_tx: Sender<PacketBatch>,
        // Channel that trusted packets get piped through.
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    ) -> Self {
        // Setup all the futures.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut set: JoinSet<_> = {
            let _rt_guard = rt.enter();

            std::iter::once(Either::Left(block_engine_config))
                .chain(secondary_urls.into_iter().map(Either::Right))
                .enumerate()
                .map(|(index, config)| {
                    info!("starting block-engine-{}", index);

                    Self::start(
                        config,
                        cluster_info.clone(),
                        bundle_tx.clone(),
                        packet_tx.clone(),
                        banking_packet_sender.clone(),
                        exit.clone(),
                        block_builder_fee_info.clone(),
                    )
                })
                .collect()
        };

        // Spawn off to a dedicated runtime.
        let thread = Builder::new()
            .name("block-engine-runtime".to_string())
            .spawn(move || {
                rt.block_on(async move {
                    // Wait for all tasks to complete
                    while let Some(res) = set.join_next().await {
                        match res {
                            Ok(_) => continue,
                            Err(e) => {
                                error!("Block engine task failed: {}", e);
                            }
                        }
                    }
                })
            })
            .unwrap();

        Self {
            t_hdls: vec![thread],
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.t_hdls {
            t.join()?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn start(
        block_engine_config: Either<Arc<Mutex<BlockEngineConfig>>, String>,
        cluster_info: Arc<ClusterInfo>,
        bundle_tx: Sender<Vec<PacketBundle>>,
        packet_tx: Sender<PacketBatch>,
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
    ) {
        const CONNECTION_TIMEOUT: Duration = Duration::from_secs(CONNECTION_TIMEOUT_S);
        const CONNECTION_BACKOFF: Duration = Duration::from_secs(CONNECTION_BACKOFF_S);
        let mut error_count: u64 = 0;

        while !exit.load(Ordering::Relaxed) {
            // Wait until a valid config is supplied (either initially or by admin rpc)
            // Use if!/else here to avoid extra CONNECTION_BACKOFF wait on successful termination
            let local_block_engine_config = match &block_engine_config {
                Either::Left(config) => {
                    let block_engine_config = config.clone();
                    task::spawn_blocking(move || block_engine_config.lock().unwrap().clone())
                        .await
                        .unwrap()
                }
                Either::Right(url) => BlockEngineConfig {
                    block_engine_url: url.clone(),
                    trust_packets: false, // Default to false for secondary URLs
                },
            };
            if !Self::is_valid_block_engine_config(&local_block_engine_config) {
                sleep(CONNECTION_BACKOFF).await;
            } else if let Err(e) = Self::connect_auth_and_stream(
                &local_block_engine_config,
                &block_engine_config,
                &cluster_info,
                &bundle_tx,
                &packet_tx,
                &banking_packet_sender,
                &exit,
                &block_builder_fee_info,
                &CONNECTION_TIMEOUT,
            )
            .await
            {
                match e {
                    // This error is frequent on hot spares, and the parsed string does not work
                    // with datapoints (incorrect escaping).
                    ProxyError::AuthenticationPermissionDenied => {
                        warn!("block engine permission denied. not on leader schedule. ignore if hot-spare.")
                    }
                    e => {
                        error_count += 1;
                        datapoint_warn!(
                            "block_engine_stage-proxy_error",
                            ("count", error_count, i64),
                            ("error", e.to_string(), String),
                            (
                                "url",
                                local_block_engine_config.block_engine_url.clone(),
                                String
                            ),
                            ("is_primary", block_engine_config.is_left(), bool),
                        );
                    }
                }
                sleep(CONNECTION_BACKOFF).await;
            }
        }
    }

    async fn connect_auth_and_stream(
        local_block_engine_config: &BlockEngineConfig,
        global_block_engine_config: &Either<Arc<Mutex<BlockEngineConfig>>, String>,
        cluster_info: &Arc<ClusterInfo>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        banking_packet_sender: &BankingPacketSender,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        connection_timeout: &Duration,
    ) -> crate::proxy::Result<()> {
        // Get a copy of configs here in case they have changed at runtime
        let keypair = cluster_info.keypair().clone();

        let mut backend_endpoint =
            Endpoint::from_shared(local_block_engine_config.block_engine_url.clone())
                .map_err(|_| {
                    ProxyError::BlockEngineConnectionError(format!(
                        "invalid block engine url value: {}",
                        local_block_engine_config.block_engine_url
                    ))
                })?
                .tcp_keepalive(Some(Duration::from_secs(60)));
        if local_block_engine_config
            .block_engine_url
            .starts_with("https")
        {
            backend_endpoint = backend_endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .map_err(|_| {
                    ProxyError::BlockEngineConnectionError(
                        "failed to set tls_config for block engine service".to_string(),
                    )
                })?;
        }

        debug!(
            "connecting to auth: {}",
            local_block_engine_config.block_engine_url
        );
        let auth_channel = timeout(*connection_timeout, backend_endpoint.connect())
            .await
            .map_err(|_| ProxyError::AuthenticationConnectionTimeout)?
            .map_err(|e| ProxyError::AuthenticationConnectionError(e.to_string()))?;

        let mut auth_client = AuthServiceClient::new(auth_channel);

        debug!("generating authentication token");
        let (access_token, refresh_token) = timeout(
            *connection_timeout,
            generate_auth_tokens(&mut auth_client, &keypair),
        )
        .await
        .map_err(|_| ProxyError::AuthenticationTimeout)??;

        datapoint_info!(
            "block_engine_stage-tokens_generated",
            ("url", local_block_engine_config.block_engine_url, String),
            ("is_primary", global_block_engine_config.is_left(), bool),
            ("count", 1, i64),
        );

        debug!(
            "connecting to block engine: {}",
            local_block_engine_config.block_engine_url
        );
        let block_engine_channel = timeout(*connection_timeout, backend_endpoint.connect())
            .await
            .map_err(|_| ProxyError::BlockEngineConnectionTimeout)?
            .map_err(|e| ProxyError::BlockEngineConnectionError(e.to_string()))?;

        let access_token = Arc::new(Mutex::new(access_token));
        let block_engine_client = BlockEngineValidatorClient::with_interceptor(
            block_engine_channel,
            AuthInterceptor::new(access_token.clone()),
        );

        Self::start_consuming_block_engine_bundles_and_packets(
            bundle_tx,
            block_engine_client,
            packet_tx,
            local_block_engine_config,
            global_block_engine_config,
            banking_packet_sender,
            exit,
            block_builder_fee_info,
            auth_client,
            access_token,
            refresh_token,
            connection_timeout,
            keypair,
            cluster_info,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_consuming_block_engine_bundles_and_packets(
        bundle_tx: &Sender<Vec<PacketBundle>>,
        mut client: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        packet_tx: &Sender<PacketBatch>,
        local_config: &BlockEngineConfig, // local copy of config with current connections
        global_config: &Either<Arc<Mutex<BlockEngineConfig>>, String>, // guarded reference for detecting run-time updates
        banking_packet_sender: &BankingPacketSender,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        auth_client: AuthServiceClient<Channel>,
        access_token: Arc<Mutex<Token>>,
        refresh_token: Token,
        connection_timeout: &Duration,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
    ) -> crate::proxy::Result<()> {
        let subscribe_packets_stream = timeout(
            *connection_timeout,
            client.subscribe_packets(block_engine::SubscribePacketsRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("block_engine_subscribe_packets".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?
        .into_inner();

        let subscribe_bundles_stream = timeout(
            *connection_timeout,
            client.subscribe_bundles(block_engine::SubscribeBundlesRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("subscribe_bundles".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?
        .into_inner();

        // Only update block builder fee info for primary connections
        if global_config.is_left() {
            let block_builder_info = timeout(
                *connection_timeout,
                client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest {}),
            )
            .await
            .map_err(|_| ProxyError::MethodTimeout("get_block_builder_fee_info".to_string()))?
            .map_err(|e| ProxyError::MethodError(e.to_string()))?
            .into_inner();

            {
                let block_builder_fee_info = block_builder_fee_info.clone();
                task::spawn_blocking(move || {
                    let mut bb_fee = block_builder_fee_info.lock().unwrap();
                    bb_fee.block_builder_commission = block_builder_info.commission;
                    if let Ok(pk) = Pubkey::from_str(&block_builder_info.pubkey) {
                        bb_fee.block_builder = pk
                    }
                })
                .await
                .unwrap();
            }
        }

        Self::consume_bundle_and_packet_stream(
            client,
            (subscribe_bundles_stream, subscribe_packets_stream),
            bundle_tx,
            packet_tx,
            local_config,
            global_config,
            banking_packet_sender,
            exit,
            block_builder_fee_info,
            auth_client,
            access_token,
            refresh_token,
            keypair,
            cluster_info,
            connection_timeout,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume_bundle_and_packet_stream(
        mut client: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        (mut bundle_stream, mut packet_stream): (
            Streaming<block_engine::SubscribeBundlesResponse>,
            Streaming<block_engine::SubscribePacketsResponse>,
        ),
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        local_config: &BlockEngineConfig, // local copy of config with current connections
        global_config: &Either<Arc<Mutex<BlockEngineConfig>>, String>, // guarded reference for detecting run-time updates
        banking_packet_sender: &BankingPacketSender,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        mut auth_client: AuthServiceClient<Channel>,
        access_token: Arc<Mutex<Token>>,
        mut refresh_token: Token,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
        connection_timeout: &Duration,
    ) -> crate::proxy::Result<()> {
        const METRICS_TICK: Duration = Duration::from_secs(1);
        const MAINTENANCE_TICK: Duration = Duration::from_secs(10 * 60);
        let refresh_within_s: u64 = METRICS_TICK.as_secs().saturating_mul(3).saturating_div(2);

        let mut num_full_refreshes: u64 = 1;
        let mut num_refresh_access_token: u64 = 0;
        let mut block_engine_stats = BlockEngineStageStats::default();
        let mut metrics_and_auth_tick = interval(METRICS_TICK);
        let mut maintenance_tick = interval(MAINTENANCE_TICK);

        info!(
            "connected to packet and bundle stream: {} (primary: {})",
            local_config.block_engine_url,
            global_config.is_left()
        );

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    Self::handle_block_engine_packets(resp, packet_tx, banking_packet_sender, local_config.trust_packets, &mut block_engine_stats)?;
                }
                maybe_bundles = bundle_stream.message() => {
                    Self::handle_block_engine_maybe_bundles(maybe_bundles, bundle_tx, &mut block_engine_stats)?;
                }
                _ = metrics_and_auth_tick.tick() => {
                    block_engine_stats.report_with_url(&local_config.block_engine_url, global_config.is_left());
                    block_engine_stats = BlockEngineStageStats::default();

                    if cluster_info.id() != keypair.pubkey() {
                        return Err(ProxyError::AuthenticationConnectionError("validator identity changed".to_string()));
                    }

                    // Only check config changes for primary connection
                    if let Either::Left(global_config) = global_config {
                        let global_config = global_config.clone();
                        if *local_config != task::spawn_blocking(move || global_config.lock().unwrap().clone())
                            .await
                            .unwrap() {
                            return Err(ProxyError::AuthenticationConnectionError("block engine config changed".to_string()));
                        }
                    }

                    let (maybe_new_access, maybe_new_refresh) = maybe_refresh_auth_tokens(&mut auth_client,
                        &access_token,
                        &refresh_token,
                        cluster_info,
                        connection_timeout,
                        refresh_within_s,
                    ).await?;

                    if let Some(new_token) = maybe_new_access {
                        num_refresh_access_token += 1;
                        datapoint_info!(
                            "block_engine_stage-refresh_access_token",
                            ("url", &local_config.block_engine_url, String),
                            ("is_primary", global_config.is_left(), bool),
                            ("count", num_refresh_access_token, i64),
                        );

                        let access_token = access_token.clone();
                        task::spawn_blocking(move || *access_token.lock().unwrap() = new_token)
                            .await
                            .unwrap();
                    }
                    if let Some(new_token) = maybe_new_refresh {
                        num_full_refreshes += 1;
                        datapoint_info!(
                            "block_engine_stage-tokens_generated",
                            ("url", &local_config.block_engine_url, String),
                            ("is_primary", global_config.is_left(), bool),
                            ("count", num_full_refreshes, i64),
                        );
                        refresh_token = new_token;
                    }
                }
                // Only update fee info periodically for primary connection
                _ = maintenance_tick.tick(), if global_config.is_left() => {
                    let block_builder_info = timeout(
                        *connection_timeout,
                        client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest{})
                    )
                    .await
                    .map_err(|_| ProxyError::MethodTimeout("get_block_builder_fee_info".to_string()))?
                    .map_err(|e| ProxyError::MethodError(e.to_string()))?
                    .into_inner();

                    let block_builder_fee_info = block_builder_fee_info.clone();
                    task::spawn_blocking(move || {
                        let mut bb_fee = block_builder_fee_info.lock().unwrap();
                        bb_fee.block_builder_commission = block_builder_info.commission;
                        if let Ok(pk) = Pubkey::from_str(&block_builder_info.pubkey) {
                            bb_fee.block_builder = pk
                        }
                    })
                    .await
                    .unwrap();
                }
            }
        }

        Ok(())
    }

    fn handle_block_engine_maybe_bundles(
        maybe_bundles_response: Result<Option<block_engine::SubscribeBundlesResponse>, Status>,
        bundle_sender: &Sender<Vec<PacketBundle>>,
        block_engine_stats: &mut BlockEngineStageStats,
    ) -> crate::proxy::Result<()> {
        let bundles_response = maybe_bundles_response?.ok_or(ProxyError::GrpcStreamDisconnected)?;
        let bundles: Vec<PacketBundle> = bundles_response
            .bundles
            .into_iter()
            .filter_map(|bundle| {
                Some(PacketBundle {
                    batch: PacketBatch::from(
                        bundle
                            .bundle?
                            .packets
                            .into_iter()
                            .map(proto_packet_to_packet)
                            .collect::<Vec<BytesPacket>>(),
                    ),
                    bundle_id: bundle.uuid,
                })
            })
            .collect();
        block_engine_stats
            .num_bundles
            .add_assign(bundles.len() as u64);
        block_engine_stats.num_bundle_packets.add_assign(
            bundles
                .iter()
                .map(|bundle| bundle.batch.len() as u64)
                .sum::<u64>(),
        );

        // NOTE: bundles are sanitized in bundle_sanitizer module
        bundle_sender
            .send(bundles)
            .map_err(|_| ProxyError::PacketForwardError)
    }

    fn handle_block_engine_packets(
        resp: block_engine::SubscribePacketsResponse,
        packet_tx: &Sender<PacketBatch>,
        banking_packet_sender: &BankingPacketSender,
        trust_packets: bool,
        block_engine_stats: &mut BlockEngineStageStats,
    ) -> crate::proxy::Result<()> {
        if let Some(batch) = resp.batch {
            if batch.packets.is_empty() {
                block_engine_stats.num_empty_packets.add_assign(1);
                return Ok(());
            }

            let packet_batch = PacketBatch::from(
                batch
                    .packets
                    .into_iter()
                    .map(proto_packet_to_packet)
                    .collect::<Vec<BytesPacket>>(),
            );

            block_engine_stats
                .num_packets
                .add_assign(packet_batch.len() as u64);

            if trust_packets {
                banking_packet_sender
                    .send(Arc::new(vec![packet_batch]))
                    .map_err(|_| ProxyError::PacketForwardError)?;
            } else {
                packet_tx
                    .send(packet_batch)
                    .map_err(|_| ProxyError::PacketForwardError)?;
            }
        } else {
            block_engine_stats.num_empty_packets.add_assign(1);
        }

        Ok(())
    }

    pub fn is_valid_block_engine_config(config: &BlockEngineConfig) -> bool {
        if config.block_engine_url.is_empty() {
            warn!("can't connect to block_engine. missing block_engine_url.");
            return false;
        }
        if let Err(e) = Endpoint::from_str(&config.block_engine_url) {
            error!(
                "can't connect to block engine. error creating block engine endpoint - {}",
                e
            );
            return false;
        }
        true
    }
}
