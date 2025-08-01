//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

// allow multiple connections for NAT and any open/close overlap
#[deprecated(
    since = "2.2.0",
    note = "Use solana_streamer::quic::DEFAULT_MAX_QUIC_CONNECTIONS_PER_PEER instead"
)]
pub use solana_streamer::quic::DEFAULT_MAX_QUIC_CONNECTIONS_PER_PEER as MAX_QUIC_CONNECTIONS_PER_PEER;
pub use {
    crate::forwarding_stage::ForwardingClientOption, solana_streamer::quic::DEFAULT_TPU_COALESCE,
};
use {
    crate::{
        admin_rpc_post_init::{KeyUpdaterType, KeyUpdaters},
        banking_stage::BankingStage,
        banking_trace::{Channels, TracerThread},
        bundle_stage::{bundle_account_locker::BundleAccountLocker, BundleStage},
        cluster_info_vote_listener::{
            ClusterInfoVoteListener, DuplicateConfirmedSlotsSender, GossipVerifiedVoteHashSender,
            VerifiedVoteSender, VoteTracker,
        },
        fetch_stage::FetchStage,
        forwarding_stage::{
            spawn_forwarding_stage, ForwardAddressGetter, SpawnForwardingStageResult,
        },
        proxy::{
            block_engine_stage::{BlockBuilderFeeInfo, BlockEngineConfig, BlockEngineStage},
            fetch_stage_manager::FetchStageManager,
            relayer_stage::{RelayerConfig, RelayerStage},
        },
        sigverify::TransactionSigVerifier,
        sigverify_stage::SigVerifyStage,
        staked_nodes_updater_service::StakedNodesUpdaterService,
        tip_manager::{TipManager, TipManagerConfig},
        tpu_entry_notifier::TpuEntryNotifier,
        validator::{BlockProductionMethod, GeneratorConfig, TransactionStructure},
    },
    bytes::Bytes,
    crossbeam_channel::{bounded, unbounded, Receiver},
    p3_quic::P3Quic,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_ledger::{
        blockstore::Blockstore, blockstore_processor::TransactionStatusSender,
        entry_notifier_service::EntryNotifierSender, leader_schedule_cache::LeaderScheduleCache,
    },
    solana_perf::data_budget::DataBudget,
    solana_poh::{
        poh_recorder::{PohRecorder, WorkingBankEntry},
        transaction_recorder::TransactionRecorder,
    },
    solana_pubkey::Pubkey,
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSender,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        prioritization_fee_cache::PrioritizationFeeCache,
        root_bank_cache::RootBankCache,
        vote_sender_types::{ReplayVoteReceiver, ReplayVoteSender},
    },
    solana_signer::Signer,
    solana_streamer::{
        quic::{spawn_server_multi, QuicServerParams, SpawnServerResult},
        streamer::StakedNodes,
    },
    solana_turbine::broadcast_stage::{BroadcastStage, BroadcastStageType},
    std::{
        collections::{HashMap, HashSet},
        net::{SocketAddr, UdpSocket},
        sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
        thread::{self, JoinHandle},
        time::Duration,
    },
    tokio::sync::mpsc::Sender as AsyncSender,
};

pub struct TpuSockets {
    pub transactions: Vec<UdpSocket>,
    pub transaction_forwards: Vec<UdpSocket>,
    pub vote: Vec<UdpSocket>,
    pub broadcast: Vec<UdpSocket>,
    pub transactions_quic: Vec<UdpSocket>,
    pub transactions_forwards_quic: Vec<UdpSocket>,
    pub vote_quic: Vec<UdpSocket>,
    /// Client-side socket for the forwarding votes.
    pub vote_forwarding_client: UdpSocket,
}

/// For the first `reserved_ticks` ticks of a bank, the preallocated_bundle_cost is subtracted
/// from the Bank's block cost limit.
fn calculate_block_cost_limit_reservation(
    bank: &Bank,
    reserved_ticks: u64,
    preallocated_bundle_cost: u64,
) -> u64 {
    if bank.tick_height() % bank.ticks_per_slot() < reserved_ticks {
        preallocated_bundle_cost
    } else {
        0
    }
}

pub struct Tpu {
    fetch_stage: FetchStage,
    sigverify_stage: SigVerifyStage,
    vote_sigverify_stage: SigVerifyStage,
    banking_stage: BankingStage,
    forwarding_stage: JoinHandle<()>,
    cluster_info_vote_listener: ClusterInfoVoteListener,
    broadcast_stage: BroadcastStage,
    tpu_quic_t: thread::JoinHandle<()>,
    tpu_forwards_quic_t: thread::JoinHandle<()>,
    tpu_entry_notifier: Option<TpuEntryNotifier>,
    staked_nodes_updater_service: StakedNodesUpdaterService,
    tracer_thread_hdl: TracerThread,
    tpu_vote_quic_t: thread::JoinHandle<()>,
    relayer_stage: RelayerStage,
    block_engine_stage: BlockEngineStage,
    fetch_stage_manager: FetchStageManager,
    bundle_stage: BundleStage,
    p3_quic: std::thread::JoinHandle<()>,
}

impl Tpu {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_client(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        entry_receiver: Receiver<WorkingBankEntry>,
        retransmit_slots_receiver: Receiver<Slot>,
        sockets: TpuSockets,
        subscriptions: &Arc<RpcSubscriptions>,
        transaction_status_sender: Option<TransactionStatusSender>,
        entry_notification_sender: Option<EntryNotifierSender>,
        blockstore: Arc<Blockstore>,
        broadcast_type: &BroadcastStageType,
        exit: Arc<AtomicBool>,
        shred_version: u16,
        vote_tracker: Arc<VoteTracker>,
        bank_forks: Arc<RwLock<BankForks>>,
        verified_vote_sender: VerifiedVoteSender,
        gossip_verified_vote_hash_sender: GossipVerifiedVoteHashSender,
        replay_vote_receiver: ReplayVoteReceiver,
        replay_vote_sender: ReplayVoteSender,
        bank_notification_sender: Option<BankNotificationSender>,
        tpu_coalesce: Duration,
        duplicate_confirmed_slot_sender: DuplicateConfirmedSlotsSender,
        client: ForwardingClientOption,
        turbine_quic_endpoint_sender: AsyncSender<(SocketAddr, Bytes)>,
        keypair: &Keypair,
        log_messages_bytes_limit: Option<usize>,
        staked_nodes: &Arc<RwLock<StakedNodes>>,
        shared_staked_nodes_overrides: Arc<RwLock<HashMap<Pubkey, u64>>>,
        banking_tracer_channels: Channels,
        tracer_thread_hdl: TracerThread,
        tpu_enable_udp: bool,
        tpu_quic_server_config: QuicServerParams,
        tpu_fwd_quic_server_config: QuicServerParams,
        vote_quic_server_config: QuicServerParams,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        block_production_method: BlockProductionMethod,
        transaction_struct: TransactionStructure,
        enable_block_production_forwarding: bool,
        _generator_config: Option<GeneratorConfig>, /* vestigial code for replay invalidator */
        key_notifiers: Arc<RwLock<KeyUpdaters>>,
        block_engine_config: Arc<Mutex<BlockEngineConfig>>,
        secondary_block_engine_urls: Vec<String>,
        relayer_config: Arc<Mutex<RelayerConfig>>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        tip_manager_config: TipManagerConfig,
        shred_receiver_address: Arc<RwLock<Option<SocketAddr>>>,
        preallocated_bundle_cost: u64,
        batch_interval: Duration,
        (p3_socket, p3_mev_socket): (SocketAddr, SocketAddr),
    ) -> Self {
        let TpuSockets {
            transactions: transactions_sockets,
            transaction_forwards: tpu_forwards_sockets,
            vote: tpu_vote_sockets,
            broadcast: broadcast_sockets,
            transactions_quic: transactions_quic_sockets,
            transactions_forwards_quic: transactions_forwards_quic_sockets,
            vote_quic: tpu_vote_quic_sockets,
            vote_forwarding_client: vote_forwarding_client_socket,
        } = sockets;

        // [----------]
        // [-- QUIC --] \
        // [----------]  \____     [-----------------------]     [--------------------]     [------------------]
        //                    ---- [-- FetchStageManager --] --> [-- SigverifyStage --] --> [-- BankingStage --]
        // [--------------]  /     [-----------------------]     [--------------------]     [------------------]
        // [-- Vortexor --] /
        // [--------------]
        //
        //             fetch_stage_manager_*                packet_receiver

        // Packets from fetch stage and quic server are intercepted and sent through fetch_stage_manager
        // If relayer is connected, packets are dropped. If not, packets are forwarded on to packet_sender
        let (fetch_stage_manager_sender, fetch_stage_manager_receiver) = unbounded();
        let (sigverify_stage_sender, sigverify_stage_receiver) = unbounded();

        let (vote_packet_sender, vote_packet_receiver) = unbounded();
        let (forwarded_packet_sender, forwarded_packet_receiver) = unbounded();
        let fetch_stage = FetchStage::new_with_sender(
            transactions_sockets,
            tpu_forwards_sockets,
            tpu_vote_sockets,
            exit.clone(),
            &fetch_stage_manager_sender,
            &vote_packet_sender,
            &forwarded_packet_sender,
            forwarded_packet_receiver,
            poh_recorder,
            Some(tpu_coalesce),
            Some(bank_forks.read().unwrap().get_vote_only_mode_signal()),
            tpu_enable_udp,
        );

        let staked_nodes_updater_service = StakedNodesUpdaterService::new(
            exit.clone(),
            bank_forks.clone(),
            staked_nodes.clone(),
            shared_staked_nodes_overrides,
        );

        let Channels {
            non_vote_sender: banking_stage_sender,
            non_vote_receiver: banking_stage_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer_channels;

        // Streamer for Votes:
        let SpawnServerResult {
            endpoints: _,
            thread: tpu_vote_quic_t,
            key_updater: vote_streamer_key_updater,
        } = spawn_server_multi(
            "solQuicTVo",
            "quic_streamer_tpu_vote",
            tpu_vote_quic_sockets,
            keypair,
            vote_packet_sender.clone(),
            exit.clone(),
            staked_nodes.clone(),
            vote_quic_server_config,
        )
        .unwrap();

        // Streamer for TPU
        let SpawnServerResult {
            endpoints: _,
            thread: tpu_quic_t,
            key_updater,
        } = spawn_server_multi(
            "solQuicTpu",
            "quic_streamer_tpu",
            transactions_quic_sockets,
            keypair,
            fetch_stage_manager_sender.clone(),
            exit.clone(),
            staked_nodes.clone(),
            tpu_quic_server_config,
        )
        .unwrap();

        // Streamer for TPU forward
        let SpawnServerResult {
            endpoints: _,
            thread: tpu_forwards_quic_t,
            key_updater: forwards_key_updater,
        } = spawn_server_multi(
            "solQuicTpuFwd",
            "quic_streamer_tpu_forwards",
            transactions_forwards_quic_sockets,
            keypair,
            forwarded_packet_sender,
            exit.clone(),
            staked_nodes.clone(),
            tpu_fwd_quic_server_config,
        )
        .unwrap();

        let (forward_stage_sender, forward_stage_receiver) = bounded(1024);
        let sigverify_stage = {
            let verifier = TransactionSigVerifier::new(
                banking_stage_sender.clone(),
                enable_block_production_forwarding.then(|| forward_stage_sender.clone()),
            );
            SigVerifyStage::new(
                sigverify_stage_receiver,
                verifier,
                "solSigVerTpu",
                "tpu-verifier",
            )
        };

        let vote_sigverify_stage = {
            let verifier = TransactionSigVerifier::new_reject_non_vote(
                tpu_vote_sender,
                Some(forward_stage_sender),
            );
            SigVerifyStage::new(
                vote_packet_receiver,
                verifier,
                "solSigVerTpuVot",
                "tpu-vote-verifier",
            )
        };

        let block_builder_fee_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: cluster_info.keypair().pubkey(),
            block_builder_commission: 0,
        }));

        let (bundle_sender, bundle_receiver) = unbounded();
        let block_engine_stage = BlockEngineStage::new(
            block_engine_config,
            secondary_block_engine_urls,
            bundle_sender,
            cluster_info.clone(),
            sigverify_stage_sender.clone(),
            banking_stage_sender.clone(),
            exit.clone(),
            &block_builder_fee_info,
        );

        // Launch paladin threads.
        let (p3_quic, p3_quic_key_updaters) = P3Quic::spawn(
            exit.clone(),
            fetch_stage_manager_sender,
            poh_recorder.clone(),
            keypair,
            (p3_socket, p3_mev_socket),
        );

        let (heartbeat_tx, heartbeat_rx) = unbounded();
        let fetch_stage_manager = FetchStageManager::new(
            cluster_info.clone(),
            heartbeat_rx,
            fetch_stage_manager_receiver,
            sigverify_stage_sender.clone(),
            exit.clone(),
        );

        let relayer_stage = RelayerStage::new(
            relayer_config,
            cluster_info.clone(),
            heartbeat_tx,
            sigverify_stage_sender,
            banking_stage_sender,
            exit.clone(),
        );

        let cluster_info_vote_listener = ClusterInfoVoteListener::new(
            exit.clone(),
            cluster_info.clone(),
            gossip_vote_sender,
            vote_tracker,
            bank_forks.clone(),
            subscriptions.clone(),
            verified_vote_sender,
            gossip_verified_vote_hash_sender,
            replay_vote_receiver,
            blockstore.clone(),
            bank_notification_sender,
            duplicate_confirmed_slot_sender,
        );

        let tip_manager = TipManager::new(
            blockstore.clone(),
            leader_schedule_cache.clone(),
            tip_manager_config,
        );

        let bundle_account_locker = BundleAccountLocker::default();

        // The tip program can't be used in BankingStage to avoid someone from stealing tips mid-slot.
        // The first 80% of the block, based on poh ticks, has `preallocated_bundle_cost` less compute units.
        // The last 20% has has full compute so blockspace is maximized if BundleStage is idle.
        let reserved_ticks = poh_recorder
            .read()
            .unwrap()
            .ticks_per_slot()
            .saturating_mul(8)
            .saturating_div(10);

        let mut blacklisted_accounts = HashSet::new();
        blacklisted_accounts.insert(tip_manager.tip_payment_program_id());
        let banking_stage = BankingStage::new(
            block_production_method,
            transaction_struct,
            cluster_info,
            poh_recorder,
            transaction_recorder.clone(),
            banking_stage_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            transaction_status_sender.clone(),
            replay_vote_sender.clone(),
            log_messages_bytes_limit,
            bank_forks.clone(),
            prioritization_fee_cache,
            blacklisted_accounts,
            bundle_account_locker.clone(),
            move |bank| {
                calculate_block_cost_limit_reservation(
                    bank,
                    reserved_ticks,
                    preallocated_bundle_cost,
                )
            },
            batch_interval,
        );

        let SpawnForwardingStageResult {
            join_handle: forwarding_stage,
            client_updater,
        } = spawn_forwarding_stage(
            forward_stage_receiver,
            client,
            vote_forwarding_client_socket,
            RootBankCache::new(bank_forks.clone()),
            ForwardAddressGetter::new(cluster_info.clone(), poh_recorder.clone()),
            DataBudget::default(),
        );

        let bundle_stage = BundleStage::new(
            cluster_info,
            poh_recorder,
            transaction_recorder,
            bundle_receiver,
            transaction_status_sender,
            replay_vote_sender,
            log_messages_bytes_limit,
            exit.clone(),
            tip_manager,
            bundle_account_locker,
            &block_builder_fee_info,
            prioritization_fee_cache,
        );

        let (entry_receiver, tpu_entry_notifier) =
            if let Some(entry_notification_sender) = entry_notification_sender {
                let (broadcast_entry_sender, broadcast_entry_receiver) = unbounded();
                let tpu_entry_notifier = TpuEntryNotifier::new(
                    entry_receiver,
                    entry_notification_sender,
                    broadcast_entry_sender,
                    exit.clone(),
                );
                (broadcast_entry_receiver, Some(tpu_entry_notifier))
            } else {
                (entry_receiver, None)
            };

        let broadcast_stage = broadcast_type.new_broadcast_stage(
            broadcast_sockets,
            cluster_info.clone(),
            entry_receiver,
            retransmit_slots_receiver,
            exit,
            blockstore,
            bank_forks,
            shred_version,
            turbine_quic_endpoint_sender,
            shred_receiver_address,
        );

        let mut key_notifiers = key_notifiers.write().unwrap();
        key_notifiers.add(KeyUpdaterType::Tpu, key_updater);
        key_notifiers.add(KeyUpdaterType::TpuForwards, forwards_key_updater);
        key_notifiers.add(KeyUpdaterType::TpuVote, vote_streamer_key_updater);
        key_notifiers.add(KeyUpdaterType::Forward, client_updater);
        let [p3_regular, p3_mev] = p3_quic_key_updaters;
        key_notifiers.add(KeyUpdaterType::P3Regular, p3_regular);
        key_notifiers.add(KeyUpdaterType::P3Mev, p3_mev);

        Self {
            fetch_stage,
            sigverify_stage,
            vote_sigverify_stage,
            banking_stage,
            forwarding_stage,
            cluster_info_vote_listener,
            broadcast_stage,
            tpu_quic_t,
            tpu_forwards_quic_t,
            tpu_entry_notifier,
            staked_nodes_updater_service,
            tracer_thread_hdl,
            tpu_vote_quic_t,
            block_engine_stage,
            relayer_stage,
            fetch_stage_manager,
            bundle_stage,
            p3_quic,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        let results = vec![
            self.fetch_stage.join(),
            self.sigverify_stage.join(),
            self.vote_sigverify_stage.join(),
            self.cluster_info_vote_listener.join(),
            self.banking_stage.join(),
            self.forwarding_stage.join(),
            self.staked_nodes_updater_service.join(),
            self.tpu_quic_t.join(),
            self.tpu_forwards_quic_t.join(),
            self.tpu_vote_quic_t.join(),
            self.bundle_stage.join(),
            self.relayer_stage.join(),
            self.block_engine_stage.join(),
            self.fetch_stage_manager.join(),
            self.p3_quic.join(),
        ];
        let broadcast_result = self.broadcast_stage.join();
        for result in results {
            result?;
        }
        if let Some(tpu_entry_notifier) = self.tpu_entry_notifier {
            tpu_entry_notifier.join()?;
        }
        let _ = broadcast_result?;
        if let Some(tracer_thread_hdl) = self.tracer_thread_hdl {
            if let Err(tracer_result) = tracer_thread_hdl.join()? {
                error!(
                    "banking tracer thread returned error after successful thread join: {:?}",
                    tracer_result
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use {
        super::calculate_block_cost_limit_reservation,
        solana_ledger::genesis_utils::create_genesis_config, solana_pubkey::Pubkey,
        solana_runtime::bank::Bank, std::sync::Arc,
    };

    #[test]
    fn test_calculate_block_cost_limit_reservation() {
        const BUNDLE_BLOCK_COST_LIMITS_RESERVATION: u64 = 100;
        const RESERVED_TICKS: u64 = 5;
        let genesis_config_info = create_genesis_config(100);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));

        for _ in 0..genesis_config_info.genesis_config.ticks_per_slot {
            bank.register_default_tick_for_test();
        }
        assert!(bank.is_complete());
        bank.freeze();
        let bank1 = Arc::new(Bank::new_from_parent(bank.clone(), &Pubkey::default(), 1));

        // wait for reservation to be over
        (0..RESERVED_TICKS).for_each(|_| {
            assert_eq!(
                calculate_block_cost_limit_reservation(
                    &bank1,
                    RESERVED_TICKS,
                    BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
                ),
                BUNDLE_BLOCK_COST_LIMITS_RESERVATION
            );
            bank1.register_default_tick_for_test();
        });
        assert_eq!(
            calculate_block_cost_limit_reservation(
                &bank1,
                RESERVED_TICKS,
                BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
            ),
            0
        );
    }
}
