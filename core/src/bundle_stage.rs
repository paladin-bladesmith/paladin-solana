//! The `bundle_stage` processes bundles, which are list of transactions to be executed
//! sequentially and atomically.

use {
    crate::{
        banking_stage::{
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            qos_service::QosService,
        },
        bundle_stage::{
            bundle_account_locker::BundleAccountLocker, bundle_consumer::BundleConsumer,
            bundle_packet_receiver::BundleReceiver,
            bundle_stage_leader_metrics::BundleStageLeaderMetrics, bundle_storage::BundleStorage,
            committer::Committer,
        },
        packet_bundle::PacketBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::TipManager,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure_us,
    solana_poh::{poh_recorder::PohRecorder, transaction_recorder::TransactionRecorder},
    solana_runtime::{
        prioritization_fee_cache::PrioritizationFeeCache, vote_sender_types::ReplayVoteSender,
    },
    solana_time_utils::AtomicInterval,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub mod bundle_account_locker;
mod bundle_consumer;
mod bundle_packet_deserializer;
mod bundle_packet_receiver;
pub(crate) mod bundle_stage_leader_metrics;
mod bundle_storage;
mod committer;
mod front_run_identifier;

const MAX_BUNDLE_RETRY_DURATION: Duration = Duration::from_millis(40);
const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);

// Stats emitted periodically
#[derive(Default)]
pub struct BundleStageLoopMetrics {
    last_report: AtomicInterval,
    id: u32,

    // total received
    num_bundles_received: AtomicU64,
    num_packets_received: AtomicU64,

    // newly buffered
    newly_buffered_bundles_count: AtomicU64,

    // currently buffered
    current_buffered_bundles_count: AtomicU64,
    current_buffered_packets_count: AtomicU64,

    // buffered due to cost model
    cost_model_buffered_bundles_count: AtomicU64,
    cost_model_buffered_packets_count: AtomicU64,

    // number of bundles dropped during insertion
    num_bundles_dropped: AtomicU64,

    // timings
    receive_and_buffer_bundles_elapsed_us: AtomicU64,
    process_buffered_bundles_elapsed_us: AtomicU64,
}

impl BundleStageLoopMetrics {
    fn new(id: u32) -> Self {
        BundleStageLoopMetrics {
            id,
            ..BundleStageLoopMetrics::default()
        }
    }

    pub fn increment_num_bundles_received(&mut self, count: u64) {
        self.num_bundles_received
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_num_packets_received(&mut self, count: u64) {
        self.num_packets_received
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_newly_buffered_bundles_count(&mut self, count: u64) {
        self.newly_buffered_bundles_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_current_buffered_bundles_count(&mut self, count: u64) {
        self.current_buffered_bundles_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_current_buffered_packets_count(&mut self, count: u64) {
        self.current_buffered_packets_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_cost_model_buffered_bundles_count(&mut self, count: u64) {
        self.cost_model_buffered_bundles_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_cost_model_buffered_packets_count(&mut self, count: u64) {
        self.cost_model_buffered_packets_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_num_bundles_dropped(&mut self, count: u64) {
        self.num_bundles_dropped.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_receive_and_buffer_bundles_elapsed_us(&mut self, count: u64) {
        self.receive_and_buffer_bundles_elapsed_us
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_process_buffered_bundles_elapsed_us(&mut self, count: u64) {
        self.process_buffered_bundles_elapsed_us
            .fetch_add(count, Ordering::Relaxed);
    }
}

impl BundleStageLoopMetrics {
    fn maybe_report(&mut self, report_interval_ms: u64) {
        if self.last_report.should_update(report_interval_ms) {
            datapoint_info!(
                "bundle_stage-loop_stats",
                ("id", self.id, i64),
                (
                    "num_bundles_received",
                    self.num_bundles_received.swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "num_packets_received",
                    self.num_packets_received.swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "newly_buffered_bundles_count",
                    self.newly_buffered_bundles_count.swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "current_buffered_bundles_count",
                    self.current_buffered_bundles_count
                        .swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "current_buffered_packets_count",
                    self.current_buffered_packets_count
                        .swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "num_bundles_dropped",
                    self.num_bundles_dropped.swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "receive_and_buffer_bundles_elapsed_us",
                    self.receive_and_buffer_bundles_elapsed_us
                        .swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "process_buffered_bundles_elapsed_us",
                    self.process_buffered_bundles_elapsed_us
                        .swap(0, Ordering::Acquire) as i64,
                    i64
                ),
            );
        }
    }
}

pub struct BundleStage {
    bundle_thread: JoinHandle<()>,
}

impl BundleStage {
    #[allow(clippy::new_ret_no_self)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
    ) -> Self {
        Self::start_bundle_thread(
            cluster_info,
            poh_recorder,
            transaction_recorder,
            bundle_receiver,
            transaction_status_sender,
            replay_vote_sender,
            log_messages_bytes_limit,
            exit,
            tip_manager,
            bundle_account_locker,
            MAX_BUNDLE_RETRY_DURATION,
            block_builder_fee_info,
            prioritization_fee_cache,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.bundle_thread.join()
    }

    #[allow(clippy::too_many_arguments)]
    fn start_bundle_thread(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_message_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        max_bundle_retry_duration: Duration,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
    ) -> Self {
        const BUNDLE_STAGE_ID: u32 = 10_000;
        let poh_recorder = poh_recorder.clone();
        let cluster_info = cluster_info.clone();

        let mut bundle_receiver = BundleReceiver::new(BUNDLE_STAGE_ID, bundle_receiver, Some(5));

        let committer = Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache.clone(),
        );
        let decision_maker = DecisionMaker::new(cluster_info.id(), poh_recorder.clone());

        let unprocessed_bundle_storage = BundleStorage::default();

        let consumer = BundleConsumer::new(
            committer,
            transaction_recorder,
            QosService::new(BUNDLE_STAGE_ID),
            log_message_bytes_limit,
            tip_manager,
            bundle_account_locker,
            block_builder_fee_info.clone(),
            max_bundle_retry_duration,
            cluster_info,
        );

        let bundle_thread = Builder::new()
            .name("solBundleStgTx".to_string())
            .spawn(move || {
                Self::process_loop(
                    &mut bundle_receiver,
                    decision_maker,
                    consumer,
                    BUNDLE_STAGE_ID,
                    unprocessed_bundle_storage,
                    exit,
                );
            })
            .unwrap();

        Self { bundle_thread }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_loop(
        bundle_receiver: &mut BundleReceiver,
        mut decision_maker: DecisionMaker,
        mut consumer: BundleConsumer,
        id: u32,
        mut bundle_storage: BundleStorage,
        exit: Arc<AtomicBool>,
    ) {
        let mut last_metrics_update = Instant::now();

        let mut bundle_stage_metrics = BundleStageLoopMetrics::new(id);
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(id);

        while !exit.load(Ordering::Relaxed) {
            if bundle_storage.unprocessed_bundles_len() > 0
                || last_metrics_update.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
            {
                let (_, process_buffered_packets_time_us) =
                    measure_us!(Self::process_buffered_bundles(
                        &mut decision_maker,
                        &mut consumer,
                        &mut bundle_storage,
                        &mut bundle_stage_leader_metrics,
                    ));
                bundle_stage_leader_metrics
                    .leader_slot_metrics_tracker()
                    .increment_process_buffered_packets_us(process_buffered_packets_time_us);
                last_metrics_update = Instant::now();
            }

            match bundle_receiver.receive_and_buffer_bundles(
                &mut bundle_storage,
                &mut bundle_stage_metrics,
                &mut bundle_stage_leader_metrics,
            ) {
                Ok(_) | Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => break,
            }

            bundle_stage_metrics.increment_current_buffered_bundles_count(
                bundle_storage.unprocessed_bundles_len() as u64,
            );
            bundle_stage_metrics.increment_current_buffered_packets_count(
                bundle_storage.unprocessed_packets_len() as u64,
            );
            bundle_stage_metrics.increment_cost_model_buffered_bundles_count(
                bundle_storage.cost_model_buffered_bundles_len() as u64,
            );
            bundle_stage_metrics.increment_cost_model_buffered_packets_count(
                bundle_storage.cost_model_buffered_packets_len() as u64,
            );
            bundle_stage_metrics.maybe_report(1_000);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_buffered_bundles(
        decision_maker: &mut DecisionMaker,
        consumer: &mut BundleConsumer,
        bundle_storage: &mut BundleStorage,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) {
        let (decision, make_decision_time_us) =
            measure_us!(decision_maker.make_consume_or_forward_decision());

        let (metrics_action, banking_stage_metrics_action) =
            bundle_stage_leader_metrics.check_leader_slot_boundary(decision.bank_start());
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .increment_make_decision_us(make_decision_time_us);

        match decision {
            // BufferedPacketsDecision::Consume means this leader is scheduled to be running at the moment.
            // Execute, record, and commit as many bundles possible given time, compute, and other constraints.
            BufferedPacketsDecision::Consume(bank_start) => {
                // Take metrics action before consume packets (potentially resetting the
                // slot metrics tracker to the next slot) so that we don't count the
                // packet processing metrics from the next slot towards the metrics
                // of the previous slot
                bundle_stage_leader_metrics
                    .apply_action(metrics_action, banking_stage_metrics_action);

                let (_, consume_buffered_packets_time_us) = measure_us!(consumer
                    .consume_buffered_bundles(
                        &bank_start,
                        bundle_storage,
                        bundle_stage_leader_metrics,
                    ));
                bundle_stage_leader_metrics
                    .leader_slot_metrics_tracker()
                    .increment_consume_buffered_packets_us(consume_buffered_packets_time_us);
            }
            // BufferedPacketsDecision::Forward means the leader is slot is far away.
            // Bundles aren't forwarded because it breaks atomicity guarantees, so just drop them.
            BufferedPacketsDecision::Forward => {
                let (_num_bundles_cleared, _num_cost_model_buffered_bundles) =
                    bundle_storage.reset();

                // TODO (LB): add metrics here for how many bundles were cleared

                bundle_stage_leader_metrics
                    .apply_action(metrics_action, banking_stage_metrics_action);
            }
            // BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold means the validator
            // is approaching the leader slot, hold bundles. Also, bundles aren't forwarded because it breaks
            // atomicity guarantees
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold => {
                bundle_stage_leader_metrics
                    .apply_action(metrics_action, banking_stage_metrics_action);
            }
        }
    }
}
