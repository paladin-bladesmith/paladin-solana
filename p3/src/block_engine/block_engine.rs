// https://github.com/jito-foundation/jito-relayer/blob/master/relayer/src/relayer.rs
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

use crate::rpc::load_balancer::LoadBalancer;
use crossbeam_channel::{bounded, Receiver, RecvError, Sender};
use histogram::Histogram;
use jito_protos::proto::{
    block_engine::{
        block_engine_validator_server::BlockEngineValidator, BlockBuilderFeeInfoRequest,
        BlockBuilderFeeInfoResponse, SubscribeBundlesRequest, SubscribeBundlesResponse,
        SubscribePacketsRequest, SubscribePacketsResponse,
    },
    packet::PacketBatch as ProtoPacketBatch,
    shared::Header,
};
use log::*;
use prost_types::Timestamp;
use solana_metrics::datapoint_info;
use solana_perf::packet::PacketBatch;
use solana_sdk::{clock::Slot, pubkey::Pubkey, saturating_add_assign};
use thiserror::Error;
use tokio::sync::mpsc::{channel, error::TrySendError, Sender as TokioSender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{block_engine::health_manager::HealthState, convert::packet_to_proto_packet};

#[derive(Default)]
struct PacketForwardStats {
    num_packets_forwarded: u64,
    num_packets_dropped: u64,
}

struct BlockEngineMetrics {
    pub num_added_connections: u64,
    pub num_removed_connections: u64,
    pub num_current_connections: u64,
    pub metrics_latency_us: u64,
    pub num_try_send_channel_full: u64,
    pub packet_latencies_us: Histogram,

    pub crossbeam_delay_packet_receiver_processing_us: Histogram,
    pub crossbeam_subscription_receiver_processing_us: Histogram,
    pub crossbeam_metrics_tick_processing_us: Histogram,

    // channel stats
    pub slot_receiver_max_len: usize,
    pub slot_receiver_capacity: usize,
    pub subscription_receiver_max_len: usize,
    pub subscription_receiver_capacity: usize,
    pub delay_packet_receiver_max_len: usize,
    pub delay_packet_receiver_capacity: usize,
    pub packet_subscriptions_total_queued: usize, // sum of all items currently queued
    packet_stats_per_validator: HashMap<Pubkey, PacketForwardStats>,
}

impl BlockEngineMetrics {
    fn new(
        slot_receiver_capacity: usize,
        subscription_receiver_capacity: usize,
        delay_packet_receiver_capacity: usize,
    ) -> Self {
        BlockEngineMetrics {
            num_added_connections: 0,
            num_removed_connections: 0,
            num_current_connections: 0,
            metrics_latency_us: 0,
            num_try_send_channel_full: 0,
            packet_latencies_us: Histogram::default(),
            crossbeam_delay_packet_receiver_processing_us: Histogram::default(),
            crossbeam_subscription_receiver_processing_us: Histogram::default(),
            crossbeam_metrics_tick_processing_us: Histogram::default(),
            slot_receiver_max_len: 0,
            slot_receiver_capacity,
            subscription_receiver_max_len: 0,
            subscription_receiver_capacity,
            delay_packet_receiver_max_len: 0,
            delay_packet_receiver_capacity,
            packet_subscriptions_total_queued: 0,
            packet_stats_per_validator: HashMap::new(),
        }
    }

    fn update_max_len(
        &mut self,
        slot_receiver_len: usize,
        subscription_receiver_len: usize,
        delay_packet_receiver_len: usize,
    ) {
        self.slot_receiver_max_len = std::cmp::max(self.slot_receiver_max_len, slot_receiver_len);
        self.subscription_receiver_max_len = std::cmp::max(
            self.subscription_receiver_max_len,
            subscription_receiver_len,
        );
        self.delay_packet_receiver_max_len = std::cmp::max(
            self.delay_packet_receiver_max_len,
            delay_packet_receiver_len,
        );
    }

    fn update_packet_subscription_total_capacity(
        &mut self,
        packet_subscriptions: &HashMap<
            Pubkey,
            TokioSender<Result<SubscribePacketsResponse, Status>>,
        >,
    ) {
        let packet_subscriptions_total_queued = packet_subscriptions
            .values()
            .map(|x| BlockEngineImpl::SUBSCRIBER_QUEUE_CAPACITY - x.capacity())
            .sum::<usize>();
        self.packet_subscriptions_total_queued = packet_subscriptions_total_queued;
    }

    fn increment_packets_forwarded(&mut self, validator_id: &Pubkey, num_packets: u64) {
        self.packet_stats_per_validator
            .entry(*validator_id)
            .and_modify(|entry| saturating_add_assign!(entry.num_packets_forwarded, num_packets))
            .or_insert(PacketForwardStats {
                num_packets_forwarded: num_packets,
                num_packets_dropped: 0,
            });
    }

    fn increment_packets_dropped(&mut self, validator_id: &Pubkey, num_packets: u64) {
        self.packet_stats_per_validator
            .entry(*validator_id)
            .and_modify(|entry| saturating_add_assign!(entry.num_packets_dropped, num_packets))
            .or_insert(PacketForwardStats {
                num_packets_forwarded: 0,
                num_packets_dropped: num_packets,
            });
    }

    fn report(&self) {
        for (pubkey, stats) in &self.packet_stats_per_validator {
            datapoint_info!("block_engine_validator_metrics",
                "pubkey" => pubkey.to_string(),
                ("num_packets_forwarded", stats.num_packets_forwarded, i64),
                ("num_packets_dropped", stats.num_packets_dropped, i64),
            );
        }
        datapoint_info!(
            "block_engine_metrics",
            ("num_added_connections", self.num_added_connections, i64),
            ("num_removed_connections", self.num_removed_connections, i64),
            ("num_current_connections", self.num_current_connections, i64),
            (
                "num_try_send_channel_full",
                self.num_try_send_channel_full,
                i64
            ),
            ("metrics_latency_us", self.metrics_latency_us, i64),
            // packet latencies
            (
                "packet_latencies_us_min",
                self.packet_latencies_us.minimum().unwrap_or_default(),
                i64
            ),
            (
                "packet_latencies_us_max",
                self.packet_latencies_us.maximum().unwrap_or_default(),
                i64
            ),
            (
                "packet_latencies_us_p50",
                self.packet_latencies_us
                    .percentile(50.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "packet_latencies_us_p90",
                self.packet_latencies_us
                    .percentile(90.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "packet_latencies_us_p99",
                self.packet_latencies_us
                    .percentile(99.0)
                    .unwrap_or_default(),
                i64
            ),
            // crossbeam arm latencies
            (
                "crossbeam_subscription_receiver_processing_us_p50",
                self.crossbeam_subscription_receiver_processing_us
                    .percentile(50.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "crossbeam_subscription_receiver_processing_us_p90",
                self.crossbeam_subscription_receiver_processing_us
                    .percentile(90.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "crossbeam_subscription_receiver_processing_us_p99",
                self.crossbeam_subscription_receiver_processing_us
                    .percentile(99.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "crossbeam_metrics_tick_processing_us_p50",
                self.crossbeam_metrics_tick_processing_us
                    .percentile(50.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "crossbeam_metrics_tick_processing_us_p90",
                self.crossbeam_metrics_tick_processing_us
                    .percentile(90.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "crossbeam_metrics_tick_processing_us_p99",
                self.crossbeam_metrics_tick_processing_us
                    .percentile(99.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "crossbeam_delay_packet_receiver_processing_us_p50",
                self.crossbeam_delay_packet_receiver_processing_us
                    .percentile(50.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "crossbeam_delay_packet_receiver_processing_us_p90",
                self.crossbeam_delay_packet_receiver_processing_us
                    .percentile(90.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "crossbeam_delay_packet_receiver_processing_us_p99",
                self.crossbeam_delay_packet_receiver_processing_us
                    .percentile(99.0)
                    .unwrap_or_default(),
                i64
            ),
            // channel lengths
            ("slot_receiver_len", self.slot_receiver_max_len, i64),
            ("slot_receiver_capacity", self.slot_receiver_capacity, i64),
            (
                "subscription_receiver_len",
                self.subscription_receiver_max_len,
                i64
            ),
            (
                "subscription_receiver_capacity",
                self.subscription_receiver_capacity,
                i64
            ),
            (
                "delay_packet_receiver_len",
                self.delay_packet_receiver_max_len,
                i64
            ),
            (
                "delay_packet_receiver_capacity",
                self.delay_packet_receiver_capacity,
                i64
            ),
            (
                "packet_subscriptions_total_queued",
                self.packet_subscriptions_total_queued,
                i64
            ),
        );
    }
}

pub enum Subscription {
    BlockEnginePacketSubscription {
        pubkey: Pubkey,
        sender: TokioSender<Result<SubscribePacketsResponse, Status>>,
    },
    BlockEngineBundleSubscription {
        pubkey: Pubkey,
        sender: TokioSender<Result<SubscribeBundlesResponse, Status>>,
    },
}

#[derive(Error, Debug)]
pub enum BlockEngineError {
    #[error("shutdown")]
    Shutdown(#[from] RecvError),
}

pub type BlockEngineResult<T> = Result<T, BlockEngineError>;

type PacketSubscriptions =
    Arc<RwLock<HashMap<Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>>>>;
type BundleSubscriptions =
    Arc<RwLock<HashMap<Pubkey, TokioSender<Result<SubscribeBundlesResponse, Status>>>>>;

pub struct BlockEngineImpl {
    subscription_sender: Sender<Subscription>,
    threads: Vec<JoinHandle<()>>,
    health_state: Arc<RwLock<HealthState>>,
    _packet_subscriptions: PacketSubscriptions,
    _bundle_subscriptions: BundleSubscriptions,
}

impl BlockEngineImpl {
    pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 50_000;

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot_receiver: Receiver<Slot>,
        delay_packet_receiver: Receiver<PacketBatch>,
        health_state: Arc<RwLock<HealthState>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        // receiver tracked as block_engine_metrics.subscription_receiver_len
        let (subscription_sender, subscription_receiver) =
            bounded(LoadBalancer::SLOT_QUEUE_CAPACITY);

        let packet_subscriptions = Arc::new(RwLock::new(HashMap::default()));
        let bundle_subscriptions = Arc::new(RwLock::new(HashMap::default()));

        let thread = {
            let health_state = health_state.clone();
            let packet_subscriptions = packet_subscriptions.clone();
            let bundle_subscriptions = bundle_subscriptions.clone();
            thread::Builder::new()
                .name("block_engine-event_loop_thread".to_string())
                .spawn(move || {
                    let res = Self::run_event_loop(
                        slot_receiver,
                        subscription_receiver,
                        delay_packet_receiver,
                        health_state,
                        exit,
                        &packet_subscriptions,
                        &bundle_subscriptions,
                    );
                    warn!("BlockEngineImpl thread exited with result {res:?}")
                })
                .unwrap()
        };

        Self {
            subscription_sender,
            threads: vec![thread],
            health_state,
            _packet_subscriptions: packet_subscriptions,
            _bundle_subscriptions: bundle_subscriptions,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_event_loop(
        slot_receiver: Receiver<Slot>,
        subscription_receiver: Receiver<Subscription>,
        delay_packet_receiver: Receiver<PacketBatch>,
        _health_state: Arc<RwLock<HealthState>>,
        exit: Arc<AtomicBool>,
        packet_subscriptions: &PacketSubscriptions,
        bundle_subscriptions: &BundleSubscriptions,
    ) -> BlockEngineResult<()> {
        let metrics_tick = crossbeam_channel::tick(Duration::from_millis(1000));

        let mut relayer_metrics = BlockEngineMetrics::new(
            slot_receiver.capacity().unwrap(),
            subscription_receiver.capacity().unwrap(),
            delay_packet_receiver.capacity().unwrap(),
        );

        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(delay_packet_receiver) -> maybe_packet_batches => {
                    let start = Instant::now();
                    let failed_forwards = Self::forward_packets(maybe_packet_batches, packet_subscriptions, &mut relayer_metrics)?;
                    Self::drop_connections(failed_forwards, packet_subscriptions, &mut relayer_metrics);
                    let _ = relayer_metrics.crossbeam_delay_packet_receiver_processing_us.increment(start.elapsed().as_micros() as u64);
                },
                recv(subscription_receiver) -> maybe_subscription => {
                    let start = Instant::now();
                    Self::handle_subscription(maybe_subscription, packet_subscriptions, bundle_subscriptions, &mut relayer_metrics)?;
                    let _ = relayer_metrics.crossbeam_subscription_receiver_processing_us.increment(start.elapsed().as_micros() as u64);
                }
                recv(metrics_tick) -> time_generated => {
                    let start = Instant::now();
                    let l_packet_subscriptions = packet_subscriptions.read().unwrap();
                    relayer_metrics.num_current_connections = l_packet_subscriptions.len() as u64;
                    relayer_metrics.update_packet_subscription_total_capacity(&l_packet_subscriptions);
                    drop(l_packet_subscriptions);

                    if let Ok(time_generated) = time_generated {
                        relayer_metrics.metrics_latency_us = time_generated.elapsed().as_micros() as u64;
                    }
                    let _ = relayer_metrics.crossbeam_metrics_tick_processing_us.increment(start.elapsed().as_micros() as u64);

                    relayer_metrics.report();
                    relayer_metrics = BlockEngineMetrics::new(
                        slot_receiver.capacity().unwrap(),
                        subscription_receiver.capacity().unwrap(),
                        delay_packet_receiver.capacity().unwrap(),
                    );
                }
            }

            relayer_metrics.update_max_len(
                slot_receiver.len(),
                subscription_receiver.len(),
                delay_packet_receiver.len(),
            );
        }
        Ok(())
    }

    fn drop_connections(
        disconnected_pubkeys: Vec<Pubkey>,
        subscriptions: &PacketSubscriptions,
        relayer_metrics: &mut BlockEngineMetrics,
    ) {
        relayer_metrics.num_removed_connections += disconnected_pubkeys.len() as u64;

        let mut l_subscriptions = subscriptions.write().unwrap();
        for disconnected in disconnected_pubkeys {
            if let Some(sender) = l_subscriptions.remove(&disconnected) {
                datapoint_info!(
                    "block_engine_removed_subscription",
                    ("pubkey", disconnected.to_string(), String)
                );
                drop(sender);
            }
        }
    }

    #[allow(dead_code)]
    fn handle_heartbeat(
        subscriptions: &PacketSubscriptions,
        relayer_metrics: &mut BlockEngineMetrics,
    ) -> Vec<Pubkey> {
        let failed_pubkey_updates = subscriptions
            .read()
            .unwrap()
            .iter()
            .filter_map(|(pubkey, sender)| {
                // For BlockEngine protocol, we send empty batches as heartbeats
                match sender.try_send(Ok(SubscribePacketsResponse {
                    header: Some(Header {
                        ts: Some(Timestamp::from(SystemTime::now())),
                    }),
                    batch: None, // Empty batch for heartbeat
                })) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => return Some(*pubkey),
                    Err(TrySendError::Full(_)) => {
                        relayer_metrics.num_try_send_channel_full += 1;
                        warn!("heartbeat channel is full for: {:?}", pubkey);
                    }
                }
                None
            })
            .collect();

        failed_pubkey_updates
    }

    /// Returns pubkeys of subscribers that failed to send
    fn forward_packets(
        maybe_packet_batch: Result<PacketBatch, RecvError>,
        subscriptions: &PacketSubscriptions,
        relayer_metrics: &mut BlockEngineMetrics,
    ) -> BlockEngineResult<Vec<Pubkey>> {
        let packet_batch = maybe_packet_batch?;
        let packets: Vec<_> = packet_batch
            .iter()
            .filter_map(packet_to_proto_packet)
            .collect();
        let batch = ProtoPacketBatch { packets };

        let l_subscriptions: std::sync::RwLockReadGuard<
            '_,
            HashMap<Pubkey, TokioSender<Result<SubscribePacketsResponse, Status>>>,
        > = subscriptions.read().unwrap();

        let senders = l_subscriptions.iter().collect::<Vec<(
            &Pubkey,
            &TokioSender<Result<SubscribePacketsResponse, Status>>,
        )>>();

        let mut failed_forwards = Vec::new();

        for (pubkey, sender) in &senders {
            // try send because it's a bounded channel and we don't want to block if the channel is full
            match sender.try_send(Ok(SubscribePacketsResponse {
                header: Some(Header {
                    ts: Some(Timestamp::from(SystemTime::now())),
                }),
                batch: Some(batch.clone()),
            })) {
                Ok(_) => {
                    relayer_metrics.increment_packets_forwarded(pubkey, batch.packets.len() as u64);
                }
                Err(TrySendError::Full(_)) => {
                    error!("packet channel is full for pubkey: {:?}", pubkey);
                    relayer_metrics.increment_packets_dropped(pubkey, batch.packets.len() as u64);
                }
                Err(TrySendError::Closed(_)) => {
                    error!("channel is closed for pubkey: {:?}", pubkey);
                    failed_forwards.push(**pubkey);
                    break;
                }
            }
        }
        Ok(failed_forwards)
    }

    fn handle_subscription(
        maybe_subscription: Result<Subscription, RecvError>,
        packet_subscriptions: &PacketSubscriptions,
        bundle_subscriptions: &BundleSubscriptions,
        relayer_metrics: &mut BlockEngineMetrics,
    ) -> BlockEngineResult<()> {
        match maybe_subscription? {
            Subscription::BlockEnginePacketSubscription { pubkey, sender } => {
                match packet_subscriptions.write().unwrap().entry(pubkey) {
                    Entry::Vacant(entry) => {
                        entry.insert(sender);

                        relayer_metrics.num_added_connections += 1;
                        datapoint_info!(
                            "block_engine_new_subscription",
                            ("pubkey", pubkey.to_string(), String)
                        );
                    }
                    Entry::Occupied(mut entry) => {
                        datapoint_info!(
                            "block_engine_duplicate_subscription",
                            ("pubkey", pubkey.to_string(), String)
                        );
                        error!("already connected, dropping old connection: {pubkey:?}");
                        entry.insert(sender);
                    }
                }
            }
            Subscription::BlockEngineBundleSubscription { pubkey, sender } => {
                match bundle_subscriptions.write().unwrap().entry(pubkey) {
                    Entry::Vacant(entry) => {
                        entry.insert(sender);

                        relayer_metrics.num_added_connections += 1;
                        datapoint_info!(
                            "block_engine_new_bundle_subscription",
                            ("pubkey", pubkey.to_string(), String)
                        );
                    }
                    Entry::Occupied(mut entry) => {
                        datapoint_info!(
                            "block_engine_duplicate_bundle_subscription",
                            ("pubkey", pubkey.to_string(), String)
                        );
                        error!(
                            "already connected for bundles, dropping old connection: {pubkey:?}"
                        );
                        entry.insert(sender);
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn update_highest_slot(
        maybe_slot: Result<u64, RecvError>,
        highest_slot: &mut Slot,
    ) -> BlockEngineResult<()> {
        *highest_slot = maybe_slot?;
        datapoint_info!("relayer-highest_slot", ("slot", *highest_slot as i64, i64));
        Ok(())
    }

    /// Prevent validators from authenticating if the block engine is unhealthy
    fn check_health(health_state: &Arc<RwLock<HealthState>>) -> Result<(), Status> {
        if *health_state.read().unwrap() != HealthState::Healthy {
            Err(Status::internal("block engine is unhealthy"))
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    pub fn join(self) -> thread::Result<()> {
        for t in self.threads {
            t.join()?;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl BlockEngineValidator for BlockEngineImpl {
    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;

    /// Validator calls this to subscribe to packets
    async fn subscribe_packets(
        &self,
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        Self::check_health(&self.health_state)?;

        let pubkey: &Pubkey = request
            .extensions()
            .get()
            .ok_or_else(|| Status::internal("internal error fetching public key"))?;

        let (sender, receiver) = channel(BlockEngineImpl::SUBSCRIBER_QUEUE_CAPACITY);
        self.subscription_sender
            .send(Subscription::BlockEnginePacketSubscription {
                pubkey: *pubkey,
                sender,
            })
            .map_err(|_| Status::internal("internal error adding subscription"))?;

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    type SubscribeBundlesStream = ReceiverStream<Result<SubscribeBundlesResponse, Status>>;

    async fn subscribe_bundles(
        &self,
        request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        Self::check_health(&self.health_state)?;

        let pubkey: &Pubkey = request
            .extensions()
            .get()
            .ok_or_else(|| Status::internal("internal error fetching public key"))?;

        let (sender, receiver) = channel(BlockEngineImpl::SUBSCRIBER_QUEUE_CAPACITY);
        self.subscription_sender
            .send(Subscription::BlockEngineBundleSubscription {
                pubkey: *pubkey,
                sender,
            })
            .map_err(|_| Status::internal("internal error adding bundle subscription"))?;

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn get_block_builder_fee_info(
        &self,
        _request: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        Self::check_health(&self.health_state)?;

        // Return a simple fee structure
        // In a real implementation, this would come from configuration
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: "11111111111111111111111111111111".to_string(), // System program
            commission: 5,                                          // 5% commission
        }))
    }
}
