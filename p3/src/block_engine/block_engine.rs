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

use crate::{
    convert::{proto_packets_to_batch, proto_packets_to_bundle},
    rpc::load_balancer::LoadBalancer,
};
use crossbeam_channel::{bounded, Receiver, RecvError, Sender};
use histogram::Histogram;
use jito_protos::proto::{
    block_engine::{
        block_engine_validator_server::BlockEngineValidator, BlockBuilderFeeInfoRequest,
        BlockBuilderFeeInfoResponse, SubscribeBundlesRequest, SubscribeBundlesResponse,
        SubscribePacketsRequest, SubscribePacketsResponse,
    },
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
    health_state: Arc<RwLock<HealthState>>,
    identity_pubkey: Pubkey,
}

impl BlockEngineImpl {
    pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 50_000;

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot_receiver: Receiver<Slot>,
        delay_packet_receiver: Receiver<PacketBatch>,
        health_state: Arc<RwLock<HealthState>>,
        identity_pubkey: Pubkey,
        exit: Arc<AtomicBool>,
    ) -> (Self, JoinHandle<()>) {
        // receiver tracked as block_engine_metrics.subscription_receiver_len
        let (subscription_sender, subscription_receiver) =
            bounded(LoadBalancer::SLOT_QUEUE_CAPACITY);

        let packet_subscriptions = Arc::new(RwLock::new(HashMap::default()));
        let bundle_subscriptions = Arc::new(RwLock::new(HashMap::default()));

        let thread = {
            let packet_subscriptions = packet_subscriptions.clone();
            let bundle_subscriptions = bundle_subscriptions.clone();
            thread::Builder::new()
                .name("block_engine-event_loop_thread".to_string())
                .spawn(move || {
                    let res = Self::run_event_loop(
                        slot_receiver,
                        subscription_receiver,
                        delay_packet_receiver,
                        exit,
                        &packet_subscriptions,
                        &bundle_subscriptions,
                    );
                    warn!("BlockEngineImpl thread exited with result {res:?}")
                })
                .unwrap()
        };

        (
            Self {
                subscription_sender,
                health_state,
                identity_pubkey,
            },
            thread,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_event_loop(
        slot_receiver: Receiver<Slot>,
        subscription_receiver: Receiver<Subscription>,
        delay_packet_receiver: Receiver<PacketBatch>,
        exit: Arc<AtomicBool>,
        packet_subscriptions: &PacketSubscriptions,
        bundle_subscriptions: &BundleSubscriptions,
    ) -> BlockEngineResult<()> {
        let metrics_tick = crossbeam_channel::tick(Duration::from_millis(1000));

        let mut block_engine_metrics = BlockEngineMetrics::new(
            slot_receiver.capacity().unwrap(),
            subscription_receiver.capacity().unwrap(),
            delay_packet_receiver.capacity().unwrap(),
        );

        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(delay_packet_receiver) -> maybe_packet_batches => {
                    let start = Instant::now();
                    let failed_forwards = Self::forward_packets(maybe_packet_batches, packet_subscriptions, bundle_subscriptions, &mut block_engine_metrics)?;
                    Self::drop_connections(failed_forwards, packet_subscriptions, &mut block_engine_metrics);
                    let _ = block_engine_metrics.crossbeam_delay_packet_receiver_processing_us.increment(start.elapsed().as_micros() as u64);
                },
                recv(subscription_receiver) -> maybe_subscription => {
                    let start = Instant::now();
                    Self::handle_subscription(maybe_subscription, packet_subscriptions, bundle_subscriptions, &mut block_engine_metrics)?;
                    let _ = block_engine_metrics.crossbeam_subscription_receiver_processing_us.increment(start.elapsed().as_micros() as u64);
                }
                recv(metrics_tick) -> time_generated => {
                    let start = Instant::now();
                    let l_packet_subscriptions = packet_subscriptions.read().unwrap();
                    block_engine_metrics.num_current_connections = l_packet_subscriptions.len() as u64;
                    block_engine_metrics.update_packet_subscription_total_capacity(&l_packet_subscriptions);
                    drop(l_packet_subscriptions);

                    if let Ok(time_generated) = time_generated {
                        block_engine_metrics.metrics_latency_us = time_generated.elapsed().as_micros() as u64;
                    }
                    let _ = block_engine_metrics.crossbeam_metrics_tick_processing_us.increment(start.elapsed().as_micros() as u64);

                    block_engine_metrics.report();
                    block_engine_metrics = BlockEngineMetrics::new(
                        slot_receiver.capacity().unwrap(),
                        subscription_receiver.capacity().unwrap(),
                        delay_packet_receiver.capacity().unwrap(),
                    );
                }
            }

            block_engine_metrics.update_max_len(
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
        block_engine_metrics: &mut BlockEngineMetrics,
    ) {
        block_engine_metrics.num_removed_connections += disconnected_pubkeys.len() as u64;

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

    /// Returns pubkeys of subscribers that failed to send
    fn forward_packets(
        maybe_packet_batch: Result<PacketBatch, RecvError>,
        packet_subscriptions: &PacketSubscriptions,
        bundle_subscriptions: &BundleSubscriptions,
        metrics: &mut BlockEngineMetrics,
    ) -> BlockEngineResult<Vec<Pubkey>> {
        let batch = maybe_packet_batch?;
        // Partition into two Vecs of proto packets
        let (p3_pkts, mev_pkts) =
            batch
                .into_iter()
                .fold((Vec::new(), Vec::new()), |(mut p3, mut mev), pkt| {
                    let proto = packet_to_proto_packet(&pkt);
                    if pkt.meta().is_p3() {
                        p3.extend(proto.clone())
                    }
                    if pkt.meta().is_mev() {
                        mev.extend(proto)
                    }
                    (p3, mev)
                });

        let now_ts = Timestamp::from(SystemTime::now());
        let p3_count = p3_pkts.len() as u64;
        let mev_count = mev_pkts.len() as u64;
        let batch = proto_packets_to_batch(p3_pkts).unwrap_or_default();
        let packet_resp = {
            let batch = batch.clone();
            let ts = now_ts.clone();
            move |_: &Pubkey| SubscribePacketsResponse {
                header: Some(Header {
                    ts: Some(ts.clone()),
                }),
                batch: Some(batch.clone()),
            }
        };
        let bundle = proto_packets_to_bundle(mev_pkts, now_ts, String::new()).unwrap_or_default();
        let bundle_resp = move |_: &Pubkey| SubscribeBundlesResponse {
            bundles: vec![bundle.clone()],
        };

        // Generic broadcaster
        fn broadcast<T, F>(
            senders: Vec<(&Pubkey, &TokioSender<Result<T, Status>>)>,
            make: F,
            metrics: &mut BlockEngineMetrics,
            count: u64,
        ) -> Vec<Pubkey>
        where
            F: Fn(&Pubkey) -> T,
        {
            let mut failed = Vec::new();
            for (pk, tx) in senders {
                match tx.try_send(Ok(make(pk))) {
                    Ok(_) => metrics.increment_packets_forwarded(pk, count),
                    Err(TrySendError::Full(_)) => metrics.increment_packets_dropped(pk, count),
                    Err(TrySendError::Closed(_)) => failed.push(*pk),
                }
            }
            failed
        }

        let mut failed = Vec::new();

        let l_senders = packet_subscriptions.read().unwrap();
        let senders = l_senders.iter().collect::<Vec<_>>();
        failed.extend(broadcast(senders, packet_resp, metrics, p3_count));

        let l_senders = bundle_subscriptions.read().unwrap();
        let senders = l_senders.iter().collect::<Vec<_>>();
        failed.extend(broadcast(senders, bundle_resp, metrics, mev_count));

        Ok(failed)
    }

    fn handle_subscription(
        maybe_subscription: Result<Subscription, RecvError>,
        packet_subscriptions: &PacketSubscriptions,
        bundle_subscriptions: &BundleSubscriptions,
        block_engine_metrics: &mut BlockEngineMetrics,
    ) -> BlockEngineResult<()> {
        let subscription = maybe_subscription?;

        fn insert_subscription<T>(
            subscriptions: &Arc<RwLock<HashMap<Pubkey, TokioSender<Result<T, Status>>>>>,
            metrics: &mut BlockEngineMetrics,
            new_event: &'static str,
            dup_event: &'static str,
            err_msg: &str,
            key: Pubkey,
            sender: TokioSender<Result<T, Status>>,
        ) {
            let mut map = subscriptions.write().unwrap();
            match map.entry(key) {
                Entry::Vacant(e) => {
                    e.insert(sender);
                    metrics.num_added_connections += 1;
                    datapoint_info!(new_event, ("pubkey", key.to_string(), String));
                }
                Entry::Occupied(mut e) => {
                    datapoint_info!(dup_event, ("pubkey", key.to_string(), String));
                    error!("{}", err_msg);
                    e.insert(sender);
                }
            }
        }

        match subscription {
            Subscription::BlockEnginePacketSubscription { pubkey, sender } => {
                let err = format!("already connected, dropping old connection: {:?}", pubkey);
                insert_subscription(
                    packet_subscriptions,
                    block_engine_metrics,
                    "block_engine_new_packet_subscription",
                    "block_engine_duplicate_packet_subscription",
                    &err,
                    pubkey,
                    sender,
                );
            }
            Subscription::BlockEngineBundleSubscription { pubkey, sender } => {
                let err = format!(
                    "already connected for bundles, dropping old connection: {:?}",
                    pubkey
                );
                insert_subscription(
                    bundle_subscriptions,
                    block_engine_metrics,
                    "block_engine_new_bundle_subscription",
                    "block_engine_duplicate_bundle_subscription",
                    &err,
                    pubkey,
                    sender,
                );
            }
        }

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

        // Return the identity pubkey as the fee publisher
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: self.identity_pubkey.to_string(),
            commission: 0,
        }))
    }
}
