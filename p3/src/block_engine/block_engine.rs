// https://github.com/jito-foundation/jito-relayer/blob/master/relayer/src/relayer.rs
use {
    crate::{
        convert::{packet_to_proto_packet, proto_packets_to_batch, proto_packets_to_bundle},
        rpc::load_balancer::LoadBalancer,
    },
    crossbeam_channel::{bounded, Receiver, RecvError, Sender},
    jito_protos::proto::{
        block_engine::{
            block_engine_validator_server::BlockEngineValidator, BlockBuilderFeeInfoRequest,
            BlockBuilderFeeInfoResponse, SubscribeBundlesRequest, SubscribeBundlesResponse,
            SubscribePacketsRequest, SubscribePacketsResponse,
        },
        shared::Header,
    },
    log::*,
    prost_types::Timestamp,
    solana_perf::packet::PacketBatch,
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::{hash_map::Entry, HashMap},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, JoinHandle},
        time::SystemTime,
    },
    thiserror::Error,
    tokio::sync::mpsc::{channel, error::TrySendError, Sender as TokioSender},
    tokio_stream::wrappers::ReceiverStream,
    tonic::{Request, Response, Status},
};

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
    identity_pubkey: Pubkey,
}

impl BlockEngineImpl {
    pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 50_000;

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        delay_packet_receiver: Receiver<PacketBatch>,
        identity_pubkey: Pubkey,
        exit: Arc<AtomicBool>,
    ) -> (Self, JoinHandle<()>) {
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
                identity_pubkey,
            },
            thread,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_event_loop(
        subscription_receiver: Receiver<Subscription>,
        delay_packet_receiver: Receiver<PacketBatch>,
        exit: Arc<AtomicBool>,
        packet_subscriptions: &PacketSubscriptions,
        bundle_subscriptions: &BundleSubscriptions,
    ) -> BlockEngineResult<()> {
        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(delay_packet_receiver) -> maybe_packet_batches => {
                    let failed_forwards = Self::forward_packets(maybe_packet_batches, packet_subscriptions, bundle_subscriptions)?;
                    Self::drop_connections(failed_forwards, packet_subscriptions);
                },
                recv(subscription_receiver) -> maybe_subscription => {
                    Self::handle_subscription(maybe_subscription, packet_subscriptions, bundle_subscriptions)?;
                }
            }
        }
        Ok(())
    }

    fn drop_connections(disconnected_pubkeys: Vec<Pubkey>, subscriptions: &PacketSubscriptions) {
        let mut l_subscriptions = subscriptions.write().unwrap();
        for disconnected in disconnected_pubkeys {
            l_subscriptions.remove(&disconnected);
        }
    }

    /// Returns pubkeys of subscribers that failed to send
    fn forward_packets(
        maybe_packet_batch: Result<PacketBatch, RecvError>,
        packet_subscriptions: &PacketSubscriptions,
        bundle_subscriptions: &BundleSubscriptions,
    ) -> BlockEngineResult<Vec<Pubkey>> {
        let batch = maybe_packet_batch?;
        // Partition into two Vecs of proto packets
        let (p3_pkts, mev_pkts) =
            batch
                .into_iter()
                .fold((Vec::new(), Vec::new()), |(mut p3, mut mev), pkt| {
                    let proto = packet_to_proto_packet(pkt);
                    if pkt.meta().is_p3() {
                        p3.extend(proto.clone())
                    }
                    if pkt.meta().is_mev() {
                        mev.extend(proto)
                    }
                    (p3, mev)
                });

        let now_ts = Timestamp::from(SystemTime::now());
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
        ) -> Vec<Pubkey>
        where
            F: Fn(&Pubkey) -> T,
        {
            let mut failed = Vec::new();
            for (pk, tx) in senders {
                match tx.try_send(Ok(make(pk))) {
                    Ok(_) => {}
                    Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Closed(_)) => failed.push(*pk),
                }
            }
            failed
        }

        let mut failed = Vec::new();

        let l_senders = packet_subscriptions.read().unwrap();
        let senders = l_senders.iter().collect::<Vec<_>>();
        failed.extend(broadcast(senders, packet_resp));

        let l_senders = bundle_subscriptions.read().unwrap();
        let senders = l_senders.iter().collect::<Vec<_>>();
        failed.extend(broadcast(senders, bundle_resp));

        Ok(failed)
    }

    fn handle_subscription(
        maybe_subscription: Result<Subscription, RecvError>,
        packet_subscriptions: &PacketSubscriptions,
        bundle_subscriptions: &BundleSubscriptions,
    ) -> BlockEngineResult<()> {
        let subscription = maybe_subscription?;

        fn insert_subscription<T>(
            subscriptions: &Arc<RwLock<HashMap<Pubkey, TokioSender<Result<T, Status>>>>>,
            err_msg: &str,
            key: Pubkey,
            sender: TokioSender<Result<T, Status>>,
        ) {
            let mut map = subscriptions.write().unwrap();
            match map.entry(key) {
                Entry::Vacant(e) => {
                    e.insert(sender);
                }
                Entry::Occupied(mut e) => {
                    error!("{}", err_msg);
                    e.insert(sender);
                }
            }
        }

        match subscription {
            Subscription::BlockEnginePacketSubscription { pubkey, sender } => {
                let err = format!("already connected, dropping old connection: {:?}", pubkey);
                insert_subscription(packet_subscriptions, &err, pubkey, sender);
            }
            Subscription::BlockEngineBundleSubscription { pubkey, sender } => {
                let err = format!(
                    "already connected for bundles, dropping old connection: {:?}",
                    pubkey
                );
                insert_subscription(bundle_subscriptions, &err, pubkey, sender);
            }
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
        // Return the identity pubkey as the fee publisher
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: self.identity_pubkey.to_string(),
            commission: 0,
        }))
    }
}
