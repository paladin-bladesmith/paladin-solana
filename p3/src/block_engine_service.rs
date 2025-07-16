use crate::convert::packet_to_proto_packet;
use jito_protos::proto::block_engine::block_engine_validator_server::{
    BlockEngineValidator, BlockEngineValidatorServer,
};
use jito_protos::proto::block_engine::{
    BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse, SubscribeBundlesRequest,
    SubscribeBundlesResponse, SubscribePacketsRequest, SubscribePacketsResponse,
};
use jito_protos::proto::shared::Header;
use solana_perf::packet::PacketBatch;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{error, info};

pub struct BlockEngine {
    exit: Arc<AtomicBool>,
    packet_tx: broadcast::Sender<PacketBatch>,
}

impl BlockEngine {
    pub fn new(exit: Arc<AtomicBool>, packet_tx: broadcast::Sender<PacketBatch>) -> Self {
        Self { exit, packet_tx }
    }

    pub async fn serve(
        self,
        bind_addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting BlockEngine gRPC server on {}", bind_addr);

        let server = Server::builder()
            .add_service(BlockEngineValidatorServer::new(self))
            .serve(bind_addr);

        server.await?;
        Ok(())
    }

    pub fn spawn_server(
        exit: Arc<AtomicBool>,
        packet_tx: broadcast::Sender<PacketBatch>,
        bind_addr: SocketAddr,
    ) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        tokio::spawn(async move {
            let block_engine = BlockEngine::new(exit, packet_tx);
            block_engine.serve(bind_addr).await
        })
    }
}

// https://github.com/jito-labs/mev-protos/blob/master/block_engine.proto#L72
#[tonic::async_trait]
impl BlockEngineValidator for BlockEngine {
    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;
    type SubscribeBundlesStream = ReceiverStream<Result<SubscribeBundlesResponse, Status>>;

    async fn subscribe_packets(
        &self,
        _request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        info!("Validator subscribed to packets");

        let (tx, rx) = mpsc::channel(1024);

        let mut packet_rx = self.packet_tx.subscribe();
        let exit = self.exit.clone();

        tokio::spawn(async move {
            while !exit.load(std::sync::atomic::Ordering::Relaxed) {
                match packet_rx.recv().await {
                    Ok(packet_batch) => {
                        // Convert PacketBatch to proto packet format
                        let proto_batch = jito_protos::proto::packet::PacketBatch {
                            packets: packet_batch
                                .iter()
                                .filter_map(|p| packet_to_proto_packet(p))
                                .collect(),
                        };

                        let response = SubscribePacketsResponse {
                            header: Some(Header {
                                ts: Some(
                                    prost_types::Timestamp::from(std::time::SystemTime::now()),
                                ),
                            }),
                            batch: Some(proto_batch),
                        };

                        if let Err(e) = tx.send(Ok(response)).await {
                            error!("Failed to send packet batch: {}", e);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Some packets were missed due to lag
                        error!("Packet receiver lagged, missed {} packets", n);
                        // Continue processing
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        error!("Packet receiver closed");
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn subscribe_bundles(
        &self,
        _request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        info!("Validator subscribed to bundles");

        let (tx, rx) = mpsc::channel(1024);
        let exit = self.exit.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let response = SubscribeBundlesResponse {
                            bundles: vec![], // Empty for now
                        };

                        if let Err(e) = tx.send(Ok(response)).await {
                            error!("Failed to send bundle response: {}", e);
                            break;
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        if exit.load(std::sync::atomic::Ordering::Relaxed) {
                            info!("Exiting bundle subscription loop");
                            break;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_block_builder_fee_info(
        &self,
        _request: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        info!("Block builder fee info requested");

        // Return default fee info (no fees for now)
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: "".to_string(), // Empty pubkey means no fees
            commission: 0,
        }))
    }
}
