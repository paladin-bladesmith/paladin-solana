use {
    crate::packet_bundle::PacketBundle,
    bytes::{Buf, BytesMut},
    crossbeam_channel::TrySendError,
    futures::{future::OptionFuture, StreamExt},
    solana_perf::packet::PacketBatch,
    solana_sdk::packet::{Meta, Packet, PACKET_DATA_SIZE},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::net::{TcpListener, TcpStream},
    tokio_util::codec::{Decoder, Framed},
};

const DEFAULT_ENDPOINT: &str = "127.0.0.1:4815";

const ERR_RETRY_DELAY: Duration = Duration::from_secs(1);
const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
enum PaladinError {
    #[error("Socket error; err={0}")]
    Socket(#[from] std::io::Error),
    #[error("Crossbeam error; err={0}")]
    Crossbeam(#[from] TrySendError<Vec<PacketBundle>>),
}

pub(crate) struct PaladinStage {
    exit: Arc<AtomicBool>,
    bundle_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,

    socket: TcpListener,
    stream: Option<Framed<TcpStream, TransactionStreamCodec>>,

    stats: PaladinStageStats,
    stats_interval: tokio::time::Interval,
    stats_creation: Instant,
}

impl PaladinStage {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        paladin_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
    ) -> std::thread::JoinHandle<()> {
        info!("Spawning PaladinStage");

        std::thread::Builder::new()
            .name("paladin-stage".to_string())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let worker = PaladinStage::new(exit, paladin_tx).await;
                        worker.run().await;
                    })
            })
            .unwrap()
    }

    async fn new(
        exit: Arc<AtomicBool>,
        paladin_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
    ) -> Self {
        // Determine socket endpoint.
        let socket_endpoint = match std::env::var("PALADIN_TX_ENDPOINT") {
            Ok(endpoint) => endpoint,
            Err(std::env::VarError::NotUnicode(raw)) => {
                error!("Failed to parse PALADIN_TX_ENDPOINT; raw={raw:?}");
                DEFAULT_ENDPOINT.to_owned()
            }
            Err(std::env::VarError::NotPresent) => DEFAULT_ENDPOINT.to_owned(),
        };

        // Setup a TCP socket to receive batches of arb bundles.
        let socket = tokio::net::TcpListener::bind(&socket_endpoint)
            .await
            .unwrap();
        info!("Paladin stage socket bound; endpoint={socket_endpoint}");

        PaladinStage {
            exit,
            bundle_tx: paladin_tx,

            socket,
            stream: None,
            stats_interval: tokio::time::interval(STATS_REPORT_INTERVAL),

            stats: PaladinStageStats::default(),
            stats_creation: Instant::now(),
        }
    }

    async fn run(mut self) {
        while let Err(err) = self.run_until_err().await {
            warn!("PaladinStage encountered error, restarting; err={err}");
            tokio::time::sleep(ERR_RETRY_DELAY).await;
        }
    }

    async fn run_until_err(&mut self) -> Result<(), PaladinError> {
        loop {
            tokio::select! {
                biased;

                res = self.socket.accept() => {
                    let (stream, address) = res?;

                    info!("Accepted TcpStream; address={address}");
                    self.stream = Some(Framed::new(stream, TransactionStreamCodec));
                }
                Some(opt) = OptionFuture::from(self.stream.as_mut().map(|stream| stream.next())) =>
                {
                    match opt {
                        Some(res) => match res {
                            Ok(bundles) => self.on_bundles(bundles)?,
                            Err(err) => {
                                warn!("TransactionStream errored; err={err}");
                                self.stream = None;
                            }
                        }
                        None => {
                            warn!("TransactionStream dropped without error");
                            self.stream = None;
                        }
                    }
                }
                _ = self.stats_interval.tick() => {
                    let now = Instant::now();
                    self.stats.report(now.duration_since(self.stats_creation));

                    self.stats = PaladinStageStats::default();
                    self.stats_creation = now;

                    if self.exit.load(Ordering::Relaxed) {
                        break Ok(());
                    }
                }
            }
        }
    }

    fn on_bundles(&mut self, bundles: Vec<Vec<Packet>>) -> Result<(), PaladinError> {
        self.stats.num_paladin_bundles = self
            .stats
            .num_paladin_bundles
            .saturating_add(bundles.len() as u64);

        // Convert bundles into Jito's expected format.
        let bundles: Vec<_> = bundles
            .into_iter()
            .map(|packets| PacketBundle {
                bundle_id: String::from("P"),
                batch: PacketBatch::new(packets),
            })
            .collect();

        println!("Received {} bundles", bundles.len());
        for bundle in &bundles {
            println!("Bundle ID: {:?}", bundle.bundle_id);
        }

        // Bon voyage.
        self.bundle_tx.try_send(bundles).map_err(Into::into)
    }
}

struct TransactionStreamCodec;

impl Decoder for TransactionStreamCodec {
    type Item = Vec<Vec<Packet>>;
    type Error = TransactionStreamError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut cursor = std::io::Cursor::new(&src);
        match bincode::deserialize_from::<_, Vec<Vec<Vec<u8>>>>(&mut cursor).map_err(|err| *err) {
            Ok(bundles) => {
                src.advance(cursor.position() as usize);

                let mut packet_batches = Vec::with_capacity(bundles.len());
                for bundle in bundles {
                    let mut batch = Vec::with_capacity(bundle.len());
                    for tx in bundle {
                        if tx.len() > PACKET_DATA_SIZE {
                            return Err(TransactionStreamError::TransactionSize);
                        }

                        // Copy transaction to fixed size packet buffer.
                        let mut buffer = [0; PACKET_DATA_SIZE];
                        buffer[0..tx.len()].copy_from_slice(&tx);

                        batch.push(Packet::new(
                            buffer,
                            Meta {
                                size: tx.len(),
                                ..Meta::default()
                            },
                        ));
                    }

                    packet_batches.push(batch);
                }

                Ok(Some(packet_batches))
            }
            Err(bincode::ErrorKind::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                Ok(None)
            }
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Debug, Error)]
enum TransactionStreamError {
    #[error("IO; err={0}")]
    Io(#[from] std::io::Error),
    #[error("Deserialize; err={0}")]
    Deserialize(#[from] bincode::ErrorKind),
    #[error("Transaction exceeded packet size")]
    TransactionSize,
}

#[derive(Default)]
struct PaladinStageStats {
    num_paladin_bundles: u64,
}

impl PaladinStageStats {
    pub(crate) fn report(&self, age: Duration) {
        datapoint_info!(
            "paladin_stage-stats",
            ("stats_age_us", age.as_micros() as i64, i64),
            ("num_paladin_bundles", self.num_paladin_bundles, i64),
        );
    }
}
