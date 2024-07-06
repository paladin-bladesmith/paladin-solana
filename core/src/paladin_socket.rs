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

const DEFAULT_ENDPOINT: &str = "0.0.0.0:4815";

const ERR_RETRY_DELAY: Duration = Duration::from_secs(1);
const METRICS_REPORT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
enum PaladinError {
    #[error("Socket error; err={0}")]
    Socket(#[from] std::io::Error),
    #[error("Crossbeam error; err={0}")]
    Crossbeam(#[from] TrySendError<Vec<PacketBundle>>),
}

pub(crate) struct PaladinSocket {
    exit: Arc<AtomicBool>,
    paladin_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,

    socket: TcpListener,
    stream: Option<Framed<TcpStream, TransactionStreamCodec>>,
    bundle_id: u16,

    metrics: PaladinSocketMetrics,
    metrics_interval: tokio::time::Interval,
    metrics_creation: Instant,
}

impl PaladinSocket {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        paladin_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
    ) -> std::thread::JoinHandle<()> {
        info!("Spawning PaladinSocket");

        std::thread::Builder::new()
            .name("paladin-socket".to_string())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let worker = PaladinSocket::new(exit, paladin_tx).await;
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

        PaladinSocket {
            exit,
            paladin_tx,

            socket,
            stream: None,
            bundle_id: 0,

            metrics_interval: tokio::time::interval(METRICS_REPORT_INTERVAL),
            metrics: PaladinSocketMetrics::default(),
            metrics_creation: Instant::now(),
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
                _ = self.metrics_interval.tick() => {
                    let now = Instant::now();
                    self.metrics.report(now.duration_since(self.metrics_creation));

                    self.metrics = PaladinSocketMetrics::default();
                    self.metrics_creation = now;

                    if self.exit.load(Ordering::Relaxed) {
                        break Ok(());
                    }
                }
            }
        }
    }

    fn on_bundles(&mut self, bundles: Vec<Vec<Packet>>) -> Result<(), PaladinError> {
        debug!("Received bundles; count={}", bundles.len());
        self.metrics.num_paladin_bundles = self
            .metrics
            .num_paladin_bundles
            .saturating_add(bundles.len() as u64);

        // Convert bundles into Jito's expected format.
        let bundles: Vec<_> = bundles
            .into_iter()
            .map(|packets| PacketBundle {
                bundle_id: format!("P{}", self.next_id()),
                batch: PacketBatch::new(packets),
            })
            .collect();

        // Bon voyage.
        self.paladin_tx.try_send(bundles).map_err(Into::into)
    }

    fn next_id(&mut self) -> u16 {
        self.bundle_id = self.bundle_id.wrapping_add(1);
        self.bundle_id
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
struct PaladinSocketMetrics {
    num_paladin_bundles: u64,
}

impl PaladinSocketMetrics {
    pub(crate) fn report(&self, age: Duration) {
        datapoint_info!(
            "paladin_socket",
            ("metrics_age_us", age.as_micros() as i64, i64),
            ("num_paladin_bundles", self.num_paladin_bundles, i64),
        );
    }
}
