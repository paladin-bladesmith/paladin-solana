use {
    crate::{packet_bundle::PacketBundle, tpu::MAX_QUIC_CONNECTIONS_PER_PEER},
    crossbeam_channel::{RecvTimeoutError, TrySendError},
    paladin_lockup_program::state::LockupPool,
    solana_perf::packet::PacketBatch,
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{
        account::ReadableAccount,
        net::DEFAULT_TPU_COALESCE,
        packet::Packet,
        pubkey::Pubkey,
        saturating_add_assign,
        signature::{Keypair, Signature},
    },
    solana_streamer::{
        nonblocking::quic::{
            ConnectionPeerType, ConnectionTable, DEFAULT_MAX_CONNECTIONS_PER_IPADDR_PER_MINUTE,
        },
        quic::{EndpointKeyUpdater, SpawnServerResult},
        streamer::StakedNodes,
    },
    spl_discriminator::discriminator::SplDiscriminate,
    std::{
        collections::HashMap,
        net::{SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        time::{Duration, Instant},
    },
};

pub const P3_QUIC_SOCKET_DEFAULT: &str = "0.0.0.0:4819";

const MAX_STAKED_CONNECTIONS: usize = 256;
const MAX_UNSTAKED_CONNECTIONS: usize = 0;
/// This results in 100 streams per 100ms, i.e. 1000 global TPS. Users with less
/// than 1% stake will be rounded up to 1 stream per 100ms.
const MAX_STREAMS_PER_MS: u64 = 1;

const STAKED_NODES_UPDATE_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes
const POOL_KEY: Pubkey = solana_sdk::pubkey!("EJi4Rj2u1VXiLpKtaqeQh3w4XxAGLFqnAG1jCorSvVmg");

pub(crate) struct P3Quic {
    exit: Arc<AtomicBool>,
    quic_server: std::thread::JoinHandle<()>,

    quic_rx: crossbeam_channel::Receiver<PacketBatch>,
    bundle_stage_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,

    staked_nodes: Arc<RwLock<StakedNodes>>,
    staked_nodes_last_update: Instant,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    staked_connection_table: Arc<Mutex<ConnectionTable>>,

    metrics: P3Metrics,
    metrics_creation: Instant,
}

impl P3Quic {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        bundle_stage_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
        addr: SocketAddr,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        keypair: &Keypair,
    ) -> (std::thread::JoinHandle<()>, Arc<EndpointKeyUpdater>) {
        // Bind the P3 QUIC UDP socket.
        let sock = UdpSocket::bind(addr).unwrap();

        // Setup initial staked nodes (empty).
        let stakes = Arc::default();
        let staked_nodes = Arc::new(RwLock::new(StakedNodes::new(stakes, HashMap::default())));

        // TODO: Would be ideal to reduce the number of threads spawned by tokio
        // in streamer (i.e. make it an argument).

        // Create the connection table here as we want to share
        let staked_connection_table: Arc<Mutex<ConnectionTable>> =
            Arc::new(Mutex::new(ConnectionTable::new()));

        // Spawn the P3 QUIC server.
        let (quic_tx, quic_rx) = crossbeam_channel::unbounded();
        let SpawnServerResult {
            endpoint: _,
            thread: quic_server,
            key_updater,
        } = solana_streamer::quic::spawn_server(
            "p3Quic-streamer",
            "p3_quic",
            sock,
            keypair,
            // NB: Packets are verified in paladin bundle stage.
            quic_tx,
            exit.clone(),
            MAX_QUIC_CONNECTIONS_PER_PEER,
            staked_nodes.clone(),
            MAX_STAKED_CONNECTIONS,
            MAX_UNSTAKED_CONNECTIONS,
            MAX_STREAMS_PER_MS,
            DEFAULT_MAX_CONNECTIONS_PER_IPADDR_PER_MINUTE,
            // Streams will be kept alive for 300s (5min) if no data is sent.
            Duration::from_secs(300),
            DEFAULT_TPU_COALESCE,
        )
        .unwrap();

        // Spawn the P3 management thread.
        let p3 = Self {
            exit: exit.clone(),
            quic_server,

            quic_rx,
            bundle_stage_tx,

            staked_nodes,
            staked_nodes_last_update: Instant::now(),
            poh_recorder: poh_recorder.clone(),
            staked_connection_table,

            metrics: P3Metrics::default(),
            metrics_creation: Instant::now(),
        };

        (
            std::thread::Builder::new()
                .name("P3Quic".to_owned())
                .spawn(move || p3.run())
                .unwrap(),
            key_updater,
        )
    }

    fn run(mut self) {
        let start = Instant::now();
        self.update_staked_nodes();
        saturating_add_assign!(
            self.metrics.staked_nodes_us,
            start.elapsed().as_micros() as u64
        );

        while !self.exit.load(Ordering::Relaxed) {
            // Block up to 1s waiting for packets.
            match self.quic_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(packets) => self.on_packets(packets),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    if !self.exit.load(Ordering::Relaxed) {
                        error!("Quic channel closed unexpectedly");
                    }

                    break;
                }
            };

            // Check if we need to report metrics for the last interval.
            let now = Instant::now();
            let age = now - self.metrics_creation;
            if age > Duration::from_secs(1) {
                self.metrics.report(age.as_millis() as u64);
                self.metrics = P3Metrics::default();
                self.metrics_creation = now;
            }

            // Check if we need to update staked nodes.
            if self.staked_nodes_last_update.elapsed() >= STAKED_NODES_UPDATE_INTERVAL {
                let start = Instant::now();
                self.update_staked_nodes();
                saturating_add_assign!(
                    self.metrics.staked_nodes_us,
                    start.elapsed().as_micros() as u64
                );

                self.staked_nodes_last_update = Instant::now();
                trace!(
                    "Updated staked_nodes; stakes={:?}",
                    self.staked_nodes.read().unwrap().stakes,
                );
            }
        }

        self.quic_server.join().unwrap();
    }

    fn on_packets(&mut self, packets: PacketBatch) {
        // Process the batch.
        saturating_add_assign!(self.metrics.packets, packets.len() as u64);
        for packet in &packets {
            if self.on_packet(packet).is_err() {
                return;
            }
        }
    }

    fn on_packet(&mut self, packet: &Packet) -> Result<(), ()> {
        // Length of signatures must be encoded in 1 byte as 128 signatures
        // (which would require two bytes) results in 8192 packet size
        // (breaching max packet size).
        let signature = packet
            .data(1..65)
            .map(|sig_bytes| Signature::try_from(sig_bytes).unwrap())
            .ok_or_else(|| saturating_add_assign!(self.metrics.err_deserialize, 1))?;
        trace!("Received TX; signature={signature}");

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet.clone()]),
            bundle_id: format!("R{signature}"),
        };

        match self.bundle_stage_tx.try_send(vec![packet_bundle]) {
            Ok(_) => Ok(()),
            Err(TrySendError::Disconnected(_)) => Err(()),
            Err(TrySendError::Full(_)) => {
                warn!("Dropping TX; signature={}", signature);
                saturating_add_assign!(self.metrics.dropped, 1);

                Ok(())
            }
        }
    }

    fn update_staked_nodes(&mut self) {
        let bank = self.poh_recorder.read().unwrap().latest_bank();

        // Load the lockup pool account.
        let Some(pool) = bank.get_account(&POOL_KEY) else {
            warn!("Lockup pool does not exist; pool={POOL_KEY}");

            return;
        };

        // Try to deserialize the pool.
        let Some(pool) = Self::try_deserialize_lockup_pool(pool.data()) else {
            warn!("Failed to deserialize lockup pool; pool={POOL_KEY}");

            return;
        };

        // Setup a new staked nodes map.
        let stakes: HashMap<_, _> = pool
            .entries
            .iter()
            .take_while(|entry| entry.lockup != Pubkey::default())
            .clone()
            .filter(|entry| entry.metadata != [0; 32])
            .map(|entry| (Pubkey::new_from_array(entry.metadata), entry.amount))
            .collect();
        let stakes = Arc::new(stakes);

        // Swap the old for the new.
        let mut staked_nodes = self.staked_nodes.write().unwrap();
        *staked_nodes = StakedNodes::new(stakes.clone(), HashMap::default());

        // Purge all connections where their stake no longer matches.
        let connection_table_l = self.staked_connection_table.lock().unwrap();
        for connection in connection_table_l.table().values().flatten() {
            match connection.peer_type {
                ConnectionPeerType::Staked(stake) => {
                    if staked_nodes
                        .get_node_stake(&connection.identity)
                        .map_or(true, |connection_stake| connection_stake != stake)
                    {
                        info!(
                            "Purging connection due to stake; identity={}",
                            connection.identity
                        );
                        connection.exit.store(true, Ordering::Relaxed);
                    }
                }
                ConnectionPeerType::Unstaked => {
                    eprintln!("BUG: Unstaked connection in staked connection table");
                    connection.exit.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    fn try_deserialize_lockup_pool(data: &[u8]) -> Option<&LockupPool> {
        if data.len() < 8 || &data[0..8] != LockupPool::SPL_DISCRIMINATOR.as_slice() {
            return None;
        }

        bytemuck::try_from_bytes::<LockupPool>(data).ok()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct RateLimit {
    cap: u64,
    remaining: u64,
    last: Instant,
}

#[derive(Default, PartialEq, Eq)]
struct P3Metrics {
    /// Number of packets (transactions) received.
    packets: u64,
    /// Number of transactions dropped due to full channel.
    dropped: u64,
    /// Number of packets that failed to deserialize to a valid transaction.
    err_deserialize: u64,
    /// Time taken to update staked nodes.
    staked_nodes_us: u64,
}

impl P3Metrics {
    fn report(&self, age_ms: u64) {
        // Suppress logs if there are no recorded metrics.
        if self == &P3Metrics::default() {
            return;
        }

        datapoint_info!(
            "p3_quic",
            ("age_ms", age_ms as i64, i64),
            ("transactions", self.packets as i64, i64),
            ("dropped", self.dropped as i64, i64),
            ("err_deserialize", self.err_deserialize as i64, i64),
            ("staked_nodes_us", self.staked_nodes_us as i64, i64),
        );
    }
}
