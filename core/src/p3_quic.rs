use {
    crate::tpu::MAX_QUIC_CONNECTIONS_PER_PEER,
    crossbeam_channel::{RecvError, TrySendError},
    paladin_lockup_program::state::LockupPool,
    solana_perf::packet::PacketBatch,
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{
        account::ReadableAccount, net::DEFAULT_TPU_COALESCE, pubkey::Pubkey, saturating_add_assign,
        signature::Keypair,
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
        net::UdpSocket,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        time::{Duration, Instant},
    },
};

const P3_SOCKET: &str = "0.0.0.0:4819";
const P3_MEV_SOCKET: &str = "0.0.0.0:4820";

const MAX_STAKED_CONNECTIONS: usize = 256;
const MAX_UNSTAKED_CONNECTIONS: usize = 0;
/// This results in 100 streams per 100ms, i.e. 1000 global TPS. Users with less
/// than 1% stake will be rounded up to 1 stream per 100ms.
const MAX_STREAMS_PER_MS: u64 = 1;

const STAKED_NODES_UPDATE_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes
const POOL_KEY: Pubkey = solana_sdk::pubkey!("EJi4Rj2u1VXiLpKtaqeQh3w4XxAGLFqnAG1jCorSvVmg");

pub(crate) struct P3Quic {
    exit: Arc<AtomicBool>,
    quic_server_regular: std::thread::JoinHandle<()>,
    quic_server_mev: std::thread::JoinHandle<()>,

    staked_nodes: Arc<RwLock<StakedNodes>>,
    staked_nodes_last_update: Instant,
    poh_recorder: Arc<RwLock<PohRecorder>>,
    staked_connection_table: Arc<Mutex<ConnectionTable>>,
    reg_packet_rx: crossbeam_channel::Receiver<PacketBatch>,
    mev_packet_rx: crossbeam_channel::Receiver<PacketBatch>,
    packet_tx: crossbeam_channel::Sender<PacketBatch>,

    metrics: P3Metrics,
    metrics_creation: Instant,
}

impl P3Quic {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        packet_tx: crossbeam_channel::Sender<PacketBatch>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        keypair: &Keypair,
    ) -> (std::thread::JoinHandle<()>, [Arc<EndpointKeyUpdater>; 2]) {
        // Bind the P3 QUIC UDP socket.
        let socket_regular = UdpSocket::bind(P3_SOCKET).unwrap();
        let socket_mev = UdpSocket::bind(P3_MEV_SOCKET).unwrap();

        // Setup initial staked nodes (empty).
        let stakes = Arc::default();
        let staked_nodes = Arc::new(RwLock::new(StakedNodes::new(stakes, HashMap::default())));

        // TODO: Would be ideal to reduce the number of threads spawned by tokio
        // in streamer (i.e. make it an argument).

        // Create the connection table here as we want to share
        let staked_connection_table: Arc<Mutex<ConnectionTable>> =
            Arc::new(Mutex::new(ConnectionTable::new()));

        // Spawn the P3 QUIC server (regular).
        let (reg_packet_tx, reg_packet_rx) = crossbeam_channel::unbounded();
        let SpawnServerResult {
            endpoints: _,
            thread: quic_server_regular,
            key_updater: key_updater_regular,
        } = solana_streamer::quic::spawn_server(
            "p3Quic-streamer",
            "p3_quic",
            socket_regular,
            keypair,
            // NB: Packets are verified using the usual TPU lane.
            reg_packet_tx,
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

        // Spawn the P3 QUIC server (mev).
        let (mev_packet_tx, mev_packet_rx) = crossbeam_channel::unbounded();
        let SpawnServerResult {
            endpoints: _,
            thread: quic_server_mev,
            key_updater: key_updater_mev,
        } = solana_streamer::quic::spawn_server(
            "p3Quic-streamer",
            "p3_quic",
            socket_mev,
            keypair,
            // NB: Packets are verified using the usual TPU lane.
            mev_packet_tx,
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
            quic_server_regular,
            quic_server_mev,

            staked_nodes,
            staked_nodes_last_update: Instant::now(),
            poh_recorder,
            staked_connection_table,
            reg_packet_rx,
            mev_packet_rx,
            packet_tx,

            metrics: P3Metrics::default(),
            metrics_creation: Instant::now(),
        };

        (
            std::thread::Builder::new()
                .name("P3Quic".to_owned())
                .spawn(move || p3.run())
                .unwrap(),
            [key_updater_regular, key_updater_mev],
        )
    }

    fn run(mut self) {
        info!("Spawned P3Quic");

        let start = Instant::now();
        self.update_staked_nodes();
        saturating_add_assign!(
            self.metrics.staked_nodes_us,
            start.elapsed().as_micros() as u64
        );

        while !self.exit.load(Ordering::Relaxed) {
            // Try to receive mev packets.
            crossbeam_channel::select_biased! {
                recv(self.mev_packet_rx) -> res => match res {
                    Ok(packets) => self.on_mev_packets(packets),
                    Err(RecvError) => break,
                },
                recv(self.reg_packet_rx) -> res => match res {
                    Ok(packets) => self.on_regular_packets(packets),
                    Err(RecvError) => break,
                }
            }

            // Check if we need to report metrics for the last interval.
            let now = Instant::now();
            let age = now - self.metrics_creation;
            if age > Duration::from_secs(1) {
                self.metrics.report(age.as_millis() as u64);
                self.metrics = P3Metrics::default();
                self.metrics_creation = now;
            }

            // Check if we need to update staked nodes.
            if now - self.staked_nodes_last_update >= STAKED_NODES_UPDATE_INTERVAL {
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

        self.quic_server_regular.join().unwrap();
        self.quic_server_mev.join().unwrap();
    }

    fn on_regular_packets(&mut self, packets: PacketBatch) {
        let len = packets.len() as u64;
        saturating_add_assign!(self.metrics.reg_forwarded, len);

        // Forward for verification & inclusion.
        if let Err(TrySendError::Full(_)) = self.packet_tx.try_send(packets) {
            saturating_add_assign!(self.metrics.reg_dropped, len)
        }
    }

    fn on_mev_packets(&mut self, mut packets: PacketBatch) {
        let len = packets.len() as u64;
        saturating_add_assign!(self.metrics.mev_forwarded, len);

        // Set drop on revert flag.
        for packet in packets.iter_mut() {
            packet.meta_mut().set_drop_on_revert(true);
        }

        // Forward for verification & inclusion.
        if let Err(TrySendError::Full(_)) = self.packet_tx.try_send(packets) {
            saturating_add_assign!(self.metrics.mev_dropped, len)
        }
    }

    fn update_staked_nodes(&mut self) {
        let bank = self.poh_recorder.read().unwrap().get_poh_recorder_bank();
        let bank = bank.bank();

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
        let stakes = pool
            .entries
            .iter()
            .take_while(|entry| entry.lockup != Pubkey::default())
            .clone()
            .filter(|entry| entry.metadata != [0; 32])
            .map(|entry| (Pubkey::new_from_array(entry.metadata), entry.amount))
            .fold(
                HashMap::with_capacity(pool.entries_len),
                |mut map, (key, stake)| {
                    *map.entry(key).or_default() += stake;

                    map
                },
            );
        let stakes = Arc::new(stakes);

        // Swap the old for the new.
        *self.staked_nodes.write().unwrap() = StakedNodes::new(stakes.clone(), HashMap::default());

        // Purge all connections where their stake no longer matches.
        let connection_table_l = self.staked_connection_table.lock().unwrap();
        for connection in connection_table_l.table().values().flatten() {
            match connection.peer_type {
                ConnectionPeerType::Staked(stake) => {
                    if stakes
                        .get(&connection.identity)
                        .map_or(true, |connection_stake| connection_stake != &stake)
                    {
                        info!(
                            "Purging connection due to stake; identity={}",
                            connection.identity
                        );
                        connection.cancel.cancel();
                    }
                }
                ConnectionPeerType::Unstaked => {
                    eprintln!("BUG: Unstaked connection in staked connection table");
                    connection.cancel.cancel();
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
    /// Number of regular packets forwarded.
    reg_forwarded: u64,
    /// Number of regular packets dropped.
    reg_dropped: u64,
    /// Number of mev packets forwarded.
    mev_forwarded: u64,
    /// Number of mev packets dropped.
    mev_dropped: u64,
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
            ("regular_packets_forwarded", self.reg_forwarded as i64, i64),
            ("regular_packets_dropped", self.reg_dropped as i64, i64),
            ("mev_packets_forwarded", self.mev_forwarded as i64, i64),
            ("mev_packets_dropped", self.mev_dropped as i64, i64),
            ("staked_nodes_us", self.staked_nodes_us as i64, i64),
        );
    }
}
