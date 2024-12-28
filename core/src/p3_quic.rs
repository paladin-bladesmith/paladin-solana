use {
    crate::{packet_bundle::PacketBundle, tpu::MAX_QUIC_CONNECTIONS_PER_PEER},
    crossbeam_channel::TrySendError,
    paladin_lockup_program::state::LockupPool,
    solana_perf::packet::PacketBatch,
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{
        account::ReadableAccount,
        net::DEFAULT_TPU_COALESCE,
        packet::{Packet, PACKET_DATA_SIZE},
        pubkey::Pubkey,
        saturating_add_assign,
        signature::{Keypair, Signature},
        transaction::VersionedTransaction,
    },
    solana_streamer::{
        nonblocking::quic::DEFAULT_MAX_CONNECTIONS_PER_IPADDR_PER_MINUTE, quic::SpawnServerResult,
        streamer::StakedNodes,
    },
    spl_discriminator::discriminator::SplDiscriminate,
    std::{
        collections::HashMap,
        net::{SocketAddr, UdpSocket},
        ops::AddAssign,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::{Duration, Instant},
    },
};

pub const P3_QUIC_SOCKET_DEFAULT: &str = "0.0.0.0:4819";

const MAX_STAKED_CONNECTIONS: usize = 1024;
const MAX_UNSTAKED_CONNECTIONS: usize = 0;
const MAX_STREAMS_PER_MS: u64 = 5;
const MAX_STREAMS_PER_SEC: u64 = MAX_STREAMS_PER_MS * 1000;
const TPU_COALESCE: Duration = Duration::from_millis(5);

const RATE_LIMIT_UPDATE_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes
const POOL_KEY: Pubkey = solana_sdk::pubkey!("EJi4Rj2u1VXiLpKtaqeQh3w4XxAGLFqnAG1jCorSvVmg");

pub(crate) struct P3Quic {
    exit: Arc<AtomicBool>,

    quic_rx: crossbeam_channel::Receiver<PacketBatch>,
    bundle_stage_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,

    rate_limits: HashMap<Pubkey, RateLimit>,
    rate_limits_last_update: Instant,

    metrics: P3Metrics,
    metrics_creation: Instant,
    poh_recorder: Arc<RwLock<PohRecorder>>,
}

impl P3Quic {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        bundle_stage_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
        addr: SocketAddr,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        keypair: &Keypair,
    ) -> std::thread::JoinHandle<()> {
        // Bind the P3 QUIC UDP socket.
        let sock = UdpSocket::bind(addr).unwrap();

        // Setup initial staked nodes (empty).
        let stakes = Arc::default();
        let staked_nodes = Arc::new(RwLock::new(StakedNodes::new(stakes, HashMap::default())));

        // TODO: Would be ideal to reduce the number of threads spawned by tokio
        // in streamer (i.e. make it an argument).

        // Spawn the P3 QUIC server.
        let (quic_tx, quic_rx) = crossbeam_channel::unbounded();
        let SpawnServerResult {
            endpoint,
            thread,
            key_updater,
        } = solana_streamer::quic::spawn_server(
            "p3Quic",
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
            // TODO: Controls how long we will wait for additional data on a
            // stream before closing. For P3 we (probably) don't want to limit
            // connection lifecycles.
            // TODO: If we make this really big or edit stream to make it
            // optional, we should make sure we have a way to purge connections
            // that become unstaked during their life time.
            todo!("Investigate what wait for chunk timeout is"),
            DEFAULT_TPU_COALESCE,
        )
        .unwrap();

        // Spawn the P3 management thread.
        let p3 = Self {
            exit: exit.clone(),

            quic_rx,
            bundle_stage_tx,

            rate_limits: HashMap::default(),
            rate_limits_last_update: Instant::now(),

            metrics: P3Metrics::default(),
            metrics_creation: Instant::now(),
            poh_recorder: poh_recorder.clone(),
        };

        std::thread::Builder::new()
            .name("P3".to_owned())
            .spawn(move || p3.run())
            .unwrap()
    }

    fn run(mut self) {
        self.update_rate_limits();

        while !self.exit.load(Ordering::Relaxed) {
            // Try receive packets.
            let packets = match self.quic_rx.recv() {
                Ok(packets) => packets,
                Err(err) => {
                    error!("Quic channel closed unexpectedly");
                    break;
                }
            };

            for packet in &packets {
                if self.on_packet(packet).is_err() {
                    return;
                }
            }

            // Check if we need to report metrics for the last interval.
            let now = Instant::now();
            if now - self.metrics_creation > Duration::from_secs(1) {
                self.metrics.report();
                self.metrics = P3Metrics::default();
                self.metrics_creation = now;
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

    fn update_rate_limits(&mut self) {
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

        // Compute the new total locked PAL.
        let entries = pool
            .entries
            .iter()
            .take_while(|entry| entry.lockup != Pubkey::default());
        let total_pal = entries.clone().map(|entry| entry.amount).sum();

        // Clear the old entries & write the new ones.
        self.rate_limits.clear();
        self.rate_limits.extend(entries.clone().map(|entry| {
            let cap = Self::compute_cap(entry.amount, total_pal);

            (
                Pubkey::new_from_array(entry.metadata),
                RateLimit {
                    cap,
                    remaining: cap,
                    last: Instant::now(),
                },
            )
        }));

        todo!("Update staked nodes")
    }

    fn try_deserialize_lockup_pool(data: &[u8]) -> Option<&LockupPool> {
        if data.len() < 8 || &data[0..8] != LockupPool::SPL_DISCRIMINATOR.as_slice() {
            return None;
        }

        bytemuck::try_from_bytes::<LockupPool>(data).ok()
    }

    fn compute_cap(amount: u64, total: u64) -> u64 {
        amount
            .saturating_mul(MAX_STREAMS_PER_SEC)
            .checked_div(total)
            .unwrap_or_else(|| {
                println!("ERR: Total == 0 but compute_cap was called");

                0
            })
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
    /// Number of transactions received.
    transactions: u64,
    /// Number of transactions dropped due to full channel.
    dropped: u64,
    /// Number of transactions that failed to deserialize.
    err_deserialize: u64,
}

impl P3Metrics {
    fn report(&self) {
        // Suppress logs if there are no recorded metrics.
        if self == &P3Metrics::default() {
            return;
        }

        datapoint_info!(
            "p3_socket",
            ("transactions", self.transactions as i64, i64),
            ("dropped", self.dropped as i64, i64),
            ("err_deserialize", self.err_deserialize as i64, i64)
        );
    }
}
