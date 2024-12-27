use {
    crate::packet_bundle::PacketBundle,
    crossbeam_channel::TrySendError,
    paladin_lockup_program::state::{Lockup, LockupPool, LockupPoolEntry},
    solana_accounts_db::accounts_index::ScanConfig,
    solana_perf::packet::PacketBatch,
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        packet::{Packet, PACKET_DATA_SIZE},
        pubkey::Pubkey,
        saturating_add_assign,
        transaction::VersionedTransaction,
    },
    std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        ops::AddAssign,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::{Duration, Instant},
    },
    tokio::runtime::Builder,
};

pub const P3_SOCKET_DEFAULT: &str = "0.0.0.0:4818";

// Whitelist
const READ_TIMEOUT: Duration = Duration::from_millis(100);
const RATE_LIMIT_UPDATE_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes
const LOCKUP_POOL_DISCRIMINATOR: [u8; 8] = [186, 147, 218, 16, 32, 177, 46, 87];
const LOCKUP_DISCRIMINATOR: [u8; 8] = [57, 179, 94, 35, 220, 100, 165, 9];

type WhitelistedIps = Arc<RwLock<HashMap<IpAddr, u64>>>;

pub(crate) struct P3 {
    exit: Arc<AtomicBool>,

    bundle_stage_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,

    socket: UdpSocket,
    buffer: [u8; PACKET_DATA_SIZE],

    metrics: P3Metrics,
    metrics_creation: Instant,
    whitelisted_ips: WhitelistedIps,
    poh_recorder: Arc<RwLock<PohRecorder>>,
}

impl P3 {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        bundle_stage_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
        addr: SocketAddr,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) -> std::thread::JoinHandle<()> {
        let socket = UdpSocket::bind(addr).unwrap();
        socket.set_read_timeout(Some(READ_TIMEOUT)).unwrap();

        let whitelisted_ips = Arc::new(RwLock::new(HashMap::new()));

        let p3 = Self {
            exit: exit.clone(),
            bundle_stage_tx,
            socket,
            buffer: [0u8; PACKET_DATA_SIZE],
            metrics: P3Metrics::default(),
            metrics_creation: Instant::now(),
            whitelisted_ips: whitelisted_ips.clone(),
            poh_recorder: poh_recorder.clone(),
        };

        let p3_handle = std::thread::Builder::new()
            .name("P3".to_owned())
            .spawn(move || p3.run())
            .unwrap();

        // Spawn a thread to periodically update the whitelist
        let whitelist_handle = std::thread::Builder::new()
            .name("P3Whitelist".to_owned())
            .spawn(move || {
                let runtime = Builder::new_current_thread().enable_all().build().unwrap();

                runtime.block_on(async move {
                    let mut last_update = Instant::now();
                    while !exit.load(Ordering::Relaxed) {
                        if last_update.elapsed() >= RATE_LIMIT_UPDATE_INTERVAL {
                            Self::update_whitelisted_ips(&whitelisted_ips, &poh_recorder);
                            last_update = Instant::now();
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
            })
            .unwrap();

        // Wait for both threads
        std::thread::Builder::new()
            .name("P3Combined".to_owned())
            .spawn(move || {
                let _ = p3_handle.join();
                let _ = whitelist_handle.join();
            })
            .unwrap()
    }

    fn update_whitelisted_ips(
        whitelisted_ips: &WhitelistedIps,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
    ) {
        let bank = if let Some(bank) = poh_recorder.read().unwrap().bank() {
            bank
        } else {
            return;
        };

        // Clear existing whitelist
        whitelisted_ips.write().unwrap().clear();

        // Find all LockupPool accounts
        let program_id = Pubkey::from_str("GrAkKfEpTKQuVHG2Y97Y2FF4i7y7Q5AHLK94JBy7Y5yv").unwrap();

        // Get all LockupPool accounts
        // TODO: Is there expected to be only one LockupPool account?
        let scan_config = ScanConfig::default();

        if let Ok(accounts) = bank.get_program_accounts(&program_id, &scan_config) {
            for (_, account) in accounts {
                // Try to deserialize as LockupPool
                if let Some(pool) = Self::try_deserialize_lockup_pool(account.data()) {
                    // Process each valid entry
                    for i in 0..pool.entries_len {
                        let entry = pool.entries[i];
                        if entry.lockup != Pubkey::default() {
                            // Get the lockup account
                            if let Some(lockup_account) = bank.get_account(&entry.lockup) {
                                if let Some(lockup) =
                                    Self::try_deserialize_lockup(lockup_account.data())
                                {
                                    // TODO: Once the program is updated to include IP addresses directly,
                                    // extract them from the lockup account. For now, use a placeholder:
                                    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
                                    whitelisted_ips.write().unwrap().insert(ip, entry.amount);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn try_deserialize_lockup_pool(data: &[u8]) -> Option<&LockupPool> {
        if data.len() < 8 || data[0..8] != LOCKUP_POOL_DISCRIMINATOR {
            return None;
        }

        bytemuck::try_from_bytes::<LockupPool>(data).ok()
    }

    fn try_deserialize_lockup(data: &[u8]) -> Option<&Lockup> {
        if data.len() < 8 || data[0..8] != LOCKUP_DISCRIMINATOR {
            return None;
        }

        bytemuck::try_from_bytes::<Lockup>(data).ok()
    }

    fn run(mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            // Try receive packets.
            let (tx, src_addr) = match self.socket_recv() {
                Some(result) => result,
                None => continue,
            };

            // Check if the source IP is whitelisted
            if !self.is_whitelisted(src_addr.ip()) {
                continue;
            }

            let now = Instant::now();
            if now - self.metrics_creation > Duration::from_secs(1) {
                self.metrics.report();
                self.metrics = P3Metrics::default();
                self.metrics_creation = now;
            }

            let Some(signature) = tx.signatures.get(0) else {
                warn!("TX received without signature");
                continue;
            };
            trace!("Received TX; signature={signature}");

            let packet_bundle = PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, &tx).unwrap()]),
                bundle_id: format!("R{signature}"),
            };

            match self.bundle_stage_tx.try_send(vec![packet_bundle]) {
                Ok(_) => {}
                Err(TrySendError::Disconnected(_)) => break,
                Err(TrySendError::Full(_)) => {
                    warn!("Dropping TX; signature={}", signature);
                    saturating_add_assign!(self.metrics.dropped, 1);
                }
            }
        }
    }

    fn is_whitelisted(&self, ip: IpAddr) -> bool {
        self.whitelisted_ips.read().unwrap().contains_key(&ip)
    }

    fn socket_recv(&mut self) -> Option<(VersionedTransaction, SocketAddr)> {
        match self.socket.recv_from(&mut self.buffer) {
            Ok((_, src_addr)) => {
                self.metrics.transactions.add_assign(1);
                bincode::deserialize::<VersionedTransaction>(&self.buffer)
                    .inspect_err(|_| saturating_add_assign!(self.metrics.err_deserialize, 1))
                    .ok()
                    .map(|tx| (tx, src_addr))
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(err) => {
                error!("Unexpected IO error; err={err}");
                None
            }
        }
    }
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
