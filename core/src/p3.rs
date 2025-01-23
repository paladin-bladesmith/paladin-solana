use {
    crossbeam_channel::TrySendError,
    solana_perf::packet::PacketBatch,
    solana_sdk::{
        packet::{Packet, PACKET_DATA_SIZE},
        saturating_add_assign,
        transaction::VersionedTransaction,
    },
    std::{
        net::{SocketAddr, UdpSocket},
        ops::AddAssign,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
};

pub const P3_SOCKET_DEFAULT: &str = "0.0.0.0:4818";

const READ_TIMEOUT: Duration = Duration::from_millis(100);

pub(crate) struct P3 {
    exit: Arc<AtomicBool>,

    packet_tx: crossbeam_channel::Sender<PacketBatch>,

    socket: UdpSocket,
    buffer: [u8; PACKET_DATA_SIZE],

    metrics: P3Metrics,
    metrics_creation: Instant,
}

impl P3 {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        packet_tx: crossbeam_channel::Sender<PacketBatch>,
        addr: SocketAddr,
    ) -> std::thread::JoinHandle<()> {
        let socket = UdpSocket::bind(addr).unwrap();
        socket.set_read_timeout(Some(READ_TIMEOUT)).unwrap();

        let p3 = Self {
            exit: exit.clone(),
            packet_tx,
            socket,
            buffer: [0u8; PACKET_DATA_SIZE],

            metrics: P3Metrics::default(),
            metrics_creation: Instant::now(),
        };

        std::thread::Builder::new()
            .name("P3".to_owned())
            .spawn(move || p3.run())
            .unwrap()
    }

    fn run(mut self) {
        info!("Spawned P3");

        while !self.exit.load(Ordering::Relaxed) {
            // Try receive packets.
            let (tx, _) = match self.socket_recv() {
                Some(Ok(result)) => result,
                Some(Err(_)) | None => continue,
            };

            // Check if we need to report metrics for the last interval.
            let now = Instant::now();
            if now - self.metrics_creation > Duration::from_secs(1) {
                self.metrics.report();
                self.metrics = P3Metrics::default();
                self.metrics_creation = now;
            }

            let Some(signature) = tx.signatures.first() else {
                warn!("TX received without signature");
                continue;
            };
            trace!("Received TX; signature={signature}");

            match self.packet_tx.try_send(PacketBatch::new(vec![
                Packet::from_data(None, &tx).unwrap()
            ])) {
                Ok(_) => {}
                Err(TrySendError::Disconnected(_)) => break,
                Err(TrySendError::Full(_)) => {
                    warn!("Dropping TX; signature={}", signature);
                    saturating_add_assign!(self.metrics.dropped, 1);
                }
            }
        }
    }

    fn socket_recv(&mut self) -> Option<Result<(VersionedTransaction, SocketAddr), ()>> {
        match self.socket.recv_from(&mut self.buffer) {
            Ok((_, src_addr)) => {
                self.metrics.transactions.add_assign(1);

                Some(
                    bincode::deserialize::<VersionedTransaction>(&self.buffer)
                        .inspect_err(|_| saturating_add_assign!(self.metrics.err_deserialize, 1))
                        .map(|tx| (tx, src_addr))
                        .map_err(|_| ()),
                )
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(err) => {
                error!("Unexpected IO error; err={err}");

                // NB: Return None here as we are unsure if the socket has more
                // packets and do not want to infinite loop.
                None
            }
        }
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
            ("err_deserialize", self.err_deserialize as i64, i64),
        );
    }
}
