use {
    crate::packet_bundle::PacketBundle,
    crossbeam_channel::TrySendError,
    solana_perf::packet::PacketBatch,
    solana_sdk::{
        packet::{Packet, PACKET_DATA_SIZE},
        saturating_add_assign,
        transaction::VersionedTransaction,
    },
    std::{
        net::{SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
};

const READ_TIMEOUT: Duration = Duration::from_millis(100);

pub const P3_SOCKET_DEFAULT: &str = "0.0.0.0:4818";

pub(crate) struct P3 {
    exit: Arc<AtomicBool>,

    leader_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,

    socket: UdpSocket,
    buffer: [u8; PACKET_DATA_SIZE],

    metrics: P3Metrics,
    metrics_creation: Instant,
}

impl P3 {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        leader_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
        addr: SocketAddr,
    ) -> std::thread::JoinHandle<()> {
        let socket = UdpSocket::bind(addr).unwrap();
        socket.set_read_timeout(Some(READ_TIMEOUT)).unwrap();
        let p3 = Self {
            exit,
            leader_tx,
            socket,
            buffer: [0u8; PACKET_DATA_SIZE],

            metrics: P3Metrics::default(),
            metrics_creation: Instant::now(),
        };

        std::thread::Builder::new()
            .name("P3".to_owned())
            .spawn(|| p3.run())
            .unwrap()
    }

    fn run(mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            // Check metrics.
            let now = Instant::now();
            if now - self.metrics_creation > Duration::from_secs(1) {
                self.metrics.report();
                self.metrics = P3Metrics::default();
                self.metrics_creation = now;
            }

            // Try recv packets.
            let tx = match self.socket_recv() {
                Some(tx) => tx,
                None => continue,
            };

            let signature = tx.signatures[0];
            trace!("Received TX; signature={signature}");

            let packet_bundle = PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, &tx).unwrap()]),
                bundle_id: format!("R{signature}"),
            };

            match self.leader_tx.try_send(vec![packet_bundle]) {
                Ok(_) => {}
                Err(TrySendError::Disconnected(_)) => break,
                Err(TrySendError::Full(_)) => {
                    warn!("Dropping TX; signature={}", signature);
                    saturating_add_assign!(self.metrics.dropped, 1);
                }
            }
        }
    }

    fn socket_recv(&mut self) -> Option<VersionedTransaction> {
        match self.socket.recv(&mut self.buffer) {
            Ok(_) => {
                saturating_add_assign!(self.metrics.transactions, 1);
                bincode::deserialize::<VersionedTransaction>(&self.buffer)
                    .inspect_err(|_| saturating_add_assign!(self.metrics.err_deserialize, 1))
                    .ok()
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
            ("err_deserialize", self.err_deserialize as i64, i64)
        );
    }
}
