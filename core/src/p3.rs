use {
    crate::packet_bundle::PacketBundle,
    crossbeam_channel::TrySendError,
    solana_perf::packet::PacketBatch,
    solana_sdk::{
        packet::{Packet, PACKET_DATA_SIZE},
        transaction::VersionedTransaction,
    },
    std::{
        net::{SocketAddr, UdpSocket},
        ops::AddAssign,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    },
};

const READ_TIMEOUT: Duration = Duration::from_secs(1);

pub const P3_SOCKET_DEFAULT: &str = "0.0.0.0:4818";

pub(crate) struct P3 {
    exit: Arc<AtomicBool>,

    leader_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,

    socket: UdpSocket,
    buffer: [u8; PACKET_DATA_SIZE],

    metrics: P3Metrics,
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
        };

        std::thread::Builder::new()
            .name("P3".to_owned())
            .spawn(|| p3.run())
            .unwrap()
    }

    fn run(mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            let tx = match self.socket_recv() {
                Some(tx) => tx,
                None => continue,
            };

            trace!("Received TX; signature={}", tx.signatures[0]);

            let signature = tx.signatures[0].to_string();
            let packet_bundle = PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, &tx).unwrap()]),
                bundle_id: signature.clone(),
            };

            match self.leader_tx.try_send(vec![packet_bundle]) {
                Ok(_) => {
                    self.metrics
                        .report_tx_send(signature, "SUCCESS".to_string());
                }
                Err(TrySendError::Disconnected(_)) => break,
                Err(TrySendError::Full(_)) => {
                    warn!("Dropping TX; signature={}", signature);
                    self.metrics.report_tx_send(signature, "FAILED".to_string());
                }
            }
        }
    }

    fn socket_recv(&mut self) -> Option<VersionedTransaction> {
        match self.socket.recv(&mut self.buffer) {
            Ok(_) => {
                self.metrics.increment_transactions(1);
                bincode::deserialize::<VersionedTransaction>(&self.buffer)
                    .inspect_err(|_| self.metrics.increment_err_deserialize(1))
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

#[derive(Default)]
struct P3Metrics {
    /// Number of transactions received.
    transactions: u64,
    /// Number of transactions that failed to deserialize.
    err_deserialize: u64,
}

impl P3Metrics {
    pub(crate) fn report_tx_send(&self, tx: String, status: String) {
        datapoint_info!(
            "p3",
            ("Transcation", tx, String),
            ("Status", status, String)
        );
    }

    pub(crate) fn increment_transactions(&mut self, val: u64) {
        self.transactions.add_assign(val);
    }

    pub(crate) fn increment_err_deserialize(&mut self, val: u64) {
        self.err_deserialize.add_assign(val);
    }
}
