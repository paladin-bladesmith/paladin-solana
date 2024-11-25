use std::net::{SocketAddr, UdpSocket};
use std::ops::AddAssign;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::TrySendError;
use solana_perf::packet::PacketBatch;
use solana_sdk::packet::{Packet, PACKET_DATA_SIZE};
use solana_sdk::transaction::VersionedTransaction;

use crate::packet_bundle::PacketBundle;

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
            .name("p3".to_owned())
            .spawn(|| p3.run())
            .unwrap()
    }

    fn run(mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            let tx = match self.socket_recv() {
                Some(tx) => tx,
                None => continue,
            };

            trace!("Received TX with signature: {}", tx.signatures[0]);

            let bundle_id = tx.signatures[0].to_string();
            let packet_bundle = PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, &tx).unwrap()]),
                bundle_id: bundle_id.clone(),
            };

            match self.leader_tx.try_send(vec![packet_bundle]) {
                Ok(_) => {
                    self.metrics
                        .report_tx_send(bundle_id.clone(), "SUCCESS".to_string());
                }
                Err(TrySendError::Disconnected(_)) => {
                    self.metrics
                        .report_tx_send(bundle_id.clone(), "FAILED".to_string());
                    break;
                }
                Err(TrySendError::Full(_)) => {
                    self.metrics
                        .report_tx_send(bundle_id.clone(), "FAILED".to_string());
                    warn!("Dropping TX, signature: {}", bundle_id)
                }
            }
        }
    }
    fn socket_recv(&mut self) -> Option<VersionedTransaction> {
        match self.socket.recv(&mut self.buffer) {
            Ok(_) => {
                self.metrics.increment_transactions(1);
                // Deserialize to match tx
                match bincode::deserialize::<VersionedTransaction>(&self.buffer) {
                    Ok(tx) => Some(tx),
                    Err(_) => {
                        self.metrics.increment_err_deserialize(1);

                        None
                    }
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(err) => panic!("Unexpected IO error; err={err}"),
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

pub(crate) fn p3_spawn(
    exit: Arc<AtomicBool>,
    p3_socket: SocketAddr,
    p3_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("P3Monitor".to_owned())
        .spawn(move || {
            let p3 = P3::spawn(exit, p3_tx, p3_socket);

            // Wait for P3Lane to finish
            if let Err(err) = p3.join() {
                error!("P3 thread failed: {:?}", err);
            } else {
                info!("P3 exited cleanly");
            }
        })
        .expect("Failed to spawn P3Monitor thread")
}
