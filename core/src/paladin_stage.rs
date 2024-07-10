use {
    crate::packet_bundle::PacketBundle,
    crossbeam_channel::TrySendError,
    solana_perf::packet::PacketBatch,
    solana_sdk::packet::{Meta, Packet, PACKET_DATA_SIZE},
    std::{
        os::linux::net::SocketAddrExt,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    thiserror::Error,
};

const SOCKET_ENDPOINT: &str = "paladin";
const SOCKET_READ_TIMEOUT: Duration = Duration::from_millis(250);

const ERR_RETRY_DELAY: Duration = Duration::from_secs(1);
const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
enum PaladinError {
    #[error("Socket error; err={0}")]
    Socket(#[from] std::io::Error),
    #[error("Crossbeam error; err={0}")]
    Crossbeam(#[from] TrySendError<Vec<PacketBundle>>),
}

pub struct PaladinStage {
    handle: std::thread::JoinHandle<()>,
}

impl PaladinStage {
    pub fn new(
        exit: Arc<AtomicBool>,
        paladin_tx: crossbeam_channel::Sender<Vec<PacketBundle>>,
    ) -> Self {
        let handle = std::thread::Builder::new()
            .name("paladin-stage".to_string())
            .spawn(move || Self::run(exit, paladin_tx))
            .unwrap();

        Self { handle }
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.handle.join()
    }

    fn run(exit: Arc<AtomicBool>, paladin_tx: crossbeam_channel::Sender<Vec<PacketBundle>>) {
        while !exit.load(Ordering::Relaxed) {
            if let Err(err) = Self::run_until_err(&paladin_tx) {
                warn!("PaladinStage encountered error, restarting; err={err}");
                if !exit.load(Ordering::Relaxed) {
                    std::thread::sleep(ERR_RETRY_DELAY);
                }
            }
        }
    }

    fn run_until_err(
        paladin_tx: &crossbeam_channel::Sender<Vec<PacketBundle>>,
    ) -> Result<(), PaladinError> {
        let mut paladin_stats_creation = Instant::now();

        // Setup abstract unix socket for geyser bot to connect to.
        let socket = std::os::unix::net::SocketAddr::from_abstract_name(SOCKET_ENDPOINT)?;
        let socket = std::os::unix::net::UnixDatagram::bind_addr(&socket)?;
        socket.set_read_timeout(Some(SOCKET_READ_TIMEOUT))?;

        // Re-usable buffer to read packets into.
        let mut buf = [0u8; PACKET_DATA_SIZE];

        loop {
            let size = match socket.recv(&mut buf) {
                Ok(size) => size,
                Err(err) => match err.kind() {
                    // NB: Returned when the timeout expires.
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => continue,
                    _ => return Err(err.into()),
                },
            };

            // Updates stats if sufficient time has passed.
            let stats_age = paladin_stats_creation.elapsed();
            if stats_age > STATS_REPORT_INTERVAL {
                paladin_stats_creation = Instant::now();
            }

            // Handle received packets.
            let packet = Packet::new(
                buf,
                Meta {
                    size,
                    ..Default::default()
                },
            );
            // TODO: Drain additional packets from the socket using try_recv.
            Self::handle_paladin(vec![packet], paladin_tx)?;
        }
    }

    fn handle_paladin(
        packets: Vec<Packet>,
        bundle_sender: &crossbeam_channel::Sender<Vec<PacketBundle>>,
    ) -> Result<(), TrySendError<Vec<PacketBundle>>> {
        // Each paladin TX (inside a packet) translates to one bundle.
        let bundles: Vec<_> = packets
            .into_iter()
            .map(|packet| PacketBundle {
                bundle_id: String::default(),
                batch: PacketBatch::new(vec![packet]),
            })
            .collect();

        println!("Paladin stage received: {}", bundles.len());

        // Bon voyage.
        bundle_sender.try_send(bundles)
    }
}
