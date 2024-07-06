use {
    crate::packet_bundle::PacketBundle,
    crossbeam_channel::TrySendError,
    solana_perf::packet::PacketBatch,
    solana_sdk::{
        packet::{Meta, Packet, PACKET_DATA_SIZE},
        saturating_add_assign,
    },
    std::{
        os::linux::net::SocketAddrExt,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    },
    thiserror::Error,
};

const PALADIN_ENDPOINT: &str = "paladin";

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
            }
        }
    }

    fn run_until_err(
        paladin_tx: &crossbeam_channel::Sender<Vec<PacketBundle>>,
    ) -> Result<(), PaladinError> {
        let mut paladin_stats = PaladinStageStats::default();

        // Setup abstract unix socket for geyser bot to connect to.
        let socket = std::os::unix::net::SocketAddr::from_abstract_name(PALADIN_ENDPOINT)?;
        let socket = std::os::unix::net::UnixDatagram::bind_addr(&socket)?;

        // Fixed re-usable buffer to read TXs into.
        let mut buf = [0u8; PACKET_DATA_SIZE];

        loop {
            // TODO: Receive with timeout to check exit condition & report stats.
            let size = socket.recv(&mut buf)?;
            let packet = Packet::new(
                buf,
                Meta {
                    size,
                    ..Default::default()
                },
            );
            // TODO: Drain additional packets from the socket using try_recv.
            Self::handle_paladin(vec![packet], paladin_tx, &mut paladin_stats)?;
        }
    }

    fn handle_paladin(
        packets: Vec<Packet>,
        bundle_sender: &crossbeam_channel::Sender<Vec<PacketBundle>>,
        paladin_stage_stats: &mut PaladinStageStats,
    ) -> Result<(), TrySendError<Vec<PacketBundle>>> {
        // Update stats.
        saturating_add_assign!(
            paladin_stage_stats.num_paladin_packets,
            packets.len() as u64
        );

        // Each paladin TX (inside a packet) translates to one bundle.
        let bundles: Vec<_> = packets
            .into_iter()
            .map(|packet| PacketBundle {
                bundle_id: String::default(),
                batch: PacketBatch::new(vec![packet]),
            })
            .collect();

        // Bon voyage.
        bundle_sender.try_send(bundles)
    }
}

#[derive(Default)]
struct PaladinStageStats {
    num_paladin_packets: u64,
}

impl PaladinStageStats {
    pub(crate) fn report(&self) {
        datapoint_info!(
            "paladin_stage-stats",
            ("num_paladin_packets", self.num_paladin_packets, i64),
        );
    }
}
