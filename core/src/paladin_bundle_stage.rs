use {
    crate::packet_bundle::PacketBundle,
    crossbeam_channel::Receiver,
    std::sync::{atomic::AtomicBool, Arc},
};

pub(crate) struct PaladinBundleStage {
    exit: Arc<AtomicBool>,

    paladin_rx: Receiver<Vec<PacketBundle>>,
}

impl PaladinBundleStage {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        paladin_rx: Receiver<Vec<PacketBundle>>,
    ) -> std::thread::JoinHandle<()> {
        info!("Spawning PaladinBundleStage");

        std::thread::Builder::new()
            .name("paladin-bundle-stage".to_string())
            .spawn(move || PaladinBundleStage { exit, paladin_rx }.run())
            .unwrap()
    }

    fn run(mut self) {
        todo!()
    }
}
