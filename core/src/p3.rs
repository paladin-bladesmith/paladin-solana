use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::express_lane::ExpressLane;

const P3_INCLUSION_BUFFER: usize = 100;
#[derive(Default, Copy, Clone)]
pub struct P3Args {
    pub transaction_socket: SocketAddr,
    pub p3_socket: SocketAddr,
}

pub(crate) fn p3_run(args: P3Args) {
    let exit = Arc::new(AtomicBool::new(false));

    let arb_rx = crossbeam_channel::never();
    let (leader_tx, leader_rx) = crossbeam_channel::bounded(P3_INCLUSION_BUFFER);

    let p3 = ExpressLane::spawn(exit.clone(), leader_tx, args.p3_socket);

    // Wait for SIGINT or thread exit.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut p3 = tokio::task::spawn_blocking(|| p3.join());

            // Wait for SIGINT or unexpected P3 exit.
            tokio::select! {
                biased;

                _ = tokio::signal::ctrl_c() => {},
                res = &mut p3 => {
                    res.unwrap().unwrap();
                    panic!("P3 exited without error");
                }
            }

            // Trigger shutdown.
            info!("SIGINT received, triggering shutdown");
            exit.store(true, Ordering::Relaxed);

            // Wait for P3 to clean up.
            p3.await.unwrap().unwrap();
            info!("P3 exited cleanly");

            // Wait for inclusion to clean up.
            // inclusion.join().unwrap();
        });
}
