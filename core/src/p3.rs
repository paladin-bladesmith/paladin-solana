use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{express_lane::ExpressLane, packet_bundle::PacketBundle};

#[derive(Copy, Clone)]
pub struct P3Args {
    pub p3_socket: SocketAddr,
}

impl Default for P3Args {
    fn default() -> Self {
        P3Args {
            p3_socket: SocketAddr::from_str("0.0.0.0:4818").unwrap(),
        }
    }
}

pub(crate) fn p3_run(args: P3Args, p3_tx: crossbeam_channel::Sender<Vec<PacketBundle>>) {
    let exit = Arc::new(AtomicBool::new(false));

    let p3 = ExpressLane::spawn(exit.clone(), p3_tx, args.p3_socket);

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
        });
}
