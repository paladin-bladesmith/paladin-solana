mod args;
mod p3_quic;

#[tokio::main]
async fn main() {
    use {
        clap::{CommandFactory, Parser},
        tokio::signal::unix::SignalKind,
        tracing::{error, info},
    };

    // Parse .env if it exists (and before args in case args want to read
    // environment).
    match dotenvy::dotenv() {
        Ok(_) | Err(dotenvy::Error::Io(_)) => {}
        Err(err) => panic!("Failed to parse .env file; err={err}"),
    }

    // Parse command-line arguments.
    let args = crate::args::Args::parse();

    // If user is requesting completions, return them and exit.
    if let Some(shell) = args.completions {
        clap_complete::generate(
            shell,
            &mut crate::args::Args::command(),
            "rust-template",
            &mut std::io::stdout(),
        );

        return;
    }

    // Setup tracing.
    let _log_guard = toolbox::tracing::setup_tracing("rust-template", args.logs.as_deref());

    // Log build information (as soon as possible).
    toolbox::log_build_info!();

    // Setup standard panic handling.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        error!(?panic_info, "Application panic");

        default_panic(panic_info);
    }));

    // Start server.
    let cxl = tokio_util::sync::CancellationToken::new();
    let cxl_child = cxl.clone();
    let mut handle = tokio::spawn(async move { cxl_child.cancelled().await });

    // Wait for server exit or SIGTERM/SIGINT.
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();
    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            res.expect("Failed to register SIGINT hook");

            info!("SIGINT caught, stopping server");
            cxl.cancel();

            handle.await.unwrap();
        }
        _ = sigterm.recv() => info!("SIGTERM caught, stopping server"),
        _ = sigint.recv() => info!("SIGINT caught, stopping server"),
        res = &mut handle => {
            res.unwrap();
        }
    }
}
