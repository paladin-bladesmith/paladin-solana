use {
    crate::{admin_rpc_service, cli::DefaultArgs, commands::Result},
    clap::{App, Arg, ArgMatches, SubCommand},
    std::path::Path,
};

pub fn command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-secondary-block-engine-urls")
        .about("Set secondary block engine urls")
        .arg(
            Arg::with_name("urls")
                .long("urls")
                .help("Secondary block engine urls. Set to empty string to remove all.")
                .takes_value(true)
                .multiple(true)
                .required(true),
        )
}

pub fn execute(subcommand_matches: &ArgMatches, ledger_path: &Path) -> Result<()> {
    let secondary_block_engine_urls = subcommand_matches
        .values_of("secondary_block_engines_urls")
        .unwrap_or_default()
        .map(ToString::to_string)
        .collect();
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime().block_on(async move {
        admin_client
            .await?
            .set_secondary_block_engine_urls(secondary_block_engine_urls)
            .await
    })?;
    Ok(())
}
