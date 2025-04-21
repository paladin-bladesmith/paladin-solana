use vergen_git2::{CargoBuilder, Emitter, Git2Builder, RustcBuilder};

fn main() {
    let git2 = Git2Builder::default()
        .sha(true)
        .dirty(true)
        .commit_message(true)
        .commit_timestamp(true)
        .build()
        .unwrap();
    let rustc = RustcBuilder::default()
        .host_triple(true)
        .semver(true)
        .build()
        .unwrap();
    let cargo = CargoBuilder::default().target_triple(true).build().unwrap();

    Emitter::default()
        .add_instructions(&git2)
        .unwrap()
        .add_instructions(&rustc)
        .unwrap()
        .add_instructions(&cargo)
        .unwrap()
        .emit()
        .unwrap();
}
