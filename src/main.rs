use rsdos::cli::run_cli;

fn main() -> anyhow::Result<()> {
    let args = std::env::args_os().collect::<Vec<_>>();
    run_cli(&args)
}
