use amitoki_l3_lab::tcp::{options, run_benchmark, run_receiver};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "amitoki-l3-tcp", version, about = "独自L3との比較用Linux TCPベースライン")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Receiver(options::Receiver),
    Bench(options::Benchmark),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    amitoki_l3_lab::runtime::install_shutdown()?;
    match Cli::parse().command {
        Command::Receiver(options) => run_receiver(options)?,
        Command::Bench(options) => run_benchmark(options)?,
    }
    Ok(())
}
