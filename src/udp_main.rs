use amitoki_l3_lab::{
    runtime::install_shutdown,
    udp::{self, Endpoint, Load},
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "amitoki-l3-udp", version, about = "独自L3との比較用Linux UDP/IPベンチ")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Receiver {
        #[command(flatten)]
        endpoint: Endpoint,
        #[arg(long)]
        ready: PathBuf,
    },
    Bench(Load),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_shutdown()?;
    match Cli::parse().command {
        Command::Receiver { endpoint, ready } => udp::receive(endpoint, ready)?,
        Command::Bench(load) => udp::benchmark(load)?,
    }
    Ok(())
}
