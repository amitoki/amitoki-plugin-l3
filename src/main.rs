use amitoki_l3_lab::{
    config::Config,
    runtime::{run_benchmark, run_node, Benchmark, NodeOptions},
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "amitoki-l3", version, about = "private Ethernet上の独自L3/期限付きメッセージ配送の実験")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 静的経路で独自L3パケットを転送する。
    Router(Node),
    /// creditを発行し、本文のfingerprintをACKする。
    Receiver {
        #[command(flatten)]
        node: Node,
        #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(1..=100_000))]
        short_credits_per_second: u64,
        #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=100_000))]
        bulk_credits_per_second: u64,
    },
    /// 指定した到着率で短文と背景負荷を送信し、期限内ACK率を測る。
    Bench {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        peer: u32,
        #[arg(long, default_value_t = 3000)]
        duration_ms: u64,
        #[arg(long, default_value_t = 100)]
        short_rate: u64,
        #[arg(long, default_value_t = 0)]
        bulk_rate: u64,
        #[arg(long, default_value_t = 128)]
        short_bytes: usize,
        #[arg(long, default_value_t = 1200)]
        bulk_bytes: usize,
        #[arg(long, default_value_t = 20_000)]
        short_deadline_us: u64,
        #[arg(long, default_value_t = 200_000)]
        bulk_deadline_us: u64,
        #[arg(long, value_delimiter = ',', default_value = "1")]
        paths: Vec<u8>,
        #[arg(long, default_value_t = 30_000)]
        replica_bytes_per_second: u64,
        #[arg(long, default_value_t = 0)]
        retries: u8,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(clap::Args)]
struct Node {
    #[arg(long)]
    config: PathBuf,
    #[arg(long, default_value_t = 10_000, value_parser = clap::value_parser!(u64).range(1..=120_000))]
    duration_ms: u64,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    ready: PathBuf,
}

impl Node {
    fn options(self, receiver_rates: Option<[u64; 2]>) -> Result<NodeOptions, Box<dyn std::error::Error>> {
        Ok(NodeOptions {
            config: Config::load(&self.config)?,
            receiver_rates,
            duration_us: self.duration_ms * 1000,
            output: self.output,
            ready: self.ready,
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    amitoki_l3_lab::runtime::install_shutdown()?;
    match Cli::parse().command {
        Command::Router(node) => run_node(node.options(None)?)?,
        Command::Receiver {
            node,
            short_credits_per_second,
            bulk_credits_per_second,
        } => run_node(node.options(Some([short_credits_per_second, bulk_credits_per_second]))?)?,
        Command::Bench {
            config,
            peer,
            duration_ms,
            short_rate,
            bulk_rate,
            short_bytes,
            bulk_bytes,
            short_deadline_us,
            bulk_deadline_us,
            paths,
            replica_bytes_per_second,
            retries,
            output,
        } => {
            run_benchmark(Benchmark {
                config: Config::load(&config)?,
                peer,
                duration_us: duration_ms.checked_mul(1000).ok_or("durationが大きすぎます")?,
                rates: [short_rate, bulk_rate],
                sizes: [short_bytes, bulk_bytes],
                deadlines: [short_deadline_us, bulk_deadline_us],
                paths,
                replica_bytes_per_second,
                retries,
                output,
            })?;
        },
    }
    Ok(())
}
