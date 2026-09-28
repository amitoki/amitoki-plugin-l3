use amitoki_l3_lab::{
    config::Config,
    delivery::{Ordering, DEFAULT_PENDING, DEFAULT_TIMEOUT_US, DEFAULT_WINDOW},
    runtime::{run_benchmark, run_node, run_reliable_benchmark, Benchmark, NodeOptions, ReliableOptions},
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "amitoki-l3", version, about = "private Ethernet上の独自L3/信頼性・期限付きメッセージ配送の実験")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 静的経路で独自L3パケットを転送する。
    Router(Node),
    /// 配送を受け付ける。ACKは受信キューへの受付を表す。
    Receiver {
        #[command(flatten)]
        node: Node,
        #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(1..=100_000))]
        short_credits_per_second: u64,
        #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=100_000))]
        bulk_credits_per_second: u64,
    },
    /// 指定した到着率で短文と背景負荷を送信する。既定は信頼性のある順序なし配送。
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
        /// deadlineモードの短文の期限。
        short_deadline_us: u64,
        #[arg(long, default_value_t = 200_000)]
        /// deadlineモードの背景負荷の期限。
        bulk_deadline_us: u64,
        #[arg(long, value_delimiter = ',', default_value = "1")]
        paths: Vec<u8>,
        #[arg(long, default_value_t = 30_000)]
        replica_bytes_per_second: u64,
        #[arg(long, default_value_t = 0)]
        /// deadlineモードだけの再送回数。reliableはtimeoutまで再送する。
        retries: u8,
        #[arg(long, value_enum, default_value = "reliable")]
        delivery: Delivery,
        #[arg(long, value_enum, default_value = "unordered")]
        short_ordering: Ordering,
        #[arg(long, value_enum, default_value = "unordered")]
        bulk_ordering: Ordering,
        #[arg(long, default_value_t = DEFAULT_TIMEOUT_US / 1000)]
        delivery_timeout_ms: u64,
        #[arg(long, default_value_t = DEFAULT_PENDING)]
        pending_limit: usize,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Delivery {
    Reliable,
    Deadline,
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
    /// 受信アプリへ渡した本文・channel・sequenceをJSONLへ記録する。
    #[arg(long)]
    delivery_log: Option<PathBuf>,
    #[arg(long, default_value_t = DEFAULT_WINDOW)]
    receive_window: usize,
    /// 比較用にアプリ配送時刻・sequence・本文fingerprintをJSONLへ記録する。
    #[arg(long)]
    receipt_log: Option<PathBuf>,
}

impl Node {
    fn options(self, receiver_rates: Option<[u64; 2]>) -> Result<NodeOptions, Box<dyn std::error::Error>> {
        Ok(NodeOptions {
            config: Config::load(&self.config)?,
            receiver_rates,
            duration_us: self.duration_ms * 1000,
            output: self.output,
            ready: self.ready,
            delivery_log: self.delivery_log,
            receive_window: self.receive_window,
            receipt_log: self.receipt_log,
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
            delivery,
            short_ordering,
            bulk_ordering,
            delivery_timeout_ms,
            pending_limit,
            output,
        } => {
            let benchmark = Benchmark {
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
            };
            match delivery {
                Delivery::Deadline => {
                    if short_ordering != Ordering::Unordered || bulk_ordering != Ordering::Unordered {
                        return Err("順序保証はreliableモードだけで使用できます".into());
                    }
                    run_benchmark(benchmark)?;
                },
                Delivery::Reliable => run_reliable_benchmark(
                    benchmark,
                    ReliableOptions {
                        ordering: [short_ordering, bulk_ordering],
                        timeout_us: delivery_timeout_ms.checked_mul(1000).ok_or("timeoutが大きすぎます")?,
                        pending_limit,
                    },
                )?,
            }
        },
    }
    Ok(())
}
