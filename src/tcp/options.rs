use std::{net::SocketAddr, path::PathBuf};

#[derive(clap::Args)]
pub struct Endpoint {
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=2), default_value_t = 1)]
    pub connections: u8,
    #[arg(long, default_value_t = 3000, value_parser = clap::value_parser!(u64).range(1..=60_000))]
    pub duration_ms: u64,
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(clap::Args)]
pub struct Receiver {
    #[command(flatten)]
    pub endpoint: Endpoint,
    #[arg(long)]
    pub bind: SocketAddr,
    #[arg(long)]
    pub ready: PathBuf,
    #[arg(long)]
    pub receipt_log: PathBuf,
}

#[derive(clap::Args)]
pub struct Benchmark {
    #[command(flatten)]
    pub endpoint: Endpoint,
    #[arg(long)]
    pub peer: SocketAddr,
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(0..=10_000))]
    pub short_rate: u64,
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u64).range(0..=10_000))]
    pub bulk_rate: u64,
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u16).range(1..=240))]
    pub short_bytes: u16,
    #[arg(long, default_value_t = 1200, value_parser = clap::value_parser!(u16).range(1..=1384))]
    pub bulk_bytes: u16,
    #[arg(long, default_value_t = 10_000, value_parser = clap::value_parser!(u64).range(1..=60_000))]
    pub delivery_timeout_ms: u64,
}
