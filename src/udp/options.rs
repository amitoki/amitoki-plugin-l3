use std::{net::SocketAddrV4, path::PathBuf};

#[derive(clap::Args)]
pub struct Endpoint {
    #[arg(long)]
    pub bind: SocketAddrV4,
    #[arg(long)]
    pub peer: SocketAddrV4,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value_t = 3000, value_parser = clap::value_parser!(u64).range(1..=60_000))]
    pub duration_ms: u64,
}

#[derive(clap::Args)]
pub struct Load {
    #[command(flatten)]
    pub endpoint: Endpoint,
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(0..=10_000))]
    pub short_rate: u64,
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u64).range(0..=10_000))]
    pub bulk_rate: u64,
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u16).range(1..=256))]
    pub short_bytes: u16,
    #[arg(long, default_value_t = 1200, value_parser = clap::value_parser!(u16).range(1..=1400))]
    pub bulk_bytes: u16,
    #[arg(long, default_value_t = 20_000, value_parser = clap::value_parser!(u64).range(1..=500_000))]
    pub short_deadline_us: u64,
    #[arg(long, default_value_t = 200_000, value_parser = clap::value_parser!(u64).range(1..=500_000))]
    pub bulk_deadline_us: u64,
}
