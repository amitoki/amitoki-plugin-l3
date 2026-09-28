use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "L3中継の補助プロセス。起動にはCAP_NET_RAWが必要です")]
struct Options {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    socket: PathBuf,
    /// このUIDのクライアントだけを受け付ける。既定は起動ユーザ。
    #[arg(long)]
    uid: Option<u32>,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let options = Options::parse();
    amitoki_plugin_l3::broker::serve(&options.config, &options.socket, options.uid.unwrap_or_else(|| unsafe { libc::geteuid() })).await
}
