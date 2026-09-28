mod benchmark;
mod connection;
mod framing;
pub mod options;
mod receiver;

pub use benchmark::run_benchmark;
pub use receiver::run_receiver;

use std::{io, path::Path};

// L3と同じ周期で生成予定・終了通知を確認する。
const POLL_US: u64 = 1000;

fn write_report(path: &Path, report: &serde_json::Value) -> io::Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(report).map_err(io::Error::other)?)
}
