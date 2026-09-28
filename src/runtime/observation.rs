use std::os::unix::fs::OpenOptionsExt;
use std::{
    io::{self, Write},
    path::PathBuf,
};

// 可視化の書き出しを10Hzに制限し、配送処理への影響を抑える。
const INTERVAL_US: u64 = 100_000;

pub(super) struct Observer {
    path: Option<PathBuf>,
    next: u64,
    pub errors: u64,
}

impl Observer {
    pub fn new(path: Option<PathBuf>) -> Self {
        Self { path, next: 0, errors: 0 }
    }

    pub fn due(&self, now: u64) -> bool {
        self.path.is_some() && now >= self.next
    }

    pub fn write(&mut self, now: u64, report: serde_json::Value) {
        self.next = now.saturating_add(INTERVAL_US);
        if self.write_report(&report).is_err() {
            self.errors += 1;
        }
    }

    fn write_report(&self, report: &serde_json::Value) -> io::Result<()> {
        let Some(path) = &self.path else { return Ok(()) };
        // 同じディレクトリへ一時保存してrenameし、閲覧中に途中のJSONを見せない。
        let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or(std::path::Path::new("."));
        std::fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(".amitoki-observation-{}.tmp", uuid::Uuid::new_v4()));
        let mut file = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&temporary)?;
        let saved = (|| {
            file.write_all(&serde_json::to_vec(report).map_err(io::Error::other)?)?;
            std::fs::rename(&temporary, path)
        })();
        if saved.is_err() {
            let _ = std::fs::remove_file(temporary);
        }
        saved
    }
}
