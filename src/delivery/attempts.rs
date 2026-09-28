use std::collections::VecDeque;

// 遅延ACKの識別に必要な直近の試行だけを保持する。
const HISTORY_LIMIT: usize = 8;

#[derive(Default)]
pub(super) struct Attempts {
    history: VecDeque<(u64, u8)>,
    pub last_path: Option<u8>,
    pub fast_retry: bool,
}

impl Attempts {
    pub fn record(&mut self, timestamp: u64, path: u8) {
        if self.history.len() == HISTORY_LIMIT {
            self.history.pop_front();
        }
        self.history.push_back((timestamp, path));
        self.last_path = Some(path);
        self.fast_retry = false;
    }

    pub fn matches(&self, timestamp: u64, path: u8) -> bool {
        self.history.contains(&(timestamp, path))
    }

    pub fn reject_latest(&mut self, timestamp: u64, path: u8) -> bool {
        if self.fast_retry || self.history.back() != Some(&(timestamp, path)) {
            return false;
        }
        self.fast_retry = true;
        true
    }
}
