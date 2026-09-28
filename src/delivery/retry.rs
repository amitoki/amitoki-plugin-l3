//! 送信時刻を照合できたACKから往復時間と変動幅を推定する。
// 初回は従来の20ms。1msのイベント周期を2回分確保し、時計読取りの揺れで再送しない。
pub(super) const INITIAL_RETRY_US: u64 = 20_000;
const MIN_RETRY_US: u64 = 2_000;
pub(super) const MAX_RETRY_US: u64 = 1_000_000;

#[derive(Default)]
pub(super) struct RetryTiming {
    pub smoothed_us: u64,
    variation_us: u64,
    pub samples: u64,
}

impl RetryTiming {
    pub fn observe(&mut self, elapsed_us: u64) {
        let elapsed_us = elapsed_us.clamp(1, super::DEFAULT_TIMEOUT_US);
        if self.samples == 0 {
            self.smoothed_us = elapsed_us;
            self.variation_us = elapsed_us / 2;
        } else {
            self.variation_us = (3 * self.variation_us + self.smoothed_us.abs_diff(elapsed_us)) / 4;
            self.smoothed_us = (7 * self.smoothed_us + elapsed_us) / 8;
        }
        self.samples += 1;
    }

    pub fn timeout(&self) -> u64 {
        if self.samples == 0 {
            INITIAL_RETRY_US
        } else {
            (self.smoothed_us + 4 * self.variation_us).clamp(MIN_RETRY_US, MAX_RETRY_US)
        }
    }
}
