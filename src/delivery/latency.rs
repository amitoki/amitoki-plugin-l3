// 1メッセージごとの履歴を持たず、指数ごと8区間の有界ヒストグラムで上側分位を求める。
const SUB_BUCKETS: usize = 8;
const BUCKETS: usize = 64 * SUB_BUCKETS;

pub(super) struct Latency {
    counts: [u64; BUCKETS],
    total: u64,
}

impl Default for Latency {
    fn default() -> Self {
        Self { counts: [0; BUCKETS], total: 0 }
    }
}

impl Latency {
    pub fn record(&mut self, elapsed: u64) {
        let elapsed = elapsed.max(1);
        let exponent = (63 - elapsed.leading_zeros()) as usize;
        let base = 1_u64 << exponent;
        let fraction = ((u128::from(elapsed - base) * SUB_BUCKETS as u128) / u128::from(base)) as usize;
        self.counts[exponent * SUB_BUCKETS + fraction] += 1;
        self.total += 1;
    }

    pub fn p99_upper_us(&self) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let target = (u128::from(self.total) * 99).div_ceil(100) as u64;
        let mut accumulated = 0;
        for (index, count) in self.counts.iter().enumerate() {
            accumulated += count;
            if accumulated >= target {
                let base = 1_u128 << (index / SUB_BUCKETS);
                let upper = base + (base * (index % SUB_BUCKETS + 1) as u128).div_ceil(SUB_BUCKETS as u128) - 1;
                return upper.min(u128::from(u64::MAX)) as u64;
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_reported_percentile_bounds_the_observed_latency() {
        let mut latency = Latency::default();
        for value in 1..=100 {
            latency.record(value);
        }
        assert!((99..=111).contains(&latency.p99_upper_us()));
        let mut extreme = Latency::default();
        extreme.record(u64::MAX);
        assert_eq!(extreme.p99_upper_us(), u64::MAX);
    }
}
