/// 時刻は呼び出し側で一度取得し、schedulerとcreditで同じ時間軸を使う。
pub struct TokenBucket {
    rate: u64,
    capacity: u64,
    available: u64,
    updated: u64,
    remainder: u64,
}
const MICROS_PER_SECOND: u64 = 1_000_000;

impl TokenBucket {
    pub fn new(rate: u64, capacity: u64, now: u64) -> Self {
        Self {
            rate,
            capacity,
            available: capacity,
            updated: now,
            remainder: 0,
        }
    }

    fn refill(&mut self, now: u64) {
        let elapsed = now.saturating_sub(self.updated);
        let units = u128::from(elapsed) * u128::from(self.rate) + u128::from(self.remainder);
        let earned = (units / u128::from(MICROS_PER_SECOND)).min(u128::from(u64::MAX)) as u64;
        self.available = self.available.saturating_add(earned).min(self.capacity);
        self.remainder = if self.available == self.capacity {
            0
        } else {
            (units % u128::from(MICROS_PER_SECOND)) as u64
        };
        self.updated = self.updated.max(now);
    }

    pub fn take(&mut self, amount: u64, now: u64) -> bool {
        self.refill(now);
        if self.available < amount {
            return false;
        }
        self.available -= amount;
        true
    }

    pub fn take_up_to(&mut self, maximum: u64, now: u64) -> u64 {
        self.refill(now);
        let amount = self.available.min(maximum);
        self.available -= amount;
        amount
    }
}
