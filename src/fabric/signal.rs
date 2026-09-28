use serde::Serialize;

/// 経路内で最長のキュー滞留を観測した出口の情報。0 nodeは未観測。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Signal {
    pub node: u32,
    pub queue_us: u32,
    pub available_bytes_per_second: u64,
    pub capacity_bytes_per_second: u64,
}

impl Signal {
    pub fn observe(&mut self, candidate: Self) {
        if candidate.node != 0 && (self.node == 0 || candidate.queue_us > self.queue_us) {
            *self = candidate;
        }
    }

    pub fn valid(&self) -> bool {
        if self.node == 0 {
            *self == Self::default()
        } else {
            self.capacity_bytes_per_second > 0
                && self.capacity_bytes_per_second <= crate::config::MAX_BYTES_PER_SECOND
                && self.available_bytes_per_second <= self.capacity_bytes_per_second
        }
    }
}
