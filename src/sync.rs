//! 4時刻の交換から、基準時計が取り得る区間を求める。OSの時計は変更しない。
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

const PARTS_PER_MILLION: u64 = 1_000_000;
// µsへの切り捨てと読み取りの境界を吸収する。
const TIMESTAMP_MARGIN_US: u64 = 2;
const SAMPLE_CAPACITY: usize = 8;
const RETIRED_DOMAIN_CAPACITY: usize = 8;
pub const SYNC_INTERVAL_US: u64 = 100_000;
pub const SYNC_TIMEOUT_US: u64 = 500_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClockSettings {
    pub authority: Option<u32>,
    pub max_error_us: u64,
    pub max_age_us: u64,
    pub drift_ppm: u64,
    pub simulation: crate::clock::Simulation,
}

impl Default for ClockSettings {
    fn default() -> Self {
        Self {
            authority: None,
            max_error_us: 2_000,
            max_age_us: 1_000_000,
            drift_ppm: 1_000,
            simulation: Default::default(),
        }
    }
}

impl ClockSettings {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.simulation.validate()?;
        if self.authority == Some(0)
            || !(10..=100_000).contains(&self.max_error_us)
            || !(SYNC_INTERVAL_US..=10_000_000).contains(&self.max_age_us)
            || !(1..=10_000).contains(&self.drift_ppm)
        {
            return Err("clockのauthority/error/age/driftが範囲外です");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Reading {
    pub domain: u64,
    pub local: u64,
    pub earliest: u64,
    pub latest: u64,
    pub uncertainty_us: u64,
    pub sample_age_us: u64,
    pub offset_us: i64,
    pub drift_ppm: u64,
}

impl Reading {
    pub fn exact(local: u64, domain: u64) -> Self {
        Self {
            domain,
            local,
            earliest: local,
            latest: local,
            uncertainty_us: 0,
            sample_age_us: 0,
            offset_us: 0,
            drift_ppm: 0,
        }
    }

    pub fn deadline(self, local_deadline: u64) -> Option<u64> {
        let remaining = local_deadline.checked_sub(self.local)?;
        // 基準時計が最も遅く進む場合に合わせ、元のローカル期限より長くしない。
        self.earliest.checked_add(remaining.saturating_sub(drift_margin(remaining, self.drift_ppm)))
    }

    pub fn local_deadline(self, deadline: u64) -> Option<u64> {
        let remaining = deadline.checked_sub(self.latest)?;
        let conservative = u128::from(remaining) * u128::from(PARTS_PER_MILLION) / u128::from(PARTS_PER_MILLION + self.drift_ppm);
        self.local.checked_add(conservative as u64)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Exchange {
    pub domain: u64,
    pub sent: u64,
    pub received_by_authority: u64,
    pub sent_by_authority: u64,
    pub received: u64,
}

struct Sample {
    local: u64,
    lower_offset: i128,
    upper_offset: i128,
}

#[derive(Default, Serialize)]
pub struct SyncMetrics {
    pub accepted: u64,
    pub rejected: u64,
    pub domain_changes: u64,
}

pub struct ClockEstimate {
    settings: ClockSettings,
    domain: Option<u64>,
    samples: VecDeque<Sample>,
    retired: VecDeque<(u64, u64)>,
    pub metrics: SyncMetrics,
}

impl ClockEstimate {
    pub fn new(settings: ClockSettings) -> Self {
        Self {
            settings,
            domain: None,
            samples: VecDeque::new(),
            retired: VecDeque::new(),
            metrics: SyncMetrics::default(),
        }
    }

    pub fn observe(&mut self, exchange: Exchange) -> bool {
        if !self.insert(exchange) {
            self.metrics.rejected += 1;
            return false;
        }
        self.metrics.accepted += 1;
        true
    }

    pub fn domain(&self) -> Option<u64> {
        self.domain
    }

    fn insert(&mut self, exchange: Exchange) -> bool {
        let Some(elapsed) = exchange.received.checked_sub(exchange.sent) else { return false };
        let Some(processing) = exchange.sent_by_authority.checked_sub(exchange.received_by_authority) else {
            return false;
        };
        let margin = drift_margin(elapsed, self.settings.drift_ppm) + TIMESTAMP_MARGIN_US;
        if exchange.domain == 0 || elapsed > SYNC_TIMEOUT_US || processing > elapsed.saturating_add(margin) {
            return false;
        }
        let sample = Sample {
            local: exchange.received,
            lower_offset: i128::from(exchange.sent_by_authority) - i128::from(exchange.received) - i128::from(margin),
            upper_offset: i128::from(exchange.received_by_authority) - i128::from(exchange.sent) + i128::from(margin),
        };
        if sample.lower_offset > sample.upper_offset {
            return false;
        }
        self.retired.retain(|(_, until)| *until > exchange.received);
        if self.retired.iter().any(|(domain, _)| *domain == exchange.domain) {
            return false;
        }
        if let Some(domain) = self.domain {
            if domain != exchange.domain {
                if self.retired.len() == RETIRED_DOMAIN_CAPACITY {
                    return false;
                }
                self.retired.push_back((domain, exchange.received.saturating_add(self.settings.max_age_us + SYNC_TIMEOUT_US)));
                self.samples.clear();
                self.metrics.domain_changes += 1;
            }
        }
        self.domain = Some(exchange.domain);
        if self.samples.len() == SAMPLE_CAPACITY {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
        true
    }

    pub fn reading(&self, now: u64) -> Option<Reading> {
        self.samples
            .iter()
            .filter_map(|sample| {
                let age = now.checked_sub(sample.local)?;
                if age > self.settings.max_age_us {
                    return None;
                }
                let growth = i128::from(drift_margin(age, self.settings.drift_ppm));
                let lower = sample.lower_offset - growth;
                let upper = sample.upper_offset + growth;
                let uncertainty = u64::try_from((upper - lower + 1) / 2).ok()?;
                if uncertainty > self.settings.max_error_us {
                    return None;
                }
                Some(Reading {
                    domain: self.domain?,
                    local: now,
                    earliest: u64::try_from(i128::from(now) + lower).ok()?,
                    latest: u64::try_from(i128::from(now) + upper).ok()?,
                    uncertainty_us: uncertainty,
                    sample_age_us: age,
                    offset_us: i64::try_from((lower + upper) / 2).ok()?,
                    drift_ppm: self.settings.drift_ppm,
                })
            })
            .min_by_key(|reading| reading.uncertainty_us)
    }
}

fn drift_margin(elapsed: u64, ppm: u64) -> u64 {
    (u128::from(elapsed) * u128::from(ppm)).div_ceil(u128::from(PARTS_PER_MILLION)).min(u128::from(u64::MAX)) as u64
}
