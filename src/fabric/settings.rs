use serde::{Deserialize, Serialize};

// ユーザー空間の1msポーリングで観測できる範囲から実験を始める。
const DEFAULT_TARGET_QUEUE_US: u64 = 2_000;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub adaptive_paths: bool,
    pub congestion_control: bool,
    pub telemetry: bool,
    pub trimming: bool,
    pub clock_independent: bool,
    pub target_queue_us: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            adaptive_paths: false,
            congestion_control: false,
            telemetry: false,
            trimming: false,
            clock_independent: false,
            target_queue_us: DEFAULT_TARGET_QUEUE_US,
        }
    }
}

impl Settings {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(100..=100_000).contains(&self.target_queue_us) {
            return Err("fabric.target_queue_usは100〜100000です");
        }
        Ok(())
    }
}
