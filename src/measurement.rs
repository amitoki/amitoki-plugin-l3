//! 両方式で共通の、送信予定数を分母にした期限内ACKの集計。
use serde::Serialize;

#[derive(Default, Serialize)]
pub(crate) struct TrafficMetrics {
    pub offered: u64,
    pub sent: u64,
    pub acknowledged: u64,
    pub no_credit: u64,
    pub expired_before_send: u64,
    pub pending_full: u64,
    pub unsynchronized: u64,
    #[serde(skip)]
    pub latencies: Vec<u64>,
}

pub(crate) fn traffic_report(metrics: &TrafficMetrics) -> serde_json::Value {
    let mut report = serde_json::to_value(metrics).expect("整数だけの検証結果");
    let mut sorted = metrics.latencies.clone();
    sorted.sort_unstable();
    let percentile = |percent: usize| -> Option<u64> {
        if sorted.is_empty() {
            None
        } else {
            Some(sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)])
        }
    };
    report["deadline_misses"] = (metrics.offered - metrics.acknowledged).into();
    report["on_time_ratio"] = if metrics.offered == 0 {
        serde_json::Value::Null
    } else {
        (metrics.acknowledged as f64 / metrics.offered as f64).into()
    };
    report["rtt_us"] = serde_json::json!({"p50":percentile(50),"p99":percentile(99),"max":sorted.last()});
    report
}
