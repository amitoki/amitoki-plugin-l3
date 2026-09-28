//! ベンチマークの配送数・遅延・プロセス資源を集計する。
use serde::Serialize;

pub(crate) fn process_usage() -> serde_json::Value {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusageが成功した場合だけ初期化済みの構造体を読む。
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return serde_json::Value::Null;
    }
    let usage = unsafe { usage.assume_init() };
    serde_json::json!({"user_us":usage.ru_utime.tv_sec * 1_000_000 + usage.ru_utime.tv_usec,
        "system_us":usage.ru_stime.tv_sec * 1_000_000 + usage.ru_stime.tv_usec,"max_rss_kib":usage.ru_maxrss})
}

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
