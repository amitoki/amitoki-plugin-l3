use super::{Settings, Signal};
use crate::packet::MAX_FRAME;
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};

// 状態とイベント履歴を有界にし、長時間観測でメモリが増え続けない。
const EVENT_LIMIT: usize = 128;
const PATH_COOLDOWN_US: u64 = 100_000;
const INITIAL_RTT_US: u64 = 5_000;
const MIN_WINDOW: u64 = 2 * MAX_FRAME as u64;
const INITIAL_WINDOW: u64 = 4 * MAX_FRAME as u64;
const MAX_WINDOW: u64 = 64 * MAX_FRAME as u64;
const MIN_CONTROL_INTERVAL_US: u64 = 1_000;

#[derive(Default, Serialize)]
struct PathState {
    path: u8,
    sent: u64,
    acknowledged: u64,
    timeouts: u64,
    nacks: u64,
    rtt_us: u64,
    last_feedback_us: u64,
    last_data_us: u64,
    probe_rtt_us: u64,
    disabled_until_us: u64,
    in_flight_bytes: u64,
    signal: Signal,
}

#[derive(Serialize)]
struct Event {
    at_us: u64,
    path: u8,
    reason: &'static str,
    window_bytes: u64,
    signal: Signal,
}

struct Change {
    reason: &'static str,
    signal: Signal,
}

pub struct Transmission {
    pub path: u8,
    pub previous_path: Option<u8>,
    pub bytes: u64,
    pub now: u64,
}

pub struct Acknowledgement {
    pub path: u8,
    pub now: u64,
    pub rtt_us: u64,
    pub bytes: u64,
    pub signal: Signal,
    pub data: bool,
}

pub struct Controller {
    settings: Settings,
    paths: BTreeMap<u8, PathState>,
    window: u64,
    next_send: u64,
    control_at: u64,
    base_rtt: u64,
    last_path: Option<u8>,
    events: VecDeque<Event>,
    events_evicted: u64,
    acknowledged_since_control: u64,
}

impl Controller {
    pub fn new(settings: Settings, paths: &[u8]) -> Self {
        Self {
            settings,
            paths: paths
                .iter()
                .map(|path| {
                    (
                        *path,
                        PathState {
                            path: *path,
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            window: INITIAL_WINDOW,
            next_send: 0,
            control_at: 0,
            base_rtt: 0,
            last_path: None,
            events: VecDeque::new(),
            events_evicted: 0,
            acknowledged_since_control: 0,
        }
    }

    pub fn select(&self, now: u64, fallback: u8) -> u8 {
        if !self.settings.adaptive_paths {
            return fallback;
        }
        self.paths
            .values()
            .min_by_key(|state| {
                let rtt = if state.rtt_us == 0 { INITIAL_RTT_US } else { state.rtt_us };
                let capacity = state.signal.capacity_bytes_per_second;
                let serialization =
                    state.in_flight_bytes.saturating_mul(1_000_000).checked_div(capacity).unwrap_or_else(|| state.in_flight_bytes.saturating_mul(rtt) / MAX_FRAME as u64);
                (state.disabled_until_us > now, rtt.saturating_add(serialization), state.sent, state.path)
            })
            .map(|state| state.path)
            .unwrap_or(fallback)
    }

    pub fn select_retry(&self, now: u64, previous: u8) -> u8 {
        let selected = self.select(now, previous);
        if !self.settings.adaptive_paths || selected != previous {
            return selected;
        }
        self.paths
            .values()
            .filter(|state| state.path != previous && state.disabled_until_us <= now)
            .min_by_key(|state| (state.rtt_us, state.sent, state.path))
            .map(|state| state.path)
            .unwrap_or(selected)
    }

    pub fn can_send(&self, now: u64, bytes: u64, retry: bool) -> bool {
        !self.settings.congestion_control || (now >= self.next_send && (retry || self.in_flight().saturating_add(bytes) <= self.window))
    }

    pub fn sent(&mut self, transmission: Transmission) {
        let Transmission { path, previous_path, bytes, now } = transmission;
        if let Some(previous) = previous_path {
            self.release(previous, bytes);
        }
        if let Some(state) = self.paths.get_mut(&path) {
            state.sent += 1;
            state.in_flight_bytes += bytes;
        }
        if self.last_path != Some(path) {
            self.event(
                now,
                path,
                Change {
                    reason: "path_selected",
                    signal: Signal::default(),
                },
            );
            self.last_path = Some(path);
        }
        if self.settings.congestion_control {
            let rtt = self.base_rtt.max(MIN_CONTROL_INTERVAL_US).saturating_add(self.settings.target_queue_us);
            self.next_send = now.saturating_add(bytes.saturating_mul(rtt).div_ceil(self.window));
        }
    }

    pub fn release(&mut self, path: u8, bytes: u64) {
        if let Some(state) = self.paths.get_mut(&path) {
            state.in_flight_bytes = state.in_flight_bytes.saturating_sub(bytes);
        }
    }

    pub fn feedback(&mut self, acknowledgement: Acknowledgement) {
        let Acknowledgement {
            path,
            now,
            rtt_us,
            bytes,
            signal,
            data,
        } = acknowledgement;
        let Some(state) = self.paths.get_mut(&path) else { return };
        if !data {
            state.probe_rtt_us = rtt_us.max(1);
        }
        // 使用を止めた経路もprobeで更新し、古い混雑RTTだけで永久に除外しない。
        if data || state.acknowledged == 0 || now.saturating_sub(state.last_data_us) >= PATH_COOLDOWN_US {
            state.rtt_us = if state.rtt_us == 0 { rtt_us.max(1) } else { (7 * state.rtt_us + rtt_us.max(1)) / 8 };
        }
        state.last_feedback_us = now;
        if data || now >= state.disabled_until_us {
            state.disabled_until_us = 0;
        }
        if signal.node != 0 {
            state.signal = signal;
        }
        if !data {
            return;
        }
        state.acknowledged += 1;
        state.last_data_us = now;
        self.base_rtt = if self.base_rtt == 0 { rtt_us.max(1) } else { self.base_rtt.min(rtt_us.max(1)) };
        self.acknowledged_since_control += bytes;
        if !self.settings.congestion_control {
            return;
        }
        if self.control_at == 0 {
            self.control_at = now;
            return;
        }
        let interval = self.base_rtt.max(MIN_CONTROL_INTERVAL_US);
        if now.saturating_sub(self.control_at) < interval {
            return;
        }
        let congested = u64::from(signal.queue_us) > self.settings.target_queue_us || rtt_us > self.base_rtt.saturating_add(self.settings.target_queue_us.saturating_mul(2));
        if congested {
            // 過去の通知が連続しても一往復に一度しか減らさない。
            let elapsed = now.saturating_sub(self.control_at).max(interval);
            let measured = self.acknowledged_since_control.saturating_mul(interval + self.settings.target_queue_us) / elapsed;
            self.window = (self.window * 3 / 4).min(measured.saturating_add(MIN_WINDOW)).max(MIN_WINDOW);
            self.event(now, path, Change { reason: "congestion", signal });
        } else {
            let increase = (MAX_FRAME as u64 * self.acknowledged_since_control / self.window).max(1);
            self.window = self.window.saturating_add(increase).min(MAX_WINDOW);
        }
        self.acknowledged_since_control = 0;
        self.control_at = now;
    }

    pub fn loss(&mut self, path: u8, now: u64, signal: Option<Signal>) {
        if let Some(state) = self.paths.get_mut(&path) {
            if let Some(signal) = signal {
                state.nacks += 1;
                state.signal = signal;
            } else {
                state.timeouts += 1;
            }
            state.disabled_until_us = now.saturating_add(PATH_COOLDOWN_US);
        }
        if self.settings.congestion_control && now.saturating_sub(self.control_at) >= self.base_rtt.max(MIN_CONTROL_INTERVAL_US) {
            self.window = (self.window / 2).max(MIN_WINDOW);
            self.control_at = now;
            self.acknowledged_since_control = 0;
        }
        self.event(
            now,
            path,
            Change {
                reason: if signal.is_some() { "trimmed" } else { "timeout" },
                signal: signal.unwrap_or_default(),
            },
        );
    }

    fn in_flight(&self) -> u64 {
        self.paths.values().map(|path| path.in_flight_bytes).sum()
    }

    fn event(&mut self, now: u64, path: u8, change: Change) {
        let Change { reason, signal } = change;
        if self.events.len() == EVENT_LIMIT {
            self.events.pop_front();
            self.events_evicted += 1;
        }
        self.events.push_back(Event {
            at_us: now,
            path,
            reason,
            window_bytes: self.window,
            signal,
        });
    }

    pub fn report(&self) -> serde_json::Value {
        serde_json::json!({ "settings": self.settings, "window_bytes": self.window, "in_flight_bytes": self.in_flight(),
            "paths": self.paths.values().collect::<Vec<_>>(), "events": self.events, "events_evicted": self.events_evicted })
    }
}
