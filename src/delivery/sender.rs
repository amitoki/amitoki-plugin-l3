use super::attempts::Attempts;
use super::retry::{RetryTiming, INITIAL_RETRY_US, MAX_RETRY_US};
use super::{wire::Metadata, Ordering, ATTEMPT_LIFETIME_US, DEFAULT_PENDING, DEFAULT_TIMEOUT_US, MAX_WINDOW, PREFIX_SIZE};
use crate::fabric::{Acknowledgement, Controller, Settings, Transmission, MAX_PATHS};
use crate::{
    packet::{fingerprint, Class, Kind, Packet, INITIAL_HOPS, LOCAL_LIFETIME, MAX_PAYLOAD, MAX_SHORT_PAYLOAD, REPLICA, TELEMETRY},
    sync::Reading,
    tokens::TokenBucket,
};
use serde::Serialize;
use std::collections::BTreeMap;

const PROBE_INTERVAL_US: u64 = 100_000;
const TRANSMIT_BURST: usize = 32;
// 停止判定を有限にし、実験CLIの最大実行時間内へ収める。
const MAX_TIMEOUT_US: u64 = 60_000_000;

pub struct ChannelOptions {
    pub source: u32,
    pub destination: u32,
    pub session: u64,
    pub channel: u32,
    pub class: Class,
    pub ordering: Ordering,
    pub paths: Vec<u8>,
    pub pending_limit: usize,
    pub timeout_us: u64,
    pub fabric: Settings,
}

impl ChannelOptions {
    pub fn new(source: u32, destination: u32, session: u64) -> Self {
        Self {
            source,
            destination,
            session,
            channel: 1,
            class: Class::Short,
            ordering: Ordering::Unordered,
            paths: vec![1],
            pending_limit: DEFAULT_PENDING,
            timeout_us: DEFAULT_TIMEOUT_US,
            fabric: Settings::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelState {
    Opening,
    Active,
    TimedOut,
    PeerReset,
    ClockChanged,
    Cancelled,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubmitError {
    #[error("送信キューが満杯です。本文を保持し、ACKを受信してから再試行してください")]
    WouldBlock,
    #[error("本文の長さが範囲外です")]
    PayloadSize,
    #[error("channelが終了しています。未確認分は受信済みの可能性があります")]
    Closed,
}

struct Pending {
    payload: Vec<u8>,
    fingerprint: u64,
    submitted: u64,
    retry_at: u64,
    retry_interval: u64,
    attempts: u64,
    last_sent: u64,
    transmissions: Attempts,
}

impl Pending {
    fn wire_size(&self) -> u64 {
        (crate::packet::ETHERNET_HEADER_SIZE + crate::packet::HEADER_SIZE + PREFIX_SIZE + self.payload.len()) as u64
    }
}

#[derive(Default, Serialize)]
pub struct SenderMetrics {
    pub submitted: u64,
    pub acknowledged: u64,
    pub unconfirmed: u64,
    pub sent: u64,
    pub retransmissions: u64,
    pub admission_blocked: u64,
    pub rtt_samples: u64,
    pub smoothed_rtt_us: u64,
    pub retry_timeout_us: u64,
    pub redundant_bytes: u64,
    pub invalid_responses: u64,
    pub nacks: u64,
    pub peak_pending: usize,
    pub acknowledgement_total_us: u64,
    pub acknowledgement_max_us: u64,
}

pub struct SendTick<'a> {
    pub now: u64,
    pub time: Option<Reading>,
    pub retry_budget: &'a mut TokenBucket,
}

pub struct Channel {
    options: ChannelOptions,
    pending: BTreeMap<u64, Pending>,
    next_sequence: u64,
    highest_sent: u64,
    receive_base: u64,
    receive_window: u64,
    epoch: Option<u64>,
    domain: Option<u64>,
    probe_at: u64,
    state: ChannelState,
    timing: RetryTiming,
    fabric: Controller,
    probes: BTreeMap<u8, u64>,
    latencies: super::latency::Latency,
    pub metrics: SenderMetrics,
}

impl Channel {
    pub fn new(options: ChannelOptions) -> Result<Self, &'static str> {
        if options.source == 0
            || options.destination == 0
            || options.source == options.destination
            || options.session == 0
            || options.channel == 0
            || options.paths.is_empty()
            || options.paths.len() > MAX_PATHS
            || options.paths.contains(&0)
            || (options.paths.iter().collect::<std::collections::BTreeSet<_>>().len() != options.paths.len())
            || options.fabric.validate().is_err()
            || options.pending_limit == 0
            || options.pending_limit > DEFAULT_PENDING
            || options.timeout_us == 0
            || options.timeout_us > MAX_TIMEOUT_US
        {
            return Err("channelの識別子/経路/キュー長/timeoutが範囲外です");
        }
        Ok(Self {
            fabric: Controller::new(options.fabric, &options.paths),
            probes: BTreeMap::new(),
            latencies: Default::default(),
            options,
            pending: BTreeMap::new(),
            next_sequence: 1,
            highest_sent: 0,
            receive_base: 1,
            receive_window: 0,
            epoch: None,
            domain: None,
            probe_at: 0,
            state: ChannelState::Opening,
            timing: RetryTiming::default(),
            metrics: SenderMetrics {
                retry_timeout_us: INITIAL_RETRY_US,
                ..Default::default()
            },
        })
    }

    pub fn state(&self) -> ChannelState {
        self.state
    }
    pub fn pending(&self) -> usize {
        self.pending.len()
    }
    pub fn is_closed(&self) -> bool {
        !matches!(self.state, ChannelState::Opening | ChannelState::Active)
    }

    /// 未ACK分を未確認として終了する。受信側で処理済みの可能性は残る。
    pub fn abort(&mut self) {
        if !self.is_closed() {
            self.fail(ChannelState::Cancelled);
        }
    }

    fn expire(&mut self, now: u64) -> bool {
        if !self.is_closed() && self.pending.values().any(|pending| now.saturating_sub(pending.submitted) >= self.options.timeout_us) {
            self.fail(ChannelState::TimedOut);
        }
        self.is_closed()
    }

    /// WouldBlock時はsequenceを消費せず、呼び出し側が本文を保持する。
    pub fn try_send(&mut self, payload: &[u8], now: u64) -> Result<u64, SubmitError> {
        if self.expire(now) || self.next_sequence == u64::MAX {
            return Err(SubmitError::Closed);
        }
        let limit = if self.options.class == Class::Short { MAX_SHORT_PAYLOAD } else { MAX_PAYLOAD } - PREFIX_SIZE;
        if payload.is_empty() || payload.len() > limit {
            return Err(SubmitError::PayloadSize);
        }
        if self.pending.len() >= self.options.pending_limit {
            return Err(SubmitError::WouldBlock);
        }
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.pending.insert(
            sequence,
            Pending {
                payload: payload.to_vec(),
                fingerprint: fingerprint(payload),
                submitted: now,
                retry_at: now,
                retry_interval: INITIAL_RETRY_US,
                attempts: 0,
                last_sent: 0,
                transmissions: Attempts::default(),
            },
        );
        self.metrics.submitted += 1;
        self.metrics.peak_pending = self.metrics.peak_pending.max(self.pending.len());
        Ok(sequence)
    }

    pub fn poll(&mut self, now: u64, time: Option<Reading>, retry_budget: &mut TokenBucket) -> Vec<Packet> {
        let mut packets = Vec::new();
        self.transmit(SendTick { now, time, retry_budget }, |packet| {
            packets.push(packet);
            true
        });
        packets
    }

    /// 下位キューが受け付けた後にだけ、試行回数と再送期限を進める。
    pub fn transmit(&mut self, tick: SendTick<'_>, mut enqueue: impl FnMut(Packet) -> bool) {
        let SendTick { now, time, retry_budget } = tick;
        if self.expire(now) {
            return;
        }
        let Some(time) = time else { return };
        if self.domain.is_some_and(|domain| domain != time.domain) {
            self.fail(ChannelState::ClockChanged);
            return;
        }
        self.domain = Some(time.domain);
        let expires = if self.options.fabric.clock_independent {
            0
        } else {
            let Some(expires) = time.deadline(now.saturating_add(ATTEMPT_LIFETIME_US)).filter(|deadline| *deadline > time.latest) else {
                return;
            };
            expires
        };
        let metadata = Metadata {
            channel: self.options.channel,
            ordering: self.options.ordering,
            epoch: self.epoch.unwrap_or(0),
        };
        let template = Packet {
            kind: Kind::ReliableOpen,
            class: self.options.class,
            hops: INITIAL_HOPS,
            source: self.options.source,
            destination: self.options.destination,
            session: self.options.session,
            message: 1,
            expires,
            clock_domain: time.domain,
            credit: 0,
            path: self.options.paths[0],
            flags: if self.options.fabric.telemetry { TELEMETRY } else { 0 } | if self.options.fabric.clock_independent { LOCAL_LIFETIME } else { 0 },
            sent_at: now.max(1),
            signal: Default::default(),
            payload: metadata.encode(&[]),
        };
        let mut accepted = 0;
        // ACKに載せた窓の更新が落ちても、定期probeで回復する。
        if now >= self.probe_at {
            for path in &self.options.paths {
                if enqueue(Packet { path: *path, ..template.clone() }) {
                    accepted += 1;
                    self.probes.insert(*path, template.sent_at);
                    self.probe_at = now.saturating_add(PROBE_INTERVAL_US);
                }
            }
        }
        if self.epoch.is_none() {
            return;
        }
        for (sequence, pending) in &mut self.pending {
            if accepted >= TRANSMIT_BURST {
                break;
            }
            let retry_at = if pending.attempts == 0 || pending.transmissions.fast_retry {
                pending.retry_at
            } else {
                pending.last_sent.saturating_add(pending.retry_interval.max(self.timing.timeout()))
            };
            if *sequence >= self.receive_base.saturating_add(self.receive_window) || now < retry_at {
                continue;
            }
            let retry = pending.attempts != 0;
            let fallback = self.options.paths[pending.attempts as usize % self.options.paths.len()];
            let path = if retry && self.options.fabric.adaptive_paths {
                self.fabric.select_retry(now, pending.transmissions.last_path.expect("送信済み経路"))
            } else {
                self.fabric.select(now, fallback)
            };
            let packet = Packet {
                kind: Kind::ReliableData,
                message: *sequence,
                path,
                flags: template.flags | if retry { REPLICA } else { 0 },
                payload: metadata.encode(&pending.payload),
                ..template.clone()
            };
            let wire_size = packet.wire_size() as u64;
            if !self.fabric.can_send(now, wire_size, retry) || (retry && !retry_budget.can_take(wire_size, now)) {
                continue;
            }
            let path = packet.path;
            if !enqueue(packet) {
                self.metrics.admission_blocked += 1;
                break;
            }
            accepted += 1;
            if retry && !pending.transmissions.fast_retry {
                self.fabric.loss(pending.transmissions.last_path.expect("送信済み経路"), now, None);
            }
            self.fabric.sent(Transmission {
                path,
                previous_path: pending.transmissions.last_path,
                bytes: wire_size,
                now,
            });
            pending.transmissions.record(template.sent_at, path);
            if retry {
                // enqueueは予算を変更しないため、同じ時刻の予約量を確実に消費できる。
                let consumed = retry_budget.take(wire_size, now);
                debug_assert!(consumed);
                self.metrics.retransmissions += 1;
                self.metrics.redundant_bytes += wire_size;
                pending.retry_interval = pending.retry_interval.saturating_mul(2).max(self.timing.timeout()).min(MAX_RETRY_US);
            } else {
                pending.retry_interval = self.timing.timeout();
                self.metrics.sent += 1;
                self.highest_sent = self.highest_sent.max(*sequence);
            }
            pending.last_sent = now;
            pending.attempts += 1;
            pending.retry_at = now.saturating_add(pending.retry_interval);
        }
    }

    /// 正しいACKだけで解放し、受付が確認できたsequenceを返す。
    pub fn receive(&mut self, packet: &Packet, now: u64) -> Option<u64> {
        if self.expire(now) {
            return None;
        }
        if !self.valid_response(packet) {
            self.metrics.invalid_responses += 1;
            return None;
        }
        let metadata = Metadata::decode(packet).expect("検証済みメタデータ");
        match packet.kind {
            Kind::ReliableReset => {
                if self.epoch == Some(metadata.epoch) {
                    self.fail(ChannelState::PeerReset);
                }
            },
            Kind::ReliableReady => {
                if self.epoch.is_some_and(|epoch| epoch != metadata.epoch) {
                    // 遅延した別世代の応答で、同じ本文を新しいsessionへ再送しない。
                    self.fail(ChannelState::PeerReset);
                    return None;
                }
                if packet.message > self.highest_sent + 1 || packet.credit > MAX_WINDOW as u64 {
                    self.metrics.invalid_responses += 1;
                    return None;
                }
                if self.probes.get(&packet.path) == Some(&packet.sent_at) && packet.sent_at <= now {
                    self.fabric.feedback(Acknowledgement {
                        path: packet.path,
                        now,
                        rtt_us: now - packet.sent_at,
                        bytes: 0,
                        signal: packet.signal,
                        data: false,
                    });
                }
                self.epoch = Some(metadata.epoch);
                self.receive_base = self.receive_base.max(packet.message);
                self.receive_window = packet.credit;
                self.state = ChannelState::Active;
            },
            Kind::ReliableNack => {
                let pending = self.pending.get_mut(&packet.message)?;
                if self.epoch != Some(metadata.epoch) || packet.payload[PREFIX_SIZE..] != pending.fingerprint.to_be_bytes() {
                    self.metrics.invalid_responses += 1;
                    return None;
                }
                if pending.transmissions.reject_latest(packet.sent_at, packet.path) {
                    pending.retry_at = now;
                    self.metrics.nacks += 1;
                    self.fabric.loss(packet.path, now, Some(packet.signal));
                }
            },
            Kind::ReliableAck => {
                if self.epoch != Some(metadata.epoch) || packet.credit > self.highest_sent + 1 {
                    self.metrics.invalid_responses += 1;
                    return None;
                }
                let pending = self.pending.get(&packet.message)?;
                if pending.attempts == 0 || packet.payload[PREFIX_SIZE..] != pending.fingerprint.to_be_bytes() {
                    self.metrics.invalid_responses += 1;
                    return None;
                }
                // 送信時刻をechoするため、再送が続いても対応する試行のRTTを学習できる。
                if pending.transmissions.matches(packet.sent_at, packet.path) && packet.sent_at <= now {
                    self.timing.observe(now - packet.sent_at);
                    self.fabric.feedback(Acknowledgement {
                        path: packet.path,
                        now,
                        rtt_us: now - packet.sent_at,
                        bytes: pending.wire_size(),
                        signal: packet.signal,
                        data: true,
                    });
                    self.metrics.rtt_samples = self.timing.samples;
                    self.metrics.smoothed_rtt_us = self.timing.smoothed_us;
                    self.metrics.retry_timeout_us = self.timing.timeout();
                }
                self.receive_base = self.receive_base.max(packet.credit);
                self.metrics.acknowledged += 1;
                let elapsed = now.saturating_sub(pending.submitted);
                self.latencies.record(elapsed);
                self.metrics.acknowledgement_total_us = self.metrics.acknowledgement_total_us.saturating_add(elapsed);
                self.metrics.acknowledgement_max_us = self.metrics.acknowledgement_max_us.max(elapsed);
                let bytes = pending.wire_size();
                self.fabric.release(pending.transmissions.last_path.expect("送信済み経路"), bytes);
                self.pending.remove(&packet.message);
                return Some(packet.message);
            },
            _ => self.metrics.invalid_responses += 1,
        }
        None
    }

    fn valid_response(&self, packet: &Packet) -> bool {
        packet.source == self.options.destination
            && packet.destination == self.options.source
            && packet.session == self.options.session
            && packet.class == self.options.class
            && self.domain == Some(packet.clock_domain)
            && self.options.paths.contains(&packet.path)
            && super::wire::validate(packet).is_ok()
            && Metadata::decode(packet).is_some_and(|metadata| metadata.channel == self.options.channel && metadata.ordering == self.options.ordering)
    }

    pub fn report(&self) -> serde_json::Value {
        let mut metrics = serde_json::json!(self.metrics);
        metrics["acknowledgement_p99_upper_us"] = self.latencies.p99_upper_us().into();
        serde_json::json!({ "source": self.options.source, "destination": self.options.destination, "channel": self.options.channel, "class": self.options.class, "metrics": metrics, "state": self.state, "fabric": self.fabric.report() })
    }

    fn fail(&mut self, state: ChannelState) {
        self.metrics.unconfirmed += self.pending.len() as u64;
        for pending in self.pending.values() {
            if let Some(path) = pending.transmissions.last_path {
                self.fabric.release(path, pending.wire_size());
            }
        }
        self.pending.clear();
        self.state = state;
    }
}
