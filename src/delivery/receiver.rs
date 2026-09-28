use super::{wire::Metadata, Ordering, DEFAULT_WINDOW, MAX_WINDOW, PREFIX_SIZE};
use crate::packet::{fingerprint, Class, Kind, Packet};
use crate::tokens::TokenBucket;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};

// sessionの再利用には新しい受信世代を要求する。古いDATAを新規配送しない。
const DEFAULT_IDLE_TIMEOUT_US: u64 = 30_000_000;
const DEFAULT_MAX_CHANNELS: usize = 128;

pub struct ReceiverOptions {
    pub window: usize,
    pub max_channels: usize,
    pub idle_timeout_us: u64,
    pub rates: [u64; 2],
}

impl Default for ReceiverOptions {
    fn default() -> Self {
        Self {
            window: DEFAULT_WINDOW,
            max_channels: DEFAULT_MAX_CHANNELS,
            idle_timeout_us: DEFAULT_IDLE_TIMEOUT_US,
            rates: [1000, 600],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Key {
    source: u32,
    session: u64,
    channel: u32,
}

struct Slot {
    fingerprint: u64,
    payload: Option<Vec<u8>>,
}

struct ReceiveChannel {
    class: Class,
    ordering: Ordering,
    epoch: u64,
    next: u64,
    last_seen: u64,
    slots: BTreeMap<u64, Slot>,
    ready: VecDeque<u64>,
}

#[derive(Default, Serialize)]
pub struct ReceiverMetrics {
    pub opened: u64,
    pub accepted: u64,
    pub delivered: [u64; 2],
    pub payload_bytes: [u64; 2],
    pub duplicates: u64,
    pub rejected: u64,
    pub window_full: u64,
    pub throttled: u64,
    pub channel_limit: u64,
    pub expired_channels: u64,
    pub abandoned_messages: u64,
    pub resets: u64,
    pub peak_buffered: usize,
}

#[derive(Debug, Serialize)]
pub struct DeliveredMessage {
    pub source: u32,
    pub session: u64,
    pub channel: u32,
    pub sequence: u64,
    pub class: Class,
    pub payload: Vec<u8>,
}

pub struct Receiver {
    options: ReceiverOptions,
    channels: HashMap<Key, ReceiveChannel>,
    next_epoch: u64,
    capacity: [TokenBucket; 2],
    pub metrics: ReceiverMetrics,
}

impl Receiver {
    pub fn new(options: ReceiverOptions, incarnation: u64) -> Result<Self, &'static str> {
        if options.window == 0
            || options.window > MAX_WINDOW
            || options.max_channels == 0
            || options.max_channels > DEFAULT_MAX_CHANNELS
            || options.idle_timeout_us < super::DEFAULT_TIMEOUT_US
            || incarnation == 0
            || incarnation == u64::MAX
        {
            return Err("受信窓/channel数/idle timeout/受信世代が範囲外です");
        }
        if options.rates.iter().any(|rate| *rate == 0 || *rate > 100_000) {
            return Err("受信レートが範囲外です");
        }
        let capacity = options.rates.map(|rate| TokenBucket::new(rate, options.window as u64, 0));
        Ok(Self {
            options,
            channels: HashMap::new(),
            next_epoch: incarnation,
            capacity,
            metrics: ReceiverMetrics::default(),
        })
    }

    pub fn receive(&mut self, packet: &Packet, now: u64) -> Option<Packet> {
        if !packet.is_reliable() || super::wire::validate(packet).is_err() {
            self.metrics.rejected += 1;
            return None;
        }
        let metadata = Metadata::decode(packet)?;
        let key = Key {
            source: packet.source,
            session: packet.session,
            channel: metadata.channel,
        };
        self.prune(now);
        if packet.kind == Kind::ReliableOpen {
            return self.open(packet, metadata, now);
        }
        if packet.kind != Kind::ReliableData {
            return None;
        }
        let Some(channel) = self.channels.get_mut(&key) else {
            return Some(self.reset(packet, metadata));
        };
        if channel.epoch != metadata.epoch {
            return Some(self.reset(packet, metadata));
        }
        if channel.class != packet.class || channel.ordering != metadata.ordering {
            self.metrics.rejected += 1;
            return None;
        }
        channel.last_seen = now;
        let body = &packet.payload[PREFIX_SIZE..];
        let hash = fingerprint(body);
        if packet.message < channel.next {
            // 解放済みのsequenceは本文履歴を持たず、再配送せずにACKだけ返す。
            self.metrics.duplicates += 1;
        } else if packet.message - channel.next >= self.options.window as u64 {
            self.metrics.window_full += 1;
            return None;
        } else if let Some(previous) = channel.slots.get(&packet.message) {
            if previous.fingerprint != hash {
                self.metrics.rejected += 1;
                return None;
            }
            self.metrics.duplicates += 1;
        } else {
            if !self.capacity[packet.class.index()].take(1, now) {
                self.metrics.throttled += 1;
                return None;
            }
            channel.slots.insert(
                packet.message,
                Slot {
                    fingerprint: hash,
                    payload: Some(body.to_vec()),
                },
            );
            if channel.ordering == Ordering::Unordered {
                channel.ready.push_back(packet.message);
            }
            self.metrics.accepted += 1;
            self.metrics.peak_buffered = self.metrics.peak_buffered.max(channel.slots.len());
        }
        let mut ack = packet.response(Kind::ReliableAck);
        ack.credit = channel.next;
        ack.payload = metadata.encode(&hash.to_be_bytes());
        Some(ack)
    }

    fn open(&mut self, packet: &Packet, metadata: Metadata, now: u64) -> Option<Packet> {
        let key = Key {
            source: packet.source,
            session: packet.session,
            channel: metadata.channel,
        };
        if !self.channels.contains_key(&key) {
            if metadata.epoch != 0 {
                return Some(self.reset(packet, metadata));
            }
            if self.channels.len() == self.options.max_channels || self.next_epoch == u64::MAX {
                self.metrics.channel_limit += 1;
                return None;
            }
            let epoch = self.next_epoch;
            self.next_epoch += 1;
            self.channels.insert(
                key,
                ReceiveChannel {
                    class: packet.class,
                    ordering: metadata.ordering,
                    epoch,
                    next: 1,
                    last_seen: now,
                    slots: BTreeMap::new(),
                    ready: VecDeque::new(),
                },
            );
            self.metrics.opened += 1;
        }
        let channel = self.channels.get_mut(&key)?;
        if (metadata.epoch != 0 && metadata.epoch != channel.epoch) || channel.ordering != metadata.ordering || channel.class != packet.class {
            if metadata.epoch != 0 {
                return Some(self.reset(packet, metadata));
            }
            self.metrics.rejected += 1;
            return None;
        }
        channel.last_seen = now;
        let mut ready = packet.response(Kind::ReliableReady);
        ready.message = channel.next;
        ready.credit = self.options.window as u64;
        ready.payload = Metadata { epoch: channel.epoch, ..metadata }.encode(&[]);
        Some(ready)
    }

    fn reset(&mut self, packet: &Packet, metadata: Metadata) -> Packet {
        self.metrics.resets += 1;
        let mut response = packet.response(Kind::ReliableReset);
        response.credit = 0;
        response.payload = metadata.encode(&[]);
        response
    }

    /// 受信アプリが読み取った時点で窓を解放する。読まなければ送信側へ待ちが伝わる。
    pub fn take_delivery(&mut self) -> Option<DeliveredMessage> {
        for (key, channel) in &mut self.channels {
            let sequence = match channel.ordering {
                Ordering::Ordered => channel.next,
                Ordering::Unordered => match channel.ready.front() {
                    Some(sequence) => *sequence,
                    None => continue,
                },
            };
            let Some(payload) = channel.slots.get_mut(&sequence).and_then(|slot| slot.payload.take()) else {
                continue;
            };
            if channel.ordering == Ordering::Unordered {
                channel.ready.pop_front();
            }
            while channel.slots.get(&channel.next).is_some_and(|slot| slot.payload.is_none()) {
                channel.slots.remove(&channel.next);
                channel.next += 1;
            }
            self.metrics.delivered[channel.class.index()] += 1;
            self.metrics.payload_bytes[channel.class.index()] += payload.len() as u64;
            return Some(DeliveredMessage {
                source: key.source,
                session: key.session,
                channel: key.channel,
                sequence,
                class: channel.class,
                payload,
            });
        }
        None
    }

    /// 同じイベント処理でアプリが読んだ分も、送信するACKへ反映する。
    pub fn refresh_ack(&self, ack: &mut Packet) {
        if ack.kind != Kind::ReliableAck {
            return;
        }
        let Some(metadata) = Metadata::decode(ack) else { return };
        let key = Key {
            source: ack.destination,
            session: ack.session,
            channel: metadata.channel,
        };
        if let Some(channel) = self.channels.get(&key).filter(|channel| channel.epoch == metadata.epoch) {
            ack.credit = channel.next;
        }
    }

    pub fn prune(&mut self, now: u64) {
        self.channels.retain(|_, channel| {
            if now.saturating_sub(channel.last_seen) < self.options.idle_timeout_us {
                return true;
            }
            self.metrics.expired_channels += 1;
            self.metrics.abandoned_messages += channel.slots.values().filter(|slot| slot.payload.is_some()).count() as u64;
            false
        });
    }
}
