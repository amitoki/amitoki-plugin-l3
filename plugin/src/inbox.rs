//! 未ACKのフレームと再試行履歴を有界メモリに保持する。
use crate::codec::{self, DigestBytes, Fragment, CHUNK_SIZE};
use amitoki_l3_lab::{delivery::DeliveredMessage, runtime::MessageSink};
use amitoki_plugin_sdk::relay::{Delivery, Frame, Receipt};
use bytes::Bytes;
use std::{
    collections::{HashMap, VecDeque},
    io,
    time::{Duration, Instant},
};
use uuid::Uuid;

// 再試行期間中は履歴を追い出さない。満杯なら新規受付にバックプレッシャーをかける。
const HISTORY_CAPACITY: usize = 65_536;
const RETENTION: Duration = Duration::from_secs(60);
// 全履歴の走査をパケットごとに繰り返さない。
const PRUNE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RESERVED_BYTES: usize = 16 * 1024 * 1024;

struct Assembly {
    hash: DigestBytes,
    bytes: Vec<u8>,
    chunks: Vec<bool>,
    remaining: usize,
    updated: Instant,
    receipt: Receipt,
}

struct History {
    hash: DigestBytes,
    total: usize,
    until: Instant,
}

pub struct Inbox {
    channel: DigestBytes,
    capacity: usize,
    prune_at: Instant,
    reserved_bytes: usize,
    pending: HashMap<Uuid, Assembly>,
    ready: VecDeque<Uuid>,
    history: HashMap<Uuid, History>,
}

impl Inbox {
    pub fn new(channel: DigestBytes, capacity: usize) -> Self {
        Self {
            channel,
            capacity,
            prune_at: Instant::now(),
            reserved_bytes: 0,
            pending: HashMap::new(),
            ready: VecDeque::new(),
            history: HashMap::new(),
        }
    }

    fn prune(&mut self, now: Instant) {
        if now < self.prune_at {
            return;
        }
        // 毎パケット履歴全件を走査せず、期限回収を1秒ごとにまとめる。
        self.prune_at = now + PRUNE_INTERVAL;
        self.history.retain(|_, entry| entry.until > now);
        self.pending.retain(|_, entry| {
            if entry.remaining != 0 && now.duration_since(entry.updated) >= RETENTION {
                self.reserved_bytes -= entry.bytes.len();
                false
            } else {
                true
            }
        });
    }

    fn reserve(&mut self, fragment: &Fragment<'_>) -> bool {
        let now = Instant::now();
        self.prune(now);
        if let Some(entry) = self.history.get(&fragment.id) {
            return entry.hash == fragment.digest && entry.total == fragment.total;
        }
        if let Some(entry) = self.pending.get(&fragment.id) {
            return entry.hash == fragment.digest
                && entry.bytes.len() == fragment.total
                && (!entry.chunks[fragment.offset / CHUNK_SIZE] || entry.bytes[fragment.offset..fragment.offset + fragment.body.len()] == *fragment.body);
        }
        if self.pending.len() >= self.capacity || self.reserved_bytes + fragment.total > MAX_RESERVED_BYTES || self.pending.len() + self.history.len() >= HISTORY_CAPACITY {
            return false;
        }
        let chunks = fragment.total.div_ceil(CHUNK_SIZE);
        self.pending.insert(
            fragment.id,
            Assembly {
                hash: fragment.digest,
                bytes: vec![0; fragment.total],
                chunks: vec![false; chunks],
                remaining: chunks,
                updated: now,
                receipt: Receipt(format!("{}:{}", fragment.id, Uuid::new_v4())),
            },
        );
        self.reserved_bytes += fragment.total;
        true
    }

    pub fn receive(&self, limit: usize) -> Vec<Delivery> {
        self.ready
            .iter()
            .take(limit)
            .filter_map(|id| {
                self.pending.get(id).map(|entry| Delivery {
                    frame: Frame {
                        id: *id,
                        bytes: Bytes::copy_from_slice(&entry.bytes),
                    },
                    receipt: entry.receipt.clone(),
                })
            })
            .collect()
    }

    pub fn acknowledge(&mut self, receipts: &[Receipt]) {
        for receipt in receipts {
            let Some((id, _)) = receipt.0.split_once(':') else { continue };
            let Ok(id) = Uuid::parse_str(id) else { continue };
            if !self.pending.get(&id).is_some_and(|entry| entry.remaining == 0 && entry.receipt == *receipt) {
                continue;
            }
            let entry = self.pending.remove(&id).expect("照合済みの受領情報");
            self.reserved_bytes -= entry.bytes.len();
            self.history.insert(
                id,
                History {
                    hash: entry.hash,
                    total: entry.bytes.len(),
                    until: Instant::now() + RETENTION,
                },
            );
            self.ready.retain(|candidate| *candidate != id);
        }
    }
}

impl MessageSink for Inbox {
    fn admit(&mut self, _source: u32, payload: &[u8]) -> bool {
        let Some(fragment) = codec::decode(payload, &self.channel) else { return false };
        self.reserve(&fragment)
    }

    fn deliver(&mut self, message: DeliveredMessage) -> io::Result<()> {
        let fragment = codec::decode(&message.payload, &self.channel).ok_or_else(|| io::Error::other("フレーム断片が不正です"))?;
        if self.history.contains_key(&fragment.id) {
            return Ok(());
        }
        let entry = self.pending.get_mut(&fragment.id).ok_or_else(|| io::Error::other("フレームの受信領域が予約されていません"))?;
        let chunk = fragment.offset / CHUNK_SIZE;
        if !entry.chunks[chunk] {
            entry.bytes[fragment.offset..fragment.offset + fragment.body.len()].copy_from_slice(fragment.body);
            entry.chunks[chunk] = true;
            entry.remaining -= 1;
            entry.updated = Instant::now();
            if entry.remaining == 0 {
                if codec::digest(&entry.bytes) != entry.hash {
                    return Err(io::Error::other("再構成したフレームのハッシュが一致しません"));
                }
                self.ready.push_back(fragment.id);
            }
        }
        Ok(())
    }
}
