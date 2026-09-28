use crate::{
    packet::{Class, Kind, Packet},
    tokens::TokenBucket,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

// 短文だけで通常の転送が停止しないよう、最大8件ごとにbulkへ機会を与える。
const SHORT_BURST: usize = 8;
const CONTROL_BURST: usize = 4;
const CONTROL_CAPACITY: usize = 32;
const SHORT_CAPACITY: usize = 32;
const BULK_CAPACITY: usize = 128;
pub const QUEUE_CAPACITY: usize = CONTROL_CAPACITY + SHORT_CAPACITY + BULK_CAPACITY;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scheduling {
    Fifo,
    Priority,
}

#[derive(Default, Serialize)]
pub struct QueueMetrics {
    pub expired: u64,
    pub full: u64,
    pub dequeued: u64,
    pub scheduled_bytes: u64,
    pub peak: usize,
}

pub struct Scheduler {
    mode: Scheduling,
    packets: VecDeque<Packet>,
    control_run: usize,
    short_run: usize,
    bandwidth: TokenBucket,
    pub metrics: QueueMetrics,
}

impl Scheduler {
    pub fn new(mode: Scheduling, bytes_per_second: u64, now: u64) -> Self {
        // バーストは最大フレーム1件に抑え、キュー制御の比較を帯域設定に合わせる。
        Self {
            mode,
            packets: VecDeque::new(),
            control_run: 0,
            short_run: 0,
            bandwidth: TokenBucket::new(bytes_per_second, crate::packet::MAX_FRAME as u64, now),
            metrics: QueueMetrics::default(),
        }
    }

    pub fn enqueue(&mut self, packet: Packet, now: u64) -> bool {
        self.expire(now);
        if packet.expires <= now {
            self.metrics.expired += 1;
            return false;
        }
        let class = queue_class(&packet);
        let limit = [CONTROL_CAPACITY, SHORT_CAPACITY, BULK_CAPACITY][class];
        if self.packets.len() == QUEUE_CAPACITY || (self.mode == Scheduling::Priority && self.packets.iter().filter(|queued| queue_class(queued) == class).count() >= limit) {
            self.metrics.full += 1;
            return false;
        }
        self.packets.push_back(packet);
        self.metrics.peak = self.metrics.peak.max(self.packets.len());
        true
    }

    fn expire(&mut self, now: u64) {
        let count = self.packets.len();
        self.packets.retain(|packet| packet.expires > now);
        self.metrics.expired += (count - self.packets.len()) as u64;
    }

    pub fn pop(&mut self, now: u64) -> Option<Packet> {
        self.expire(now);
        let index = if self.mode == Scheduling::Fifo { 0 } else { self.select()? };
        let size = self.packets.get(index)?.wire_size() as u64;
        if !self.bandwidth.take(size, now) {
            return None;
        }
        let packet = self.packets.remove(index)?;
        match queue_class(&packet) {
            0 => self.control_run += 1,
            1 => {
                self.control_run = 0;
                self.short_run += 1;
            },
            _ => {
                self.control_run = 0;
                self.short_run = 0;
            },
        }
        self.metrics.dequeued += 1;
        self.metrics.scheduled_bytes += size;
        Some(packet)
    }

    fn select(&self) -> Option<usize> {
        let control = self.packets.iter().position(|packet| queue_class(packet) == 0);
        let short = self.packets.iter().enumerate().filter(|(_, packet)| queue_class(packet) == 1).min_by_key(|(_, packet)| packet.expires).map(|(index, _)| index);
        let bulk = self.packets.iter().position(|packet| queue_class(packet) == 2);
        if control.is_some() && (self.control_run < CONTROL_BURST || (short.is_none() && bulk.is_none())) {
            return control;
        }
        if bulk.is_some() && (self.short_run >= SHORT_BURST || short.is_none()) {
            return bulk;
        }
        short.or(bulk).or(control)
    }

    pub fn pending(&self) -> bool {
        !self.packets.is_empty()
    }
}

fn queue_class(packet: &Packet) -> usize {
    if packet.kind != Kind::Data {
        0
    } else if packet.class == Class::Short {
        1
    } else {
        2
    }
}
