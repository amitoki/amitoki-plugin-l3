//! LinuxのUDP/IPを通す比較用端点。L3固有のcredit・再送・期限破棄は行わない。
mod options;
mod transport;
mod wire;
pub use options::{Endpoint, Load};
use transport::Transport;

use crate::{
    measurement::{traffic_report, TrafficMetrics},
    packet::fingerprint,
    runtime::shutdown,
};
use std::{collections::HashMap, io, path::PathBuf};
use wire::{Message, MAX_DATAGRAM};

// L3ベンチと同じ生成・受信バーストと、未応答の保持上限。
const BURST: usize = 32;
const MAX_PENDING: usize = 16_384;
const PRUNE_US: u64 = 5_000;

pub fn receive(endpoint: Endpoint, ready: PathBuf) -> io::Result<()> {
    let mut transport = Transport::open(&endpoint)?;
    let start = transport.clock.now();
    let end = start + endpoint.duration_ms * 1000;
    write(&ready, &serde_json::json!({"ready":true}))?;
    let mut delivered = [0_u64; 2];
    // 1バイト余分に受け、最大長で切り詰められたdatagramも検出する。
    let mut bytes = [0; MAX_DATAGRAM + 1];
    while transport.clock.now() < end && !shutdown::requested() {
        for _ in 0..BURST {
            let Some(count) = transport.receive(&mut bytes)? else { break };
            let Some(message) = Message::decode(&bytes[..count]).filter(|message| !message.acknowledgment) else {
                transport.malformed += 1;
                continue;
            };
            delivered[message.class] += 1;
            let reply = Message {
                acknowledgment: true,
                payload: &[],
                ..message
            }
            .encode();
            transport.send(&reply)?;
        }
        transport.wait(end)?;
    }
    let mut report = transport.report();
    report["receiver"] = serde_json::json!({"delivered":delivered});
    write(&endpoint.output, &report)
}

struct Pending {
    class: usize,
    started: u64,
    expires: u64,
    hash: u64,
}

struct Client {
    transport: Transport,
    session: u64,
    pending: HashMap<u64, Pending>,
    traffic: [TrafficMetrics; 2],
    invalid_acks: u64,
}

impl Client {
    fn receive(&mut self) -> io::Result<()> {
        let mut bytes = [0; MAX_DATAGRAM + 1];
        for _ in 0..BURST {
            let Some(count) = self.transport.receive(&mut bytes)? else { break };
            let Some(reply) = Message::decode(&bytes[..count]).filter(|reply| reply.acknowledgment && reply.session == self.session) else {
                self.invalid_acks += 1;
                continue;
            };
            let Some(pending) = self.pending.get(&reply.id) else { continue };
            if pending.hash != reply.hash || pending.class != reply.class {
                self.invalid_acks += 1;
                continue;
            }
            let now = self.transport.clock.now();
            if now < pending.expires {
                self.traffic[pending.class].acknowledged += 1;
                self.traffic[pending.class].latencies.push(now - pending.started);
            }
            self.pending.remove(&reply.id);
        }
        Ok(())
    }

    fn offer(&mut self, id: u64, pending: Pending, payload: &[u8]) -> io::Result<()> {
        let traffic = &mut self.traffic[pending.class];
        traffic.offered += 1;
        if self.transport.clock.now() >= pending.expires {
            traffic.expired_before_send += 1;
            return Ok(());
        }
        if self.pending.len() == MAX_PENDING {
            traffic.pending_full += 1;
            return Ok(());
        }
        let message = Message {
            acknowledgment: false,
            class: pending.class,
            session: self.session,
            id,
            hash: pending.hash,
            payload,
        }
        .encode();
        if self.transport.send(&message)? {
            traffic.sent += 1;
            self.pending.insert(id, pending);
        }
        Ok(())
    }
}

pub fn benchmark(load: Load) -> io::Result<()> {
    let rates = [load.short_rate, load.bulk_rate];
    if rates == [0, 0] {
        return Err(io::Error::other("少なくとも1クラスのrateを指定してください"));
    }
    let payloads = [vec![0x5a; load.short_bytes as usize], vec![0x5a; load.bulk_bytes as usize]];
    let deadlines = [load.short_deadline_us, load.bulk_deadline_us];
    let transport = Transport::open(&load.endpoint)?;
    let start = transport.clock.now();
    let duration = load.endpoint.duration_ms * 1000;
    let finish = start + duration + *deadlines.iter().max().expect("2クラス");
    let mut client = Client {
        transport,
        session: u64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().expect("UUID")),
        pending: HashMap::new(),
        traffic: Default::default(),
        invalid_acks: 0,
    };
    let counts = rates.map(|rate| duration * rate / 1_000_000);
    let mut generated = [0_u64; 2];
    let mut id = 0;
    let mut prune_at = start;
    while client.transport.clock.now() < finish {
        if shutdown::requested() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "試験を停止しました"));
        }
        client.receive()?;
        let now = client.transport.clock.now();
        if now >= prune_at {
            client.pending.retain(|_, pending| pending.expires > now);
            prune_at = now + PRUNE_US;
        }
        for class in 0..2 {
            for _ in 0..BURST {
                if generated[class] >= counts[class] {
                    break;
                }
                let due = start + generated[class] * 1_000_000 / rates[class];
                if due > client.transport.clock.now() {
                    break;
                }
                id += 1;
                client.offer(
                    id,
                    Pending {
                        class,
                        started: due,
                        expires: due + deadlines[class],
                        hash: fingerprint(&payloads[class]),
                    },
                    &payloads[class],
                )?;
                generated[class] += 1;
            }
        }
        let next = (0..2).filter(|index| generated[*index] < counts[*index]).map(|index| start + generated[index] * 1_000_000 / rates[index]).min().unwrap_or(finish);
        client.transport.wait(next.min(prune_at))?;
    }
    for (index, count) in counts.iter().enumerate() {
        let missing = count - client.traffic[index].offered;
        client.traffic[index].offered += missing;
        client.traffic[index].expired_before_send += missing;
    }
    let mut report = client.transport.report();
    report["benchmark"] = serde_json::json!({ "duration_us":duration, "elapsed_us":client.transport.clock.now() - start, "invalid_acks":client.invalid_acks,
        "short":traffic_report(&client.traffic[0]), "bulk":traffic_report(&client.traffic[1]) });
    write(&load.endpoint.output, &report)
}

fn write(path: &std::path::Path, report: &serde_json::Value) -> io::Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(report).map_err(io::Error::other)?)
}
