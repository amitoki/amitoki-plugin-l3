use super::{write_json, Network};
use crate::measurement::{traffic_report, TrafficMetrics};
use crate::{
    config::{Config, MAX_BYTES_PER_SECOND},
    credit::Allowance,
    packet::{fingerprint, Class, Kind, Packet, INITIAL_HOPS, MAX_FRAME, MAX_LIFETIME_US, MAX_PAYLOAD, MAX_SHORT_PAYLOAD, REPLICA},
    sync::Reading,
    tokens::TokenBucket,
};
use std::{
    collections::{HashMap, VecDeque},
    io,
    path::PathBuf,
};

// 制御要求の再送を抑え、credit不足時に制御パケットで混雑を増やさない。
const REQUEST_RETRY_US: u64 = 5_000;
const SETUP_TIMEOUT_US: u64 = 2_000_000;
const GENERATION_BURST: usize = 32;
const MAX_PENDING: usize = 16_384;
const REQUEST_LIFETIME_US: u64 = 200_000;
const RETRY_INTERVAL_US: u64 = 5_000;
// 生成負荷と保持する計測値を有限にし、試験を短時間で止められる範囲にする。
const MAX_DURATION_US: u64 = 60_000_000;
const MAX_MESSAGES_PER_SECOND: u64 = 10_000;
const MAX_RETRIES: u8 = 3;
// 枠の往復を隠しつつ、未使用GRANTを溜めすぎない。
const MAX_ALLOWANCES: usize = 2;
const REFILL_THRESHOLD: u32 = 8;

pub struct Benchmark {
    pub config: Config,
    pub peer: u32,
    pub duration_us: u64,
    pub rates: [u64; 2],
    pub sizes: [usize; 2],
    pub deadlines: [u64; 2],
    pub paths: Vec<u8>,
    pub replica_bytes_per_second: u64,
    pub retries: u8,
    pub output: PathBuf,
}

struct Pending {
    packet: Packet,
    started: u64,
    hash: u64,
    retry_at: u64,
    retries: u8,
    local_deadline: u64,
}
struct Request {
    message: u64,
    sent: u64,
}

struct Client {
    network: Network,
    peer: u32,
    session: u64,
    next_message: u64,
    paths: Vec<u8>,
    allowances: [VecDeque<Allowance>; 2],
    requests: [Option<Request>; 2],
    pending: HashMap<u64, Pending>,
    traffic: [TrafficMetrics; 2],
    duplicate_budget: TokenBucket,
    replicas: u64,
    replica_suppressed: u64,
    retries: u64,
    redundant_bytes: u64,
    duplicate_acks: u64,
    invalid_acks: u64,
    clock_domain: Option<u64>,
}

impl Client {
    fn packet(&mut self, class: Class, time: Reading) -> Option<Packet> {
        let expires = time.deadline(time.local.checked_add(REQUEST_LIFETIME_US)?)?;
        let message = self.next_message;
        self.next_message += 1;
        Some(Packet {
            kind: Kind::Request,
            class,
            hops: INITIAL_HOPS,
            source: self.network.node,
            destination: self.peer,
            session: self.session,
            message,
            expires,
            clock_domain: time.domain,
            credit: 0,
            path: self.paths[0],
            flags: 0,
            sent_at: 0,
            signal: Default::default(),
            payload: Vec::new(),
        })
    }

    fn receive(&mut self) -> io::Result<()> {
        let packets = self.network.receive()?;
        if let Some(time) = self.network.time() {
            if self.clock_domain != Some(time.domain) {
                // 基準ノードの再起動前の枠・ACKを、新しい時計の世代へ持ち越さない。
                self.allowances = Default::default();
                self.requests = Default::default();
                self.pending.clear();
                self.clock_domain = Some(time.domain);
            }
        }
        for packet in packets {
            if packet.destination != self.network.node || packet.source != self.peer || packet.session != self.session {
                self.invalid_acks += 1;
                continue;
            }
            let index = packet.class.index();
            match packet.kind {
                Kind::Grant => {
                    if self.requests[index].as_ref().is_some_and(|request| request.message == packet.message) {
                        if let Some(allowance) = Allowance::from_grant(&packet) {
                            self.allowances[index].push_back(allowance);
                            self.requests[index] = None;
                        }
                    }
                },
                Kind::Ack => {
                    let Some(pending) = self.pending.get(&packet.message) else {
                        self.duplicate_acks += 1;
                        continue;
                    };
                    if packet.credit != pending.packet.credit
                        || packet.class != pending.packet.class
                        || packet.expires != pending.packet.expires
                        || packet.payload != pending.hash.to_be_bytes()
                    {
                        self.invalid_acks += 1;
                        continue;
                    }
                    let now = self.network.clock.now();
                    if now < pending.local_deadline {
                        self.traffic[index].acknowledged += 1;
                        self.traffic[index].latencies.push(now - pending.started);
                    }
                    self.pending.remove(&packet.message);
                },
                _ => self.invalid_acks += 1,
            }
        }
        Ok(())
    }

    fn replenish(&mut self, deadlines: [u64; 2], enabled: [bool; 2]) {
        let Some(time) = self.network.time() else {
            return;
        };
        let now = time.local;
        for class in [Class::Short, Class::Bulk] {
            let index = class.index();
            if !enabled[index] {
                continue;
            }
            let Some(required_deadline) = time.deadline(now + deadlines[index]) else {
                continue;
            };
            self.allowances[index].retain(|allowance| allowance.used < allowance.count && allowance.expires >= required_deadline);
            if self.allowances[index].len() >= MAX_ALLOWANCES {
                continue;
            }
            let remaining: u32 = self.allowances[index].iter().map(|allowance| allowance.count - allowance.used).sum();
            // 残り8件の段階で補充し、個々のメッセージをcredit往復待ちにしない。
            if remaining > REFILL_THRESHOLD {
                continue;
            }
            if self.requests[index].as_ref().is_some_and(|request| now - request.sent < REQUEST_RETRY_US) {
                continue;
            }
            let Some(mut packet) = self.packet(class, time) else {
                continue;
            };
            if let Some(request) = &self.requests[index] {
                packet.message = request.message;
            }
            self.requests[index] = Some(Request {
                message: packet.message,
                sent: now,
            });
            // 経路断でcredit交換だけが永久停止しないよう、設定した経路へ送る。
            for path in &self.paths {
                packet.path = *path;
                self.network.enqueue(packet.clone());
            }
        }
    }

    fn offer(&mut self, offer: Offer) {
        let index = offer.class.index();
        self.traffic[index].offered += 1;
        let now = self.network.clock.now();
        let expires = offer.due + offer.lifetime;
        if expires <= now {
            self.traffic[index].expired_before_send += 1;
            return;
        }
        if self.pending.len() >= MAX_PENDING {
            self.traffic[index].pending_full += 1;
            return;
        }
        let Some(time) = self.network.time() else {
            self.traffic[index].unsynchronized += 1;
            return;
        };
        let Some(mut packet) = self.packet(offer.class, time) else {
            return;
        };
        let Some(wire_deadline) = time.deadline(expires).filter(|deadline| *deadline > time.latest) else {
            self.traffic[index].unsynchronized += 1;
            return;
        };
        let Some(credit) = self.allowances[index].iter_mut().find_map(|allowance| allowance.take(wire_deadline)) else {
            self.traffic[index].no_credit += 1;
            return;
        };
        packet.kind = Kind::Data;
        packet.expires = wire_deadline;
        packet.credit = credit;
        packet.payload = (0..offer.size).map(|offset| (packet.message.wrapping_add(offset as u64) & 0xff) as u8).collect();
        self.network.enqueue(packet.clone());
        self.traffic[index].sent += 1;
        if offer.class == Class::Short && self.paths.len() == 2 {
            if self.duplicate_budget.take(packet.wire_size() as u64, now) {
                let mut replica = packet.clone();
                replica.path = self.paths[1];
                replica.flags = REPLICA;
                self.replicas += 1;
                self.redundant_bytes += replica.wire_size() as u64;
                self.network.enqueue(replica);
            } else {
                self.replica_suppressed += 1;
            }
        }
        self.pending.insert(
            packet.message,
            Pending {
                hash: fingerprint(&packet.payload),
                packet,
                started: offer.due,
                retry_at: now + RETRY_INTERVAL_US,
                retries: offer.retries,
                local_deadline: expires,
            },
        );
    }

    fn retry(&mut self) {
        let now = self.network.clock.now();
        self.pending.retain(|_, pending| pending.local_deadline > now);
        if self.network.time().is_none() {
            return;
        }
        for pending in self.pending.values_mut() {
            if pending.retries == 0 || pending.retry_at > now {
                continue;
            }
            pending.retry_at = now + RETRY_INTERVAL_US;
            if !self.duplicate_budget.take(pending.packet.wire_size() as u64, now) {
                continue;
            }
            let mut packet = pending.packet.clone();
            // 再送と複製は同じID・credit・期限を維持する。
            packet.flags = REPLICA;
            pending.retries -= 1;
            self.retries += 1;
            self.redundant_bytes += packet.wire_size() as u64;
            self.network.enqueue(packet);
        }
    }
}

struct Offer {
    class: Class,
    due: u64,
    lifetime: u64,
    size: usize,
    retries: u8,
}

pub fn run_benchmark(options: Benchmark) -> io::Result<()> {
    if options.paths.len() > 2 {
        return Err(io::Error::other("deadlineモードの経路は最大2本です"));
    }
    validate(&options).map_err(io::Error::other)?;
    let network = Network::open(options.config)?;
    let setup_start = network.clock.now();
    let session = u64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().expect("UUIDの先頭8バイト")).max(1);
    let mut client = Client {
        network,
        peer: options.peer,
        session,
        next_message: 1,
        paths: options.paths,
        allowances: Default::default(),
        requests: Default::default(),
        pending: HashMap::new(),
        traffic: Default::default(),
        duplicate_budget: TokenBucket::new(
            options.replica_bytes_per_second,
            if options.replica_bytes_per_second == 0 { 0 } else { MAX_FRAME as u64 * 2 },
            setup_start,
        ),
        replicas: 0,
        replica_suppressed: 0,
        retries: 0,
        redundant_bytes: 0,
        duplicate_acks: 0,
        invalid_acks: 0,
        clock_domain: None,
    };
    let enabled = options.rates.map(|rate| rate != 0);
    loop {
        if super::shutdown::requested() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "試験を停止しました"));
        }
        client.receive()?;
        client.replenish(options.deadlines, enabled);
        client.network.flush()?;
        if (0..2).all(|index| !enabled[index] || !client.allowances[index].is_empty()) {
            break;
        }
        if client.network.clock.now() - setup_start >= SETUP_TIMEOUT_US {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "受信側からcreditを取得できません"));
        }
        client.network.wait(client.network.clock.now() + REQUEST_RETRY_US)?;
    }
    let start = client.network.clock.now();
    let end = start + options.duration_us;
    let finish = end + *options.deadlines.iter().max().expect("2クラス");
    let mut generated = [0_u64; 2];
    let counts = options.rates.map(|rate| options.duration_us * rate / 1_000_000);
    while client.network.clock.now() < finish {
        if super::shutdown::requested() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "試験を停止しました"));
        }
        client.receive()?;
        if client.network.clock.now() < end {
            client.replenish(options.deadlines, enabled);
        }
        for class in [Class::Short, Class::Bulk] {
            let index = class.index();
            if !enabled[index] {
                continue;
            }
            for _ in 0..GENERATION_BURST {
                if generated[index] >= counts[index] {
                    break;
                }
                let due = start + generated[index] * 1_000_000 / options.rates[index];
                if due > client.network.clock.now() {
                    break;
                }
                client.offer(Offer {
                    class,
                    due,
                    lifetime: options.deadlines[index],
                    size: options.sizes[index],
                    retries: options.retries,
                });
                generated[index] += 1;
            }
        }
        client.retry();
        client.network.flush()?;
        let next = (0..2).filter(|index| generated[*index] < counts[*index]).map(|index| start + generated[index] * 1_000_000 / options.rates[index]).min().unwrap_or(finish);
        client.network.wait(next.min(client.network.clock.now() + REQUEST_RETRY_US))?;
    }
    // OSの停止時間が試験時間を超えても、未生成分を成功率の分母から消さない。
    for (index, count) in counts.iter().enumerate() {
        let missing = count - client.traffic[index].offered;
        client.traffic[index].offered += missing;
        client.traffic[index].expired_before_send += missing;
    }
    let mut report = client.network.report();
    report["benchmark"] = serde_json::json!({
        "duration_us": options.duration_us,
        "elapsed_us": client.network.clock.now() - start,
        "setup_us": start - setup_start,
        "paths": client.paths,
        "replica_bytes_per_second": options.replica_bytes_per_second,
        "replicas": client.replicas,
        "replica_suppressed": client.replica_suppressed,
        "retries": client.retries,
        "redundant_bytes": client.redundant_bytes,
        "duplicate_acks": client.duplicate_acks,
        "invalid_acks": client.invalid_acks,
        "pending": client.pending.len(),
        "short": traffic_report(&client.traffic[0]),
        "bulk": traffic_report(&client.traffic[1])
    });
    write_json(&options.output, &report)
}

pub(super) fn validate(options: &Benchmark) -> Result<(), &'static str> {
    if options.peer == 0
        || options.peer == options.config.node
        || options.duration_us == 0
        || options.duration_us > MAX_DURATION_US
        || options.rates == [0, 0]
        || options.rates.iter().any(|rate| *rate > MAX_MESSAGES_PER_SECOND)
        || options.retries > MAX_RETRIES
    {
        return Err("peer/duration/rate/retriesが範囲外です");
    }
    if options.sizes[0] > MAX_SHORT_PAYLOAD
        || options.sizes.iter().any(|size| *size == 0 || *size > MAX_PAYLOAD)
        || options.deadlines.iter().any(|deadline| *deadline == 0 || *deadline > MAX_LIFETIME_US / 2)
    {
        return Err("payloadまたはdeadlineが範囲外です");
    }
    if options.paths.is_empty()
        || options.paths.len() > crate::fabric::MAX_PATHS
        || options.paths.contains(&0)
        || (options.paths.iter().collect::<std::collections::BTreeSet<_>>().len() != options.paths.len())
        || options.paths.iter().any(|path| !options.config.routes.iter().any(|route| route.destination == options.peer && route.path == *path))
    {
        return Err("1〜8本の異なる、設定済み経路を指定してください");
    }
    if options.replica_bytes_per_second > MAX_BYTES_PER_SECOND {
        return Err("複製帯域が範囲外です");
    }
    Ok(())
}
