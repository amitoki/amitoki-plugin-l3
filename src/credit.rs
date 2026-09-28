use crate::{
    packet::{credit_parts, credit_token, fingerprint, Class, Kind, Packet, MAX_LIFETIME_US},
    sync::Reading,
    tokens::TokenBucket,
};
use serde::Serialize;
use std::collections::HashMap;

// 生存中の重複履歴は追い出さない。容量超過時は新規配送を拒否する。
pub const MAX_GRANTS: usize = 256;
pub const MAX_DELIVERIES: usize = 8192;
pub const GRANT_BATCH: u32 = 32;

struct Grant {
    source: u32,
    session: u64,
    request: u64,
    class: Class,
    expires: u64,
    slots: Vec<Option<u64>>,
}

struct Delivered {
    fingerprint: u64,
    expires: u64,
    credit: u64,
    class: Class,
}

#[derive(Default, Serialize)]
pub struct ReceiverMetrics {
    pub requests: u64,
    pub grants: u64,
    pub granted_slots: [u64; 2],
    pub throttled: u64,
    pub delivered: [u64; 2],
    pub payload_bytes: [u64; 2],
    pub duplicates: u64,
    pub expired: u64,
    pub rejected: u64,
}

pub struct Receiver {
    grants: HashMap<u32, Grant>,
    delivered: HashMap<(u32, u64, u64), Delivered>,
    next_grant: u32,
    capacity: [TokenBucket; 2],
    pub metrics: ReceiverMetrics,
}

impl Receiver {
    pub fn new(rates: [u64; 2], now: u64) -> Self {
        Self {
            grants: HashMap::new(),
            delivered: HashMap::new(),
            next_grant: 1,
            capacity: rates.map(|rate| TokenBucket::new(rate, u64::from(GRANT_BATCH), now)),
            metrics: ReceiverMetrics::default(),
        }
    }

    pub fn prune(&mut self, now: u64) {
        self.grants.retain(|_, grant| grant.expires > now);
        self.delivered.retain(|_, message| message.expires > now);
    }

    pub fn grant(&mut self, request: &Packet, now: u64) -> Option<Packet> {
        self.grant_at(request, Reading::exact(now, request.clock_domain))
    }

    pub fn grant_at(&mut self, request: &Packet, time: Reading) -> Option<Packet> {
        self.metrics.requests += 1;
        if request.kind != Kind::Request || request.expires <= time.latest || request.clock_domain != time.domain {
            return None;
        }
        self.prune(time.earliest);
        let existing =
            self.grants.iter().find(|(_, grant)| (grant.source, grant.session, grant.request, grant.class) == (request.source, request.session, request.message, request.class));
        let (id, count, expires) = if let Some((id, grant)) = existing {
            (*id, grant.slots.len() as u32, grant.expires)
        } else {
            if self.grants.len() >= MAX_GRANTS || self.next_grant == u32::MAX {
                self.metrics.throttled += 1;
                return None;
            }
            let count = self.capacity[request.class.index()].take_up_to(u64::from(GRANT_BATCH), time.local) as u32;
            if count == 0 {
                self.metrics.throttled += 1;
                return None;
            }
            let id = self.next_grant;
            self.next_grant += 1;
            let expires = time.deadline(time.local.checked_add(MAX_LIFETIME_US)?)?;
            self.grants.insert(
                id,
                Grant {
                    source: request.source,
                    session: request.session,
                    request: request.message,
                    class: request.class,
                    expires,
                    slots: vec![None; count as usize],
                },
            );
            self.metrics.grants += 1;
            self.metrics.granted_slots[request.class.index()] += u64::from(count);
            (id, count, expires)
        };
        let mut response = request.response(Kind::Grant);
        response.credit = credit_token(id, count);
        response.expires = expires;
        Some(response)
    }

    pub fn receive(&mut self, packet: &Packet, now: u64) -> Option<Packet> {
        // NICでの確認から本文処理までの間にも期限を過ぎ得る。不正なcreditとは区別する。
        if packet.expires <= now {
            self.metrics.expired += 1;
            return None;
        }
        match self.accept(packet, now) {
            Some(hash) => {
                let mut ack = packet.response(Kind::Ack);
                ack.payload = hash.to_be_bytes().to_vec();
                Some(ack)
            },
            None => {
                self.metrics.rejected += 1;
                None
            },
        }
    }

    fn accept(&mut self, packet: &Packet, now: u64) -> Option<u64> {
        if packet.kind != Kind::Data || packet.expires <= now {
            return None;
        }
        let (id, slot) = credit_parts(packet.credit);
        let grant = self.grants.get(&id)?;
        if (grant.source, grant.session, grant.class) != (packet.source, packet.session, packet.class) || grant.expires < packet.expires {
            return None;
        }
        let previous = *grant.slots.get(slot as usize)?;
        if previous.is_some_and(|message| message != packet.message) {
            return None;
        }
        let hash = fingerprint(&packet.payload);
        let key = (packet.source, packet.session, packet.message);
        if let Some(delivered) = self.delivered.get(&key) {
            if (delivered.fingerprint, delivered.expires, delivered.credit, delivered.class) != (hash, packet.expires, packet.credit, packet.class) {
                return None;
            }
            self.metrics.duplicates += 1;
            return Some(hash);
        }
        // 履歴の期限後に同じslotを別のdeadlineで再利用しても再配送しない。
        if previous.is_some() || self.delivered.len() >= MAX_DELIVERIES {
            return None;
        }
        self.grants.get_mut(&id)?.slots[slot as usize] = Some(packet.message);
        self.delivered.insert(
            key,
            Delivered {
                fingerprint: hash,
                expires: packet.expires,
                credit: packet.credit,
                class: packet.class,
            },
        );
        self.metrics.delivered[packet.class.index()] += 1;
        self.metrics.payload_bytes[packet.class.index()] += packet.payload.len() as u64;
        Some(hash)
    }
}

pub struct Allowance {
    pub id: u32,
    pub used: u32,
    pub count: u32,
    pub expires: u64,
}

impl Allowance {
    pub fn from_grant(packet: &Packet) -> Option<Self> {
        let (id, count) = credit_parts(packet.credit);
        if packet.kind != Kind::Grant || id == 0 || count == 0 || count > GRANT_BATCH {
            return None;
        }
        Some(Self {
            id,
            used: 0,
            count,
            expires: packet.expires,
        })
    }

    pub fn take(&mut self, deadline: u64) -> Option<u64> {
        if self.used >= self.count || deadline > self.expires {
            return None;
        }
        let token = credit_token(self.id, self.used);
        self.used += 1;
        Some(token)
    }
}
