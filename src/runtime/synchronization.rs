use crate::{
    config::Config,
    packet::{Class, Kind, Packet, INITIAL_HOPS},
    sync::{ClockEstimate, Exchange, Reading, SYNC_INTERVAL_US, SYNC_TIMEOUT_US},
    tokens::TokenBucket,
};
use std::collections::HashMap;

// 同期要求の再送も有限にする。2経路×100ms間隔×500msの応答待ちを収める。
const MAX_PENDING: usize = 16;
const MAX_PATHS: usize = 2;
const REPLIES_PER_SECOND: u64 = 100;
const REPLY_BURST: u64 = 16;

struct Pending {
    path: u8,
    sent: u64,
}

pub(super) struct Synchronization {
    node: u32,
    authority: Option<u32>,
    local_domain: u64,
    session: u64,
    paths: Vec<u8>,
    estimate: ClockEstimate,
    pending: HashMap<u64, Pending>,
    next_message: u64,
    next_request: u64,
    replies: TokenBucket,
    requests_sent: u64,
    invalid_replies: u64,
}

impl Synchronization {
    pub fn new(config: &Config, local_domain: u64, now: u64) -> Self {
        let mut paths: Vec<_> = config.routes.iter().filter(|route| Some(route.destination) == config.clock.authority).map(|route| route.path).collect();
        paths.sort_unstable();
        paths.dedup();
        paths.truncate(MAX_PATHS);
        Self {
            node: config.node,
            authority: config.clock.authority,
            local_domain,
            session: u64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().expect("UUIDの先頭8バイト")).max(1),
            paths,
            estimate: ClockEstimate::new(config.clock.clone()),
            pending: HashMap::new(),
            next_message: 1,
            next_request: now,
            replies: TokenBucket::new(REPLIES_PER_SECOND, REPLY_BURST, now),
            requests_sent: 0,
            invalid_replies: 0,
        }
    }

    pub fn reading(&self, now: u64) -> Option<Reading> {
        match self.authority {
            None => Some(Reading::exact(now, self.local_domain)),
            Some(authority) if authority == self.node => Some(Reading::exact(now, self.session)),
            Some(_) => self.estimate.reading(now),
        }
    }

    pub fn requests(&mut self, now: u64) -> Vec<Packet> {
        let Some(authority) = self.authority else { return Vec::new() };
        if authority == self.node || now < self.next_request {
            return Vec::new();
        }
        self.next_request = now + SYNC_INTERVAL_US;
        self.pending.retain(|_, request| now.saturating_sub(request.sent) < SYNC_TIMEOUT_US);
        let mut packets = Vec::new();
        for path in &self.paths {
            if self.pending.len() >= MAX_PENDING {
                break;
            }
            let message = self.next_message;
            self.next_message += 1;
            self.pending.insert(message, Pending { path: *path, sent: now });
            packets.push(Packet {
                kind: Kind::SyncRequest,
                class: Class::Short,
                hops: INITIAL_HOPS,
                source: self.node,
                destination: authority,
                session: self.session,
                message,
                expires: 0,
                clock_domain: self.local_domain,
                credit: 0,
                path: *path,
                flags: 0,
                payload: now.to_be_bytes().to_vec(),
            });
            self.requests_sent += 1;
        }
        packets
    }

    pub fn receive(&mut self, packet: &Packet, now: u64) -> Option<Packet> {
        if packet.kind == Kind::SyncRequest {
            if self.authority != Some(self.node) || !self.replies.take(1, now) {
                return None;
            }
            let mut reply = packet.response(Kind::SyncReply);
            reply.clock_domain = self.session;
            reply.payload = packet.payload.clone();
            reply.payload.extend_from_slice(&now.to_be_bytes());
            reply.payload.extend_from_slice(&now.to_be_bytes());
            return Some(reply);
        }
        let pending = self.pending.get(&packet.message)?;
        let timestamp = |offset| u64::from_be_bytes(packet.payload[offset..offset + 8].try_into().expect("検証済み時計応答"));
        if packet.kind != Kind::SyncReply
            || Some(packet.source) != self.authority
            || packet.session != self.session
            || packet.path != pending.path
            || timestamp(0) != pending.sent
            || now.saturating_sub(pending.sent) > SYNC_TIMEOUT_US
        {
            self.invalid_replies += 1;
            return None;
        }
        let sent = pending.sent;
        self.pending.remove(&packet.message);
        let previous_domain = self.estimate.domain();
        let accepted = self.estimate.observe(Exchange {
            domain: packet.clock_domain,
            sent,
            received_by_authority: timestamp(8),
            sent_by_authority: timestamp(16),
            received: now,
        });
        if accepted && previous_domain != Some(packet.clock_domain) {
            // 新世代を知る前に送った要求への応答で、未観測の旧世代へ戻らない。
            self.pending.clear();
        }
        None
    }

    pub fn stamp_reply(&self, packet: &mut Packet, now: u64) {
        if packet.kind == Kind::SyncReply && packet.source == self.node && self.authority == Some(self.node) {
            packet.payload[16..24].copy_from_slice(&now.to_be_bytes());
        }
    }

    pub fn report(&self, now: u64) -> serde_json::Value {
        serde_json::json!({
            "authority": self.authority, "reading": self.reading(now), "samples": self.estimate.metrics,
            "requests_sent": self.requests_sent, "invalid_replies": self.invalid_replies,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration(node: u32) -> Config {
        serde_json::from_value(serde_json::json!({
            "node":node, "links":[{"interface":"eth0","peer_mac":"02:00:00:00:00:01"}],
            "routes":[{"destination":3-node,"path":1,"interface":"eth0"}],
            "scheduler":"priority", "bytes_per_second":1000000, "clock":{"authority":2}
        }))
        .unwrap()
    }

    fn exchange() -> (Synchronization, Packet, u64) {
        let mut follower = Synchronization::new(&configuration(1), 91, 1_000_000);
        let mut authority = Synchronization::new(&configuration(2), 92, 6_000_000);
        let request = follower.requests(1_000_000).remove(0);
        let reply = authority.receive(&request, 6_000_010).unwrap();
        (follower, reply, 1_000_020)
    }

    #[test]
    fn replies_are_bound_to_authority_session_path_and_request_timestamp() {
        for corruption in 0..5 {
            let (mut follower, mut reply, now) = exchange();
            match corruption {
                0 => reply.source += 1,
                1 => reply.session += 1,
                2 => reply.path += 1,
                3 => reply.payload[0] ^= 1,
                _ => reply.message += 1,
            }
            follower.receive(&reply, now);
            assert!(follower.reading(now).is_none());
        }
    }

    #[test]
    fn duplicate_or_timed_out_replies_do_not_refresh_the_clock() {
        let (mut follower, reply, now) = exchange();
        follower.receive(&reply, now);
        assert!(follower.reading(now).is_some());
        follower.receive(&reply, now + 900_000);
        assert_eq!(follower.estimate.metrics.accepted, 1);
        assert!(follower.reading(now + 1_000_001).is_none());
        let (mut follower, reply, now) = exchange();
        follower.receive(&reply, now + SYNC_TIMEOUT_US);
        assert!(follower.reading(now + SYNC_TIMEOUT_US).is_none());
    }

    #[test]
    fn authority_restarts_create_a_new_generation() {
        let config = configuration(2);
        let first = Synchronization::new(&config, 99, 1_000_000);
        let second = Synchronization::new(&config, 99, 1_000_000);
        assert_ne!(first.reading(1_000_000).unwrap().domain, second.reading(1_000_000).unwrap().domain);
    }

    #[test]
    fn a_late_first_reply_cannot_replace_the_new_authority_generation() {
        let mut config = configuration(1);
        config.routes.push(crate::config::Route {
            destination: 2,
            path: 2,
            interface: "eth0".into(),
        });
        let mut follower = Synchronization::new(&config, 91, 1_000_000);
        let requests = follower.requests(1_000_000);
        let mut old = Synchronization::new(&configuration(2), 92, 6_000_000);
        let mut new = Synchronization::new(&configuration(2), 92, 6_000_000);
        let late = old.receive(&requests[0], 6_000_010).unwrap();
        let fast = new.receive(&requests[1], 6_000_010).unwrap();
        follower.receive(&fast, 1_000_020);
        follower.receive(&late, 1_000_030);
        assert_eq!(follower.reading(1_000_030).unwrap().domain, fast.clock_domain);
    }
}
