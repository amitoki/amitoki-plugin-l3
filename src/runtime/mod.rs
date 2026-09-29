mod endpoint;
mod observation;
pub use endpoint::{Endpoint, MessageSink, Submission};
mod client;
mod node;
mod reliable;
pub(crate) mod shutdown;
mod synchronization;
pub use client::{run_benchmark, Benchmark};
pub use node::{run_node, NodeOptions};
pub use reliable::{run_reliable_benchmark, ReliableOptions};
pub use shutdown::install_shutdown;

use crate::{
    clock::Clock,
    config::Config,
    ethernet::Ethernet,
    packet::Packet,
    scheduler::Scheduler,
    sync::{Reading, SYNC_TIMEOUT_US},
};
use serde::Serialize;
use std::{collections::HashMap, io, path::Path};
use synchronization::Synchronization;

// 一つの受信キューだけで他の経路と期限処理を飢餓させない。
const RECEIVE_BURST: usize = 32;
const TRANSMIT_BURST: usize = 8;
// downしたNICのエラーをbusy loopで読み続けず、復旧は10msごとに検出する。
const LINK_RECHECK_US: u64 = 10_000;

fn random_session() -> u64 {
    u64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().expect("UUIDの先頭8バイト")).clamp(1, u64::MAX - 1)
}

#[derive(Default, Serialize)]
pub struct NetworkMetrics {
    pub received: u64,
    pub sent: u64,
    pub wire_bytes: u64,
    pub malformed: u64,
    pub expired: u64,
    pub wrong_clock: u64,
    pub lifetime_rejected: u64,
    pub no_route: u64,
    pub hop_limit: u64,
    pub send_errors: u64,
    pub receive_errors: u64,
    pub unsynchronized: u64,
    pub sync_sent: u64,
    pub trimmed: u64,
    pub trim_dropped: u64,
}

pub struct Network {
    pub node: u32,
    links: Vec<Ethernet>,
    names: Vec<String>,
    receive_after: Vec<u64>,
    routes: HashMap<(u32, u8), usize>,
    queues: Vec<Scheduler>,
    pub metrics: NetworkMetrics,
    pub clock: Clock,
    synchronization: Synchronization,
    fabric: crate::fabric::Settings,
    observer: observation::Observer,
}

impl Network {
    fn open(config: Config) -> io::Result<Self> {
        config.validate().map_err(io::Error::other)?;
        let clock = Clock::with_simulation(config.clock.simulation)?;
        let synchronization = Synchronization::new(&config, clock.domain, clock.now());
        let mut links = Vec::new();
        let mut names = Vec::new();
        let mut queues = Vec::new();
        for link in config.links {
            links.push(Ethernet::open(&link)?);
            names.push(link.interface);
            queues.push(
                Scheduler::new(config.scheduler, link.bytes_per_second.unwrap_or(config.bytes_per_second), clock.now()).with_observation(config.node, config.fabric.telemetry),
            );
        }
        let routes = config
            .routes
            .into_iter()
            .map(|route| {
                (
                    (route.destination, route.path),
                    names.iter().position(|name| *name == route.interface).expect("検証済み経路"),
                )
            })
            .collect();
        Ok(Self {
            node: config.node,
            fabric: config.fabric,
            observer: observation::Observer::new(config.observation),
            receive_after: vec![0; links.len()],
            links,
            names,
            routes,
            queues,
            metrics: NetworkMetrics::default(),
            clock,
            synchronization,
        })
    }

    fn receive(&mut self) -> io::Result<Vec<Packet>> {
        for request in self.synchronization.requests(self.clock.now()) {
            self.enqueue(request);
        }
        let mut packets = Vec::new();
        let mut synchronization_packets = Vec::new();
        for (index, link) in self.links.iter().enumerate() {
            if self.clock.now() < self.receive_after[index] {
                continue;
            }
            for _ in 0..RECEIVE_BURST {
                let received = match link.receive() {
                    Ok(received) => received,
                    Err(error) if matches!(error.raw_os_error(), Some(libc::ENETDOWN | libc::ENETUNREACH | libc::EHOSTUNREACH)) => {
                        self.metrics.receive_errors += 1;
                        self.receive_after[index] = self.clock.now().saturating_add(LINK_RECHECK_US);
                        break;
                    },
                    Err(error) => return Err(error),
                };
                let Some(received) = received else {
                    break;
                };
                let packet = match received {
                    Ok(packet) => packet,
                    Err(_) => {
                        self.metrics.malformed += 1;
                        continue;
                    },
                };
                self.metrics.received += 1;
                if packet.is_sync() {
                    synchronization_packets.push(packet);
                    continue;
                }
                packets.push(packet);
            }
        }
        for packet in synchronization_packets {
            if packet.destination != self.node {
                self.forward(packet);
            } else if let Some(reply) = self.synchronization.receive(&packet, self.clock.now()) {
                self.enqueue(reply);
            }
        }
        // 同じ受信バッチで基準ノードが再起動しても、旧世代のDATA/GRANTを返さない。
        let reading = self.time();
        let domain = self.synchronization.domain();
        packets.retain(|packet| accept_packet(&mut self.metrics, packet, PacketTime { reading, domain }));
        Ok(packets)
    }

    fn time(&self) -> Option<Reading> {
        self.synchronization.reading(self.clock.now())
    }

    fn reliable_time(&self) -> Option<Reading> {
        self.time().or_else(|| self.fabric.clock_independent.then(|| self.synchronization.domain().map(|domain| Reading::exact(self.clock.now(), domain))).flatten())
    }

    fn observe(&mut self, channels: impl FnOnce() -> serde_json::Value) {
        let now = self.clock.now();
        if self.observer.due(now) {
            let mut report = self.report();
            report["channels"] = channels();
            self.observer.write(now, report);
        }
    }

    fn enqueue(&mut self, packet: Packet) {
        self.try_enqueue(packet);
    }

    fn try_enqueue(&mut self, packet: Packet) -> bool {
        let Some(index) = self.routes.get(&(packet.destination, packet.path)) else {
            self.metrics.no_route += 1;
            return false;
        };
        let now = self.clock.now();
        let expires = if packet.is_sync() {
            now + SYNC_TIMEOUT_US
        } else if packet.has_local_lifetime() {
            if !accept_response(&mut self.metrics, &packet, self.synchronization.domain()) {
                return false;
            }
            // ACKの絶対期限は使わないが、各中継キューでの滞留は有限にする。
            now + crate::delivery::ATTEMPT_LIFETIME_US
        } else {
            let reading = self.synchronization.reading(now);
            if !accept_data(&mut self.metrics, &packet, reading) {
                return false;
            }
            let Some(expires) = reading.and_then(|reading| reading.local_deadline(packet.expires)) else {
                return false;
            };
            expires
        };
        self.queues[*index].enqueue_until(packet, now, expires)
    }

    fn forward(&mut self, mut packet: Packet) {
        if packet.hops <= 1 {
            self.metrics.hop_limit += 1;
            return;
        }
        packet.hops -= 1;
        let candidate = (self.fabric.trimming && packet.kind == crate::packet::Kind::ReliableData).then(|| packet.clone());
        if self.try_enqueue(packet) {
            return;
        }
        let Some(candidate) = candidate else { return };
        let Some(index) = self.routes.get(&(candidate.destination, candidate.path)) else {
            return;
        };
        let signal = self.queues[*index].congestion_signal(self.queues[*index].backlog_us());
        if let Some(header) = crate::fabric::trim(candidate, signal) {
            if self.try_enqueue(header) {
                self.metrics.trimmed += 1;
            } else {
                self.metrics.trim_dropped += 1;
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        for (link, queue) in self.links.iter().zip(&mut self.queues) {
            for _ in 0..TRANSMIT_BURST {
                let Some(mut packet) = queue.pop(self.clock.now()) else {
                    break;
                };
                let now = self.clock.now();
                if packet.is_sync() {
                    self.synchronization.stamp_reply(&mut packet, now);
                } else if !accept_packet(
                    &mut self.metrics,
                    &packet,
                    PacketTime {
                        reading: self.synchronization.reading(now),
                        domain: self.synchronization.domain(),
                    },
                ) {
                    continue;
                }
                match link.send(&packet) {
                    Ok(()) => {
                        self.metrics.sent += 1;
                        self.metrics.wire_bytes += packet.wire_size() as u64;
                        self.metrics.sync_sent += u64::from(packet.is_sync());
                    },
                    Err(error)
                        if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted)
                            || matches!(error.raw_os_error(), Some(libc::ENOBUFS | libc::ENETDOWN | libc::ENETUNREACH | libc::EHOSTUNREACH)) =>
                    {
                        // qdiscの破棄や一時的な送信バッファ不足はパケット損失として扱う。
                        self.metrics.send_errors += 1;
                    },
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    fn wait(&self, until: u64) -> io::Result<()> {
        if self.queues.iter().any(Scheduler::pending) {
            std::hint::spin_loop();
            return Ok(());
        }
        let duration = until.saturating_sub(self.clock.now());
        // sub-msはpollの切り上げで遅らせず、試作ではCPUを使って待つ。
        if duration < 1000 {
            std::hint::spin_loop();
            return Ok(());
        }
        let mut descriptors: Vec<_> =
            self.links.iter().enumerate().filter(|(index, _)| self.receive_after[*index] <= self.clock.now()).map(|(_, link)| link.poll_descriptor()).collect();
        let milliseconds = (duration / 1000).min(10) as i32;
        // SAFETY: descriptorsは初期化済みで、渡した個数分の領域がある。
        let status = unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as libc::nfds_t, milliseconds) };
        if status < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn report(&self) -> serde_json::Value {
        let queues: HashMap<_, _> = self.names.iter().zip(&self.queues).map(|(name, queue)| (name, &queue.metrics)).collect();
        serde_json::json!({
            "node": self.node,
            "observed_us": self.clock.now(),
            "fabric": self.fabric,
            "observation_errors": self.observer.errors,
            "clock_domain": self.clock.domain,
            "clock_simulation": self.clock.simulation(),
            "clock_sync": self.synchronization.report(self.clock.now()),
            "process_usage": crate::measurement::process_usage(),
            "network": self.metrics,
            "queues": queues
        })
    }
}

struct PacketTime {
    reading: Option<Reading>,
    domain: Option<u64>,
}

fn accept_packet(metrics: &mut NetworkMetrics, packet: &Packet, time: PacketTime) -> bool {
    let PacketTime { reading, domain } = time;
    if packet.has_local_lifetime() {
        accept_response(metrics, packet, domain)
    } else {
        accept_data(metrics, packet, reading)
    }
}

fn accept_response(metrics: &mut NetworkMetrics, packet: &Packet, domain: Option<u64>) -> bool {
    if domain == Some(packet.clock_domain) {
        true
    } else {
        metrics.wrong_clock += 1;
        false
    }
}

fn accept_data(metrics: &mut NetworkMetrics, packet: &Packet, reading: Option<Reading>) -> bool {
    let Some(reading) = reading else {
        metrics.unsynchronized += 1;
        return false;
    };
    if packet.clock_domain != reading.domain {
        metrics.wrong_clock += 1;
    } else if packet.expires <= reading.latest {
        metrics.expired += 1;
    } else if !packet.valid_at(reading.latest, reading.domain) {
        metrics.lifetime_rejected += 1;
    } else {
        return true;
    }
    false
}

fn write_json(path: &Path, report: &serde_json::Value) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(report).map_err(io::Error::other)?)
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::delivery::{Channel, ChannelOptions, Receiver, ReceiverOptions};
    use crate::tokens::TokenBucket;

    #[test]
    fn accepted_acknowledgements_survive_clock_uncertainty_but_not_generation_changes() {
        let start = 1_000_000;
        let domain = 42;
        let mut sender = Channel::new(ChannelOptions::new(1, 2, 77)).unwrap();
        let mut receiver = Receiver::new(ReceiverOptions::default(), 900).unwrap();
        let mut budget = TokenBucket::new(1_000_000, 1_000_000, start);
        let time = Some(Reading::exact(start, domain));
        let open = sender.poll(start, time, &mut budget).remove(0);
        sender.receive(&receiver.receive(&open, start).unwrap(), start);
        sender.try_send(&[7], start).unwrap();
        let data = sender.poll(start, time, &mut budget).remove(0);
        let ack = receiver.receive(&data, start).unwrap();
        let mut metrics = NetworkMetrics::default();
        assert!(accept_packet(
            &mut metrics,
            &ack,
            PacketTime {
                reading: None,
                domain: Some(domain)
            }
        ));
        assert!(accept_packet(
            &mut metrics,
            &ack,
            PacketTime {
                reading: Some(Reading::exact(ack.expires + 1, domain)),
                domain: Some(domain)
            }
        ));
        assert!(!accept_packet(&mut metrics, &ack, PacketTime { reading: None, domain: None }));
        assert!(!accept_packet(
            &mut metrics,
            &ack,
            PacketTime {
                reading: None,
                domain: Some(domain + 1)
            }
        ));
        assert!(!accept_packet(
            &mut metrics,
            &data,
            PacketTime {
                reading: None,
                domain: Some(domain)
            }
        ));
        assert!(!accept_packet(
            &mut metrics,
            &data,
            PacketTime {
                reading: Some(Reading::exact(data.expires + 1, domain)),
                domain: Some(domain)
            }
        ));
        assert_eq!(sender.receive(&ack, start + 1), Some(1));
        assert_eq!(sender.pending(), 0);
    }
}
