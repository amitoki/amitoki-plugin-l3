mod client;
mod node;
mod shutdown;
pub use client::{run_benchmark, Benchmark};
pub use node::{run_node, NodeOptions};
pub use shutdown::install_shutdown;

use crate::{clock::Clock, config::Config, ethernet::Ethernet, packet::Packet, scheduler::Scheduler};
use serde::Serialize;
use std::{collections::HashMap, io, path::Path};

// 一つの受信キューだけで他の経路と期限処理を飢餓させない。
const RECEIVE_BURST: usize = 32;
const TRANSMIT_BURST: usize = 8;

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
}

pub struct Network {
    pub node: u32,
    links: Vec<Ethernet>,
    names: Vec<String>,
    routes: HashMap<(u32, u8), usize>,
    queues: Vec<Scheduler>,
    pub metrics: NetworkMetrics,
    pub clock: Clock,
}

impl Network {
    fn open(config: Config) -> io::Result<Self> {
        config.validate().map_err(io::Error::other)?;
        let clock = Clock::local()?;
        let mut links = Vec::new();
        let mut names = Vec::new();
        let mut queues = Vec::new();
        for link in config.links {
            links.push(Ethernet::open(&link)?);
            names.push(link.interface);
            queues.push(Scheduler::new(config.scheduler, config.bytes_per_second, clock.now()));
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
            links,
            names,
            routes,
            queues,
            metrics: NetworkMetrics::default(),
            clock,
        })
    }

    fn receive(&mut self) -> io::Result<Vec<Packet>> {
        let mut packets = Vec::new();
        for link in &self.links {
            for _ in 0..RECEIVE_BURST {
                let Some(received) = link.receive()? else {
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
                let now = self.clock.now();
                if packet.clock_domain != self.clock.domain {
                    self.metrics.wrong_clock += 1;
                    continue;
                }
                if packet.expires <= now {
                    self.metrics.expired += 1;
                    continue;
                }
                if !packet.valid_at(now, self.clock.domain) {
                    self.metrics.lifetime_rejected += 1;
                    continue;
                }
                packets.push(packet);
            }
        }
        Ok(packets)
    }

    fn enqueue(&mut self, packet: Packet) {
        let Some(index) = self.routes.get(&(packet.destination, packet.path)) else {
            self.metrics.no_route += 1;
            return;
        };
        self.queues[*index].enqueue(packet, self.clock.now());
    }

    fn forward(&mut self, mut packet: Packet) {
        if packet.hops <= 1 {
            self.metrics.hop_limit += 1;
            return;
        }
        packet.hops -= 1;
        self.enqueue(packet);
    }

    fn flush(&mut self) -> io::Result<()> {
        for (link, queue) in self.links.iter().zip(&mut self.queues) {
            for _ in 0..TRANSMIT_BURST {
                let Some(packet) = queue.pop(self.clock.now()) else {
                    break;
                };
                match link.send(&packet) {
                    Ok(()) => {
                        self.metrics.sent += 1;
                        self.metrics.wire_bytes += packet.wire_size() as u64;
                    },
                    Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => self.metrics.send_errors += 1,
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
        let mut descriptors: Vec<_> = self.links.iter().map(Ethernet::poll_descriptor).collect();
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
            "clock_domain": self.clock.domain,
            "network": self.metrics,
            "queues": queues
        })
    }
}

fn write_json(path: &Path, report: &serde_json::Value) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(report).map_err(io::Error::other)?)
}
