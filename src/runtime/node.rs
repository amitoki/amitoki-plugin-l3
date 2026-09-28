use super::{write_json, Network};
use crate::{config::Config, credit::Receiver, delivery, packet::Kind};
use std::{
    io::{self, Write},
    path::PathBuf,
};

// 期限切れ履歴を定期回収し、通信が止まっても保持し続けない。
const PRUNE_INTERVAL_US: u64 = 10_000;

pub struct NodeOptions {
    pub config: Config,
    pub receiver_rates: Option<[u64; 2]>,
    pub duration_us: u64,
    pub output: PathBuf,
    pub ready: PathBuf,
    pub delivery_log: Option<PathBuf>,
    pub receive_window: usize,
}

pub fn run_node(options: NodeOptions) -> io::Result<()> {
    let mut network = Network::open(options.config)?;
    let start = network.clock.now();
    let mut receiver = options.receiver_rates.map(|rates| Receiver::new(rates, start));
    let mut reliable = options
        .receiver_rates
        .map(|rates| {
            delivery::Receiver::new(
                delivery::ReceiverOptions {
                    rates,
                    window: options.receive_window,
                    ..Default::default()
                },
                super::random_session(),
            )
        })
        .transpose()
        .map_err(io::Error::other)?;
    let mut delivery_log = options.delivery_log.map(std::fs::File::create).transpose()?.map(io::BufWriter::new);
    let mut domain = network.time().map(|reading| reading.domain);
    let end = start + options.duration_us;
    let mut prune_at = start + PRUNE_INTERVAL_US;
    write_json(&options.ready, &serde_json::json!({"node":network.node,"clock_domain":network.clock.domain,"ready":true}))?;
    while network.clock.now() < end && !super::shutdown::requested() {
        for packet in network.receive()? {
            if packet.destination != network.node {
                if receiver.is_none() {
                    network.forward(packet);
                } else {
                    network.metrics.no_route += 1;
                }
                continue;
            }
            if packet.is_reliable() {
                let time = network.time();
                if !super::accept_data(&mut network.metrics, &packet, time) {
                    continue;
                }
                if let Some(reliable) = &mut reliable {
                    let mut response = reliable.receive(&packet, network.clock.now());
                    while let Some(message) = reliable.take_delivery() {
                        if let Some(log) = &mut delivery_log {
                            serde_json::to_writer(&mut *log, &message).map_err(io::Error::other)?;
                            writeln!(log)?;
                        }
                    }
                    if let Some(response) = &mut response {
                        reliable.refresh_ack(response);
                    }
                    if let Some(response) = response {
                        network.enqueue(response);
                    }
                }
                continue;
            }
            if let Some(receiver) = &mut receiver {
                let Some(time) = network.time() else {
                    continue;
                };
                if domain != Some(time.domain) {
                    *receiver = Receiver::new(options.receiver_rates.expect("受信ノード"), time.local);
                    domain = Some(time.domain);
                }
                let response = match packet.kind {
                    Kind::Request => receiver.grant_at(&packet, time),
                    Kind::Data => receiver.receive(&packet, time.latest),
                    _ => None,
                };
                if let Some(response) = response {
                    network.enqueue(response);
                }
            }
        }
        let now = network.clock.now();
        if now >= prune_at {
            if let Some(reliable) = &mut reliable {
                reliable.prune(now);
            }
            if let Some(receiver) = &mut receiver {
                if let Some(time) = network.time() {
                    receiver.prune(time.earliest);
                }
            }
            prune_at = now + PRUNE_INTERVAL_US;
        }
        network.flush()?;
        network.wait(prune_at.min(end))?;
    }
    let mut report = network.report();
    report["elapsed_us"] = (network.clock.now() - start).into();
    if let Some(receiver) = receiver {
        report["receiver"] = serde_json::to_value(receiver.metrics).map_err(io::Error::other)?;
    }
    if let Some(reliable) = reliable {
        report["reliable_receiver"] = serde_json::to_value(reliable.metrics).map_err(io::Error::other)?;
    }
    if let Some(log) = &mut delivery_log {
        log.flush()?;
    }
    write_json(&options.output, &report)
}
