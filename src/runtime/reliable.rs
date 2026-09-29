use super::{client, random_session, write_json, Benchmark, Network};
use crate::{
    delivery::{Channel, ChannelOptions, Ordering, SendTick, SubmitError, DEFAULT_PENDING, DEFAULT_TIMEOUT_US, PREFIX_SIZE},
    packet::{Class, MAX_FRAME, MAX_PAYLOAD, MAX_SHORT_PAYLOAD},
    tokens::TokenBucket,
};
use std::io;

// 生成と受信を交互に進め、一つのchannelが他方のACK処理を占有しない。
const GENERATION_BURST: usize = 32;
const POLL_INTERVAL_US: u64 = 1000;

pub struct ReliableOptions {
    pub ordering: [Ordering; 2],
    pub timeout_us: u64,
    pub pending_limit: usize,
}

impl Default for ReliableOptions {
    fn default() -> Self {
        Self {
            ordering: [Ordering::Unordered; 2],
            timeout_us: DEFAULT_TIMEOUT_US,
            pending_limit: DEFAULT_PENDING,
        }
    }
}

pub fn run_reliable_benchmark(options: Benchmark, reliable: ReliableOptions) -> io::Result<()> {
    client::validate(&options).map_err(io::Error::other)?;
    if options.retries != 0 {
        return Err(io::Error::other("--retriesはdeadlineモードだけで使用します。reliableはdelivery-timeoutまで再送します"));
    }
    if options.sizes[0] + PREFIX_SIZE > MAX_SHORT_PAYLOAD || options.sizes[1] + PREFIX_SIZE > MAX_PAYLOAD || options.replica_bytes_per_second == 0 {
        return Err(io::Error::other("信頼性配送はshort最大240B/bulk最大1384B、再送帯域は正数を指定してください"));
    }
    let mut network = Network::open(options.config)?;
    let start = network.clock.now();
    let session = random_session();
    let mut channels = Vec::new();
    for class in [Class::Short, Class::Bulk] {
        let index = class.index();
        channels.push(
            Channel::new(ChannelOptions {
                channel: index as u32 + 1,
                fabric: network.fabric,
                class,
                ordering: reliable.ordering[index],
                paths: options.paths.clone(),
                pending_limit: reliable.pending_limit,
                timeout_us: reliable.timeout_us,
                ..ChannelOptions::new(network.node, options.peer, session)
            })
            .map_err(io::Error::other)?,
        );
    }
    let finish = start.checked_add(options.duration_us).and_then(|end| end.checked_add(reliable.timeout_us)).ok_or_else(|| io::Error::other("試験の終了時刻が範囲外です"))?;
    let mut retry_budget = TokenBucket::new(options.replica_bytes_per_second, MAX_FRAME as u64 * 2, start);
    let counts = options.rates.map(|rate| options.duration_us * rate / 1_000_000);
    let mut generated = [0_u64; 2];
    let mut blocked = [0_u64; 2];
    while network.clock.now() < finish && !super::shutdown::requested() {
        for packet in network.receive()? {
            if packet.is_reliable() {
                channels[packet.class.index()].receive(&packet, network.clock.now());
            }
        }
        for (index, channel) in channels.iter_mut().enumerate() {
            if counts[index] == 0 || channel.is_closed() {
                continue;
            }
            for _ in 0..GENERATION_BURST {
                if generated[index] >= counts[index] {
                    break;
                }
                let due = start + generated[index] * 1_000_000 / options.rates[index];
                let now = network.clock.now();
                if now < due {
                    break;
                }
                let sequence = generated[index] + 1;
                let payload: Vec<_> = (0..options.sizes[index]).map(|offset| sequence.wrapping_add(offset as u64) as u8).collect();
                match channel.try_send(&payload, now) {
                    Ok(_) => generated[index] += 1,
                    Err(SubmitError::WouldBlock) => {
                        blocked[index] += 1;
                        break;
                    },
                    Err(SubmitError::Closed) => break,
                    Err(error) => return Err(io::Error::other(error)),
                }
            }
            let tick = SendTick {
                now: network.clock.now(),
                time: network.reliable_time(),
                retry_budget: &mut retry_budget,
            };
            channel.transmit(tick, |packet| network.try_enqueue(packet));
        }
        network.flush()?;
        network.observe(|| serde_json::json!(channels.iter().map(Channel::report).collect::<Vec<_>>()));
        if channels.iter().enumerate().all(|(index, channel)| counts[index] == 0 || channel.is_closed() || (generated[index] == counts[index] && channel.pending() == 0)) {
            break;
        }
        network.wait((network.clock.now() + POLL_INTERVAL_US).min(finish))?;
    }
    for channel in &mut channels {
        if channel.pending() > 0 {
            channel.abort();
        }
    }
    let mut report = network.report();
    let reports: Vec<_> = channels
        .iter()
        .enumerate()
        .map(|(index, channel)| {
            serde_json::json!({
                "channel": index + 1, "ordering": reliable.ordering[index], "state": channel.state(), "offered": counts[index],
                "unsubmitted": counts[index] - generated[index], "pending": channel.pending(), "backpressure_events": blocked[index], "metrics": channel.report()["metrics"], "fabric": channel.report()["fabric"]
            })
        })
        .collect();
    let complete = channels.iter().enumerate().all(|(index, channel)| channel.metrics.acknowledged == counts[index]);
    report["reliable_benchmark"] = serde_json::json!({ "complete": complete, "start_us": start, "elapsed_us": network.clock.now() - start,
        "timeout_us": reliable.timeout_us, "channels": reports });
    write_json(&options.output, &report)?;
    if complete {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::TimedOut, "配送未確認のメッセージがあります。JSONレポートを確認してください"))
    }
}
