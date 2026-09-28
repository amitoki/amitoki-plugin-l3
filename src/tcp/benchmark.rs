use super::{
    connection::{wait, Connection},
    framing::Frame,
    options, write_report, POLL_US,
};
use crate::{
    clock::Clock,
    packet::{fingerprint, Class},
};
use std::{collections::HashMap, io, net::TcpStream, time::Duration};

// L3の生成burst・送信待ち上限と合わせる。socket bufferはLinuxの既定を使う。
const GENERATION_BURST: usize = 32;
const PENDING_LIMIT: usize = 256;
// ローカルな試験ネットワークの接続失敗を有限時間で検出する。
const CONNECT_TIMEOUT_SECONDS: u64 = 2;

pub fn run_benchmark(options: options::Benchmark) -> io::Result<()> {
    let rates = [options.short_rate, options.bulk_rate];
    if rates == [0, 0] {
        return Err(io::Error::other("送信レートが両方0です"));
    }
    let sizes = [options.short_bytes as usize, options.bulk_bytes as usize];
    let clock = Clock::local()?;
    let start = clock.now();
    let mut connections = (0..options.endpoint.connections)
        .map(|_| Connection::new(TcpStream::connect_timeout(&options.peer, Duration::from_secs(CONNECT_TIMEOUT_SECONDS))?))
        .collect::<io::Result<Vec<_>>>()?;
    let algorithms = connections.iter().map(Connection::congestion_control).collect::<io::Result<Vec<_>>>()?;
    let finish = start + (options.endpoint.duration_ms + options.delivery_timeout_ms) * 1000;
    let counts = rates.map(|rate| options.endpoint.duration_ms * rate / 1000);
    let mut generated = [0_u64; 2];
    let mut acknowledged = [0_u64; 2];
    let mut blocked = [0_u64; 2];
    let mut pending: [HashMap<u64, u64>; 2] = Default::default();
    while clock.now() < finish && !crate::runtime::shutdown::requested() {
        for connection in &mut connections {
            for frame in connection.receive()? {
                let index = frame.class.index();
                let expected = pending[index].remove(&frame.sequence);
                if !frame.acknowledgement || expected.is_none_or(|hash| frame.payload != hash.to_be_bytes()) {
                    return Err(io::Error::other("TCP ACKの本文/sequenceが一致しません"));
                }
                acknowledged[index] += 1;
            }
        }
        for class in [Class::Short, Class::Bulk] {
            let index = class.index();
            let connection_index = index % connections.len();
            for _ in 0..GENERATION_BURST {
                if generated[index] >= counts[index] {
                    break;
                }
                let due = start + generated[index] * 1_000_000 / rates[index];
                if due > clock.now() {
                    break;
                }
                if pending[index].len() == PENDING_LIMIT {
                    blocked[index] += 1;
                    break;
                }
                let sequence = generated[index] + 1;
                let payload: Vec<_> = (0..sizes[index]).map(|offset| sequence.wrapping_add(offset as u64) as u8).collect();
                let hash = fingerprint(&payload);
                if !connections[connection_index].enqueue(&Frame {
                    acknowledgement: false,
                    class,
                    sequence,
                    payload,
                })? {
                    blocked[index] += 1;
                    break;
                }
                pending[index].insert(sequence, hash);
                generated[index] += 1;
            }
        }
        for connection in &mut connections {
            connection.flush()?;
        }
        if acknowledged == counts {
            break;
        }
        wait(&connections, POLL_US.min(finish.saturating_sub(clock.now())))?;
    }
    let complete = acknowledged == counts;
    write_report(
        &options.endpoint.output,
        &serde_json::json!({"clock_domain":clock.domain,"process_usage":crate::measurement::process_usage(),
        "benchmark":{"start_us":start,"elapsed_us":clock.now()-start,"complete":complete,"offered":counts,"submitted":generated,
        "acknowledged":acknowledged,"backpressure_events":blocked,"connections":options.endpoint.connections,"tcp_nodelay":true,"congestion_control":algorithms}}),
    )?;
    if complete {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::TimedOut, "TCP比較に未確認のメッセージがあります"))
    }
}
