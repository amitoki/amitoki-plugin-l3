use super::{
    connection::{wait, Connection},
    framing::Frame,
    options, write_report, POLL_US,
};
use crate::{
    clock::Clock,
    packet::fingerprint,
    receipt_log::{Receipt, ReceiptLog},
};
use std::{io, net::TcpListener};

pub fn run_receiver(options: options::Receiver) -> io::Result<()> {
    let clock = Clock::local()?;
    let listener = TcpListener::bind(options.bind)?;
    listener.set_nonblocking(true)?;
    let start = clock.now();
    let end = start + options.endpoint.duration_ms * 1000;
    let mut connections = Vec::new();
    let mut receipt_log = ReceiptLog::open(Some(options.receipt_log))?;
    let mut delivered = [0_u64; 2];
    write_report(&options.ready, &serde_json::json!({"ready":true, "clock_domain":clock.domain}))?;
    while clock.now() < end && !crate::runtime::shutdown::requested() {
        if connections.len() < options.endpoint.connections as usize {
            match listener.accept() {
                Ok((stream, _)) => connections.push(Connection::new(stream)?),
                Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => {},
                Err(error) => return Err(error),
            }
        }
        for connection in &mut connections {
            for frame in connection.receive()? {
                let index = frame.class.index();
                if frame.acknowledgement || frame.sequence != delivered[index] + 1 {
                    return Err(io::Error::other("TCP受信メッセージの種別/順序が一致しません"));
                }
                receipt_log.record(Receipt {
                    channel: index as u32 + 1,
                    sequence: frame.sequence,
                    received_us: clock.now(),
                    payload: &frame.payload,
                })?;
                delivered[index] += 1;
                let ack = Frame {
                    acknowledgement: true,
                    class: frame.class,
                    sequence: frame.sequence,
                    payload: fingerprint(&frame.payload).to_be_bytes().to_vec(),
                };
                if !connection.enqueue(&ack)? {
                    return Err(io::Error::other("TCP ACKキューの上限を超えました"));
                }
            }
            connection.flush()?;
        }
        if connections.len() == options.endpoint.connections as usize && connections.iter().all(|connection| connection.closed) {
            break;
        }
        wait(&connections, POLL_US)?;
    }
    receipt_log.finish()?;
    write_report(
        &options.endpoint.output,
        &serde_json::json!({"clock_domain":clock.domain,"delivered":delivered,
        "process_usage":crate::measurement::process_usage(),"elapsed_us":clock.now()-start}),
    )
}
