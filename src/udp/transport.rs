//! Linux UDP socketの非同期入出力と、転送量の計測。
use super::Endpoint;
use crate::clock::Clock;
use std::{io, net::UdpSocket, os::fd::AsRawFd};

// 受信側が停止指示を定期的に確認するための待機上限。
const POLL_MS: i32 = 10;

pub(super) struct Transport {
    socket: UdpSocket,
    pub clock: Clock,
    sent: u64,
    bytes: u64,
    received: u64,
    send_errors: u64,
    pub malformed: u64,
    peer_unreachable: u64,
}

impl Transport {
    pub fn open(endpoint: &Endpoint) -> io::Result<Self> {
        let socket = UdpSocket::bind(endpoint.bind)?;
        socket.connect(endpoint.peer)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            clock: Clock::local()?,
            sent: 0,
            bytes: 0,
            received: 0,
            send_errors: 0,
            malformed: 0,
            peer_unreachable: 0,
        })
    }

    pub fn send(&mut self, bytes: &[u8]) -> io::Result<bool> {
        match self.socket.send(bytes) {
            Ok(count) if count == bytes.len() => {
                self.sent += 1;
                self.bytes += count as u64;
                Ok(true)
            },
            Ok(_) => Err(io::Error::new(io::ErrorKind::WriteZero, "UDP送信が不完全です")),
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) || error.raw_os_error() == Some(libc::ENOBUFS) => {
                self.send_errors += 1;
                Ok(false)
            },
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                // 期限後のキューが排出される間に送信側が終了するとICMPが返る。
                self.peer_unreachable += 1;
                Ok(false)
            },
            Err(error) => Err(error),
        }
    }

    pub fn receive(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
        match self.socket.recv(bytes) {
            Ok(count) => {
                self.received += 1;
                Ok(Some(count))
            },
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => Ok(None),
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                self.peer_unreachable += 1;
                Ok(None)
            },
            Err(error) => Err(error),
        }
    }

    pub fn wait(&self, until: u64) -> io::Result<()> {
        let remaining = until.saturating_sub(self.clock.now());
        if remaining < 1000 {
            std::hint::spin_loop();
            return Ok(());
        }
        let mut descriptor = libc::pollfd {
            fd: self.socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: descriptorは初期化済みで呼出中は生存する。
        if unsafe { libc::poll(&mut descriptor, 1, (remaining / 1000).min(POLL_MS as u64) as i32) } < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn report(&self) -> serde_json::Value {
        // UDP/IPv4/Ethernetの固定ヘッダ。ARP・FCS・preamble・IFGは含めない。
        const NETWORK_HEADERS: u64 = 8 + 20 + 14;
        serde_json::json!({ "transport":"udp_ipv4", "clock_domain":self.clock.domain,
            "network": { "sent":self.sent, "received":self.received, "wire_bytes":self.bytes + NETWORK_HEADERS * self.sent,
                "send_errors":self.send_errors, "malformed":self.malformed, "peer_unreachable":self.peer_unreachable } })
    }
}
