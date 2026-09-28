use super::framing::{decode, Frame};
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::TcpStream,
    os::fd::AsRawFd,
};

// 2クラス各256件を扱い、未送信のフレームを無制限に積まない。
const QUEUE_LIMIT: usize = 512;
const WRITE_BURST: usize = 32;
// 1回のreadで最大約170件のACK。空き256件を確保してから受信する。
const READ_BUFFER_SIZE: usize = 4096;
const RECEIVE_RESERVE: usize = 256;

pub struct Connection {
    stream: TcpStream,
    outbound: VecDeque<Vec<u8>>,
    written: usize,
    inbound: Vec<u8>,
    pub closed: bool,
}

impl Connection {
    pub fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            outbound: VecDeque::new(),
            written: 0,
            inbound: Vec::new(),
            closed: false,
        })
    }

    pub fn enqueue(&mut self, frame: &Frame) -> io::Result<bool> {
        if self.outbound.len() >= QUEUE_LIMIT {
            return Ok(false);
        }
        self.outbound.push_back(frame.encode()?);
        Ok(true)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        for _ in 0..WRITE_BURST {
            let Some(bytes) = self.outbound.front() else { break };
            match self.stream.write(&bytes[self.written..]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => {
                    self.written += count;
                    if self.written == bytes.len() {
                        self.outbound.pop_front();
                        self.written = 0;
                    }
                },
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub fn receive(&mut self) -> io::Result<Vec<Frame>> {
        if self.closed || self.outbound.len() > QUEUE_LIMIT - RECEIVE_RESERVE {
            return Ok(Vec::new());
        }
        let mut buffer = [0; READ_BUFFER_SIZE];
        match self.stream.read(&mut buffer) {
            Ok(0) => {
                self.closed = true;
                if !self.inbound.is_empty() {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            },
            Ok(count) => self.inbound.extend_from_slice(&buffer[..count]),
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => {},
            Err(error) => return Err(error),
        }
        let mut consumed = 0;
        let mut frames = Vec::new();
        while let Some((frame, length)) = decode(&self.inbound[consumed..])? {
            frames.push(frame);
            consumed += length;
        }
        self.inbound.drain(..consumed);
        Ok(frames)
    }

    pub fn descriptor(&self) -> libc::pollfd {
        libc::pollfd {
            fd: self.stream.as_raw_fd(),
            events: libc::POLLIN | if self.outbound.is_empty() { 0 } else { libc::POLLOUT },
            revents: 0,
        }
    }

    pub fn congestion_control(&self) -> io::Result<String> {
        let mut name = [0_u8; 32];
        let mut length = name.len() as libc::socklen_t;
        // SAFETY: 書込み先はlengthで指定した領域があり、fdは有効なTCP socket。
        if unsafe { libc::getsockopt(self.stream.as_raw_fd(), libc::IPPROTO_TCP, libc::TCP_CONGESTION, name.as_mut_ptr().cast(), &mut length) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(String::from_utf8_lossy(&name[..length as usize]).trim_end_matches('\0').to_owned())
    }
}

pub fn wait(connections: &[Connection], duration_us: u64) -> io::Result<()> {
    if duration_us < 1000 {
        std::hint::spin_loop();
        return Ok(());
    }
    let mut descriptors: Vec<_> = connections.iter().filter(|connection| !connection.closed).map(Connection::descriptor).collect();
    // 同じ周期で生成予定を確認する。readyなsocketがあれば直ちに戻る。
    let milliseconds = (duration_us / 1000).min(1) as i32;
    // SAFETY: descriptorsには初期化済みのpollfdが要素数分ある。
    if unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as libc::nfds_t, milliseconds) } < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
