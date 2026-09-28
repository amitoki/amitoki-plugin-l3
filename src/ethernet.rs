use crate::{
    config::{parse_mac, Link},
    packet::{Packet, PacketError, ETHERNET_HEADER_SIZE, ETHER_TYPE, MAX_FRAME},
};
use std::{
    ffi::CString,
    io, mem,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

pub struct Ethernet {
    socket: OwnedFd,
    address: libc::sockaddr_ll,
    local_mac: [u8; 6],
    peer_mac: [u8; 6],
}

impl Ethernet {
    pub fn open(link: &Link) -> io::Result<Self> {
        let name = CString::new(link.interface.as_str()).map_err(io::Error::other)?;
        // interface名はConfig::validateでパス区切りと長さを検査済み。
        let local_mac = parse_mac(&std::fs::read_to_string(format!("/sys/class/net/{}/address", link.interface))?).map_err(io::Error::other)?;
        let peer_mac = parse_mac(&link.peer_mac).map_err(io::Error::other)?;
        // SAFETY: NUL終端の名前と固定のAF_PACKET引数を渡す。
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        if index == 0 {
            return Err(io::Error::last_os_error());
        }
        let descriptor = unsafe { libc::socket(libc::AF_PACKET, libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, i32::from(ETHER_TYPE.to_be())) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: この関数が作ったfdを一度だけ所有する。
        let socket = unsafe { OwnedFd::from_raw_fd(descriptor) };
        let mut address: libc::sockaddr_ll = unsafe { mem::zeroed() };
        address.sll_family = libc::AF_PACKET as u16;
        address.sll_protocol = ETHER_TYPE.to_be();
        address.sll_ifindex = index as i32;
        address.sll_halen = 6;
        address.sll_addr[..6].copy_from_slice(&peer_mac);
        // SAFETY: sockaddr_llとその長さが一致し、呼出中は生存する。
        check(unsafe {
            libc::bind(
                socket.as_raw_fd(),
                (&address as *const libc::sockaddr_ll).cast(),
                mem::size_of_val(&address) as libc::socklen_t,
            )
        })?;
        let ignore: libc::c_int = 1;
        // 自分の注入を再び受信してループさせない。
        check(unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_PACKET,
                libc::PACKET_IGNORE_OUTGOING,
                (&ignore as *const libc::c_int).cast(),
                mem::size_of_val(&ignore) as libc::socklen_t,
            )
        })?;
        Ok(Self {
            socket,
            address,
            local_mac,
            peer_mac,
        })
    }

    pub fn receive(&self) -> io::Result<Option<Result<Packet, PacketError>>> {
        let mut frame = [0_u8; MAX_FRAME];
        // MSG_TRUNCで元の長さを得て、切り詰められたフレームを誤って採用しない。
        let count = unsafe { libc::recv(self.socket.as_raw_fd(), frame.as_mut_ptr().cast(), frame.len(), libc::MSG_DONTWAIT | libc::MSG_TRUNC) };
        if count < 0 {
            let error = io::Error::last_os_error();
            return if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        let count = count as usize;
        if count < ETHERNET_HEADER_SIZE || count > frame.len() || frame[..6] != self.local_mac || frame[6..12] != self.peer_mac || frame[12..14] != ETHER_TYPE.to_be_bytes() {
            return Ok(Some(Err(PacketError("Ethernet長/MAC/EtherType"))));
        }
        Ok(Some(Packet::decode(&frame[ETHERNET_HEADER_SIZE..count])))
    }

    pub fn send(&self, packet: &Packet) -> io::Result<()> {
        let mut frame = [0_u8; MAX_FRAME];
        frame[..6].copy_from_slice(&self.peer_mac);
        frame[6..12].copy_from_slice(&self.local_mac);
        frame[12..14].copy_from_slice(&ETHER_TYPE.to_be_bytes());
        let length = ETHERNET_HEADER_SIZE + packet.encode_into(&mut frame[ETHERNET_HEADER_SIZE..]).map_err(io::Error::other)?;
        // SAFETY: frameとaddressは呼出中に生存し、それぞれ正確な長さを渡す。
        let count = unsafe {
            libc::sendto(
                self.socket.as_raw_fd(),
                frame.as_ptr().cast(),
                length,
                libc::MSG_DONTWAIT,
                (&self.address as *const libc::sockaddr_ll).cast(),
                mem::size_of_val(&self.address) as libc::socklen_t,
            )
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        if count as usize != length {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "Ethernetフレームの送信が不完全です"));
        }
        Ok(())
    }

    pub fn poll_descriptor(&self) -> libc::pollfd {
        libc::pollfd {
            fd: self.socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }
    }
}

fn check(status: libc::c_int) -> io::Result<()> {
    if status < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
