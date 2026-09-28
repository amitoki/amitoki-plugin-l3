use super::Signal;
use crate::{
    delivery::PREFIX_SIZE,
    packet::{fingerprint, Kind, Packet, REPLICA},
};

/// 本文は破棄し、送信側が照合できるfingerprintだけを転送する。
pub fn trim(mut packet: Packet, signal: Signal) -> Option<Packet> {
    if packet.kind != Kind::ReliableData || packet.payload.len() <= PREFIX_SIZE {
        return None;
    }
    let hash = fingerprint(&packet.payload[PREFIX_SIZE..]);
    packet.payload.truncate(PREFIX_SIZE);
    packet.payload.extend_from_slice(&hash.to_be_bytes());
    packet.kind = Kind::ReliableTrim;
    packet.flags &= !REPLICA;
    packet.signal.observe(signal);
    Some(packet)
}
