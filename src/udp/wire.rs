//! UDPの比較用ヘッダ。本文を往復させず、L3と同じ8バイトのfingerprintをACKする。
use crate::packet::fingerprint;

pub const HEADER: usize = 32;
pub const MAX_DATAGRAM: usize = HEADER + crate::packet::MAX_PAYLOAD;
const MAGIC: &[u8; 4] = b"AMTB";
const DATA: u8 = 1;
const ACK: u8 = 2;

pub struct Message<'a> {
    pub acknowledgment: bool,
    pub class: usize,
    pub session: u64,
    pub id: u64,
    pub hash: u64,
    pub payload: &'a [u8],
}

impl Message<'_> {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER + self.payload.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&[1, if self.acknowledgment { ACK } else { DATA }, self.class as u8, 0]);
        bytes.extend_from_slice(&self.session.to_be_bytes());
        bytes.extend_from_slice(&self.id.to_be_bytes());
        bytes.extend_from_slice(&self.hash.to_be_bytes());
        bytes.extend_from_slice(self.payload);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Message<'_>> {
        if !(HEADER..=MAX_DATAGRAM).contains(&bytes.len()) || &bytes[..4] != MAGIC || bytes[4] != 1 || ![DATA, ACK].contains(&bytes[5]) || bytes[6] > 1 || bytes[7] != 0 {
            return None;
        }
        let number = |offset| u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("検証済みヘッダ"));
        let message = Message {
            acknowledgment: bytes[5] == ACK,
            class: bytes[6] as usize,
            session: number(8),
            id: number(16),
            hash: number(24),
            payload: &bytes[HEADER..],
        };
        if message.acknowledgment && !message.payload.is_empty() || !message.acknowledgment && (message.payload.is_empty() || fingerprint(message.payload) != message.hash) {
            return None;
        }
        Some(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corrupted_and_truncated_datagrams_are_rejected() {
        let payload = [0x5a; 128];
        let original = Message {
            acknowledgment: false,
            class: 0,
            session: 1,
            id: 2,
            hash: fingerprint(&payload),
            payload: &payload,
        }
        .encode();
        assert_eq!(Message::decode(&original).unwrap().payload, payload);
        for index in 0..original.len() {
            let mut damaged = original.clone();
            damaged[index] ^= 0x80;
            // session/idは送信側の未応答表で照合し、payloadはfingerprintで検証する。
            if (8..24).contains(&index) {
                continue;
            }
            assert!(Message::decode(&damaged).is_none());
        }
        for length in 0..original.len() {
            assert!(Message::decode(&original[..length]).is_none());
        }
    }
}
