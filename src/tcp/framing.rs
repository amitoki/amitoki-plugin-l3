use crate::packet::{Class, MAX_PAYLOAD};
use std::io;

// TCPのバイト列へメッセージ境界・種別・sequenceを追加する比較用形式。
pub const HEADER_SIZE: usize = 16;
const MAGIC: &[u8; 2] = b"AT";

#[derive(Debug, PartialEq, Eq)]
pub struct Frame {
    pub acknowledgement: bool,
    pub class: Class,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = vec![0; HEADER_SIZE];
        bytes[..2].copy_from_slice(MAGIC);
        bytes[2] = u8::from(self.acknowledgement);
        bytes[3] = self.class.index() as u8;
        bytes[4..8].copy_from_slice(&(self.payload.len() as u32).to_be_bytes());
        bytes[8..16].copy_from_slice(&self.sequence.to_be_bytes());
        bytes.extend_from_slice(&self.payload);
        Ok(bytes)
    }

    fn validate(&self) -> io::Result<()> {
        if self.sequence == 0 || self.payload.is_empty() || self.payload.len() > MAX_PAYLOAD || (self.acknowledgement && self.payload.len() != 8) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "TCP比較フレームのsequence/本文長が不正です"));
        }
        Ok(())
    }
}

pub fn decode(bytes: &[u8]) -> io::Result<Option<(Frame, usize)>> {
    if bytes.len() < HEADER_SIZE {
        return Ok(None);
    }
    if &bytes[..2] != MAGIC || bytes[2] > 1 || bytes[3] > 1 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "TCP比較フレームのヘッダが不正です"));
    }
    let length = u32::from_be_bytes(bytes[4..8].try_into().expect("固定ヘッダ")) as usize;
    if length == 0 || length > MAX_PAYLOAD || (bytes[2] == 1 && length != 8) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "TCP比較フレームの本文長が不正です"));
    }
    let end = HEADER_SIZE + length;
    let Some(payload) = bytes.get(HEADER_SIZE..end) else { return Ok(None) };
    let frame = Frame {
        acknowledgement: bytes[2] == 1,
        class: if bytes[3] == 0 { Class::Short } else { Class::Bulk },
        sequence: u64::from_be_bytes(bytes[8..16].try_into().expect("固定ヘッダ")),
        payload: payload.to_vec(),
    };
    frame.validate()?;
    Ok(Some((frame, end)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Frame {
        Frame {
            acknowledgement: false,
            class: Class::Bulk,
            sequence: 13,
            payload: vec![42; 1200],
        }
    }

    #[test]
    fn partial_reads_wait_for_the_complete_message_and_preserve_the_next_frame() {
        let frame = sample();
        let bytes = frame.encode().unwrap();
        for length in 0..bytes.len() {
            assert_eq!(decode(&bytes[..length]).unwrap(), None);
        }
        let pair = [bytes.clone(), bytes.clone()].concat();
        assert_eq!(decode(&pair).unwrap(), Some((frame, bytes.len())));
    }

    #[test]
    fn oversized_or_malformed_frames_fail_before_waiting_for_a_body() {
        let mut bytes = sample().encode().unwrap();
        bytes[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode(&bytes[..HEADER_SIZE]).is_err());
        let mut bytes = sample().encode().unwrap();
        bytes[2] = 1;
        assert!(decode(&bytes[..HEADER_SIZE]).is_err());
    }
}
