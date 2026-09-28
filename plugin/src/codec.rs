//! L3の本文上限を超えるEthernetフレームを固定長の断片へ分割する。
use amitoki_l3_lab::{delivery::PREFIX_SIZE, packet::MAX_PAYLOAD};
use amitoki_plugin_sdk::relay::{Frame, MAX_FRAME_SIZE, MIN_FRAME_SIZE};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAGIC: &[u8; 4] = b"ARL1";
// magic + channelのSHA256 + UUID + 全長/offset + 本文のSHA256。
const HEADER_SIZE: usize = 92;
pub const CHUNK_SIZE: usize = MAX_PAYLOAD - PREFIX_SIZE - HEADER_SIZE;
pub type DigestBytes = [u8; 32];

pub fn digest(bytes: &[u8]) -> DigestBytes {
    Sha256::digest(bytes).into()
}

pub struct Fragment<'a> {
    pub id: Uuid,
    pub total: usize,
    pub offset: usize,
    pub digest: DigestBytes,
    pub body: &'a [u8],
}

pub fn encode(frame: &Frame, channel: &DigestBytes) -> Vec<Vec<u8>> {
    let hash = digest(&frame.bytes);
    frame
        .bytes
        .chunks(CHUNK_SIZE)
        .enumerate()
        .map(|(index, body)| {
            let mut payload = Vec::with_capacity(HEADER_SIZE + body.len());
            payload.extend_from_slice(MAGIC);
            payload.extend_from_slice(channel);
            payload.extend_from_slice(frame.id.as_bytes());
            payload.extend_from_slice(&(frame.bytes.len() as u32).to_be_bytes());
            payload.extend_from_slice(&((index * CHUNK_SIZE) as u32).to_be_bytes());
            payload.extend_from_slice(&hash);
            payload.extend_from_slice(body);
            payload
        })
        .collect()
}

pub fn decode<'a>(payload: &'a [u8], channel: &DigestBytes) -> Option<Fragment<'a>> {
    if payload.len() <= HEADER_SIZE || payload.len() > MAX_PAYLOAD - PREFIX_SIZE || &payload[..4] != MAGIC || &payload[4..36] != channel {
        return None;
    }
    let total = u32::from_be_bytes(payload[52..56].try_into().ok()?) as usize;
    let offset = u32::from_be_bytes(payload[56..60].try_into().ok()?) as usize;
    let body = &payload[HEADER_SIZE..];
    if !(MIN_FRAME_SIZE..=MAX_FRAME_SIZE).contains(&total) || offset >= total || !offset.is_multiple_of(CHUNK_SIZE) || body.len() != (total - offset).min(CHUNK_SIZE) {
        return None;
    }
    Some(Fragment {
        id: Uuid::from_slice(&payload[36..52]).ok()?,
        total,
        offset,
        digest: payload[60..92].try_into().ok()?,
        body,
    })
}
