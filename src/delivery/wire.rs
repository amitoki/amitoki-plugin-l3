use crate::packet::{Kind, Packet, PacketError};
use serde::{Deserialize, Serialize};

// 既存64Bヘッダを保ち、信頼性配送だけ本文先頭にchannelと受信世代を載せる。
pub const PREFIX_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Ordering {
    #[default]
    Unordered,
    Ordered,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Metadata {
    pub channel: u32,
    pub ordering: Ordering,
    pub epoch: u64,
}

impl Metadata {
    pub fn encode(self, body: &[u8]) -> Vec<u8> {
        let mut payload = vec![0; PREFIX_SIZE];
        payload[..4].copy_from_slice(&self.channel.to_be_bytes());
        payload[4] = u8::from(self.ordering == Ordering::Ordered);
        payload[8..16].copy_from_slice(&self.epoch.to_be_bytes());
        payload.extend_from_slice(body);
        payload
    }

    pub fn decode(packet: &Packet) -> Option<Self> {
        let prefix = packet.payload.get(..PREFIX_SIZE)?;
        if prefix[5..8] != [0; 3] {
            return None;
        }
        let ordering = match prefix[4] {
            0 => Ordering::Unordered,
            1 => Ordering::Ordered,
            _ => return None,
        };
        Some(Self {
            channel: u32::from_be_bytes(prefix[..4].try_into().ok()?),
            ordering,
            epoch: u64::from_be_bytes(prefix[8..16].try_into().ok()?),
        })
    }
}

pub(crate) fn validate(packet: &Packet) -> Result<(), PacketError> {
    let metadata = Metadata::decode(packet).ok_or(PacketError("信頼性配送のメタデータ"))?;
    if metadata.channel == 0 || (packet.kind != Kind::ReliableOpen && metadata.epoch == 0) {
        return Err(PacketError("channel/受信世代"));
    }
    let length = packet.payload.len();
    let valid = match packet.kind {
        Kind::ReliableData => length > PREFIX_SIZE && packet.credit == 0 && packet.message < u64::MAX,
        Kind::ReliableAck => length == PREFIX_SIZE + 8 && packet.credit > 0,
        Kind::ReliableReady => length == PREFIX_SIZE && (1..=super::MAX_WINDOW as u64).contains(&packet.credit),
        Kind::ReliableOpen | Kind::ReliableReset => length == PREFIX_SIZE && packet.credit == 0,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(PacketError("信頼性配送の本文/窓"))
    }
}
