use serde::{Deserialize, Serialize};

// RFC 9542のLocal Experimental。magicで同じ番号を使う別の実験と区別する。
pub const ETHER_TYPE: u16 = 0x88b5;
pub const HEADER_SIZE: usize = 64;
pub const ETHERNET_HEADER_SIZE: usize = 14;
// 標準MTU 1500に収め、試作ではフラグメントを扱わない。
pub const MAX_PAYLOAD: usize = 1400;
// 大きい本文をshort扱いして優先枠を占有できないようにする。
pub const MAX_SHORT_PAYLOAD: usize = 256;
pub const MAX_FRAME: usize = ETHERNET_HEADER_SIZE + HEADER_SIZE + MAX_PAYLOAD;
pub const MAX_LIFETIME_US: u64 = 1_000_000;
pub const INITIAL_HOPS: u8 = 16;
pub const REPLICA: u8 = 1;
const MAGIC: &[u8; 4] = b"AMTK";
const VERSION: u8 = 1;
const CHECKSUM_OFFSET: usize = 62;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    Short,
    Bulk,
}

impl Class {
    pub fn index(self) -> usize {
        match self {
            Self::Short => 0,
            Self::Bulk => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Data = 1,
    Ack = 2,
    Request = 3,
    Grant = 4,
    SyncRequest = 5,
    SyncReply = 6,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub kind: Kind,
    pub class: Class,
    pub hops: u8,
    pub source: u32,
    pub destination: u32,
    pub session: u64,
    pub message: u64,
    pub expires: u64,
    pub clock_domain: u64,
    pub credit: u64,
    pub path: u8,
    pub flags: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("独自L3パケットが不正です: {0}")]
pub struct PacketError(pub &'static str);

impl Packet {
    pub fn encode(&self) -> Result<Vec<u8>, PacketError> {
        self.validate()?;
        let mut bytes = vec![0; HEADER_SIZE + self.payload.len()];
        self.encode_into(&mut bytes)?;
        Ok(bytes)
    }

    pub fn encode_into(&self, buffer: &mut [u8]) -> Result<usize, PacketError> {
        self.validate()?;
        let length = HEADER_SIZE + self.payload.len();
        let bytes = buffer.get_mut(..length).ok_or(PacketError("出力バッファ長"))?;
        bytes[..HEADER_SIZE].fill(0);
        bytes[..4].copy_from_slice(MAGIC);
        bytes[4] = VERSION;
        bytes[5] = self.kind as u8;
        bytes[6] = self.class.index() as u8;
        bytes[7] = self.hops;
        bytes[8..12].copy_from_slice(&self.source.to_be_bytes());
        bytes[12..16].copy_from_slice(&self.destination.to_be_bytes());
        for (offset, value) in [
            (16, self.session),
            (24, self.message),
            (32, self.expires),
            (40, self.clock_domain),
            (48, self.credit),
        ] {
            bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
        }
        bytes[56..58].copy_from_slice(&(self.payload.len() as u16).to_be_bytes());
        bytes[58] = self.path;
        bytes[59] = self.flags;
        bytes[HEADER_SIZE..].copy_from_slice(&self.payload);
        let checksum = checksum(bytes);
        bytes[CHECKSUM_OFFSET..HEADER_SIZE].copy_from_slice(&checksum.to_be_bytes());
        Ok(length)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, PacketError> {
        if bytes.len() < HEADER_SIZE || &bytes[..4] != MAGIC || bytes[4] != VERSION {
            return Err(PacketError("magic/version/長さ"));
        }
        let length = u16::from_be_bytes([bytes[56], bytes[57]]) as usize;
        if length > MAX_PAYLOAD || bytes.len() != HEADER_SIZE + length || checksum(bytes) != 0 || bytes[60..62] != [0, 0] {
            return Err(PacketError("payload長/checksum/予約領域"));
        }
        let integer = |offset| u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("検証済みヘッダ"));
        let packet = Self {
            kind: match bytes[5] {
                1 => Kind::Data,
                2 => Kind::Ack,
                3 => Kind::Request,
                4 => Kind::Grant,
                5 => Kind::SyncRequest,
                6 => Kind::SyncReply,
                _ => return Err(PacketError("kind")),
            },
            class: match bytes[6] {
                0 => Class::Short,
                1 => Class::Bulk,
                _ => return Err(PacketError("class")),
            },
            hops: bytes[7],
            source: u32::from_be_bytes(bytes[8..12].try_into().expect("検証済みヘッダ")),
            destination: u32::from_be_bytes(bytes[12..16].try_into().expect("検証済みヘッダ")),
            session: integer(16),
            message: integer(24),
            expires: integer(32),
            clock_domain: integer(40),
            credit: integer(48),
            path: bytes[58],
            flags: bytes[59],
            payload: bytes[HEADER_SIZE..].to_vec(),
        };
        packet.validate()?;
        Ok(packet)
    }

    fn validate(&self) -> Result<(), PacketError> {
        if self.source == 0
            || self.destination == 0
            || self.session == 0
            || self.message == 0
            || self.clock_domain == 0
            || self.path == 0
            || self.hops == 0
            || self.flags & !REPLICA != 0
        {
            return Err(PacketError("識別子/経路/hop/flags"));
        }
        if self.payload.len() > MAX_PAYLOAD || (self.kind != Kind::Data && self.flags != 0) {
            return Err(PacketError("payload/flags"));
        }
        if self.kind == Kind::Data && self.class == Class::Short && self.payload.len() > MAX_SHORT_PAYLOAD {
            return Err(PacketError("short payload長"));
        }
        if self.is_sync() && (self.class != Class::Short || self.credit != 0 || self.expires != 0) {
            return Err(PacketError("時計制御のclass/credit/expires"));
        }
        match self.kind {
            Kind::Ack if self.payload.len() != 8 => return Err(PacketError("ACK長")),
            Kind::Grant | Kind::Request if !self.payload.is_empty() => return Err(PacketError("制御payload")),
            Kind::SyncRequest if self.payload.len() != 8 => return Err(PacketError("時計要求長")),
            Kind::SyncReply if self.payload.len() != 24 => return Err(PacketError("時計応答長")),
            _ => {},
        }
        Ok(())
    }

    pub fn valid_at(&self, now: u64, domain: u64) -> bool {
        self.clock_domain == domain && self.expires > now && self.expires - now <= MAX_LIFETIME_US
    }

    pub fn is_sync(&self) -> bool {
        matches!(self.kind, Kind::SyncRequest | Kind::SyncReply)
    }

    pub fn wire_size(&self) -> usize {
        ETHERNET_HEADER_SIZE + HEADER_SIZE + self.payload.len()
    }

    pub fn response(&self, kind: Kind) -> Self {
        Self {
            kind,
            class: self.class,
            source: self.destination,
            destination: self.source,
            hops: INITIAL_HOPS,
            flags: 0,
            payload: Vec::new(),
            session: self.session,
            message: self.message,
            expires: self.expires,
            clock_domain: self.clock_domain,
            credit: self.credit,
            path: self.path,
        }
    }
}

// 改ざん認証ではなく、実験時の破損を検出する16bit one's complement checksum。
fn checksum(bytes: &[u8]) -> u16 {
    let mut sum: u32 = bytes.chunks(2).map(|pair| u32::from(u16::from_be_bytes([pair[0], *pair.get(1).unwrap_or(&0)]))).sum();
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

// ACKで本文の一致を確認する。暗号学的認証には使用しない。
pub fn fingerprint(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3))
}

pub fn credit_token(grant: u32, slot: u32) -> u64 {
    (u64::from(grant) << 32) | u64::from(slot)
}
pub fn credit_parts(token: u64) -> (u32, u32) {
    ((token >> 32) as u32, token as u32)
}
