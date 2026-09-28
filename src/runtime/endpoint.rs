//! 双方向の信頼性配送。プラグインと実験CLIで同じNetwork/Channelを使う。
use super::{random_session, Network};
use crate::{
    config::Config,
    delivery::{Channel, ChannelOptions, ChannelState, DeliveredMessage, Ordering, Receiver, ReceiverOptions, SendTick, SubmitError, PREFIX_SIZE},
    packet::{Class, Kind, MAX_FRAME},
    tokens::TokenBucket,
};
use std::{collections::HashMap, io};

// 制御・受信処理の間に送信を占有しない。停止通知もこの周期で確認する。
const POLL_INTERVAL_US: u64 = 1_000;
const RETRY_BYTES_PER_SECOND: u64 = 1_000_000;

pub trait MessageSink {
    /// falseなら受信窓へも入れず、送信側に本文を保持させる。
    fn admit(&mut self, source: u32, payload: &[u8]) -> bool;
    fn deliver(&mut self, message: DeliveredMessage) -> io::Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Submission {
    pub peer: u32,
    pub class: Class,
    pub sequence: u64,
}

pub struct Endpoint {
    network: Network,
    peers: HashMap<u32, Vec<u8>>,
    channels: HashMap<(u32, Class), Channel>,
    receiver: Receiver,
    retry_budget: TokenBucket,
}

impl Endpoint {
    pub fn open(config: Config, peers: &[u32]) -> io::Result<Self> {
        let routes: HashMap<_, _> = peers
            .iter()
            .map(|peer| {
                let mut paths: Vec<_> = config.routes.iter().filter(|route| route.destination == *peer).map(|route| route.path).collect();
                paths.sort_unstable();
                (*peer, paths)
            })
            .collect();
        if peers.is_empty() || routes.len() != peers.len() || routes.values().any(|paths| paths.is_empty() || paths.len() > crate::fabric::MAX_PATHS) {
            return Err(io::Error::other("peerが重複しているか、経路がありません"));
        }
        let network = Network::open(config)?;
        let retry_budget = TokenBucket::new(RETRY_BYTES_PER_SECOND, MAX_FRAME as u64 * 2, network.clock.now());
        let receiver = Receiver::new(
            ReceiverOptions {
                rates: [100_000; 2],
                ..Default::default()
            },
            random_session(),
        )
        .map_err(io::Error::other)?;
        Ok(Self {
            network,
            peers: routes,
            channels: HashMap::new(),
            receiver,
            retry_budget,
        })
    }

    pub fn submit(&mut self, peer: u32, payload: &[u8]) -> Result<Submission, SubmitError> {
        let class = if payload.len() <= crate::packet::MAX_SHORT_PAYLOAD - PREFIX_SIZE {
            Class::Short
        } else {
            Class::Bulk
        };
        let paths = self.peers.get(&peer).ok_or(SubmitError::Closed)?;
        let channel = self.channels.entry((peer, class)).or_insert_with(|| {
            Channel::new(ChannelOptions {
                class,
                fabric: self.network.fabric,
                channel: class.index() as u32 + 1,
                ordering: Ordering::Unordered,
                paths: paths.clone(),
                ..ChannelOptions::new(self.network.node, peer, random_session())
            })
            .expect("Endpointで検証済みのchannel")
        });
        let sequence = channel.try_send(payload, self.network.clock.now())?;
        Ok(Submission { peer, class, sequence })
    }

    pub fn poll(&mut self, sink: &mut impl MessageSink) -> io::Result<Vec<Submission>> {
        let mut confirmed = Vec::new();
        for packet in self.network.receive()? {
            if packet.destination != self.network.node || !self.peers.contains_key(&packet.source) || !packet.is_reliable() {
                continue;
            }
            let now = self.network.clock.now();
            if packet.is_reliable_response() {
                if let Some(channel) = self.channels.get_mut(&(packet.source, packet.class)) {
                    if let Some(sequence) = channel.receive(&packet, now) {
                        confirmed.push(Submission {
                            peer: packet.source,
                            class: packet.class,
                            sequence,
                        });
                    }
                }
                continue;
            }
            if packet.kind == Kind::ReliableData && !sink.admit(packet.source, &packet.payload[PREFIX_SIZE..]) {
                continue;
            }
            let mut response = self.receiver.receive(&packet, now);
            while let Some(message) = self.receiver.take_delivery() {
                sink.deliver(message)?;
            }
            if let Some(response) = &mut response {
                self.receiver.refresh_ack(response);
            }
            if let Some(response) = response {
                self.network.enqueue(response);
            }
        }
        for channel in self.channels.values_mut() {
            let tick = SendTick {
                now: self.network.clock.now(),
                time: self.network.reliable_time(),
                retry_budget: &mut self.retry_budget,
            };
            channel.transmit(tick, |packet| self.network.try_enqueue(packet));
        }
        self.receiver.prune(self.network.clock.now());
        self.network.flush()?;
        self.network.observe(|| serde_json::json!(self.channels.values().map(Channel::report).collect::<Vec<_>>()));
        Ok(confirmed)
    }

    pub fn failed(&self) -> Option<ChannelState> {
        self.channels.values().find(|channel| channel.is_closed()).map(Channel::state)
    }

    pub fn reset_senders(&mut self) {
        // 再試行は新しいsessionにする。アプリのFrame.idで重複を排除する。
        self.channels.clear();
    }

    pub fn wait(&self) -> io::Result<()> {
        self.network.wait(self.network.clock.now() + POLL_INTERVAL_US)
    }
}
