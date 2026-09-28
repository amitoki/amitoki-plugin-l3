use amitoki_l3_lab::config::Config;
use amitoki_plugin_sdk::relay::RelayError;
use serde::Deserialize;
use std::collections::HashSet;

// フレーム再構成の予約量を16MiB以下に保ち、未ACKの大きなフレームも保持する。
const DEFAULT_QUEUE_CAPACITY: usize = 256;
pub const MAX_PEERS: usize = 8;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    pub network: Config,
    pub peers: Vec<u32>,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
}

fn default_queue_capacity() -> usize {
    DEFAULT_QUEUE_CAPACITY
}

impl Options {
    pub fn validate(&self) -> Result<(), RelayError> {
        self.network.validate().map_err(RelayError::permanent)?;
        let distinct: HashSet<_> = self.peers.iter().collect();
        if self.peers.is_empty()
            || self.peers.len() > MAX_PEERS
            || distinct.len() != self.peers.len()
            || self.peers.iter().any(|peer| *peer == 0 || *peer == self.network.node || !self.network.routes.iter().any(|route| route.destination == *peer))
            || !(1..=DEFAULT_QUEUE_CAPACITY).contains(&self.queue_capacity)
        {
            return Err(RelayError::permanent("peersまたはqueue_capacityが範囲外です"));
        }
        if self.network.clock.authority.is_none() {
            return Err(RelayError::permanent("別マシン間の配送にはnetwork.clock.authorityを指定してください"));
        }
        Ok(())
    }
}
