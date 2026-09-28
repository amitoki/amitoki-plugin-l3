use crate::scheduler::Scheduling;
use serde::Deserialize;
use std::{collections::HashSet, path::Path};

// 試験の設定ミスでsocket・経路表・帯域予算を際限なく増やさない。
const MAX_LINKS: usize = 8;
const MAX_ROUTES: usize = 64;
pub const MAX_BYTES_PER_SECOND: u64 = 100_000_000_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    pub interface: String,
    pub peer_mac: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub destination: u32,
    pub path: u8,
    pub interface: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: u32,
    pub links: Vec<Link>,
    pub routes: Vec<Route>,
    pub scheduler: Scheduling,
    pub bytes_per_second: u64,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let config: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.node == 0
            || self.links.is_empty()
            || self.links.len() > MAX_LINKS
            || self.routes.len() > MAX_ROUTES
            || self.bytes_per_second == 0
            || self.bytes_per_second > MAX_BYTES_PER_SECOND
        {
            return Err("node/links/routes/bytes_per_secondの範囲が不正です");
        }
        let mut names = HashSet::new();
        for link in &self.links {
            if link.interface.is_empty()
                || link.interface.len() >= libc::IFNAMSIZ
                || !link.interface.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                || !names.insert(&link.interface)
                || parse_mac(&link.peer_mac).is_err()
            {
                return Err("interfaceまたはpeer_macが不正です");
            }
        }
        let mut routes = HashSet::new();
        for route in &self.routes {
            if route.destination == 0 || route.destination == self.node || route.path == 0 || !names.contains(&route.interface) || !routes.insert((route.destination, route.path)) {
                return Err("経路が不正または重複しています");
            }
        }
        Ok(())
    }
}

pub fn parse_mac(value: &str) -> Result<[u8; 6], &'static str> {
    let octets: Vec<_> = value.trim().split(':').collect();
    if octets.len() != 6 {
        return Err("MAC長が不正です");
    }
    let mut address = [0; 6];
    for (destination, octet) in address.iter_mut().zip(octets) {
        if octet.len() != 2 {
            return Err("MACの桁数が不正です");
        }
        *destination = u8::from_str_radix(octet, 16).map_err(|_| "MACが不正です")?;
    }
    if address == [0; 6] || address[0] & 1 != 0 {
        return Err("unicast MACを指定してください");
    }
    Ok(address)
}
