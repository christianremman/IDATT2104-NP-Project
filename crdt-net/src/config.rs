//! Configuration for the gossip engine.
//!
//! [`GossipConfig`] uses a builder pattern: construct with [`new`](GossipConfig::new),
//! then chain `.with_*` methods to customize. Only `node_id` and `gossip_addr` are required.
use std::net::SocketAddr;
use std::time::Duration;

use uuid::Uuid;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

/// Configuration for a [`GossipEngine`](crate::GossipEngine) instance.
///
/// All fields have sensible defaults: 5-second gossip interval, mDNS
/// enabled, no bootstrap peers. Use the `with_*` builder methods to
/// override.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GossipConfig {
    pub node_id: Uuid,
    pub gossip_addr: SocketAddr,
    /// Address others should use to reach this node (what we put in `from.addr`
    /// of outgoing `Sync` messages and announce over mDNS). If `None`, derived
    /// from `gossip_addr` at engine startup , when `gossip_addr` is a wildcard
    /// (`0.0.0.0`/`::`) the engine falls back to the first non-loopback local
    /// IPv4.
    pub advertise_addr: Option<SocketAddr>,
    /// Bootstrap peers whose `node_id` isn't yet known. The engine tries
    /// gossiping to these addresses each tick until one responds, after which
    /// the peer is migrated into the resolved peer map.
    pub peers: Vec<SocketAddr>,
    pub interval: Duration,
    pub enable_mdns: bool,
}

impl GossipConfig {
    pub fn new(node_id: Uuid, gossip_addr: SocketAddr) -> Self {
        Self {
            node_id,
            gossip_addr,
            advertise_addr: None,
            peers: Vec::new(),
            interval: DEFAULT_INTERVAL,
            enable_mdns: true,
        }
    }

    pub fn with_peers(mut self, peers: Vec<SocketAddr>) -> Self {
        self.peers = peers;
        self
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_advertise_addr(mut self, addr: SocketAddr) -> Self {
        self.advertise_addr = Some(addr);
        self
    }

    pub fn with_mdns(mut self, enable: bool) -> Self {
        self.enable_mdns = enable;
        self
    }
}
