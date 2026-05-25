//! Peer-to-peer gossip transport for state-based CRDTs.
//!
//! The crate exposes a generic [`GossipEngine`] over any type implementing
//! [`crdt_core::DeltaCrdt`] plus serde traits. The engine supports two
//! sync modes: full-state [`Sync`](GossipMessage::Sync) for first contact
//! with a new peer, and incremental [`SyncDelta`](GossipMessage::SyncDelta)
//! for established peers. Peer discovery is handled via mDNS on the local
//! subnet and peer-list gossip for transitive cross-subnet discovery.
//! 
//! The engine requires [`DeltaCrdt`](crdt_core::DeltaCrdt) (which
//! extends [`Crdt`](crdt_core::Crdt)). Full-state sync uses the
//! `Crdt::merge` trait, while delta sync uses `DeltaCrdt::delta_since`.

// Internal modules. Types are re-exported at the crate root below. Keeping
// the modules `pub(crate)` prevents external callers from depending on
// internal items (e.g. `PeerRegistry`, raw codec helpers) via the module
// path. If a new public surface item is needed, re-export it explicitly.
pub(crate) mod config;
pub(crate) mod discovery;
pub(crate) mod engine;
pub(crate) mod message;

pub use config::GossipConfig;
pub use engine::GossipEngine;
pub use message::{GossipMessage, MAX_FRAME, PeerEntry};
