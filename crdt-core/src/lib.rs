//! General-purpose state-based CRDT library.
//!
//! Implements nine conflict-free replicated data types. Each type
//! guarantees convergence, meaning that replicas can be merged in any order, by any
//! grouping, and any number of times. They will always produce the same result.
//!
//! The library is just pure data tructures and operations.
//! It is designed to be composed into application-specific documents
//! and synced by an external transport layer.
//!
//! ## Traits
//!
//! All CRDTs implement [`Crdt`] with three operations:
//!
//! - [`value`](Crdt::value): read the current state, stripped of
//!   internal metadata.
//! - [`merge`](Crdt::merge): combine two replicas into one.
//!   Commutative, associative, and idempotent.
//! - [`compare`](Crdt::compare): check if one replica is a subset of
//!   another (i.e. merge would be a useless operation).
//!
//! All crdts will also have a update method (or more, e.g. delete also),
//! but this is type specific, and therefore not part of this trait.
//!
//! Types that support incremental replication also implement
//! [`DeltaCrdt`], which extends `Crdt` with:
//!
//! - [`version`](DeltaCrdt::version): a compact summary of what this
//!   replica knows.
//! - [`delta_since`](DeltaCrdt::delta_since): compute only what the
//!   receiver is missing.
//! - [`merge_delta`](DeltaCrdt::merge_delta): apply a delta with the
//!   same convergence guarantees as full merge.
//!
//! ## Modules
//!
//! - [`clocks`]: `VectorClock` for causality tracking and timestamp
//!   generation
//! - [`counters`]: `GCounter` (grow-only), `PNCounter`
//!   (positive-negative)
//! - [`sets`]: `GSet` (grow-only), `TwoPSet` (two-phase),
//!   `ORSet` (observed-remove, add-wins)
//! - [`registers`]: `LWWRegister` (last-writer-wins),
//!   `MVRegister` (multi-value)
//! - [`maps`] : `LWWMap` (per-key last-writer-wins)
//!
//! ## Feature flags
//!
//! - **`serde`**: enables `Serialize` and `Deserialize` on all CRDT
//!   types. Off by default, since it is not strictly required (eg. if crdts
//!   are used by multiple threads in the same process). Enable this if
//!   serialization is needet for eg. networking.
//!
//! ## Example
//!
//! ```
//! use crdt_core::{Crdt, DeltaCrdt};
//! use crdt_core::counters::GCounter;
//! use uuid::Uuid;
//!
//! let node_a = Uuid::new_v4();
//! let node_b = Uuid::new_v4();
//!
//! // Two peers increment independently
//! let mut peer_a = GCounter::new();
//! peer_a.increment(node_a);
//! peer_a.increment(node_a);
//!
//! let mut peer_b = GCounter::new();
//! peer_b.increment(node_b);
//!
//! //Commutative, order doesn't matter
//! let mut merged = peer_a.clone();
//! merged.merge(peer_b.clone());
//! assert_eq!(merged.value(), 3);
//!
//! // Send only what the receiver is missing
//! let delta = peer_a.delta_since(&peer_b.version());
//! peer_b.merge_delta(delta);
//! assert_eq!(peer_b.value(), 3);
//! ```
//!
//! The same pattern applies to every type in the library: create,
//! mutate, merge (or delta), read.
//!
//! ## Testing
//!
//! Every CRDT is tested at two levels:
//! - **Unit tests** verify type-specific behavior: add-wins in ORSet,
//!   permanent removal in TwoPSet, timestamp tiebreaking in LWWRegister,
//!   concurrent write preservation in MVRegister, and so on.
//! - **Property-based tests** (`proptest`) verify the three CRDT pomises:
//!   commutativity, associativity, and idempotency. This is done across hundreds of
//!   randomly generated inputs. If any input violates a promise, proptest
//!   shrinks it to the smallest failing case.
//!
//! Types implementing [`DeltaCrdt`] are additionally tested for delta
//! correctness.
pub mod clocks;
pub mod counters;
pub mod maps;
pub mod registers;
pub mod sets;
pub mod traits;

pub use traits::{Crdt, DeltaCrdt, NodeId};
