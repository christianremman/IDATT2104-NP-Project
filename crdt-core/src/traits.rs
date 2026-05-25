//! Core traits that all CRDTs implement.
//!
//! [`Crdt`] defines the three operations every CRDT must support:
//! `value`, `merge`, and `compare`. [`DeltaCrdt`] extends this with
//! delta-state support for bandwidth-efficient replication (only the
//! changes since last version seen).
use uuid::Uuid;

/// Unique identifier for a node in the P2P network.
pub type NodeId = Uuid;

/// Trait for state-based Conflict-free Replicated Data Types
///
/// All CRDTs should have four operations, where three of them is
/// implemented the same way for all (part of this trait).
/// - `value`: reads the current state.
/// - `compare`: check if self is a part of other replica.
/// - `merge`: merge another replica to self.
/// - `update`: type specific, and not part of this trait.
///
/// Requires `Clone` because `merge` consumes `other`, so callers
/// must be able to clone replicas to merge while keeping a copy.
pub trait Crdt: Clone {
    type Value;

    /// Query the current value of this CRDT
    ///
    /// This clones the internal state so the caller gets an
    /// independent copy. Modifying the returned value does not
    /// affect the CRDT.
    fn value(&self) -> Self::Value;

    /// Merge another replica into this one
    /// Guarantees convergence. The result is the same
    /// regardless of merge order, grouping, or repetition.
    ///
    /// Takes ownership of `other` because merge is a one-way absorption.
    /// After merging, `other`'s data lives inside `self` and `other` has
    /// no further purpose. Consuming it avoids unnecessary cloning and
    /// lets the compiler prevent accidental use of the stale replica.
    /// Callers who need `other` alive after merging can `.clone()` before calling.
    ///
    /// Merge order:
    /// `A.merge(B) == B.merge(A)`
    /// In P2P, you can't control which peer's state arrives first.
    ///
    /// Grouping:
    /// `A.merge(B).merge(C) == A.merge(B.merge(C))`
    /// It doesn't matter which two peers you merge first.
    ///
    /// Repetition
    /// `A.merge(A) == A`
    /// Network messages can be duplicated or retransmitted safely.
    fn merge(&mut self, other: Self);

    /// Returns `true` if self is a subset of other.
    ///
    /// If `self.compare(other)` is `true`, then `self.merge(other)` would
    /// result in `other` (i.e. merging is not required for the other side).
    ///
    /// Borrows `other` because compare is a read-only check,
    /// both replicas remain unchanged and usable afterward.
    fn compare(&self, other: &Self) -> bool;
}

/// Extension of [`Crdt`] for delta-state replication.
///
/// Where [`Crdt::merge`] absorbs an entire replica, [`DeltaCrdt`] lets a
/// sender ship only the part of its state the receiver does not yet have.
/// The receiver applies the delta via [`merge_delta`](Self::merge_delta),
/// which preserves the same convergence guarantees as `merge`.
///
/// # Soundness
///
/// For every pair of replicas `(a, b)` of the same `DeltaCrdt`:
///
/// ```text
/// let delta = a.delta_since(&b.version());
/// b.merge_delta(delta);
/// assert!(a.compare(&b));  // b now knows everything a knew
/// ```
///
/// `merge_delta` must remain commutative, associative, and idempotent so
/// out-of-order / duplicated deltas are safe.
pub trait DeltaCrdt: Crdt {
    /// Compact, mergeable description of "what `self` knows that a peer at
    /// `Version` does not".
    type Delta: Clone;

    /// Cheap summary of the causal frontier a peer needs to ship a useful
    /// delta. Typically a vector clock or a per-node `HashMap<NodeId, u64>`.
    type Version: Clone;

    /// Returns the sender's current version, suitable to pass back as the
    /// `since` argument of [`delta_since`](Self::delta_since) on a peer.
    fn version(&self) -> Self::Version;

    /// Computes the delta of `self` relative to the receiver's `since`
    /// version.
    fn delta_since(&self, since: &Self::Version) -> Self::Delta;

    /// Applies a delta. Equivalent to `merge` for the subset of state the
    /// delta covers; idempotent.
    fn merge_delta(&mut self, delta: Self::Delta);

    /// True when `delta` would not change any replica's state. Used by the
    /// network layer to skip empty broadcasts.
    fn is_empty_delta(delta: &Self::Delta) -> bool;

    /// True when a replica at `current` already knows everything a peer at
    /// `other` did. The network layer uses this on receiving a `SyncDelta`
    /// to detect when the sender's baseline (`since`) is no longer
    /// dominated by the receiver's state , in that case the delta must be
    /// dropped, and the sender's next periodic full `Sync` allowed to
    /// catch the receiver up. Without this check, a peer whose state has
    /// regressed (restart without graceful `Goodbye`, manual reset) would
    /// silently apply a delta computed against a version it never had.
    fn version_includes(current: &Self::Version, other: &Self::Version) -> bool;
}
