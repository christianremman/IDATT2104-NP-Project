//! Last-Writer-Wins Register (LWWRegister) CRDT.
//!
//! Stores a single value with a timestamp. Concurrent writes are resolved
//! by highest timestamp, with `node_id` as tiebreaker for equal timestamps.
use crate::traits::{Crdt, DeltaCrdt, NodeId};

/// Last-Writer-Wins Register.
///
/// Each write carries a timestamp and node ID. On merge, the write with
/// the highest timestamp wins. Equal timestamps are broken by comparing
/// node IDs (higher UUID wins), ensuring deterministic convergence.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct LWWRegister<T> {
    value: T,
    timestamp: u64,
    node_id: NodeId,
}

impl<T: Clone + PartialEq> LWWRegister<T> {
    pub fn new(value: T, timestamp: u64, node_id: NodeId) -> Self {
        Self {
            value,
            timestamp,
            node_id,
        }
    }

    /// Returns true if `other` would replace `self` in a merge.
    fn is_superseded_by(&self, other: &Self) -> bool {
        other.timestamp > self.timestamp
            || (other.timestamp == self.timestamp && other.node_id > self.node_id)
    }

    /// Updates the register value if the new write wins.
    /// Returns `true` if the value was updated, `false` if
    /// the existing value has a higher or equal timestamp.
    pub fn set(&mut self, value: T, timestamp: u64, node_id: NodeId) -> bool {
        let incoming = LWWRegister::new(value, timestamp, node_id);
        if self.is_superseded_by(&incoming) {
            *self = incoming;
            return true;
        }
        false
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl<T: Clone + PartialEq> Crdt for LWWRegister<T> {
    type Value = T;

    fn value(&self) -> T {
        self.value.clone()
    }

    /// Higher timestamp wins; equal timestamp -> higher `node_id` wins.
    fn merge(&mut self, other: Self) {
        if self.is_superseded_by(&other) {
            *self = other;
        }
    }

    /// Returns `true` if `self` is dominated by or equal to `other`.
    ///
    /// When `true`, merging `self` into `other` would not change `other`.
    /// For LWWRegister this means `other` has a higher timestamp,
    /// or equal timestamp with a higher or equal node_id.
    fn compare(&self, other: &Self) -> bool {
        !other.is_superseded_by(self)
    }
}

impl<T: Clone + PartialEq> DeltaCrdt for LWWRegister<T> {
    /// `Some(self)` iff this register would supersede the receiver's known
    /// `(timestamp, node_id)`; `None` otherwise (receiver is already at or
    /// past this register).
    type Delta = Option<LWWRegister<T>>;
    /// `(0, Uuid::nil())` represents "receiver knows no register yet" , any
    /// register dominates that.
    type Version = (u64, NodeId);

    fn version(&self) -> Self::Version {
        (self.timestamp, self.node_id)
    }

    fn delta_since(&self, since: &Self::Version) -> Self::Delta {
        let (since_ts, since_node) = *since;
        let exceeds =
            self.timestamp > since_ts || (self.timestamp == since_ts && self.node_id > since_node);
        if exceeds {
            Some(self.clone())
        } else {
            None
        }
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        if let Some(other) = delta {
            self.merge(other);
        }
    }

    fn is_empty_delta(delta: &Self::Delta) -> bool {
        delta.is_none()
    }

    /// `(ts, node)` is included when `current` is at least as recent as
    /// `other` under the same lex order LWW uses for tiebreaks.
    fn version_includes(current: &Self::Version, other: &Self::Version) -> bool {
        let (cur_ts, cur_node) = *current;
        let (oth_ts, oth_node) = *other;
        cur_ts > oth_ts || (cur_ts == oth_ts && cur_node >= oth_node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn n(id: u128) -> NodeId {
        Uuid::from_u128(id)
    }

    fn arb_lww() -> impl Strategy<Value = LWWRegister<u32>> {
        (0u32..=100u32, 0u64..=10u64, 0usize..3).prop_map(|(value, timestamp, idx)| {
            LWWRegister::new(value, timestamp, [n(1), n(2), n(3)][idx])
        })
    }

    #[test]
    fn lww_value_returns_initial() {
        let r = LWWRegister::new(42u32, 0, n(1));
        assert_eq!(r.value(), 42);
    }

    #[test]
    fn lww_merge_higher_timestamp_wins() {
        let a = LWWRegister::new(1u32, 10, n(1));
        let b = LWWRegister::new(2u32, 5, n(2));
        let mut r1 = a.clone();
        r1.merge(b.clone());
        assert_eq!(r1.value(), 1);
        let mut r2 = b.clone();
        r2.merge(a.clone());
        assert_eq!(r2.value(), 1);
    }

    #[test]
    fn lww_merge_equal_timestamp_higher_node_wins() {
        let a = LWWRegister::new(1u32, 5, n(2));
        let b = LWWRegister::new(2u32, 5, n(1));
        let mut a1 = a.clone();
        a1.merge(b.clone());
        let mut b1 = b.clone();
        b1.merge(a.clone());
        assert_eq!(a1, b1);
    }

    #[test]
    fn lww_set_updates_when_newer() {
        let mut r = LWWRegister::new(1u32, 1, n(1));
        r.set(99, 5, n(1));
        assert_eq!(r.value(), 99);
    }

    #[test]
    fn lww_set_ignores_older_write() {
        let mut r = LWWRegister::new(1u32, 10, n(1));
        r.set(99, 3, n(1));
        assert_eq!(r.value(), 1);
    }

    #[test]
    fn lww_compare_other_dominates() {
        let a = LWWRegister::new(1u32, 5, n(1));
        let b = LWWRegister::new(2u32, 10, n(1));
        assert!(a.compare(&b));
        assert!(!b.compare(&a));
    }

    proptest! {
        #[test]
        fn lww_commutative(a in arb_lww(), b in arb_lww()) {
            prop_assume!(
                a.node_id != b.node_id || a.timestamp != b.timestamp || a.value == b.value
            );
            let mut a1 = a.clone();
            a1.merge(b.clone());
            let mut b1 = b.clone();
            b1.merge(a.clone());
            prop_assert_eq!(a1, b1);
        }

        #[test]
        fn lww_associative(a in arb_lww(), b in arb_lww(), c in arb_lww()) {
            let mut ab = a.clone();
            ab.merge(b.clone());
            ab.merge(c.clone());
            let mut bc = b.clone();
            bc.merge(c.clone());
            let mut a2 = a.clone();
            a2.merge(bc);
            prop_assert_eq!(ab, a2);
        }

        #[test]
        fn lww_idempotent(a in arb_lww()) {
            let mut a1 = a.clone();
            a1.merge(a.clone());
            prop_assert_eq!(a1, a);
        }
    }
}
