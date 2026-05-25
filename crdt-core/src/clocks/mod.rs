//! Tracking via vector clocks.
//!
//! [`VectorClock`] is the foundational primitive used by other CRDTs
//! for timestamp generation and ordering.
use std::collections::HashMap;

use crate::traits::{Crdt, DeltaCrdt, NodeId};

/// Causality primitive for tracking events across nodes.
///
/// Tracks the number of events seen from each node. A missing entry is
/// equivalent to a count of zero, so `{A:1}` and `{A:1, B:0}` are equal.
///
/// Used by [`super::registers::MVRegister`] to detect concurrent
/// writes, and as a Lamport timestamp source for [`super::registers::LWWRegister`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VectorClock {
    pub(crate) clock: HashMap<NodeId, u64>,
}

/// Result of comparing two [`VectorClock`]s under the causal partial order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockOrder {
    /// Every component of `self` is strictly ≤ the corresponding component of `other`,
    /// and at least one is strictly less. `self` happened before `other`.
    Before,
    /// Every component of `self` is ≥ the corresponding component of `other`,
    /// and at least one is strictly greater. `self` happened after `other`.
    After,
    /// All components are equal.
    Equal,
    /// Neither clock dominates the other; the events are causally independent.
    Concurrent,
}

impl VectorClock {
    /// Creates an empty clock (all components implicitly zero).
    pub fn new() -> Self {
        Self::default()
    }

    /// Advances `node`'s counter past all observed values and returns it.
    ///
    /// This is the Lamport clock rule applied to the vector clock.
    /// The returned timestamp is strictly greater than any component
    /// in the clock, making it safe to use as an LWW timestamp.
    pub fn increment(&mut self, node: NodeId) -> u64 {
        let max = self.lamport_timestamp();
        let entry = self.clock.entry(node).or_insert(0);
        *entry = (*entry).max(max) + 1;
        *entry
    }

    /// Returns the current component for `node`, or `0` if unseen.
    pub fn get(&self, node: &NodeId) -> u64 {
        *self.clock.get(node).unwrap_or(&0)
    }

    /// Returns `max` over all components , used as a Lamport timestamp for
    /// [`super::registers::LWWRegister`] tie-breaking.
    pub fn lamport_timestamp(&self) -> u64 {
        self.clock.values().copied().max().unwrap_or(0)
    }

    /// Compares `self` to `other` under the causal partial order.
    pub fn partial_order(&self, other: &Self) -> ClockOrder {
        let self_dom = self.dominates(other);
        let other_dom = other.dominates(self);
        match (self_dom, other_dom) {
            (true, true) => ClockOrder::Equal,
            (true, false) => ClockOrder::After,
            (false, true) => ClockOrder::Before,
            (false, false) => ClockOrder::Concurrent,
        }
    }

    /// Returns `true` if `self` ≥ `other` component-wise.
    ///
    /// Public so other crates implementing [`DeltaCrdt`] can reuse it in
    /// `version_includes` without re-deriving the comparison.
    pub fn dominates(&self, other: &Self) -> bool {
        other.clock.iter().all(|(k, v)| self.get(k) >= *v)
    }
}

impl Crdt for VectorClock {
    type Value = HashMap<NodeId, u64>;

    fn value(&self) -> Self::Value {
        self.clock.clone()
    }

    /// Merge rule: element-wise maximum of each node's component.
    fn merge(&mut self, other: Self) {
        for (k, v) in other.clock {
            let e = self.clock.entry(k).or_insert(0);
            *e = (*e).max(v);
        }
    }

    /// Returns true if other dominates self component-wise,
    /// i.e. merging other into self would produce other unchanged.
    fn compare(&self, other: &Self) -> bool {
        other.dominates(self)
    }
}

impl DeltaCrdt for VectorClock {
    /// A delta is itself a (sparse) vector clock: only the entries whose
    /// component strictly exceeds the receiver's known value.
    type Delta = VectorClock;
    type Version = VectorClock;

    fn version(&self) -> Self::Version {
        self.clone()
    }

    fn delta_since(&self, since: &Self::Version) -> Self::Delta {
        let mut delta = VectorClock::new();
        for (node, &count) in &self.clock {
            if count > since.get(node) {
                delta.clock.insert(*node, count);
            }
        }
        delta
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        self.merge(delta);
    }

    fn is_empty_delta(delta: &Self::Delta) -> bool {
        delta.clock.is_empty()
    }

    fn version_includes(current: &Self::Version, other: &Self::Version) -> bool {
        current.dominates(other)
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

    fn arb_node() -> impl Strategy<Value = NodeId> {
        prop::sample::select(vec![n(1), n(2), n(3)])
    }

    fn arb_clock() -> impl Strategy<Value = VectorClock> {
        proptest::collection::hash_map(arb_node(), 1u64..=10u64, 0..=3)
            .prop_map(|clock| VectorClock { clock })
    }

    #[test]
    fn new_returns_zero_for_unknown_node() {
        let vc = VectorClock::new();
        assert_eq!(vc.get(&n(1)), 0);
    }

    #[test]
    fn increment_increases_own_component() {
        let mut vc = VectorClock::new();
        vc.increment(n(1));
        vc.increment(n(1));
        assert_eq!(vc.get(&n(1)), 2);
        assert_eq!(vc.get(&n(2)), 0);
    }

    #[test]
    fn merge_takes_element_wise_max() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(n(1));
        a.increment(n(1)); // n1=2
        b.increment(n(1)); // n1=1
        b.increment(n(2)); // sees max=1 from n(1), so n2=2 (Lamport rule)
        a.merge(b);
        assert_eq!(a.get(&n(1)), 2);
        assert_eq!(a.get(&n(2)), 2);
    }

    #[test]
    fn partial_order_before() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(n(1));
        b.increment(n(1));
        b.increment(n(1));
        assert_eq!(a.partial_order(&b), ClockOrder::Before);
    }

    #[test]
    fn partial_order_after() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(n(1));
        a.increment(n(1));
        b.increment(n(1));
        assert_eq!(a.partial_order(&b), ClockOrder::After);
    }

    #[test]
    fn partial_order_equal() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(n(1));
        b.increment(n(1));
        assert_eq!(a.partial_order(&b), ClockOrder::Equal);
    }

    #[test]
    fn partial_order_concurrent() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(n(1));
        b.increment(n(2));
        assert_eq!(a.partial_order(&b), ClockOrder::Concurrent);
    }

    #[test]
    fn lamport_timestamp_is_max_component() {
        let mut vc = VectorClock::new();
        vc.increment(n(1));
        vc.increment(n(1)); // n1=2
        vc.increment(n(2)); // sees max=2, so n2=3
        assert_eq!(vc.lamport_timestamp(), 3);
    }

    #[test]
    fn compare_true_when_other_dominates() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(n(1));
        b.increment(n(1));
        b.increment(n(1));
        assert!(a.compare(&b));
        assert!(!b.compare(&a));
    }

    #[test]
    fn delta_since_empty_replays_all() {
        let mut vc = VectorClock::new();
        vc.increment(n(1));
        vc.increment(n(2));
        let delta = vc.delta_since(&VectorClock::new());
        let mut fresh = VectorClock::new();
        fresh.merge_delta(delta);
        assert_eq!(fresh, vc);
    }

    #[test]
    fn delta_since_current_version_is_empty() {
        let mut vc = VectorClock::new();
        vc.increment(n(1));
        vc.increment(n(2));
        let delta = vc.delta_since(&vc.version());
        assert!(VectorClock::is_empty_delta(&delta));
    }

    #[test]
    fn delta_catches_up_lagging_peer() {
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.increment(n(1));
        b.merge(a.clone());

        a.increment(n(1));
        a.increment(n(2));

        let delta = a.delta_since(&b.version());
        b.merge_delta(delta);
        assert_eq!(a, b);
    }

    #[test]
    fn delta_is_idempotent() {
        let mut a = VectorClock::new();
        a.increment(n(1));
        a.increment(n(2));
        let delta = a.delta_since(&VectorClock::new());
        let mut b = VectorClock::new();
        b.merge_delta(delta.clone());
        let snapshot = b.clone();
        b.merge_delta(delta);
        assert_eq!(b, snapshot);
    }

    #[test]
    fn version_includes_detects_lagging() {
        let mut a = VectorClock::new();
        a.increment(n(1));
        let fresh = VectorClock::new();
        assert!(!VectorClock::version_includes(&fresh, &a.version()));
        assert!(VectorClock::version_includes(&a.version(), &fresh));
    }

    proptest! {
        #[test]
        fn commutative(a in arb_clock(), b in arb_clock()) {
            let mut a1 = a.clone();
            a1.merge(b.clone());
            let mut b1 = b.clone();
            b1.merge(a.clone());
            prop_assert_eq!(a1, b1);
        }

        #[test]
        fn associative(a in arb_clock(), b in arb_clock(), c in arb_clock()) {
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
        fn idempotent(a in arb_clock()) {
            let mut a1 = a.clone();
            a1.merge(a.clone());
            prop_assert_eq!(a1, a);
        }
    }
}
