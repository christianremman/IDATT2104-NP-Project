//! Positive-Negative counter.
//!
//! Two [`GCounter`]s: one for increments, one for decrements.
//! Value = increments.value() - decrements.value() (may be negative).
use super::g_counter::GCounter;
use crate::traits::{Crdt, DeltaCrdt, NodeId};

/// Positive-Negative counter. Supports both increment and decrement
/// by composing two [`GCounter`]s internally.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct PNCounter {
    increments: GCounter,
    decrements: GCounter,
}

impl PNCounter {
    pub fn new() -> Self {
        Self {
            increments: GCounter::new(),
            decrements: GCounter::new(),
        }
    }

    pub fn increment(&mut self, node_id: NodeId) {
        self.increments.increment(node_id);
    }

    pub fn decrement(&mut self, node_id: NodeId) {
        self.decrements.increment(node_id);
    }
}

impl Default for PNCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl Crdt for PNCounter {
    type Value = i64;

    fn value(&self) -> i64 {
        self.increments.value() as i64 - self.decrements.value() as i64
    }

    fn merge(&mut self, other: Self) {
        self.increments.merge(other.increments);
        self.decrements.merge(other.decrements);
    }

    fn compare(&self, other: &Self) -> bool {
        self.increments.compare(&other.increments) && self.decrements.compare(&other.decrements)
    }
}

/// Per-side counter delta. `(increments_delta, decrements_delta)`.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct PNCounterDelta {
    pub increments: <GCounter as DeltaCrdt>::Delta,
    pub decrements: <GCounter as DeltaCrdt>::Delta,
}

/// Receiver-side version of a `PNCounter`: the two `GCounter` versions
/// kept side by side.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct PNCounterVersion {
    pub increments: <GCounter as DeltaCrdt>::Version,
    pub decrements: <GCounter as DeltaCrdt>::Version,
}

impl DeltaCrdt for PNCounter {
    type Delta = PNCounterDelta;
    type Version = PNCounterVersion;

    fn version(&self) -> Self::Version {
        PNCounterVersion {
            increments: self.increments.version(),
            decrements: self.decrements.version(),
        }
    }

    fn delta_since(&self, since: &Self::Version) -> Self::Delta {
        PNCounterDelta {
            increments: self.increments.delta_since(&since.increments),
            decrements: self.decrements.delta_since(&since.decrements),
        }
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        self.increments.merge_delta(delta.increments);
        self.decrements.merge_delta(delta.decrements);
    }

    fn is_empty_delta(delta: &Self::Delta) -> bool {
        GCounter::is_empty_delta(&delta.increments) && GCounter::is_empty_delta(&delta.decrements)
    }

    fn version_includes(current: &Self::Version, other: &Self::Version) -> bool {
        GCounter::version_includes(&current.increments, &other.increments)
            && GCounter::version_includes(&current.decrements, &other.decrements)
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

    #[test]
    fn new_starts_at_zero() {
        assert_eq!(PNCounter::new().value(), 0);
    }

    #[test]
    fn increment_increases_value() {
        let mut c = PNCounter::new();
        c.increment(n(1));
        c.increment(n(1));
        assert_eq!(c.value(), 2);
    }

    #[test]
    fn decrement_decreases_value() {
        let mut c = PNCounter::new();
        c.increment(n(1));
        c.increment(n(1));
        c.decrement(n(1));
        assert_eq!(c.value(), 1);
    }

    #[test]
    fn value_can_be_negative() {
        let mut c = PNCounter::new();
        c.decrement(n(1));
        assert_eq!(c.value(), -1);
    }

    #[test]
    fn merge_commutativity() {
        let mut a = PNCounter::new();
        a.increment(n(1));
        a.decrement(n(1));
        let mut b = PNCounter::new();
        b.increment(n(2));
        let mut ab = a.clone();
        ab.merge(b.clone());
        let mut ba = b.clone();
        ba.merge(a.clone());
        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn merge_idempotency() {
        let mut a = PNCounter::new();
        a.increment(n(1));
        a.decrement(n(1));
        let before = a.value();
        a.merge(a.clone());
        assert_eq!(a.value(), before);
    }

    #[test]
    fn compare_subset() {
        let mut a = PNCounter::new();
        a.increment(n(1));
        let mut b = PNCounter::new();
        b.increment(n(1));
        b.increment(n(1));
        assert!(a.compare(&b));
        assert!(!b.compare(&a));
    }
    fn arb_pncounter() -> impl Strategy<Value = PNCounter> {
        proptest::collection::vec(
            (
                prop::sample::select(vec![n(1), n(2), n(3)]),
                proptest::bool::ANY,
            ),
            0..=6,
        )
        .prop_map(|ops| {
            let mut c = PNCounter::new();
            for (node, is_dec) in ops {
                if is_dec {
                    c.decrement(node);
                } else {
                    c.increment(node);
                }
            }
            c
        })
    }

    proptest! {
        #[test]
        fn commutative(a in arb_pncounter(), b in arb_pncounter()) {
            let mut ab = a.clone();
            ab.merge(b.clone());
            let mut ba = b.clone();
            ba.merge(a.clone());
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn associative(a in arb_pncounter(), b in arb_pncounter(), c in arb_pncounter()) {
            let mut ab_c = a.clone();
            ab_c.merge(b.clone());
            ab_c.merge(c.clone());
            let mut bc = b.clone();
            bc.merge(c.clone());
            let mut a_bc = a.clone();
            a_bc.merge(bc);
            prop_assert_eq!(ab_c, a_bc);
        }

        #[test]
        fn idempotent(a in arb_pncounter()) {
            let mut a1 = a.clone();
            a1.merge(a.clone());
            prop_assert_eq!(a1, a);
        }
    }

    // -- DeltaCrdt tests --

    #[test]
    fn delta_since_empty_replays_all() {
        let mut c = PNCounter::new();
        c.increment(n(1));
        c.increment(n(1));
        c.decrement(n(2));
        let delta = c.delta_since(&PNCounter::new().version());
        let mut fresh = PNCounter::new();
        fresh.merge_delta(delta);
        assert_eq!(fresh.value(), 1);
    }

    #[test]
    fn delta_since_current_version_is_empty() {
        let mut c = PNCounter::new();
        c.increment(n(1));
        c.decrement(n(2));
        let delta = c.delta_since(&c.version());
        assert!(PNCounter::is_empty_delta(&delta));
    }

    #[test]
    fn delta_catches_up_lagging_peer() {
        let mut a = PNCounter::new();
        let mut b = PNCounter::new();
        a.increment(n(1));
        b.merge(a.clone());

        a.increment(n(1));
        a.decrement(n(1));

        let delta = a.delta_since(&b.version());
        b.merge_delta(delta);
        assert_eq!(b.value(), a.value());
    }

    #[test]
    fn delta_is_idempotent() {
        let mut a = PNCounter::new();
        a.increment(n(1));
        a.decrement(n(2));
        let delta = a.delta_since(&PNCounter::new().version());
        let mut b = PNCounter::new();
        b.merge_delta(delta.clone());
        let before = b.value();
        b.merge_delta(delta);
        assert_eq!(b.value(), before);
    }
}
