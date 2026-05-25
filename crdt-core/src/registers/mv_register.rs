use crate::clocks::{ClockOrder, VectorClock};
use crate::traits::{Crdt, DeltaCrdt};

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
pub struct MVRegister<T> {
    entries: Vec<(VectorClock, T)>,
}

impl<T: Clone + PartialEq> PartialEq for MVRegister<T> {
    fn eq(&self, other: &Self) -> bool {
        self.entries.len() == other.entries.len()
            && self.entries.iter().all(|e| other.entries.contains(e))
    }
}

impl<T: Clone + PartialEq> Default for MVRegister<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<T: Clone + PartialEq> MVRegister<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn write(&mut self, value: T, clock: VectorClock) {
        // Remove entries dominated by or equal to the new clock (same logical time = replace).
        self.entries.retain(|(vc, _)| {
            !matches!(
                clock.partial_order(vc),
                ClockOrder::After | ClockOrder::Equal
            )
        });
        let dominated = self
            .entries
            .iter()
            .any(|(vc, _)| vc.partial_order(&clock) == ClockOrder::After);
        if !dominated {
            self.entries.push((clock, value));
        }
    }
}

impl<T: Clone + PartialEq> Crdt for MVRegister<T> {
    type Value = Vec<T>;

    fn value(&self) -> Vec<T> {
        self.entries.iter().map(|(_, v)| v.clone()).collect()
    }

    /// Union both entry sets, retaining only entries not dominated by any other.
    fn merge(&mut self, other: Self) {
        let mut all: Vec<(VectorClock, T)> = self.entries.clone();
        for e in other.entries {
            if !all.contains(&e) {
                all.push(e);
            }
        }
        self.entries = all
            .iter()
            .filter(|(vc, _)| {
                !all.iter()
                    .any(|(ovc, _)| ovc != vc && ovc.partial_order(vc) == ClockOrder::After)
            })
            .cloned()
            .collect();
    }

    /// Returns true if every entry in self is dominated by or equal to some entry in other.
    fn compare(&self, other: &Self) -> bool {
        self.entries.iter().all(|(vc, _)| {
            other.entries.iter().any(|(ovc, _)| {
                matches!(ovc.partial_order(vc), ClockOrder::After | ClockOrder::Equal)
            })
        })
    }
}

impl<T: Clone + PartialEq> DeltaCrdt for MVRegister<T> {
    /// Delta is itself an `MVRegister` carrying only the entries the
    /// receiver hasn't yet observed.
    type Delta = MVRegister<T>;
    type Version = VectorClock;

    fn version(&self) -> Self::Version {
        let mut frontier = VectorClock::new();
        for (vc, _) in &self.entries {
            frontier.merge(vc.clone());
        }
        frontier
    }

    fn delta_since(&self, since: &Self::Version) -> Self::Delta {
        // Include entries whose clock has at least one component strictly
        // greater than `since` , i.e. `since` does not dominate `vc`.
        let entries = self
            .entries
            .iter()
            .filter(|(vc, _)| !since.dominates(vc))
            .cloned()
            .collect();
        MVRegister { entries }
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        self.merge(delta);
    }

    fn is_empty_delta(delta: &Self::Delta) -> bool {
        delta.entries.is_empty()
    }

    fn version_includes(current: &Self::Version, other: &Self::Version) -> bool {
        current.dominates(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::NodeId;
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

    fn arb_mv() -> impl Strategy<Value = MVRegister<u32>> {
        proptest::collection::vec((arb_clock(), 0u32..=100u32), 0..=4).prop_map(|writes| {
            let mut reg = MVRegister::new();
            for (clock, val) in writes {
                reg.write(val, clock);
            }
            reg
        })
    }

    #[test]
    fn mv_new_is_empty() {
        let r: MVRegister<u32> = MVRegister::new();
        assert!(r.value().is_empty());
    }

    #[test]
    fn mv_single_write_returns_one_value() {
        let mut r = MVRegister::new();
        let mut vc = VectorClock::new();
        vc.increment(n(1));
        r.write(42u32, vc);
        assert_eq!(r.value(), vec![42]);
    }

    #[test]
    fn mv_sequential_write_keeps_latest() {
        let mut r = MVRegister::new();
        let mut vc1 = VectorClock::new();
        vc1.increment(n(1));
        let mut vc2 = vc1.clone();
        vc2.increment(n(1));
        r.write(1u32, vc1);
        r.write(2u32, vc2);
        assert_eq!(r.value(), vec![2]);
    }

    #[test]
    fn mv_concurrent_writes_keeps_both() {
        let mut r = MVRegister::new();
        let mut vc_a = VectorClock::new();
        vc_a.increment(n(1));
        let mut vc_b = VectorClock::new();
        vc_b.increment(n(2));
        r.write(1u32, vc_a);
        r.write(2u32, vc_b);
        let mut vals = r.value();
        vals.sort();
        assert_eq!(vals, vec![1, 2]);
    }

    #[test]
    fn mv_merge_unions_concurrent_entries() {
        let mut vc_a = VectorClock::new();
        vc_a.increment(n(1));
        let mut vc_b = VectorClock::new();
        vc_b.increment(n(2));
        let mut ra = MVRegister::new();
        ra.write(1u32, vc_a.clone());
        let mut rb = MVRegister::new();
        rb.write(2u32, vc_b.clone());
        ra.merge(rb);
        let mut vals = ra.value();
        vals.sort();
        assert_eq!(vals, vec![1, 2]);
    }

    #[test]
    fn mv_merge_discards_dominated_entries() {
        let mut vc1 = VectorClock::new();
        vc1.increment(n(1));
        let mut vc2 = vc1.clone();
        vc2.increment(n(1));
        let mut ra = MVRegister::new();
        ra.write(1u32, vc1);
        let mut rb = MVRegister::new();
        rb.write(2u32, vc2);
        ra.merge(rb);
        assert_eq!(ra.value(), vec![2]);
    }

    proptest! {
        #[test]
        fn mv_commutative(a in arb_mv(), b in arb_mv()) {
            let mut a1 = a.clone();
            a1.merge(b.clone());
            let mut b1 = b.clone();
            b1.merge(a.clone());
            prop_assert_eq!(a1, b1);
        }

        #[test]
        fn mv_associative(a in arb_mv(), b in arb_mv(), c in arb_mv()) {
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
        fn mv_idempotent(a in arb_mv()) {
            let mut a1 = a.clone();
            a1.merge(a.clone());
            prop_assert_eq!(a1, a);
        }
    }
}
