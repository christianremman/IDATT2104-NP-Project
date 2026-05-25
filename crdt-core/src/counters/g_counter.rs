//! Grow-only counter.
//!
//! Each node tracks its own increment count. Value = sum of all nodes.
//! Merge = element-wise max. Never decreases.
use crate::traits::{Crdt, DeltaCrdt, NodeId};
use std::collections::HashMap;

/// Grow-only counter. Each node maintains its own monotonic count;
/// the value is the sum across all nodes. Merge is element-wise max.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct GCounter {
    counts: HashMap<NodeId, u64>,
}

impl GCounter {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }

    /// Increment this node's count by 1.
    pub fn increment(&mut self, node_id: NodeId) {
        *self.counts.entry(node_id).or_insert(0) += 1;
    }

    pub fn get(&self, node_id: &NodeId) -> u64 {
        *self.counts.get(node_id).unwrap_or(&0)
    }
}

impl Default for GCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl Crdt for GCounter {
    type Value = u64;

    /// Sum of all node counts.
    fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Element-wise max of each node's count.
    fn merge(&mut self, other: Self) {
        for (node, count) in other.counts {
            let e = self.counts.entry(node).or_insert(0);
            *e = (*e).max(count);
        }
    }

    /// True if every component of self ≤ other.
    fn compare(&self, other: &Self) -> bool {
        self.counts.iter().all(|(k, v)| *v <= other.get(k))
    }
}

impl DeltaCrdt for GCounter {
    /// Sparse per-node delta containing only nodes whose count exceeds
    /// the receiver's view.
    type Delta = HashMap<NodeId, u64>;
    type Version = HashMap<NodeId, u64>;

    fn version(&self) -> Self::Version {
        self.counts.clone()
    }

    fn delta_since(&self, since: &Self::Version) -> Self::Delta {
        self.counts
            .iter()
            .filter_map(|(node, &count)| {
                let known = since.get(node).copied().unwrap_or(0);
                (count > known).then_some((*node, count))
            })
            .collect()
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        for (node, count) in delta {
            let e = self.counts.entry(node).or_insert(0);
            *e = (*e).max(count);
        }
    }

    fn is_empty_delta(delta: &Self::Delta) -> bool {
        delta.is_empty()
    }

    fn version_includes(current: &Self::Version, other: &Self::Version) -> bool {
        other
            .iter()
            .all(|(k, v)| current.get(k).copied().unwrap_or(0) >= *v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn n(id: u128) -> NodeId {
        Uuid::from_u128(id)
    }

    #[test]
    fn new_starts_at_zero() {
        assert_eq!(GCounter::new().value(), 0);
    }

    #[test]
    fn get_unknown_node_returns_zero() {
        assert_eq!(GCounter::new().get(&n(1)), 0);
    }

    #[test]
    fn increment_increases_count() {
        let mut c = GCounter::new();
        c.increment(n(1));
        c.increment(n(1));
        assert_eq!(c.get(&n(1)), 2);
    }

    #[test]
    fn value_sums_all_nodes() {
        let mut c = GCounter::new();
        c.increment(n(1));
        c.increment(n(1));
        c.increment(n(2));
        assert_eq!(c.value(), 3);
    }

    #[test]
    fn merge_takes_element_wise_max() {
        let mut a = GCounter::new();
        a.increment(n(1));
        a.increment(n(1)); // n1=2
        let mut b = GCounter::new();
        b.increment(n(1)); // n1=1
        b.increment(n(2)); // n2=1
        a.merge(b);
        assert_eq!(a.get(&n(1)), 2);
        assert_eq!(a.get(&n(2)), 1);
        assert_eq!(a.value(), 3);
    }

    #[test]
    fn merge_commutativity() {
        let mut a = GCounter::new();
        a.increment(n(1));
        let mut b = GCounter::new();
        b.increment(n(2));
        let mut ab = a.clone();
        ab.merge(b.clone());
        let mut ba = b.clone();
        ba.merge(a.clone());
        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn merge_idempotency() {
        let mut a = GCounter::new();
        a.increment(n(1));
        let before = a.value();
        a.merge(a.clone());
        assert_eq!(a.value(), before);
    }

    #[test]
    fn compare_subset() {
        let mut a = GCounter::new();
        a.increment(n(1));
        let mut b = GCounter::new();
        b.increment(n(1));
        b.increment(n(1));
        assert!(a.compare(&b));
        assert!(!b.compare(&a));
    }
}
