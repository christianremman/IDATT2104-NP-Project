//! Last-Writer-Wins Map (LWWMap) CRDT.
//!
//! A key-value map where each key holds an [`LWWRegister`].
//! Concurrent writes to the same key are resolved by highest
//! timestamp, with node_id as tiebreaker.
//! Internally composed of [`LWWRegister`]s, same pattern
//! as [`TwoPSet`](crate::sets::TwoPSet) composing two [`GSet`](crate::sets::GSet)s.
use crate::registers::LWWRegister;
use crate::traits::{Crdt, NodeId};
use std::collections::HashMap;
use std::hash::Hash;

/// A map where each key independently resolves conflicts
/// using last-writer-wins.
///
/// Peer A: sets "x" = 5 at time 3
/// Peer B: sets "x" = 9 at time 7
/// After merge: "x" = 9 (higher timestamp wins)
///
/// Keys are created on first write and never removed.
/// Key removal would require ORSet-style tracking.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct LWWMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone + PartialEq,
{
    entries: HashMap<K, LWWRegister<V>>,
}

impl<K, V> Default for LWWMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone + PartialEq,
{
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

/// Update operations for LWWMap.
///
/// The map itself has no concept of time or identity, it
/// forwards the timestamp and node_id to the underlying
/// [`LWWRegister`]` for each key. This means the caller
/// is responsible for generating timestamps (can be done with [`VectorClock`])
///
/// Keys are created on first write and never removed.
impl<K, V> LWWMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone + PartialEq,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a key to a value at the given timestamp.
    ///
    /// If the key already exists, the write is forwarded to its
    /// LWWRegister, which only accepts it if the timestamp is
    /// higher (or equal with a higher node_id). Will return `false`
    /// the timestap is lower, and the write is rejected.
    ///
    /// If the key is new, a fresh LWWRegister is created.
    pub fn set(&mut self, key: K, value: V, timestamp: u64, node_id: NodeId) -> bool {
        match self.entries.get_mut(&key) {
            Some(register) => register.set(value, timestamp, node_id),
            None => {
                self.entries
                    .insert(key, LWWRegister::new(value, timestamp, node_id));
                true
            }
        }
    }

    /// Returns the current value for a key, if it exists.
    pub fn get(&self, key: &K) -> Option<V> {
        self.entries.get(key).map(|r| r.value())
    }

    /// Returns true if the key exists in the map.
    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }
}

impl<K, V> Crdt for LWWMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone + PartialEq,
{
    type Value = HashMap<K, V>;

    /// Returns a snapshot of all keys and their current values.
    fn value(&self) -> Self::Value {
        self.entries
            .iter()
            .map(|(k, r)| (k.clone(), r.value()))
            .collect()
    }

    /// Returns `true` if every key in `self` exists in `other`
    /// and each register is dominated by or equal to other's.
    fn compare(&self, other: &Self) -> bool {
        self.entries
            .iter()
            .all(|(key, reg)| match other.entries.get(key) {
                Some(other_reg) => reg.compare(other_reg),
                None => false,
            })
    }

    /// Merges by merging each register independently.
    /// Keys only in `other` are added. Keys only in `self` stay.
    fn merge(&mut self, other: Self) {
        for (key, other_reg) in other.entries {
            match self.entries.get_mut(&key) {
                Some(local_reg) => local_reg.merge(other_reg),
                None => {
                    self.entries.insert(key, other_reg);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Crdt;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn node(n: u128) -> NodeId {
        Uuid::from_u128(n)
    }

    #[test]
    fn set_and_get() {
        let mut map = LWWMap::new();
        map.set("x", 5, 1, node(1));
        assert_eq!(map.get(&"x"), Some(5));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let map: LWWMap<&str, u32> = LWWMap::new();
        assert_eq!(map.get(&"x"), None);
    }

    #[test]
    fn set_higher_timestamp_overwrites() {
        let mut map = LWWMap::new();
        map.set("x", 5, 1, node(1));
        assert!(map.set("x", 9, 3, node(1)));
        assert_eq!(map.get(&"x"), Some(9));
    }

    #[test]
    fn set_lower_timestamp_rejected() {
        let mut map = LWWMap::new();
        map.set("x", 5, 3, node(1));
        assert!(!map.set("x", 9, 1, node(1)));
        assert_eq!(map.get(&"x"), Some(5));
    }

    #[test]
    fn set_equal_timestamp_higher_node_wins() {
        let mut map = LWWMap::new();
        map.set("x", 5, 1, node(1));
        assert!(map.set("x", 9, 1, node(2)));
        assert_eq!(map.get(&"x"), Some(9));
    }

    #[test]
    fn contains_key() {
        let mut map = LWWMap::new();
        assert!(!map.contains_key(&"x"));
        map.set("x", 5, 1, node(1));
        assert!(map.contains_key(&"x"));
    }

    #[test]
    fn value_returns_all_resolved() {
        let mut map = LWWMap::new();
        map.set("x", 5, 1, node(1));
        map.set("y", 9, 1, node(1));
        let snapshot = map.value();
        assert_eq!(snapshot.get(&"x"), Some(&5));
        assert_eq!(snapshot.get(&"y"), Some(&9));
    }

    #[test]
    fn merge_different_keys() {
        let mut a = LWWMap::new();
        a.set("x", 5, 1, node(1));
        let mut b = LWWMap::new();
        b.set("y", 9, 1, node(2));

        a.merge(b);
        assert_eq!(a.get(&"x"), Some(5));
        assert_eq!(a.get(&"y"), Some(9));
    }

    #[test]
    fn merge_same_key_higher_timestamp_wins() {
        let mut a = LWWMap::new();
        a.set("x", 5, 3, node(1));
        let mut b = LWWMap::new();
        b.set("x", 9, 7, node(2));

        a.merge(b);
        assert_eq!(a.get(&"x"), Some(9));
    }

    #[test]
    fn merge_commutativity() {
        let mut a = LWWMap::new();
        a.set("x", 5, 3, node(1));
        a.set("y", 2, 1, node(1));
        let mut b = LWWMap::new();
        b.set("x", 9, 7, node(2));
        b.set("z", 4, 2, node(2));

        let mut ab = a.clone();
        ab.merge(b.clone());
        let mut ba = b.clone();
        ba.merge(a.clone());

        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn merge_idempotency() {
        let mut a = LWWMap::new();
        a.set("x", 5, 1, node(1));
        let before = a.value();
        a.merge(a.clone());
        assert_eq!(a.value(), before);
    }

    #[test]
    fn merge_associativity() {
        let mut a = LWWMap::new();
        a.set("x", 1, 1, node(1));
        let mut b = LWWMap::new();
        b.set("x", 2, 2, node(2));
        let mut c = LWWMap::new();
        c.set("x", 3, 3, node(3));

        let mut ab_c = a.clone();
        ab_c.merge(b.clone());
        ab_c.merge(c.clone());

        let mut a_bc = a.clone();
        let mut bc = b.clone();
        bc.merge(c.clone());
        a_bc.merge(bc);

        assert_eq!(ab_c.value(), a_bc.value());
    }

    #[test]
    fn compare_subset() {
        let mut a = LWWMap::new();
        a.set("x", 5, 1, node(1));
        let mut b = LWWMap::new();
        b.set("x", 9, 3, node(1));
        b.set("y", 4, 1, node(2));

        assert!(a.compare(&b));
        assert!(!b.compare(&a));
    }

    fn nodes_a() -> [NodeId; 2] {
        [node(1), node(2)]
    }
    fn nodes_b() -> [NodeId; 2] {
        [node(3), node(4)]
    }
    fn nodes_c() -> [NodeId; 2] {
        [node(5), node(6)]
    }

    fn arb_lwwmap_for(nodes: [NodeId; 2]) -> impl Strategy<Value = LWWMap<u8, u32>> {
        proptest::collection::vec((0u8..=3u8, 0u32..=100u32, 0..2usize), 0..=5).prop_map(
            move |ops| {
                let mut map = LWWMap::new();
                for (i, (key, value, node_idx)) in ops.into_iter().enumerate() {
                    map.set(key, value, (i + 1) as u64, nodes[node_idx]);
                }
                map
            },
        )
    }

    proptest! {
        #[test]
        fn commutative(a in arb_lwwmap_for(nodes_a()), b in arb_lwwmap_for(nodes_b())) {
            let mut ab = a.clone();
            ab.merge(b.clone());
            let mut ba = b.clone();
            ba.merge(a.clone());
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn associative(
            a in arb_lwwmap_for(nodes_a()),
            b in arb_lwwmap_for(nodes_b()),
            c in arb_lwwmap_for(nodes_c()),
        ) {
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
        fn idempotent(a in arb_lwwmap_for(nodes_a())) {
             let mut a1 = a.clone();
            a1.merge(a.clone());
            prop_assert_eq!(a1, a);
        }
    }
}
