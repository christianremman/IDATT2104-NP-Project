//! Observed-Remove Set (ORSet) CRDT.
//!
//! Solves the permanent removal limitation of [`TwoPSet`](super::TwoPSet) by
//! //! tagging each add operation with a unique identifier.
//! Concurrent add and remove of the same element results in
//! the element being present.
use crate::traits::{Crdt, DeltaCrdt, NodeId};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// Unique identifier for a single add operation.
///
/// The (node_id, seq) pair is guaranteed unique across all peers.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag {
    pub(crate) node_id: NodeId,
    pub(crate) seq: u64,
}

/// An observed-remove set with add-wins.
///
/// Each add creates a unique [`Tag`]. Remove only tombstones tags
/// that are currently visible. A concurrent add (with an unseen tag)
/// survives a concurrent remove.
#[derive(Debug, Clone, PartialEq)]
pub struct ORSet<T>
where
    T: Eq + Hash + Clone,
{
    /// Map of active tags keeping it alive
    entries: HashMap<T, HashSet<Tag>>,
    /// Tags that have been removed, the tombstones
    /// Grows unbounded. ideally should use a GC strategy.
    removed_tags: HashSet<Tag>,
}

#[cfg(feature = "serde")]
impl<T> serde::Serialize for ORSet<T>
where
    T: Eq + std::hash::Hash + Clone + serde::Serialize,
{
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("ORSet", 2)?;
        let entries_vec: Vec<(&T, Vec<&Tag>)> = self
            .entries
            .iter()
            .map(|(k, v)| (k, v.iter().collect()))
            .collect();
        st.serialize_field("entries", &entries_vec)?;
        st.serialize_field(
            "removed_tags",
            &self.removed_tags.iter().collect::<Vec<_>>(),
        )?;
        st.end()
    }
}

#[cfg(feature = "serde")]
impl<'de, T> serde::Deserialize<'de> for ORSet<T>
where
    T: Eq + std::hash::Hash + Clone + serde::Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Helper<T> {
            entries: Vec<(T, Vec<Tag>)>,
            removed_tags: Vec<Tag>,
        }
        let h = Helper::<T>::deserialize(d)?;
        Ok(ORSet {
            entries: h
                .entries
                .into_iter()
                .map(|(k, v)| (k, v.into_iter().collect()))
                .collect(),
            removed_tags: h.removed_tags.into_iter().collect(),
        })
    }
}

impl<T> Default for ORSet<T>
where
    T: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            removed_tags: HashSet::new(),
        }
    }
}

/// Implementation with specific logic for re-adding items
///
/// Functions like this:
/// Peer A: adds "milk" -> tag (A,1)
/// Peer B: adds "milk" -> tag (B,1), then removes "milk" -> `removed_tags` {(B,1)}
///
/// After merge:
///   tag (A,1) was never tombstoned -> "milk" survives
///   tag (B,1) was tombstoned       -> dead
///   Result: {"milk"} with one active tag
impl<T> ORSet<T>
where
    T: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `element` with a tag stamped `(node_id, seq)`.
    ///
    /// `seq` must be monotonically increasing for a given `node_id` across
    /// all replicas , pass the result of [`VectorClock::increment`](crate::clocks::VectorClock::increment)
    /// so the tag sequence aligns with the document's causal clock. This makes the
    /// per-node tag frontier a sub-projection of the document's
    /// `VectorClock`, which is what [`DeltaCrdt::delta_since`] relies on.
    pub fn insert(&mut self, element: T, node_id: &NodeId, seq: u64) {
        let tag = Tag {
            node_id: *node_id,
            seq,
        };
        self.entries.entry(element).or_default().insert(tag);
    }

    pub fn remove(&mut self, element: &T) -> bool {
        if let Some(tags) = self.entries.remove(element) {
            self.removed_tags.extend(tags);
            return true;
        }
        false
    }

    pub fn contains(&self, element: &T) -> bool {
        self.entries.contains_key(element)
    }
}

/// Merge combines two replicas by keeping every tag that
/// neither side has tombstoned:
///
/// Self:   entries{"milk": {(A,1)}}  removed_tags: {}
/// Other:  entries{"milk": {(B,1)}}  removed_tags: {(A,1)}
///
/// Result: entries{"milk": {(B,1)}}  removed_tags: {(A,1)}
/// - (A,1) was tombstoned by other, removed
/// - (B,1) was not tombstoned, survives
/// - "milk" still in the set with one active tag
impl<T> Crdt for ORSet<T>
where
    T: Eq + Hash + Clone,
{
    type Value = HashSet<T>;

    /// Returns the set of elements that have at least one active tag.
    fn value(&self) -> Self::Value {
        self.entries.keys().cloned().collect()
    }

    /// Returns `true` if all active tags in `self` exist in `other`,
    /// and all tombstones in `self` exist in `other`.
    fn compare(&self, other: &Self) -> bool {
        for (element, tags) in &self.entries {
            match other.entries.get(element) {
                None => return false,
                Some(other_tags) => {
                    if !tags.is_subset(other_tags) {
                        return false;
                    }
                }
            }
        }
        self.removed_tags.is_subset(&other.removed_tags)
    }

    /// Merges another replica into this one.
    ///
    /// A tag survives if it exists in either replica and
    /// is not tombstoned (in `removed_tags`) by either replica.
    ///
    /// Consist of four steps:
    /// 1: Combine both sides' removal knowledge.
    ///     We do this FIRST because we need the full picture
    ///     of what's been removed before deciding what survives.
    /// 2: Add tags from the other replica (to the `entries`), but only
    ///     if they haven't been removed by either side.
    /// 3: Clean our own tags against the newly learned removals from step 1.
    /// 4:  If an element has zero surviving tags, it's fully removed,
    ///     and we drop it from the map.
    fn merge(&mut self, other: Self) {
        self.removed_tags.extend(other.removed_tags.iter().cloned());

        for (element, other_tags) in other.entries {
            let local = self.entries.entry(element).or_default();
            for tag in other_tags {
                if !self.removed_tags.contains(&tag) {
                    local.insert(tag);
                }
            }
        }

        for tags in self.entries.values_mut() {
            tags.retain(|tag| !self.removed_tags.contains(tag));
        }

        self.entries.retain(|_, tags| !tags.is_empty());
    }
}

/// Sparse delta for an [`ORSet`].
///
/// Both `adds` and `removed_tags` are filtered by the same per-node
/// frontier: each tag carries `(node_id, seq)`, and only tags whose `seq`
/// exceeds `since[node_id]` are shipped. This makes `is_empty_delta`
/// truthful , once a peer has absorbed a tombstone, subsequent deltas
/// omit it instead of re-shipping the full tombstone set every tick.
/// Without this filtering, any removal permanently disables the
/// `is_empty_delta` skip on the WS path and inflates idle traffic.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct ORSetDelta<T>
where
    T: Eq + Hash + Clone,
{
    pub adds: Vec<(T, Tag)>,
    pub removed_tags: HashSet<Tag>,
}

impl<T> ORSet<T>
where
    T: Eq + Hash + Clone,
{
    /// Highest `tag.seq` observed per `node_id`, across both active tags
    /// and tombstones. Used as the `Version` for delta queries.
    fn add_frontier(&self) -> HashMap<NodeId, u64> {
        let mut frontier: HashMap<NodeId, u64> = HashMap::new();
        for tags in self.entries.values() {
            for tag in tags {
                let e = frontier.entry(tag.node_id).or_insert(0);
                *e = (*e).max(tag.seq);
            }
        }
        for tag in &self.removed_tags {
            let e = frontier.entry(tag.node_id).or_insert(0);
            *e = (*e).max(tag.seq);
        }
        frontier
    }
}

impl<T> DeltaCrdt for ORSet<T>
where
    T: Eq + Hash + Clone,
{
    type Delta = ORSetDelta<T>;
    type Version = HashMap<NodeId, u64>;

    fn version(&self) -> Self::Version {
        self.add_frontier()
    }

    fn delta_since(&self, since: &Self::Version) -> Self::Delta {
        let exceeds =
            |tag: &Tag| -> bool { tag.seq > since.get(&tag.node_id).copied().unwrap_or(0) };
        let mut adds: Vec<(T, Tag)> = Vec::new();
        for (elem, tags) in &self.entries {
            for tag in tags {
                if exceeds(tag) {
                    adds.push((elem.clone(), tag.clone()));
                }
            }
        }
        let removed_tags: HashSet<Tag> = self
            .removed_tags
            .iter()
            .filter(|t| exceeds(t))
            .cloned()
            .collect();
        ORSetDelta { adds, removed_tags }
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        // Mirror `merge`'s ordering: absorb tombstones first, then add new
        // tags only if they aren't already tombstoned, then sweep our own
        // tags against the newly-learned removals.
        self.removed_tags.extend(delta.removed_tags);

        for (elem, tag) in delta.adds {
            if self.removed_tags.contains(&tag) {
                continue;
            }
            let local = self.entries.entry(elem).or_default();
            local.insert(tag);
        }

        for tags in self.entries.values_mut() {
            tags.retain(|tag| !self.removed_tags.contains(tag));
        }
        self.entries.retain(|_, tags| !tags.is_empty());
    }

    fn is_empty_delta(delta: &Self::Delta) -> bool {
        delta.adds.is_empty() && delta.removed_tags.is_empty()
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
    use crate::traits::{Crdt, NodeId};
    use proptest::collection::vec as prop_vec;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn node(n: u128) -> NodeId {
        Uuid::from_u128(n)
    }

    fn arb_orset() -> impl Strategy<Value = ORSet<u8>> {
        prop_vec((0u8..=3u8, 0usize..3usize, proptest::bool::ANY), 0..10).prop_map(|ops| {
            let nodes = [node(1), node(2), node(3)];
            let mut set = ORSet::new();
            let mut seq = 0u64;
            for (elem, node_idx, is_remove) in ops {
                if is_remove {
                    set.remove(&elem);
                } else {
                    seq += 1;
                    set.insert(elem, &nodes[node_idx], seq);
                }
            }
            set
        })
    }

    #[test]
    fn insert_and_contains() {
        let mut set = ORSet::new();
        let a = node(1);
        assert!(!set.contains(&"milk"));
        set.insert("milk", &a, 1);
        assert!(set.contains(&"milk"));
    }

    #[test]
    fn remove_and_read() {
        let mut set = ORSet::new();
        let a = node(1);
        set.insert("milk", &a, 1);
        set.remove(&"milk");
        assert!(!set.contains(&"milk"));

        // re-add works unlike in TwoPSet
        set.insert("milk", &a, 2);
        assert!(set.contains(&"milk"));
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut set: ORSet<&str> = ORSet::new();
        assert!(!set.remove(&"milk"));
    }

    #[test]
    fn value_returns_active_elements() {
        let mut set = ORSet::new();
        let a = node(1);
        set.insert("milk", &a, 1);
        set.insert("eggs", &a, 2);
        set.remove(&"milk");
        assert_eq!(set.value(), HashSet::from(["eggs"]));
    }

    #[test]
    fn concurrent_add_and_remove_add_wins() {
        // Peer A adds "milk", peer B independently adds then removes "milk"
        // After merge: "milk" survives because A's tag was never tombstoned
        let a = node(1);
        let b = node(2);

        let mut peer_a = ORSet::new();
        peer_a.insert("milk", &a, 1);

        let mut peer_b = ORSet::new();
        peer_b.insert("milk", &b, 1);
        peer_b.remove(&"milk");

        peer_a.merge(peer_b);
        assert!(peer_a.contains(&"milk")); // add wins, not removed like in 2P
    }

    #[test]
    fn merge_commutativity() {
        let a = node(1);
        let b = node(2);

        let mut peer_a = ORSet::new();
        peer_a.insert("milk", &a, 1);
        peer_a.insert("bread", &a, 2);

        let mut peer_b = ORSet::new();
        peer_b.insert("eggs", &b, 1);
        peer_b.insert("milk", &b, 2);
        peer_b.remove(&"milk");

        let mut ab = peer_a.clone();
        ab.merge(peer_b.clone());
        let mut ba = peer_b.clone();
        ba.merge(peer_a.clone());

        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn merge_idempotency() {
        let mut set = ORSet::new();
        let a = node(1);
        set.insert("milk", &a, 1);
        set.insert("eggs", &a, 2);
        set.remove(&"milk");
        let before = set.value();
        set.merge(set.clone());
        assert_eq!(set.value(), before);
    }

    #[test]
    fn merge_associativity() {
        let a = node(1);
        let b = node(2);
        let c = node(3);

        let mut peer_a = ORSet::new();
        peer_a.insert("milk", &a, 1);
        let mut peer_b = ORSet::new();
        peer_b.insert("eggs", &b, 1);
        let mut peer_c = ORSet::new();
        peer_c.insert("bread", &c, 1);

        // (A merge B) merge C
        let mut ab_c = peer_a.clone();
        ab_c.merge(peer_b.clone());
        ab_c.merge(peer_c.clone());

        // A merge (B merge C)
        let mut a_bc = peer_a.clone();
        let mut bc = peer_b.clone();
        bc.merge(peer_c.clone());
        a_bc.merge(bc);

        assert_eq!(ab_c.value(), a_bc.value());
    }

    #[test]
    fn compare_subset() {
        let a = node(1);
        let b = node(2);

        let mut small = ORSet::new();
        small.insert("milk", &a, 1);

        let mut big = ORSet::new();
        big.insert("milk", &a, 1);
        big.insert("eggs", &b, 1);

        assert!(small.compare(&big));
        assert!(!big.compare(&small));
    }

    /// After a peer has absorbed a tombstone, subsequent deltas computed
    /// against the post-removal version must omit that tombstone. Without
    /// the filter, `removed_tags` would re-ship forever and
    /// `is_empty_delta` would never return true again.
    #[test]
    fn delta_omits_already_seen_tombstones() {
        let a = node(1);
        let mut sender = ORSet::new();
        sender.insert("milk", &a, 1);
        sender.remove(&"milk");
        // Tombstone now exists in `removed_tags` with seq=1.

        let post_removal_version = sender.version();
        let delta = sender.delta_since(&post_removal_version);
        assert!(
            delta.adds.is_empty(),
            "no new adds since post-removal version"
        );
        assert!(
            delta.removed_tags.is_empty(),
            "tombstone already known at the post-removal version must be filtered out"
        );
        assert!(<ORSet<&str> as DeltaCrdt>::is_empty_delta(&delta));
    }

    /// A delta computed against an empty version must still carry the
    /// tombstone, so a fresh receiver learns about removals.
    #[test]
    fn delta_includes_tombstones_for_fresh_receiver() {
        let a = node(1);
        let mut sender = ORSet::new();
        sender.insert("milk", &a, 1);
        sender.remove(&"milk");

        let delta = sender.delta_since(&HashMap::new());
        assert_eq!(delta.removed_tags.len(), 1, "fresh receiver gets tombstone");

        // Apply to a fresh replica and verify tombstone propagated.
        let mut receiver: ORSet<&str> = ORSet::new();
        receiver.merge_delta(delta);
        assert!(!receiver.contains(&"milk"));
        assert!(receiver
            .removed_tags
            .iter()
            .any(|t| t.node_id == a && t.seq == 1));
    }

    proptest! {
        #[test]
        fn orset_commutative(a in arb_orset(), b in arb_orset()) {
            let mut ab = a.clone();
            ab.merge(b.clone());
            let mut ba = b.clone();
            ba.merge(a.clone());
            prop_assert_eq!(ab.value(), ba.value());
        }

        #[test]
        fn orset_idempotent(a in arb_orset()) {
            let mut a1 = a.clone();
            a1.merge(a.clone());
            prop_assert_eq!(a1.value(), a.value());
        }

        #[test]
        fn orset_associative(a in arb_orset(), b in arb_orset(), c in arb_orset()) {
            let mut ab_c = a.clone();
            ab_c.merge(b.clone());
            ab_c.merge(c.clone());

            let mut bc = b.clone();
            bc.merge(c.clone());
            let mut a_bc = a.clone();
            a_bc.merge(bc);

            prop_assert_eq!(ab_c.value(), a_bc.value());
        }
    }
}
