//! Two-Phase Set (2PSet) CRDT.
//!
//! Extends [`GSet`] by allowing removal. Once removed, an element
//! can never be re-added. This limitation is solved by [`ORSet`](super::ORSet).
use std::collections::HashSet;
use std::hash::Hash;

use super::gset::GSet;
use crate::traits::Crdt;

/// A set that supports both add and remove, but removal is permanent.
///
/// Internally composed of two [`GSet`]s: one tracking additions,
/// one tracking removals. An element is in the set
/// if it has been added and not removed.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct TwoPSet<T>
where
    T: Eq + Hash + Clone,
{
    added: GSet<T>,
    removed: GSet<T>,
}

impl<T> Default for TwoPSet<T>
where
    T: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self {
            added: GSet::new(),
            removed: GSet::new(),
        }
    }
}

impl<T> TwoPSet<T>
where
    T: Eq + Hash + Clone,
{
    /// Creates an empty TwoPSet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an element to the set.
    ///
    /// If the element has been previously removed, the add is ignored.
    /// Removal is permanent in a TwoPSet.
    pub fn insert(&mut self, element: T) -> bool {
        if self.removed.contains(&element) {
            return false;
        }
        self.added.insert(element)
    }

    /// Removes an element from the set permanently.
    ///
    /// Returns `true` if the element was present before removal.
    /// Once removed, the element can never be re-added.
    pub fn remove(&mut self, element: T) -> bool {
        if self.added.contains(&element) {
            self.removed.insert(element);
            return true;
        }
        false
    }

    /// Returns `true` if the element is in the set.
    /// Meaning in added and not removed set.
    pub fn contains(&self, element: &T) -> bool {
        self.added.contains(element) && !self.removed.contains(element)
    }
}

impl<T> Crdt for TwoPSet<T>
where
    T: Eq + Hash + Clone,
{
    type Value = HashSet<T>;

    /// Returns the set of elements that have been added
    /// but not removed.
    fn value(&self) -> Self::Value {
        let added = self.added.value();
        let removed = self.removed.value();
        added.difference(&removed).cloned().collect()
    }

    /// Returns `true` if both the added and removed sets
    /// of `self` are subsets of `other`'s two sets.
    fn compare(&self, other: &Self) -> bool {
        self.added.compare(&other.added) && self.removed.compare(&other.removed)
    }

    /// Merges by merging both inner GSets independently.
    fn merge(&mut self, other: Self) {
        self.added.merge(other.added);
        self.removed.merge(other.removed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Crdt;
    use proptest::collection::vec as prop_vec;
    use proptest::prelude::*;

    #[test]
    fn insert_and_contains() {
        let mut set = TwoPSet::new();
        set.insert("milk");
        assert!(set.contains(&"milk"));
    }

    #[test]
    fn remove_makes_element_gone() {
        let mut set = TwoPSet::new();
        set.insert("milk");
        assert!(set.remove("milk"));
        assert!(!set.contains(&"milk"));
    }

    #[test]
    fn remove_is_permanent() {
        let mut set = TwoPSet::new();
        set.insert("milk");
        set.remove("milk");
        assert!(!set.insert("milk"));
        assert!(!set.contains(&"milk"));
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut set = TwoPSet::new();
        assert!(!set.remove("milk"));
    }

    #[test]
    fn value_excludes_removed() {
        let mut set = TwoPSet::new();
        set.insert("milk");
        set.insert("eggs");
        set.remove("milk");
        assert_eq!(set.value(), HashSet::from(["eggs"]));
    }

    #[test]
    fn merge_commutativity() {
        let mut a = TwoPSet::new();
        a.insert("milk");
        let mut b = TwoPSet::new();
        b.insert("eggs");
        b.insert("milk");
        b.remove("milk");

        let mut ab = a.clone();
        ab.merge(b.clone());
        let mut ba = b.clone();
        ba.merge(a.clone());

        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn merge_remove_wins_over_add() {
        // peer A has "milk", peer B removed "milk"
        // after merge: "milk" is gone (remove-wins in 2PSet)
        let mut a = TwoPSet::new();
        a.insert("milk");
        let mut b = TwoPSet::new();
        b.insert("milk");
        b.remove("milk");

        a.merge(b);
        assert!(!a.contains(&"milk"));
    }

    #[test]
    fn merge_idempotency() {
        let mut a = TwoPSet::new();
        a.insert("milk");
        a.insert("eggs");
        a.remove("milk");
        let before = a.value();
        a.merge(a.clone());
        assert_eq!(a.value(), before);
    }

    #[test]
    fn compare_subset() {
        let mut a = TwoPSet::new();
        a.insert("milk");
        let mut b = TwoPSet::new();
        b.insert("milk");
        b.insert("eggs");

        assert!(a.compare(&b));
        assert!(!b.compare(&a));
    }
    fn arb_twopset() -> impl Strategy<Value = TwoPSet<u8>> {
        prop_vec((0u8..=3u8, proptest::bool::ANY), 0..=6).prop_map(|ops| {
            let mut set = TwoPSet::new();
            for (elem, is_remove) in ops {
                if is_remove {
                    set.remove(elem);
                } else {
                    set.insert(elem);
                }
            }
            set
        })
    }

    proptest! {
        #[test]
        fn commutative(a in arb_twopset(), b in arb_twopset()) {
            let mut ab = a.clone();
            ab.merge(b.clone());
            let mut ba = b.clone();
            ba.merge(a.clone());
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn associative(a in arb_twopset(), b in arb_twopset(), c in arb_twopset()) {
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
        fn idempotent(a in arb_twopset()) {
            let mut a1 = a.clone();
            a1.merge(a.clone());
            prop_assert_eq!(a1, a);
        }
    }
}
