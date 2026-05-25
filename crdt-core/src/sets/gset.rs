//! Grow-only Set (GSet) CRDT.
//!
//! The simplest set CRDT. Elements can be added but never removed.
//! Merge is set union. Serves as a building block for [`TwoPSet`](super::TwoPSet).
use std::collections::HashSet;
use std::hash::Hash;

use crate::traits::Crdt;

/// A grow-only set where elements can be added but never removed.
/// Is a set union, guaranteeing convergence across peers
///
/// Peer A: {"milk", "bread"}
/// Peer B: {"milk", "eggs"}
/// will give us
/// Union:  {"milk", "bread", "eggs"}
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct GSet<T>
where
    T: Eq + Hash + Clone,
{
    elements: HashSet<T>,
}

impl<T> Default for GSet<T>
where
    T: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self {
            elements: HashSet::new(),
        }
    }
}

impl<T> GSet<T>
where
    T: Eq + Hash + Clone,
{
    /// Creates an empty GSet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an element to the set. Duplicate inserts are ignored.
    pub fn insert(&mut self, element: T) -> bool {
        self.elements.insert(element)
    }

    /// Returns `true` if the set contains the element.
    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains(element)
    }
}

impl<T> Crdt for GSet<T>
where
    T: Eq + Hash + Clone,
{
    type Value = HashSet<T>;

    /// Returns the current set of elements.
    fn value(&self) -> Self::Value {
        self.elements.clone()
    }

    /// Merges another GSet into this one by taking the union.
    fn merge(&mut self, other: Self) {
        for element in other.elements {
            self.elements.insert(element);
        }
    }

    /// Returns `true` if every element in `self` is also in `other`.
    fn compare(&self, other: &Self) -> bool {
        self.elements.is_subset(&other.elements)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Crdt;

    #[test]
    fn insert_and_contains() {
        let mut set = GSet::new();
        assert!(!set.contains(&"milk"));
        set.insert("milk");
        assert!(set.contains(&"milk"));
    }

    #[test]
    fn duplicate_insert_ignored() {
        let mut set = GSet::new();
        assert!(set.insert("milk")); // first add will be true
        assert!(!set.insert("milk")); // duplicate will be ignored aka. return false
    }

    #[test]
    fn value_returns_all_elements() {
        let mut set = GSet::new();
        set.insert("milk");
        set.insert("eggs");
        let val = set.value();
        assert_eq!(val, HashSet::from(["milk", "eggs"]));
    }

    #[test]
    fn merge_is_union() {
        let mut a = GSet::new();
        a.insert("milk");
        let mut b = GSet::new();
        b.insert("eggs");

        a.merge(b);
        assert!(a.contains(&"milk"));
        assert!(a.contains(&"eggs"));
    }

    #[test]
    fn merge_commutativity() {
        let mut a = GSet::new();
        a.insert("milk");
        let mut b = GSet::new();
        b.insert("eggs");

        let mut ab = a.clone();
        ab.merge(b.clone());
        let mut ba = b.clone();
        ba.merge(a.clone());

        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn merge_idempotency() {
        let mut a = GSet::new();
        a.insert("milk");
        let before = a.value();
        a.merge(a.clone());
        assert_eq!(a.value(), before);
    }

    #[test]
    fn compare_subset() {
        let mut a = GSet::new();
        a.insert("milk");
        let mut b = GSet::new();
        b.insert("milk");
        b.insert("eggs");

        assert!(a.compare(&b)); // a in b
        assert!(!b.compare(&a)); // b not in a
    }
}
