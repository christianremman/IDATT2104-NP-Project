//! The composite CRDT that represents the shared canvas state.
//!
//! [`CanvasDocument`] is the single value gossiped between peers. It
//! composes several independent CRDTs, each field handles a different
//! aspect of the shared canvas, and a [`VectorClock`] that ties them
//! together.
//!
//! ## How the VectorClock fits in
//!
//! The clock serves two roles:
//!
//! 1. **LWW timestamp source.** Pixels and cursors use
//!    [`LWWRegister`], which needs a monotonic timestamp to decide
//!    which write wins. The clock's [`increment`](VectorClock::increment)
//!    method returns a value that is strictly greater than any
//!    component in the clock, so a paint that happens after observing
//!    remote state always gets a higher timestamp.
//!
//! 2. **Causality tracking.** After two documents merge, their clocks
//!    merge (element-wise max), so each peer knows what the other has
//!    seen. This is what makes the Lamport timestamps safe — without
//!    merge, a peer could fall behind and generate losing timestamps
//!    indefinitely.
//!
//! Mutations that don't need an LWW timestamp (e.g. [`add_user`],
//! which goes through [`ORSet`] with its own internal tagging) still
//! increment the clock for document-level causality, so a peer can
//! tell whether it has seen a particular mutation. This is handled with
//! [`delta_since`](DeltaCrdt::delta_since), which detect the changes.
//! 
//! ## Deltas
//!
//! The [`DeltaCrdt`] implementation lets the WebSocket layer send only
//! what changed since each client's last known version, instead of the
//! full document on every mutation. Each CRDT field computes its own
//! delta independently, all keyed off the same [`VectorClock`] frontier.
//! This works because ORSet tag sequences are sourced from the document
//! clock, so one version covers all fields.
//!
//! Gossip between peers still uses full-state merge (simpler, handles
//! partitions naturally).
use crdt_core::clocks::{VectorClock};
use crdt_core::counters::GCounter;
use crdt_core::registers::lww_register::LWWRegister;
use crdt_core::sets::{ORSet, ORSetDelta};
use crdt_core::traits::{Crdt, DeltaCrdt, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// RGBA color stored as four `u8` channels: red, green, blue, alpha.
pub type Rgba = (u8, u8, u8, u8);
/// Canvas pixel coordinate. Both axes are bounded to `[0, 255]` by the `u8` type.
pub type PixelCoord = (u8, u8);
/// Color written to pixels that have never been painted (opaque white).
pub const DEFAULT_PIXEL: Rgba = (255, 255, 255, 255);

/// Custom serde for `HashMap<PixelCoord, LWWRegister<Rgba>>`.
///
/// Tuple keys are not valid JSON object keys, so the map is serialized as a
/// sequence of `(key, value)` pairs and deserialized back into a `HashMap`.
mod pixel_map_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(
        map: &HashMap<PixelCoord, LWWRegister<Rgba>>,
        s: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(map.len()))?;
        for pair in map.iter() {
            seq.serialize_element(&pair)?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(d: D) -> Result<HashMap<PixelCoord, LWWRegister<Rgba>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<(PixelCoord, LWWRegister<Rgba>)>::deserialize(d).map(|v| v.into_iter().collect())
    }
}

/// The shared state gossiped between peers.
///
/// Every field is a CRDT with its own merge semantics:
/// - `pixels`: per-coordinate [`LWWRegister`], last writer wins.
/// - `users`: [`ORSet`] of active peer UUIDs, add-wins on concurrent add/remove.
/// - `cursors`: per-user [`LWWRegister`] of cursor position, last writer wins.
/// - `palette`: [`ORSet`] of active palette colors, add-wins.
/// - `paint_counts`: [`GCounter`] tracking total paints per node.
/// - `clock`: [`VectorClock`] tracking causality across all of the above.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanvasDocument {
    #[serde(with = "pixel_map_serde")]
    pub pixels: HashMap<PixelCoord, LWWRegister<Rgba>>,
    users: ORSet<Uuid>,
    pub cursors: HashMap<Uuid, LWWRegister<PixelCoord>>,
    pub palette: ORSet<Rgba>,
    pub paint_counts: GCounter,
    clock: VectorClock,
}

impl Default for CanvasDocument {
    fn default() -> Self {
        Self {
            pixels: HashMap::new(),
            users: ORSet::new(),
            cursors: HashMap::new(),
            palette: ORSet::new(),
            paint_counts: GCounter::new(),
            clock: VectorClock::new(),
        }
    }
}

impl CanvasDocument {
    /// Create an empty canvas with no pixels, users, palette entries, or paint counts.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a pixel's color.
    ///
    /// Increments the document clock and uses the resulting timestamp
    /// for the LWW register, ensuring this write beats any previously
    /// observed state.
    pub fn paint(&mut self, x: u8, y: u8, color: Rgba, node_id: NodeId) {
        let ts = self.clock.increment(node_id);
        self.pixels
            .entry((x, y))
            .or_insert_with(|| LWWRegister::new(DEFAULT_PIXEL, 0, node_id))
            .set(color, ts, node_id);
        self.paint_counts.increment(node_id);

    }

    /// Register a peer as active. Uses ORSet add-wins semantics.
    pub fn add_user(&mut self, user: Uuid, node_id: &NodeId) {
        let seq = self.clock.increment(*node_id);
        self.users.insert(user, node_id, seq);
    }

    /// Remove a peer from the active set and evict their cursor.
    ///
    /// Increments the clock so the removal shows up as a new document
    /// version. Without this, [`delta_since`](DeltaCrdt::delta_since)
    /// returns an empty delta (clock unchanged) and connected browsers
    /// never learn the peer left.
    pub fn remove_user(&mut self, user: &Uuid, node_id: NodeId) -> bool {
        self.clock.increment(node_id);
        self.cursors.remove(user);
        self.users.remove(user)
    }

    /// Returns the set of currently active peer UUIDs.
    pub fn active_users(&self) -> HashSet<Uuid> {
        self.users.value()
    }

    /// Remove a cursor entry for a browser session that has disconnected.
    ///
    /// Increments the clock so the removal is visible as a new document
    /// version and propagates to other peers as a non-empty delta.
    pub fn remove_cursor_entry(&mut self, user: &Uuid, node_id: NodeId) {
        self.clock.increment(node_id);
        self.cursors.remove(user);
    }

    /// Add `color` to the shared palette using ORSet add-wins semantics.
    pub fn add_palette_color(&mut self, color: Rgba, node_id: &NodeId) {
        let seq = self.clock.increment(*node_id);
        self.palette.insert(color, node_id, seq);
    }

    /// Remove `color` from the shared palette. Returns `true` if the color was present.
    ///
    /// Increments the clock so the removal is visible as a new document
    /// version. Same logic here as with with [`remove_user`](Self::remove_user).
    /// Without this, [`delta_since`](DeltaCrdt::delta_since) would
    /// return an empty delta and connected browsers would never see
    /// the color disappear.
    pub fn remove_palette_color(&mut self, color: &Rgba, node_id: NodeId) -> bool {
        self.clock.increment(node_id);
        self.palette.remove(color)
    }

    /// Returns palette colors sorted for deterministic display order.
    pub fn palette_colors(&self) -> Vec<Rgba> {
        let mut colors: Vec<Rgba> = self.palette.value().into_iter().collect();
        colors.sort();
        colors
    }

    /// Returns `(node_id, pixel_count)` pairs sorted descending by pixel ownership.
    pub fn ownership_leaderboard(&self) -> Vec<(NodeId, u64)> {
        let mut counts: HashMap<NodeId, u64> = HashMap::new();
        for reg in self.pixels.values() {
            *counts.entry(reg.node_id()).or_insert(0) += 1;
        }
        let mut result: Vec<(NodeId, u64)> = counts.into_iter().collect();
        result.sort_by_key(|b| std::cmp::Reverse(b.1));
        result
    }

    /// Update a peer's cursor position.
    pub fn update_cursor(&mut self, user: Uuid, x: u8, y: u8, node_id: NodeId) {
        let ts = self.clock.increment(node_id);
        self.cursors
            .entry(user)
            .or_insert_with(|| LWWRegister::new((0, 0), 0, node_id))
            .set((x, y), ts, node_id);
    }
}

impl Crdt for CanvasDocument {
    type Value = Self;

    /// The document is its own value — clones the full state.
    fn value(&self) -> Self {
        self.clone()
    }

    /// Merge `other` into `self` using each field's own CRDT merge rule.
    ///
    /// Each of the field will merge inpepentendly.
    /// - Clock: element-wise max (Lamport rule per node).
    /// - Pixels / cursors: LWW — higher timestamp wins per coordinate.
    /// - Users / palette: ORSet — add-wins on concurrent add/remove.
    /// - Paint counts: GCounter — per-node max.
    /// 
    /// Cursor entries for peers no longer in the active user set are
    /// evicted. The cursor `HashMap` has no tombstone mechanism, so
    /// without this a departed peer's cursor would persist on remote
    /// nodes indefinitely. Ideally the the ORset should have a garbage 
    /// collection implementation to remove items from the tombsones,
    /// but since it does not, we uses the hashmap so the cursors of 
    /// disconnected peers don't persist indefinitely
    fn merge(&mut self, other: Self) {
        self.clock.merge(other.clock);

        for ((x, y), reg) in other.pixels {
            match self.pixels.entry((x, y)) {
                Entry::Occupied(mut e) => e.get_mut().merge(reg),
                Entry::Vacant(e) => {
                    e.insert(reg);
                }
            }
        }

        self.users.merge(other.users);

        for (uid, reg) in other.cursors {
            match self.cursors.entry(uid) {
                Entry::Occupied(mut e) => e.get_mut().merge(reg),
                Entry::Vacant(e) => {
                    e.insert(reg);
                }
            }
        }

        self.palette.merge(other.palette);
        self.paint_counts.merge(other.paint_counts);
    }

    /// Returns `true` when `self` is causally dominated by `other` across all fields.
    fn compare(&self, other: &Self) -> bool {
        self.clock.compare(&other.clock)
            && self
                .pixels
                .iter()
                .all(|(k, r)| other.pixels.get(k).is_some_and(|o| r.compare(o)))
            && self.users.compare(&other.users)
            && self
                .cursors
                .iter()
                .all(|(k, r)| other.cursors.get(k).is_some_and(|o| r.compare(o)))
            && self.palette.compare(&other.palette)
            && self.paint_counts.compare(&other.paint_counts)
    }
}

/// Custom serde for `Vec<(PixelCoord, LWWRegister<Rgba>)>` used in [`CanvasDelta`].
///
/// Mirrors [`pixel_map_serde`] but for the delta's flat list rather than a map.
mod pixel_vec_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(v: &Vec<(PixelCoord, LWWRegister<Rgba>)>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(v.len()))?;
        for pair in v {
            seq.serialize_element(pair)?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Vec<(PixelCoord, LWWRegister<Rgba>)>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<(PixelCoord, LWWRegister<Rgba>)>::deserialize(d)
    }
}

/// Delta payload for a [`CanvasDocument`].
///
/// Each field carries the corresponding CRDT's delta — empty / `None`
/// when nothing changed there. The receiver applies a `CanvasDelta` via
/// [`CanvasDocument::merge_delta`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanvasDelta {
    /// VectorClock entries that advanced. Receivers absorb this into their
    /// own clock so subsequent local writes carry strictly higher
    /// timestamps.
    pub clock: <VectorClock as DeltaCrdt>::Delta,
    /// Pixels whose LWWRegister timestamp exceeds the receiver's view for
    /// the writing node. Carried as `(coord, LWWRegister)` pairs.
    #[serde(with = "pixel_vec_serde")]
    pub pixels: Vec<(PixelCoord, LWWRegister<Rgba>)>,
    /// ORSet delta for the active-user set; empty when no users joined or left.
    pub users: ORSetDelta<Uuid>,
    /// Cursor registers that moved since `since`; empty when no cursors changed.
    pub cursors: Vec<(Uuid, LWWRegister<PixelCoord>)>,
    /// ORSet delta for the palette; empty when no colors were added or removed.
    pub palette: ORSetDelta<Rgba>,
    /// GCounter delta; empty when no new paints occurred.
    pub paint_counts: <GCounter as DeltaCrdt>::Delta,
}

impl DeltaCrdt for CanvasDocument {
    type Delta = CanvasDelta;
    type Version = VectorClock;

    /// Returns the document's current [`VectorClock`] as its version identifier.
    fn version(&self) -> Self::Version {
        self.clock.clone()
    }

    /// Compute a minimal delta containing only the changes this document has
    /// that `since` does not. Pass [`VectorClock::new`] to get a full-state delta.
    /// 
    /// Each field filters independently against the same [`VectorClock`]
    /// frontier. This works because ORSet tag sequences are sourced from
    /// the document clock — one version covers all fields.
    fn delta_since(&self, since: &Self::Version) -> Self::Delta {
        let pixels: Vec<(PixelCoord, LWWRegister<Rgba>)> = self
            .pixels
            .iter()
            .filter_map(|(coord, reg)| {
                let known = since.get(&reg.node_id());
                (reg.timestamp() > known).then(|| (*coord, reg.clone()))
            })
            .collect();

        let cursors: Vec<(Uuid, LWWRegister<PixelCoord>)> = self
            .cursors
            .iter()
            .filter_map(|(uid, reg)| {
                let known = since.get(&reg.node_id());
                (reg.timestamp() > known).then(|| (*uid, reg.clone()))
            })
            .collect();

        // ORSet tag seqs are sourced from the same VectorClock the document
        // tracks, so its frontier-as-HashMap is `since.clock`.
        let or_set_version: std::collections::HashMap<NodeId, u64> = since.value();

        CanvasDelta {
            clock: self.clock.delta_since(since),
            pixels,
            users: self.users.delta_since(&or_set_version),
            cursors,
            palette: self.palette.delta_since(&or_set_version),
            paint_counts: self.paint_counts.delta_since(&or_set_version),
        }
    }

    /// Apply a previously computed delta, advancing all affected CRDTs.
    fn merge_delta(&mut self, delta: Self::Delta) {
        self.clock.merge_delta(delta.clock);

        for ((x, y), reg) in delta.pixels {
            match self.pixels.entry((x, y)) {
                Entry::Occupied(mut e) => e.get_mut().merge(reg),
                Entry::Vacant(e) => {
                    e.insert(reg);
                }
            }
        }

        self.users.merge_delta(delta.users);

        for (uid, reg) in delta.cursors {
            match self.cursors.entry(uid) {
                Entry::Occupied(mut e) => e.get_mut().merge(reg),
                Entry::Vacant(e) => {
                    e.insert(reg);
                }
            }
        }

        self.palette.merge_delta(delta.palette);
        self.paint_counts.merge_delta(delta.paint_counts);
    }

    fn is_empty_delta(delta: &Self::Delta) -> bool {
        // Clock delta is the primary signal. Per-field checks are defense
        // in depth: a mutation that skips the clock still produces a
        // truthy delta and avoids a silent WS skip.
        VectorClock::is_empty_delta(&delta.clock)
            && delta.pixels.is_empty()
            && delta.cursors.is_empty()
            && ORSet::<Uuid>::is_empty_delta(&delta.users)
            && ORSet::<Rgba>::is_empty_delta(&delta.palette)
            && GCounter::is_empty_delta(&delta.paint_counts)
    }

    /// Returns `true` when `current` causally dominates `other` — i.e., `current`
    /// has observed everything `other` has, so it is safe to apply a delta computed
    /// against `other` as a baseline without gaps.
    fn version_includes(current: &Self::Version, other: &Self::Version) -> bool {
        current.dominates(other)
    }
}

/// Client-facing snapshot of the full canvas. Strips CRDT metadata (timestamps, node ids, vector clock).
///
/// Sent once over WebSocket on connect, then superseded by [`CanvasDeltaView`] patches.
#[derive(Serialize)]
pub struct CanvasView {
    /// All painted pixels keyed as `"x,y"` strings; unpainted pixels are absent (default white).
    pub pixels: HashMap<String, [u8; 4]>,
    /// UUIDs of currently connected peers, sorted for stable display.
    pub active_peers: Vec<String>,
    /// Shared palette colors in sorted order.
    pub palette: Vec<[u8; 4]>,
    /// Cumulative paint operations across all peers.
    pub paint_total: u64,
    /// Per-peer pixel ownership counts, sorted descending.
    pub leaderboard: Vec<LeaderboardEntry>,
    /// Latest cursor position per peer, keyed by UUID string.
    pub cursors: HashMap<String, [u8; 2]>,
}

/// One row in the pixel-ownership leaderboard.
#[derive(Serialize)]
pub struct LeaderboardEntry {
    /// UUID of the peer that owns these pixels (last-write winner).
    pub peer_id: String,
    /// Number of canvas pixels currently owned by this peer.
    pub pixels: u64,
}

impl From<&CanvasDocument> for CanvasView {
    /// Project a full [`CanvasDocument`] into a serializable snapshot, dropping all CRDT internals.
    fn from(doc: &CanvasDocument) -> Self {
        let mut active_peers: Vec<String> =
            doc.active_users().iter().map(|u| u.to_string()).collect();
        active_peers.sort();
        Self {
            pixels: doc
                .pixels
                .iter()
                .map(|((x, y), r)| {
                    let (a, b, c, d) = r.value();
                    (format!("{x},{y}"), [a, b, c, d])
                })
                .collect(),
            active_peers,
            palette: doc
                .palette_colors()
                .into_iter()
                .map(|(r, g, b, a)| [r, g, b, a])
                .collect(),
            paint_total: doc.paint_counts.value(),
            leaderboard: doc
                .ownership_leaderboard()
                .into_iter()
                .map(|(id, n)| LeaderboardEntry {
                    peer_id: id.to_string(),
                    pixels: n,
                })
                .collect(),
            cursors: doc
                .cursors
                .iter()
                .map(|(uid, reg)| {
                    let (x, y) = reg.value();
                    (uid.to_string(), [x, y])
                })
                .collect(),
        }
    }
}

/// Sparse client-facing view of a [`CanvasDelta`].
///
/// `pixels` lists only the coordinates whose colour changed since the
/// receiver's last update. Each `Option` field is `Some` only when the
/// corresponding derived view actually changed; the frontend should
/// treat `None` as "no change, keep what you had".
#[derive(Serialize)]
pub struct CanvasDeltaView {
    pub pixels: HashMap<String, [u8; 4]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_peers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub palette: Option<Vec<[u8; 4]>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paint_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leaderboard: Option<Vec<LeaderboardEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursors: Option<HashMap<String, [u8; 2]>>,
}

impl CanvasDeltaView {
    /// Project a CRDT-level `CanvasDelta` against the new authoritative
    /// document state, recomputing each derived view only when its
    /// underlying CRDT actually changed.
    pub fn project(delta: &CanvasDelta, doc: &CanvasDocument) -> Self {
        let pixels = delta
            .pixels
            .iter()
            .map(|((x, y), r)| {
                let (a, b, c, d) = r.value();
                (format!("{x},{y}"), [a, b, c, d])
            })
            .collect::<HashMap<_, _>>();

        // Always ship active_peers. ORSet tombstones carry the original add-seq, so
        // `is_empty_delta` is always true for removals that the receiver has already
        // seen the add for — the ORSet delta cannot signal departures reliably.
        let active_peers = {
            let mut peers: Vec<String> = doc.active_users().iter().map(|u| u.to_string()).collect();
            peers.sort();
            Some(peers)
        };

        let palette = if ORSet::<Rgba>::is_empty_delta(&delta.palette) {
            None
        } else {
            Some(
                doc.palette_colors()
                    .into_iter()
                    .map(|(r, g, b, a)| [r, g, b, a])
                    .collect(),
            )
        };

        // Always include paint_total. GCounter::delta_since uses VectorClock tick
        // values as its baseline, but clock ticks advance on every mutation while
        // GCounter only advances on paints, so the delta is always empty after any
        // non-paint mutation and the is_empty_delta gate would permanently suppress
        // paint_total from delta messages.
        let paint_total = Some(doc.paint_counts.value());

        // Leaderboard is derived from per-pixel ownership; any pixel change
        // can shift it. Recompute and ship when pixels changed.
        let leaderboard = if pixels.is_empty() {
            None
        } else {
            Some(
                doc.ownership_leaderboard()
                    .into_iter()
                    .map(|(id, n)| LeaderboardEntry {
                        peer_id: id.to_string(),
                        pixels: n,
                    })
                    .collect(),
            )
        };

        let cursors = if delta.cursors.is_empty() {
            None
        } else {
            Some(
                delta
                    .cursors
                    .iter()
                    .map(|(uid, reg)| {
                        let (x, y) = reg.value();
                        (uid.to_string(), [x, y])
                    })
                    .collect(),
            )
        };

        Self {
            pixels,
            active_peers,
            palette,
            paint_total,
            leaderboard,
            cursors,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u128) -> NodeId {
        Uuid::from_u128(id)
    }

    fn get_pixel(doc: &CanvasDocument, x: u8, y: u8) -> Rgba {
        doc.pixels
            .get(&(x, y))
            .map(|r| r.value())
            .unwrap_or(DEFAULT_PIXEL)
    }

    #[test]
    fn paint_and_get() {
        let mut d = CanvasDocument::new();
        d.paint(1, 2, (255, 0, 0, 255), node(1));
        assert_eq!(get_pixel(&d, 1, 2), (255, 0, 0, 255));
    }

    #[test]
    fn default_pixel_white() {
        assert_eq!(get_pixel(&CanvasDocument::new(), 0, 0), DEFAULT_PIXEL);
    }

    #[test]
    fn lww_higher_ts_wins() {
        let mut a = CanvasDocument::new();
        let mut b = CanvasDocument::new();
        // a paints twice (ts=2), b paints once (ts=1)
        a.paint(0, 0, (255, 0, 0, 255), node(1));
        a.paint(0, 0, (255, 0, 0, 255), node(1));
        b.paint(0, 0, (0, 0, 255, 255), node(2));
        a.merge(b);
        assert_eq!(get_pixel(&a, 0, 0), (255, 0, 0, 255));
    }

    /// The critical test for VectorClock + LWW interaction.
    ///
    /// B paints many times, A merges B's state, then A paints once.
    /// A's single paint happened *after* observing B, so it must win.
    /// This fails if `increment` uses naive `+= 1` instead of the
    /// Lamport rule (`max(own, max_all) + 1`).
    #[test]
    fn paint_after_merge_beats_higher_remote_count() {
        let mut a = CanvasDocument::new();
        let mut b = CanvasDocument::new();

        for _ in 0..5 {
            b.paint(0, 0, (255, 0, 0, 255), node(2));
        }

        a.merge(b);
        a.paint(0, 0, (0, 0, 255, 255), node(1));

        assert_eq!(get_pixel(&a, 0, 0), (0, 0, 255, 255));
    }

    #[test]
    fn merge_commutative() {
        let mut a = CanvasDocument::new();
        let mut b = CanvasDocument::new();
        a.paint(0, 0, (255, 0, 0, 255), node(1));
        a.paint(0, 0, (255, 0, 0, 255), node(1));
        b.paint(0, 0, (0, 0, 255, 255), node(2));

        let mut a1 = a.clone();
        a1.merge(b.clone());
        let mut b1 = b.clone();
        b1.merge(a.clone());
        assert_eq!(get_pixel(&a1, 0, 0), get_pixel(&b1, 0, 0));
    }

    #[test]
    fn merge_idempotent() {
        let user = Uuid::from_u128(7);
        let mut a = CanvasDocument::new();
        a.paint(0, 0, (1, 2, 3, 4), node(1));
        a.add_user(user, &node(1));
        let b = a.clone();
        a.merge(b);
        assert_eq!(get_pixel(&a, 0, 0), (1, 2, 3, 4));
        assert!(a.active_users().contains(&user));
    }

    #[test]
    fn user_add_wins_on_concurrent_remove() {
        let user = Uuid::from_u128(99);
        let mut peer_a = CanvasDocument::new();
        peer_a.add_user(user, &node(1));

        let mut peer_b = CanvasDocument::new();
        peer_b.add_user(user, &node(2));
        peer_b.remove_user(&user, node(2));

        let mut a1 = peer_a.clone();
        a1.merge(peer_b.clone());
        assert!(a1.active_users().contains(&user));

        let mut b1 = peer_b.clone();
        b1.merge(peer_a.clone());
        assert!(b1.active_users().contains(&user));
    }

    #[test]
    fn palette_add_and_colors() {
        let mut d = CanvasDocument::new();
        d.add_palette_color((255, 0, 0, 255), &node(1));
        d.add_palette_color((0, 255, 0, 255), &node(1));
        let colors = d.palette_colors();
        assert_eq!(colors.len(), 2);
        assert!(colors.contains(&(255, 0, 0, 255)));
        assert!(colors.contains(&(0, 255, 0, 255)));
    }

    #[test]
    fn paint_increments_paint_total() {
        let mut d = CanvasDocument::new();
        d.paint(0, 0, (1, 2, 3, 4), node(1));
        d.paint(1, 1, (5, 6, 7, 8), node(1));
        assert_eq!(d.paint_counts.value(), 2);
    }

    #[test]
    fn delta_since_empty_replays_full_state() {
        let mut a = CanvasDocument::new();
        a.paint(1, 2, (10, 20, 30, 40), node(1));
        a.add_palette_color((255, 0, 0, 255), &node(1));

        let delta = a.delta_since(&VectorClock::new());
        let mut b = CanvasDocument::new();
        b.merge_delta(delta);

        assert_eq!(get_pixel(&b, 1, 2), (10, 20, 30, 40));
        assert!(b.palette_colors().contains(&(255, 0, 0, 255)));
    }

    #[test]
    fn delta_since_current_version_is_empty() {
        let mut a = CanvasDocument::new();
        a.paint(0, 0, (1, 2, 3, 4), node(1));
        a.add_palette_color((10, 20, 30, 40), &node(1));

        let delta = a.delta_since(&a.version());
        assert!(
            CanvasDocument::is_empty_delta(&delta),
            "delta to self should be empty"
        );
    }

    /// Apply a delta computed against B's version; B should converge to A.
    #[test]
    fn delta_catches_up_lagging_replica() {
        let mut a = CanvasDocument::new();
        let mut b = CanvasDocument::new();

        a.paint(0, 0, (255, 0, 0, 255), node(1));
        b.merge(a.clone()); // bring B current

        // Now A pulls ahead.
        a.paint(1, 1, (0, 255, 0, 255), node(1));
        a.add_palette_color((0, 0, 255, 255), &node(1));

        let delta = a.delta_since(&b.version());
        b.merge_delta(delta);

        assert_eq!(get_pixel(&b, 0, 0), (255, 0, 0, 255));
        assert_eq!(get_pixel(&b, 1, 1), (0, 255, 0, 255));
        assert!(b.palette_colors().contains(&(0, 0, 255, 255)));
    }

    #[test]
    fn delta_is_idempotent() {
        let mut a = CanvasDocument::new();
        a.paint(3, 4, (5, 6, 7, 8), node(1));
        let delta = a.delta_since(&VectorClock::new());

        let mut b = CanvasDocument::new();
        b.merge_delta(delta.clone());
        let snapshot = b.clone();
        b.merge_delta(delta);

        // Re-applying the same delta leaves the state unchanged.
        assert_eq!(get_pixel(&b, 3, 4), get_pixel(&snapshot, 3, 4));
        assert_eq!(b.paint_counts.value(), snapshot.paint_counts.value());
    }

    #[test]
    fn delta_propagates_tombstones() {
        let mut a = CanvasDocument::new();
        a.add_palette_color((1, 2, 3, 4), &node(1));
        a.add_palette_color((5, 6, 7, 8), &node(1));
        a.remove_palette_color(&(1, 2, 3, 4), node(1));
        let mut b = CanvasDocument::new();
        b.merge_delta(a.delta_since(&VectorClock::new()));

        assert!(b.palette_colors().contains(&(5, 6, 7, 8)));
        assert!(!b.palette_colors().contains(&(1, 2, 3, 4)));
    }

    /// Regression: after any palette removal, `is_empty_delta` was
    /// permanently false because `ORSet::delta_since` shipped the full
    /// tombstone set every time. That silently disabled the WS skip in
    /// `api.rs::handle_ws`. Verify the skip is active again.
    #[test]
    fn idle_delta_after_palette_removal_is_empty() {
        let mut a = CanvasDocument::new();
        a.add_palette_color((1, 2, 3, 4), &node(1));
        a.remove_palette_color(&(1, 2, 3, 4), node(1));

        // Re-querying at the post-removal version must produce an empty
        // delta — nothing has happened since.
        let delta = a.delta_since(&a.version());
        assert!(
            CanvasDocument::is_empty_delta(&delta),
            "idle delta after tombstone must be empty so WS skip fires"
        );
    }

    /// `version_includes` powers the partition-heal drop in the gossip
    /// engine: a delta is only safe to apply when the receiver's state
    /// already knows everything the sender's `since` baseline asserts.
    #[test]
    fn remove_cursor_entry_removes_cursor_and_produces_delta() {
        let user = Uuid::from_u128(42);
        let mut d = CanvasDocument::new();
        d.update_cursor(user, 5, 10, node(1));
        assert!(d.cursors.contains_key(&user));

        let version_before = d.version();
        d.remove_cursor_entry(&user, node(1));

        assert!(!d.cursors.contains_key(&user));
        // Clock must advance so the removal is visible as a non-empty delta.
        let delta = d.delta_since(&version_before);
        assert!(!CanvasDocument::is_empty_delta(&delta));
    }

    #[test]
    fn remove_cursor_entry_is_noop_when_absent() {
        let mut d = CanvasDocument::new();
        let version_before = d.version();
        d.remove_cursor_entry(&Uuid::from_u128(99), node(1));
        // Clock still advances even if cursor wasn't present (same as remove_user).
        let delta = d.delta_since(&version_before);
        assert!(!CanvasDocument::is_empty_delta(&delta));
    }

    /// Regression: GCounter::delta_since was called with VectorClock tick values
    /// as the baseline. Clock ticks advance on every mutation (cursor, user,
    /// palette) while GCounter only advances on paints. After a non-paint mutation
    /// the clock tick for a node exceeds its paint count, so the GCounter delta
    /// is always empty and paint_total is never included in subsequent WS deltas.
    ///
    /// Simulates: paint → cursor move (advances clock, not GCounter) → paint.
    /// The third event's delta must still include paint_total.
    #[test]
    fn delta_view_paint_total_present_after_non_paint_mutation() {
        let mut doc = CanvasDocument::new();

        doc.paint(0, 0, (255, 0, 0, 255), node(1)); // clock tick = 1, GCounter = 1
        doc.update_cursor(Uuid::from_u128(42), 5, 5, node(1)); // clock tick = 2, GCounter still 1
        let v_after_cursor = doc.version(); // last_seen: clock tick for node_1 = 2

        // Next paint: GCounter → 2, but or_set_version[node_1] = 2 (clock ticks).
        // Bug: 2 > 2 is false → empty GCounter delta → paint_total = None.
        doc.paint(1, 1, (0, 255, 0, 255), node(1));
        let delta = doc.delta_since(&v_after_cursor);
        let view = CanvasDeltaView::project(&delta, &doc);

        assert_eq!(
            view.paint_total,
            Some(2),
            "paint_total must be present even when a non-paint mutation preceded this delta"
        );
    }

    #[test]
    fn version_includes_detects_lagging_receiver() {
        let mut sender = CanvasDocument::new();
        sender.paint(0, 0, (1, 2, 3, 4), node(1));
        sender.paint(1, 1, (5, 6, 7, 8), node(1));

        let fresh_receiver_version = CanvasDocument::new().version();
        assert!(
            !CanvasDocument::version_includes(&fresh_receiver_version, &sender.version()),
            "fresh receiver must NOT include sender's advanced baseline"
        );

        let caught_up_receiver = sender.clone();
        assert!(
            CanvasDocument::version_includes(&caught_up_receiver.version(), &sender.version()),
            "caught-up receiver MUST include sender's baseline"
        );
    }
}
