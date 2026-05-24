//! Application state, the state (source of truth) of the shared canvas.
//!
//! [`AppState`] sits between the network layer (`crdt-net`) and the API
//! layer (`api.rs`). It holds the canvas document and this node's
//! identity. Every mutation — whether from a local browser or a remote
//! gossip merge — flows through here.
//!
//! **Timestamps**
//!
//! There is no standalone timestamp counter on `AppState`. Timestamps
//! live on the [`CanvasDocument`]'s own `VectorClock`, which is part of
//! the replicated state. When a document is gossiped to another peer,
//! the clock travels with it and merges automatically, there is no
//! manual syncing needed.
//!
//! Earlier design kept an `AtomicU64` clock counter here and
//! advanced it by manually after each gossip merge. This was a worse implementations
//! of what `VectorClock::merge` already does, with a race window between
//! the merge and the counter advance. Moving the clock into the document
//! eliminates both problems.
//!
//! **Why all methods are synchronous**
//!
//! State lives inside a [`watch::Sender`], and all writes use its
//! [`send_modify`](watch::Sender::send_modify) method, which is sync.
//! This has two practical benefits:
//!
//! - The closure runs to completion without yielding, so there is no
//!   risk of holding a lock across an await point.
//! - When we add delta support, the same closure can mutate the document
//!   and compute the diff in one atomic step — no gap where another
//!   write could sneak in.
//!
//! The tradeoff is that the closure blocks a tokio worker thread for its
//! duration. For our canvas this is microseconds.
use crate::canvas::CanvasDocument;
use crdt_core::Crdt;
use crdt_net::GossipEngine;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use tokio::sync::watch;
use uuid::Uuid;

/// Shared application state, wrapped in `Arc` and passed to all tasks.
///
/// Only three fields: the node's identity, the canvas channel, and gossip engine.
///  The document's internal [`VectorClock`] handles all timestamp concerns,
/// it increments on local mutations, and merges automatically when
/// remote state arrives via gossip.
///
/// The canvas is stored inside a [`watch::Sender`] which serves as the
/// single source of truth. Readers (WebSocket handlers, the gossip
/// engine) obtain a [`watch::Receiver`] via [`subscribe`](Self::subscribe)
/// and get notified whenever the document changes.
///
/// The [`GossipEngine`] is created *after* `AppState` (it needs the
/// [`watch::Receiver`] that `new` returns), so the engine handle is
/// wired in via [`set_engine`](Self::set_engine) using a [`OnceLock`].
/// This lets the API layer add bootstrap peers at runtime without
/// holding a separate handle to the engine.
pub struct AppState {
    node_id: Uuid,
    /// Single source of truth. Readers subscribe via `self.subscribe()`.
    canvas: watch::Sender<CanvasDocument>,
    /// Gossip engine handle, wired in after construction via `set_engine`.
    /// `OnceLock` because the engine needs the `watch::Receiver` that `new`
    /// produces — the chicken-and-egg is resolved by initializing exactly once
    /// with no runtime locking cost after startup.
    engine: OnceLock<Arc<GossipEngine>>,
}

impl AppState {
    /// Create a new `AppState` with an empty canvas.
    ///
    /// Returns `(state, rx)` where `rx` is the initial
    /// [`watch::Receiver`] used by the gossip engine. Additional
    /// receivers for WebSocket handlers are obtained via
    /// [`subscribe`](Self::subscribe).
    pub fn new(node_id: Uuid) -> (Arc<Self>, watch::Receiver<CanvasDocument>) {
        let (tx, rx) = watch::channel(CanvasDocument::new());
        let state = Arc::new(Self {
            node_id,
            canvas: tx,
            engine: OnceLock::new(),
        });
        (state, rx)
    }

    pub fn node_id(&self) -> Uuid {
        self.node_id
    }

    /// Returns a reference to the gossip engine, if wired in.
    pub fn engine(&self) -> Option<&Arc<GossipEngine>> {
        self.engine.get()
    }

    /// Wire the gossip engine in after construction.
    ///
    /// Called once from `main.rs` after `GossipEngine::run` returns.
    /// Subsequent calls are silently ignored (`OnceLock` guarantees
    /// at-most-once initialization).
    pub fn set_engine(&self, engine: Arc<GossipEngine>) {
        let _ = self.engine.set(engine);
    }

    /// Add a bootstrap peer to the gossip engine at runtime.
    ///
    /// Delegates to [`GossipEngine::add_bootstrap`]. `api.rs` only holds
    /// `Arc<AppState>`, so it can't reach the engine directly. The engine
    /// discovers the peer's UUID on first successful gossip exchange and
    /// migrates it from the bootstrap set into the resolved peer map. TODO: Explain this simpler
    ///
    /// No-op if the engine hasn't been wired in yet via `set_engine`.
    pub fn add_bootstrap(&self, addr: SocketAddr) {
        if let Some(engine) = self.engine.get() {
            engine.add_bootstrap(addr);
        }
    }

    /// Apply an arbitrary mutation to the canvas.
    ///
    /// The closure receives `&mut CanvasDocument` and this node's ID.
    /// Timestamp handling is internal to the document, its `VectorClock`
    /// is incremented by whichever mutation method the closure calls
    /// (e.g. `paint`, `add_user`).
    ///
    /// Returns whatever the closure returns, so callers can extract
    /// a value (e.g. a delta for future WebSocket delivery).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Paint a pixel
    /// state.mutate(|doc, node_id| {
    ///     doc.paint(x, y, color, node_id);
    /// });
    ///
    /// // Add a user
    /// state.mutate(|doc, node_id| doc.add_user(user, &node_id));
    /// ```
    pub fn mutate<R>(&self, f: impl FnOnce(&mut CanvasDocument, Uuid) -> R) -> R {
        let mut result = None;
        self.canvas.send_modify(|doc| {
            result = Some(f(doc, self.node_id));
        });
        tracing::debug!("canvas mutated");
        // `send_modify` calls the closure exactly once, synchronously,
        // before returning, `result` is always `Some` here.
        result.expect("send_modify did not invoke closure")
    }

    /// Merge a remotely-received document into local state.
    ///
    /// The document's `VectorClock` merges as part of [`Crdt::merge`],
    /// so subsequent local writes will automatically have higher
    /// timestamps than anything observed from the remote peer.
    pub fn apply_gossip(&self, incoming: CanvasDocument) {
        self.canvas.send_modify(|doc| doc.merge(incoming));
    }

    /// Borrow the current canvas state.
    ///
    /// Returns a read guard into the watch channel. Hold it only
    /// briefly, while it's alive, `[`mutate`](Self::mutate) and
    /// [`apply_gossip`](Self::apply_gossip) will block waiting
    /// for the guard to drop.
    ///
    /// Use this for quick reads like serializing an HTTP response.
    /// For longer-lived access, use [`snapshot`](Self::snapshot) instead.
    pub fn canvas(&self) -> watch::Ref<'_, CanvasDocument> {
        self.canvas.borrow()
    }

    /// Obtain a receiver that is notified on every state change.
    ///
    /// Used by WebSocket handlers (to push updates to browsers) and
    /// by `main.rs` (to feed the gossip engine).
    pub fn subscribe(&self) -> watch::Receiver<CanvasDocument> {
        self.canvas.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make() -> (Arc<AppState>, watch::Receiver<CanvasDocument>) {
        AppState::new(Uuid::from_u128(1))
    }

    #[test]
    fn paint_via_mutate() {
        let (state, rx) = make();
        state.mutate(|doc, node_id| {
            doc.paint(1, 2, (255, 0, 0, 255), node_id);
        });
        let pixel = rx.borrow().pixels.get(&(1, 2)).map(|r| r.value());
        assert_eq!(pixel, Some((255, 0, 0, 255)));
    }

    #[test]
    fn mutate_returns_value() {
        let (state, _rx) = make();
        let result = state.mutate(|_doc, _id| 42);
        assert_eq!(result, 42);
    }

    #[test]
    fn apply_gossip_merges() {
        let (state, rx) = make();
        let mut incoming = CanvasDocument::new();
        incoming.paint(5, 5, (0, 255, 0, 255), Uuid::from_u128(2));
        state.apply_gossip(incoming);
        let pixel = rx.borrow().pixels.get(&(5, 5)).map(|r| r.value());
        assert_eq!(pixel, Some((0, 255, 0, 255)));
    }

    #[test]
    fn gossip_merge_advances_clock() {
        let (state, _rx) = make();

        let mut incoming = CanvasDocument::new();
        let remote_id = Uuid::from_u128(2);
        incoming.paint(0, 0, (255, 0, 0, 255), remote_id);

        state.apply_gossip(incoming);

        // A subsequent local paint should have a higher timestamp
        // than the remote one, because VectorClock merged.
        state.mutate(|doc, node_id| {
            doc.paint(0, 0, (0, 0, 255, 255), node_id);
        });

        // Local write should win (its clock entry is newer).
        let pixel = state.canvas().pixels.get(&(0, 0)).map(|r| r.value());
        assert_eq!(pixel, Some((0, 0, 255, 255)));
    }

    #[test]
    fn subscribe_sees_changes() {
        let (state, _rx) = make();
        let watcher = state.subscribe();
        state.mutate(|doc, id| doc.paint(0, 0, (1, 2, 3, 4), id));
        assert!(watcher.has_changed().unwrap());
    }

    #[test]
    fn add_bootstrap_without_engine_is_noop() {
        let (state, _rx) = make();
        // Must not panic when engine not yet wired in.
        state.add_bootstrap("127.0.0.1:9090".parse().unwrap());
    }
}
