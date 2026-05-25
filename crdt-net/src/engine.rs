use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crdt_core::DeltaCrdt;
use rand::seq::IteratorRandom;
use serde::{Serialize, de::DeserializeOwned};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore, broadcast, watch};
use tokio::time;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use crate::config::GossipConfig;
use crate::discovery;
use crate::message::{GossipMessage, PeerEntry, read_frame, write_frame};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Per-connection deadline for reading the first (and only) frame. A peer
/// that opens TCP and sends only a partial header would otherwise hold
/// the handler task open indefinitely.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const FANOUT: usize = 2;
const KNOWN_PEERS_CAP: usize = 64;
/// Number of consecutive failed sends to a peer address before we evict it.
/// With the default 1s tick interval this is ~10 seconds of unreachability.
const FAILURE_THRESHOLD: u32 = 10;
/// Maximum number of peers we attempt to notify during graceful shutdown.
const GOODBYE_FANOUT: usize = 4;
const GOODBYE_TIMEOUT: Duration = Duration::from_millis(500);
/// Cap on simultaneous in-flight inbound connections. Excess connections
/// are dropped immediately rather than allowed to pile up handler tasks.
const MAX_CONCURRENT_CONNECTIONS: usize = 64;

/// Tracks the set of peers known to this engine.
///
/// `resolved` is keyed by node UUID: one definitive address per remote node.
/// `bootstraps` is a side-set of addresses we were told about (via
/// `config.peers` or `add_bootstrap`) but haven't yet exchanged messages
/// with — we don't know their UUIDs until they reply. Once a bootstrap
/// address gossips with us, it migrates into `resolved`.
///
/// `tombstones` records UUIDs that have departed (gracefully or by
/// repeated failure). 2P-Set semantics: once a UUID is tombstoned it can
/// never be re-added to `resolved`. Tombstones propagate to peers via the
/// `departed` field of every outgoing `Sync` and `Goodbye`.
///
/// `failure_counts` powers the K-consecutive-failure eviction heuristic.
/// Each successful send to an address resets it to zero; each failure
/// increments. When it hits `FAILURE_THRESHOLD` the corresponding UUID (if
/// any) is tombstoned; an unresolved bootstrap is just dropped.
///
/// # Lock discipline
///
/// All four mutexes are leaf locks. **No method holds more than one of
/// them simultaneously across an `await` point or a nested method call.**
/// Where two need to be touched in one operation (e.g. `tombstone` removes
/// from `resolved` and clears the matching `failure_counts` entry), each
/// critical section is short and the locks are released between them.
/// This avoids deadlock concerns regardless of which order future callers
/// touch them in.
pub(crate) struct PeerRegistry {
    self_id: Uuid,
    self_addr: SocketAddr,
    resolved: Mutex<HashMap<Uuid, SocketAddr>>,
    bootstraps: Mutex<HashSet<SocketAddr>>,
    tombstones: Mutex<HashSet<Uuid>>,
    failure_counts: Mutex<HashMap<SocketAddr, u32>>,
    /// JSON-encoded `T::Version` last successfully sent to each peer
    /// address. Drives the choice between `Sync` (full state) on the
    /// first contact and `SyncDelta` (incremental) on later ticks. Keyed
    /// by `SocketAddr` rather than `Uuid` so unresolved bootstraps and
    /// resolved peers share the same code path.
    last_sent: Mutex<HashMap<SocketAddr, serde_json::Value>>,
}

impl PeerRegistry {
    fn new(self_id: Uuid, self_addr: SocketAddr) -> Self {
        Self {
            self_id,
            self_addr,
            resolved: Mutex::new(HashMap::new()),
            bootstraps: Mutex::new(HashSet::new()),
            tombstones: Mutex::new(HashSet::new()),
            failure_counts: Mutex::new(HashMap::new()),
            last_sent: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the JSON-encoded `T::Version` we last successfully delivered
    /// to `addr`, or `None` if we have never gossiped to this address.
    pub(crate) fn last_sent_version(&self, addr: SocketAddr) -> Option<serde_json::Value> {
        self.last_sent.lock().unwrap().get(&addr).cloned()
    }

    /// Record the version that just left the wire for `addr`.
    pub(crate) fn record_sent_version(&self, addr: SocketAddr, version: serde_json::Value) {
        self.last_sent.lock().unwrap().insert(addr, version);
    }

    pub(crate) fn add_resolved(&self, id: Uuid, addr: SocketAddr) {
        if id == self.self_id || addr == self.self_addr {
            return;
        }
        // 2P-Set rule: once tombstoned, never re-added.
        if self.tombstones.lock().unwrap().contains(&id) {
            return;
        }
        self.bootstraps.lock().unwrap().remove(&addr);
        let is_new = self.resolved.lock().unwrap().insert(id, addr).is_none();
        if is_new {
            tracing::info!(%id, %addr, "peer joined");
        }
    }

    pub(crate) fn add_bootstrap(&self, addr: SocketAddr) {
        if addr == self.self_addr {
            return;
        }
        if self.resolved.lock().unwrap().values().any(|a| *a == addr) {
            return;
        }
        self.bootstraps.lock().unwrap().insert(addr);
    }

    /// Manually drop a peer by UUID. Does **not** add a tombstone — callers
    /// that want propagation should use `tombstone(id)`.
    pub(crate) fn remove(&self, id: Uuid) {
        self.resolved.lock().unwrap().remove(&id);
    }

    /// Tombstone a peer UUID: drop it from `resolved` and add to
    /// `tombstones` so it propagates and can't be re-added.
    pub(crate) fn tombstone(&self, id: Uuid) {
        if id == self.self_id {
            return;
        }
        let addr = {
            let mut resolved = self.resolved.lock().unwrap();
            resolved.remove(&id)
        };
        if let Some(addr) = addr {
            self.failure_counts.lock().unwrap().remove(&addr);
            self.last_sent.lock().unwrap().remove(&addr);
        }
        self.tombstones.lock().unwrap().insert(id);
        tracing::info!(%id, "peer departed");
    }

    /// Absorb a batch of tombstones learned via gossip.
    ///
    /// Follows the registry's "one lock at a time" discipline: insert into
    /// `tombstones` in one critical section, then update `resolved` and
    /// `failure_counts` in subsequent ones. Self-tombstones in `incoming`
    /// are ignored.
    pub(crate) fn absorb_tombstones(&self, incoming: &[Uuid]) {
        if incoming.is_empty() {
            return;
        }
        // 1. Record the tombstones we don't already have, filtering out self.
        let newly_added: Vec<Uuid> = {
            let mut ts = self.tombstones.lock().unwrap();
            incoming
                .iter()
                .filter(|id| **id != self.self_id && ts.insert(**id))
                .copied()
                .collect()
        };
        // We also need to drop any *previously* tombstoned id that somehow
        // got re-added; fold the full incoming list (minus self) into the
        // eviction sweep below so we're idempotent and self-healing.
        let to_evict: Vec<Uuid> = if newly_added.len() == incoming.len() {
            newly_added
        } else {
            incoming
                .iter()
                .filter(|id| **id != self.self_id)
                .copied()
                .collect()
        };
        // 2. Drop those UUIDs from `resolved`, collecting their addresses.
        let removed_addrs: Vec<SocketAddr> = {
            let mut resolved = self.resolved.lock().unwrap();
            to_evict
                .iter()
                .filter_map(|id| resolved.remove(id))
                .collect()
        };
        // 3. Clear failure counts and last-sent versions for removed
        //    addresses. Tombstoning drops the trust we had in the prior
        //    delta high-water mark — a future re-resolution must start
        //    over with a full `Sync`.
        if !removed_addrs.is_empty() {
            let mut failures = self.failure_counts.lock().unwrap();
            for addr in &removed_addrs {
                failures.remove(addr);
            }
            drop(failures);

            let mut sent = self.last_sent.lock().unwrap();
            for addr in &removed_addrs {
                sent.remove(addr);
            }
        }
    }

    /// Record a successful send. Resets the failure counter for this
    /// address.
    pub(crate) fn mark_success(&self, addr: SocketAddr) {
        self.failure_counts.lock().unwrap().remove(&addr);
    }

    /// Record a failed send. Returns `Some(id)` if the threshold was hit
    /// and a UUID was tombstoned, `None` otherwise (also `None` for a
    /// bootstrap which is silently dropped on threshold).
    pub(crate) fn mark_failure(&self, addr: SocketAddr) -> Option<Uuid> {
        let mut failures = self.failure_counts.lock().unwrap();
        let count = failures.entry(addr).or_insert(0);
        *count += 1;
        if *count < FAILURE_THRESHOLD {
            return None;
        }
        // Threshold hit: clear the counter for this addr and decide what to evict.
        failures.remove(&addr);
        drop(failures);

        // If the address corresponds to a resolved peer, tombstone its UUID.
        let resolved = self.resolved.lock().unwrap();
        let dead = resolved
            .iter()
            .find(|(_, a)| **a == addr)
            .map(|(id, _)| *id);
        drop(resolved);

        if let Some(id) = dead {
            self.tombstone(id);
            return Some(id);
        }

        // Otherwise it's an unresolved bootstrap — drop it silently and
        // forget any delta watermark we held for it.
        self.bootstraps.lock().unwrap().remove(&addr);
        self.last_sent.lock().unwrap().remove(&addr);
        None
    }

    /// Snapshot used by each gossip tick: returns the targets to attempt
    /// this tick (resolved peers + unresolved bootstraps), the `known_peers`
    /// to include in outgoing `Sync` messages, and the current tombstone
    /// set as the `departed` field.
    fn gossip_snapshot(&self) -> (Vec<SocketAddr>, Vec<PeerEntry>, Vec<Uuid>) {
        let resolved = self.resolved.lock().unwrap();
        let bootstraps = self.bootstraps.lock().unwrap();
        let tombstones = self.tombstones.lock().unwrap();

        let mut targets: Vec<SocketAddr> = resolved.values().copied().collect();
        targets.extend(bootstraps.iter().copied());

        let known: Vec<PeerEntry> = resolved
            .iter()
            .take(KNOWN_PEERS_CAP)
            .map(|(id, addr)| PeerEntry {
                node_id: *id,
                addr: *addr,
            })
            .collect();

        let departed: Vec<Uuid> = tombstones.iter().copied().collect();

        (targets, known, departed)
    }

    pub(crate) fn known_peers(&self) -> Vec<PeerEntry> {
        self.resolved
            .lock()
            .unwrap()
            .iter()
            .map(|(id, addr)| PeerEntry {
                node_id: *id,
                addr: *addr,
            })
            .collect()
    }

    pub(crate) fn known_tombstones(&self) -> Vec<Uuid> {
        self.tombstones.lock().unwrap().iter().copied().collect()
    }
}

/// Handle to a running gossip engine.
///
/// The engine spawns its listener, tick loop, and (optionally) mDNS
/// announce and browse tasks on construction. The handle is used to mutate
/// the peer registry at runtime and to request shutdown.
///
/// # State contract
///
/// * `local` — the engine reads the current local state from this `watch`
///   each time it gossips.
/// * `merged` — every time the engine receives a remote `Sync` and merges it
///   into the local snapshot, the resulting value is published on this
///   broadcast.
///
/// Consumers of `merged` MUST install the value by **merging** it into their
/// own state (e.g. `watch_tx.send_modify(|s| s.merge(incoming))`), not
/// by replacing. See `docs/crdt-net.md` §7.
///
/// # Graceful shutdown
///
/// Callers who want to leave the mesh cleanly should `await
/// engine.graceful_shutdown()` before dropping. This sends a `Goodbye`
/// message to a few peers so the rest of the mesh learns immediately that
/// this node has departed. Plain `Drop` only stops the engine's tasks; it
/// does not send the farewell (no async in `Drop`).
pub struct GossipEngine {
    registry: Arc<PeerRegistry>,
    self_id: Uuid,
    local_addr: SocketAddr,
    advertise_addr: SocketAddr,
    shutdown: Arc<Notify>,
}

impl GossipEngine {
    pub async fn run<T>(
        config: GossipConfig,
        local: watch::Receiver<T>,
        merged: broadcast::Sender<T>,
    ) -> io::Result<Self>
    where
        T: DeltaCrdt + Serialize + DeserializeOwned + Send + Sync + 'static,
        T::Delta: Serialize + DeserializeOwned + Send + Sync + 'static,
        T::Version: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let listener = TcpListener::bind(config.gossip_addr).await?;
        let local_addr = listener.local_addr()?;
        let advertise_addr = resolve_advertise_addr(&config, local_addr);
        let self_id = config.node_id;

        let registry = Arc::new(PeerRegistry::new(self_id, advertise_addr));
        for boot in &config.peers {
            registry.add_bootstrap(*boot);
        }

        let shutdown = Arc::new(Notify::new());

        spawn_listener::<T>(
            listener,
            local.clone(),
            merged,
            registry.clone(),
            shutdown.clone(),
        );
        spawn_ticker::<T>(
            local,
            registry.clone(),
            self_id,
            advertise_addr,
            config.interval,
            shutdown.clone(),
        );

        if config.enable_mdns
            && let Err(e) =
                discovery::spawn_mdns(self_id, advertise_addr, registry.clone(), shutdown.clone())
        {
            warn!(error = %e, "mDNS init failed; continuing without auto-discovery");
        }

        Ok(Self {
            registry,
            self_id,
            local_addr,
            advertise_addr,
            shutdown,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn advertise_addr(&self) -> SocketAddr {
        self.advertise_addr
    }

    pub fn node_id(&self) -> Uuid {
        self.self_id
    }

    /// Add a peer we know the UUID of (e.g. from mDNS resolution).
    pub fn add_peer(&self, node_id: Uuid, addr: SocketAddr) {
        self.registry.add_resolved(node_id, addr);
    }

    /// Add a peer address whose UUID we don't yet know. The engine will
    /// attempt to gossip to it until it responds, at which point it migrates
    /// to the resolved peer map.
    pub fn add_bootstrap(&self, addr: SocketAddr) {
        self.registry.add_bootstrap(addr);
    }

    /// Remove a peer from the resolved map by UUID without tombstoning.
    /// The peer may come back via mDNS, bootstrap, or peer-list gossip.
    pub fn remove_peer(&self, node_id: Uuid) {
        self.registry.remove(node_id);
    }

    /// Tombstone a peer by UUID. The tombstone propagates to other peers
    /// via the next gossip tick, and the UUID can no longer be re-added.
    pub fn tombstone_peer(&self, node_id: Uuid) {
        self.registry.tombstone(node_id);
    }

    pub fn known_peers(&self) -> Vec<PeerEntry> {
        self.registry.known_peers()
    }

    pub fn known_tombstones(&self) -> Vec<Uuid> {
        self.registry.known_tombstones()
    }

    /// Notify peers that this node is leaving and stop the background tasks.
    ///
    /// Sends a `Goodbye` message (with this node's UUID in `departed`) to
    /// up to [`GOODBYE_FANOUT`] random peers in parallel, each with a
    /// [`GOODBYE_TIMEOUT`] connect/write deadline. Then triggers the
    /// engine's tasks to exit.
    pub async fn graceful_shutdown(&self) {
        let (targets, known_peers, mut departed) = self.registry.gossip_snapshot();
        // Include ourselves in the goodbye's departed set.
        if !departed.contains(&self.self_id) {
            departed.push(self.self_id);
        }
        let goodbye = GossipMessage::<()>::Goodbye {
            from: PeerEntry {
                node_id: self.self_id,
                addr: self.advertise_addr,
            },
            departed,
            known_peers,
        };

        let mut handles = Vec::new();
        for addr in targets.into_iter().take(GOODBYE_FANOUT) {
            let g = goodbye.clone();
            handles.push(tokio::spawn(async move {
                let _ = time::timeout(GOODBYE_TIMEOUT, send_goodbye(addr, &g)).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }

        self.shutdown.notify_waiters();
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

impl Drop for GossipEngine {
    fn drop(&mut self) {
        self.shutdown.notify_waiters();
    }
}

fn spawn_listener<T>(
    listener: TcpListener,
    local: watch::Receiver<T>,
    merged: broadcast::Sender<T>,
    registry: Arc<PeerRegistry>,
    shutdown: Arc<Notify>,
) where
    T: DeltaCrdt + Serialize + DeserializeOwned + Send + Sync + 'static,
    T::Delta: Serialize + DeserializeOwned + Send + Sync + 'static,
    T::Version: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    debug!("listener shutdown");
                    return;
                }
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, peer)) => {
                            // Bound the number of concurrently-handled
                            // connections. Dropping over-cap connections
                            // immediately is correct gossip behaviour —
                            // the sender will retry next tick.
                            let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
                                warn!(%peer, "dropping connection: at MAX_CONCURRENT_CONNECTIONS ({})", MAX_CONCURRENT_CONNECTIONS);
                                drop(stream);
                                continue;
                            };
                            debug!(%peer, "accepted gossip connection");
                            let local = local.clone();
                            let merged = merged.clone();
                            let registry = registry.clone();
                            tokio::spawn(async move {
                                let _permit = permit; // released on drop
                                handle_connection::<T>(stream, peer, local, merged, registry).await;
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "accept failed");
                        }
                    }
                }
            }
        }
    });
}

async fn handle_connection<T>(
    mut stream: TcpStream,
    peer: SocketAddr,
    local: watch::Receiver<T>,
    merged: broadcast::Sender<T>,
    registry: Arc<PeerRegistry>,
) where
    T: DeltaCrdt + Serialize + DeserializeOwned + Send + Sync + 'static,
    T::Delta: Serialize + DeserializeOwned + Send + Sync + 'static,
    T::Version: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    // Cap how long a single connection can keep this task alive. A peer
    // that opens TCP and never sends data (or only sends a partial header)
    // would otherwise hold a handler task indefinitely.
    let read = time::timeout(READ_TIMEOUT, read_frame::<_, T>(&mut stream)).await;
    match read {
        Err(_) => {
            trace!(%peer, "connection idle past READ_TIMEOUT, dropping");
        }
        Ok(Ok(GossipMessage::Sync {
            from,
            state,
            known_peers,
            departed,
        })) => {
            info!(%peer, sender = %from.node_id, "received Sync, merging");
            // Absorb tombstones FIRST so a freshly-tombstoned UUID in
            // `known_peers` can't be re-added by the same message.
            registry.absorb_tombstones(&departed);
            registry.add_resolved(from.node_id, from.addr);
            for entry in known_peers {
                registry.add_resolved(entry.node_id, entry.addr);
            }
            let mut merged_value = local.borrow().clone();
            merged_value.merge(state);
            let _ = merged.send(merged_value);
        }
        Ok(Ok(GossipMessage::SyncDelta {
            from,
            delta,
            since,
            known_peers,
            departed,
        })) => {
            // Decode the sender's baseline first. If our local state is
            // not at least as advanced as that baseline, the delta was
            // computed against state we never had — applying it would
            // silently miss intervening updates. Drop and wait for the
            // sender's next periodic full `Sync` to catch us up.
            let typed_since: T::Version = match serde_json::from_value(since) {
                Ok(v) => v,
                Err(e) => {
                    trace!(error = %e, %peer, "discarding SyncDelta with malformed `since`");
                    return;
                }
            };
            let local_value = local.borrow().clone();
            if !T::version_includes(&local_value.version(), &typed_since) {
                trace!(
                    %peer,
                    sender = %from.node_id,
                    "dropping SyncDelta — local state behind sender's baseline, waiting for full Sync"
                );
                return;
            }
            // Decode the typed delta. A type mismatch surfaces as a
            // decode error and we drop the frame — the sender's next
            // tick falls back to a full `Sync` automatically once the
            // peer is re-resolved.
            let typed_delta: T::Delta = match serde_json::from_value(delta) {
                Ok(d) => d,
                Err(e) => {
                    trace!(error = %e, %peer, "discarding malformed SyncDelta payload");
                    return;
                }
            };
            debug!(%peer, sender = %from.node_id, "received SyncDelta, merging");
            registry.absorb_tombstones(&departed);
            registry.add_resolved(from.node_id, from.addr);
            for entry in known_peers {
                registry.add_resolved(entry.node_id, entry.addr);
            }
            let mut merged_value = local_value;
            merged_value.merge_delta(typed_delta);
            let _ = merged.send(merged_value);
        }
        Ok(Ok(GossipMessage::Goodbye {
            from,
            departed,
            known_peers,
        })) => {
            debug!(%peer, sender = %from.node_id, "received Goodbye");
            registry.absorb_tombstones(&departed);
            for entry in known_peers {
                registry.add_resolved(entry.node_id, entry.addr);
            }
        }
        Ok(Err(e)) => {
            trace!(error = %e, %peer, "discarding malformed frame");
        }
    }
}

fn spawn_ticker<T>(
    local: watch::Receiver<T>,
    registry: Arc<PeerRegistry>,
    self_id: Uuid,
    advertise_addr: SocketAddr,
    interval: Duration,
    shutdown: Arc<Notify>,
) where
    T: DeltaCrdt + Serialize + Send + Sync + 'static,
    T::Delta: Serialize + Send + Sync + 'static,
    T::Version: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        // Skip the immediate first tick so callers can set up subscribers.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    debug!("ticker shutdown");
                    return;
                }
                _ = ticker.tick() => {
                    let snapshot = local.borrow().clone();
                    let current_version = snapshot.version();
                    let (all_targets, known_peers, departed) = registry.gossip_snapshot();
                    let chosen: Vec<SocketAddr> = {
                        let mut rng = rand::thread_rng();
                        all_targets.into_iter().choose_multiple(&mut rng, FANOUT)
                    };
                    let from = PeerEntry { node_id: self_id, addr: advertise_addr };
                    for addr in chosen {
                        let from = from.clone();
                        let known = known_peers.clone();
                        let dep = departed.clone();
                        let registry = registry.clone();

                        // Decide per-peer: full `Sync` on first contact,
                        // `SyncDelta` thereafter. We do NOT short-circuit
                        // on `is_empty_delta` — even when the CRDT state
                        // hasn't moved, each tick still carries the
                        // current `known_peers` and `departed` lists,
                        // which is how tombstones propagate through the
                        // mesh.
                        let prev_version: Option<T::Version> = registry
                            .last_sent_version(addr)
                            .and_then(|v| serde_json::from_value(v).ok());
                        let mode = match prev_version {
                            None => SendMode::Full(snapshot.clone()),
                            // Ship the *receiver's prior baseline* (`prev`)
                            // as `since`. The receiver uses this to verify
                            // its local state already includes the
                            // baseline before applying the delta — if not,
                            // it drops the frame and waits for a full
                            // `Sync`. Shipping `current_version` here
                            // (sender's new state) would always fail that
                            // check whenever the sender pulled ahead,
                            // which is precisely when deltas matter.
                            Some(prev) => SendMode::Delta(
                                snapshot.delta_since(&prev),
                                prev.clone(),
                            ),
                        };

                        // `next_version` reflects the snapshot at tick
                        // time, not the state at send-completion time.
                        // In the gap between tick and ack the local
                        // state may have advanced further, but the
                        // watermark only needs to mark "what the peer
                        // is known to have absorbed." Recording too low
                        // is corrected on the next tick (the next
                        // delta covers the gap); recording too high
                        // would be the bug — the receiver's
                        // `version_includes` check drops frames whose
                        // `since` baseline we never had.
                        let next_version = current_version.clone();
                        tokio::spawn(async move {
                            let send_result = match mode {
                                SendMode::Full(state) => {
                                    send_sync::<T>(addr, from, state, known, dep).await
                                }
                                SendMode::Delta(delta, since) => {
                                    send_sync_delta::<T>(addr, from, delta, since, known, dep)
                                        .await
                                }
                            };
                            match send_result {
                                Ok(()) => {
                                    registry.mark_success(addr);
                                    // Record what the peer now knows so
                                    // the next tick can ship a fresh
                                    // delta on top of it.
                                    if let Ok(v) = serde_json::to_value(&next_version) {
                                        registry.record_sent_version(addr, v);
                                    }
                                    debug!(%addr, "gossip send ok");
                                }
                                Err(e) => {
                                    let evicted = registry.mark_failure(addr);
                                    if let Some(id) = evicted {
                                        info!(%addr, %id, "peer evicted after repeated failures");
                                    }
                                    warn!(%addr, error = %e, "gossip send failed");
                                }
                            }
                        });
                    }
                }
            }
        }
    });
}

/// Per-tick decision: ship the full state to a new peer, or just the
/// delta against what they last acknowledged.
enum SendMode<T: DeltaCrdt> {
    Full(T),
    Delta(T::Delta, T::Version),
}

fn resolve_advertise_addr(config: &GossipConfig, bound: SocketAddr) -> SocketAddr {
    if let Some(a) = config.advertise_addr {
        return a;
    }
    let port = bound.port();
    let ip = config.gossip_addr.ip();
    if !ip.is_unspecified() {
        return SocketAddr::new(ip, port);
    }
    let ip = match local_ip_address::local_ip() {
        Ok(IpAddr::V4(v4)) if !v4.is_loopback() => IpAddr::V4(v4),
        Ok(other) => other,
        Err(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
    };
    SocketAddr::new(ip, port)
}

async fn send_sync<T>(
    addr: SocketAddr,
    from: PeerEntry,
    state: T,
    known_peers: Vec<PeerEntry>,
    departed: Vec<Uuid>,
) -> io::Result<()>
where
    T: Serialize + Send + Sync,
{
    let mut stream = time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;
    let msg = GossipMessage::Sync {
        from,
        state,
        known_peers,
        departed,
    };
    write_frame(&mut stream, &msg).await
}

async fn send_sync_delta<T>(
    addr: SocketAddr,
    from: PeerEntry,
    delta: T::Delta,
    since: T::Version,
    known_peers: Vec<PeerEntry>,
    departed: Vec<Uuid>,
) -> io::Result<()>
where
    T: DeltaCrdt + Serialize + Send + Sync,
    T::Delta: Serialize,
    T::Version: Serialize,
{
    let delta_value = serde_json::to_value(&delta).map_err(io::Error::other)?;
    let since_value = serde_json::to_value(&since).map_err(io::Error::other)?;
    let mut stream = time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;
    let msg: GossipMessage<T> = GossipMessage::SyncDelta {
        from,
        delta: delta_value,
        since: since_value,
        known_peers,
        departed,
    };
    write_frame(&mut stream, &msg).await
}

async fn send_goodbye(addr: SocketAddr, msg: &GossipMessage<()>) -> io::Result<()> {
    let mut stream = time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;
    write_frame::<_, ()>(&mut stream, msg).await
}
