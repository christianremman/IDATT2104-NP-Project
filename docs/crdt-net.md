# `crdt-net` , Implementation Walkthrough

This document explains how the `crdt-net` crate works end-to-end: what each
file contains, what each method does, how the pieces fit together, and why
certain design decisions look the way they do. It assumes you have read
[specs/2026-05-12-canvas-design.md](specs/2026-05-12-canvas-design.md).

> If the spec tells you **what** to build, this doc tells you **how the code
> in this repo actually does it**.

---

## 1. What `crdt-net` is, in one paragraph

`crdt-net` is a small peer-to-peer gossip layer that synchronises state-based
CRDTs across a dynamically-discovered set of TCP peers. Each node periodically
picks up to two random peers, dumps its current CRDT state to them as a
length-prefixed JSON frame (along with the list of *other* peers it knows
about), and merges any incoming state into its own. New peers join via two
complementary mechanisms: **mDNS** for zero-config auto-discovery on the local
subnet, and **peer-list gossip** for transitive membership propagation across
networks where mDNS doesn't traverse. The crate is **generic over the CRDT
type** , it knows nothing about `CanvasDocument`. It only needs
`T: Crdt + Serialize + DeserializeOwned + Send + Sync + 'static`.

It is **not** an application. It does not own application state, does not
expose HTTP/WebSocket endpoints, and does not parse CLI args. That all lives
in `crdt-app`. `crdt-net`'s only job is: discover peers, gossip CRDT-shaped
values, merge what comes back.

---

## 2. The mental model

Think of three things flowing through the system:

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   App code                                                      │
│   ────────                                                      │
│                                                                 │
│   watch::Sender<T>  ─────────►   watch::Receiver<T>  ─────┐     │
│        ▲                                                   │     │
│        │ "I edited locally,                                │     │
│        │  here's my new state"                             │     │
│        │                                                   ▼     │
│        │                                          ┌──────────────┐│
│   forwarder                                       │              ││
│   (merge-not-replace)                             │ GossipEngine ││
│        ▲                                          │              ││
│        │                                          │  - listener  ││
│        │                                          │  - ticker    ││
│        │                                          │              ││
│   broadcast::Receiver<T>  ◄───  broadcast::Sender<T>  ◄───┘     │
│                                                                 │
│                            └────────── TCP listener ─── peers   │
│                            └────────── TCP dialer   ─── peers   │
└─────────────────────────────────────────────────────────────────┘
```

There are two **channels** between the app and the engine:

| Channel | Direction | Carries |
|---|---|---|
| `watch::Receiver<T>` | app → engine | the latest local snapshot the engine should gossip |
| `broadcast::Sender<T>` | engine → app | every state the engine produced by merging a remote message |

The engine **owns neither side's `Sender`** , the app owns both. That is why
the engine has no notion of "the canvas". It just reads-and-sends, then
receives-and-merges-and-publishes.

The **forwarder** in the diagram is a small piece of glue the app must
provide: it subscribes to the broadcast and pushes every value back into the
watch (merging, not overwriting , section 7 explains why). Without the
forwarder, the engine's outgoing gossip would never reflect incoming merges.

---

## 3. File-by-file tour

```
crdt-net/
├── Cargo.toml          # dependencies
└── src/
    ├── lib.rs          # module declarations + public re-exports
    ├── config.rs       # GossipConfig struct
    ├── message.rs      # wire format: PeerEntry, GossipMessage<T>, write/read_frame
    ├── engine.rs       # GossipEngine, PeerRegistry, listener + ticker tasks
    └── discovery.rs    # mDNS announce + browse (auto-fills the peer registry)

crdt-net/tests/
├── gossip.rs           # 3 integration tests: convergence / partition / malformed
└── discovery.rs        # 2 integration tests: peer-list propagation + bootstrap resolution
```

Five source files, five integration tests. Everything below explains those
files in detail.

### 3.1 `lib.rs`

Just module declarations and a small re-export surface so consumers can
`use crdt_net::{GossipEngine, GossipConfig, GossipMessage, MAX_FRAME}`
without reaching into modules.

### 3.2 `config.rs` , `GossipConfig`

A plain data struct with a small builder, mirroring the spec plus two extras
for discovery.

```rust
pub struct GossipConfig {
    pub node_id: Uuid,                       // who am I
    pub gossip_addr: SocketAddr,             // where I listen
    pub advertise_addr: Option<SocketAddr>,  // address others should reach me at
    pub peers: Vec<SocketAddr>,              // bootstrap peers (UUIDs unknown until first contact)
    pub interval: Duration,                  // gossip tick period
    pub enable_mdns: bool,                   // toggle mDNS announce+browse
}
```

Builder methods:

| Method | Purpose |
|---|---|
| `new(node_id, gossip_addr)` | Defaults: `peers = []`, `interval = 5s`, `enable_mdns = true` |
| `with_peers(peers)` | Replace the initial bootstrap list |
| `with_interval(duration)` | Set the gossip tick period |
| `with_advertise_addr(addr)` | Override the address put into outgoing `from`/mDNS records |
| `with_mdns(bool)` | Disable mDNS (tests, server-only deployments) |

**`advertise_addr` resolution** (done at engine startup, not in the config):
if explicitly set, used as-is. Otherwise, derived from `gossip_addr` ,
non-wildcard IPs are used directly, and wildcard binds (`0.0.0.0` / `::`)
resolve to a non-loopback local IPv4 via the `local_ip_address` crate. The
port always comes from the actually-bound socket so OS-assigned ports
(useful in tests with `127.0.0.1:0`) propagate correctly.

**`interval`** is stored as `Duration` (not `u64 secs` like the spec) so
tests can use millisecond-scale intervals without surprises.

### 3.3 `message.rs` , wire format

Three types and two functions. The on-the-wire protocol carries both CRDT
state and peer membership in one envelope.

```rust
pub const MAX_FRAME: usize = 16 * 1024 * 1024;   // 16 MiB

pub struct PeerEntry {
    pub node_id: Uuid,
    pub addr: SocketAddr,
}

pub enum GossipMessage<T> {
    Sync {
        from: PeerEntry,             // who I am + how to reach me
        state: T,                    // my current CRDT snapshot
        known_peers: Vec<PeerEntry>, // peers I'm aware of (capped at 64)
        departed: Vec<Uuid>,         // tombstones I've absorbed
    },
    Goodbye {
        from: PeerEntry,             // who is leaving
        departed: Vec<Uuid>,         // tombstones (includes self.uuid)
        known_peers: Vec<PeerEntry>, // who survives
    },
}

pub async fn write_frame<W, T>(w: &mut W, msg: &GossipMessage<T>) -> io::Result<()>
where W: AsyncWriteExt + Unpin, T: Serialize;

pub async fn read_frame<R, T>(r: &mut R) -> io::Result<GossipMessage<T>>
where R: AsyncReadExt + Unpin, T: DeserializeOwned;
```

`known_peers` is the peer-list-gossip primitive: each `Sync` carries a
snapshot of the sender's resolved peer map. Recipients merge new entries
into their own registry. After one or two gossip rounds, a node that
started knowing only one bootstrap peer ends up knowing the whole mesh.

`departed` is the tombstone primitive (2P-Set semantics). Any UUID in
`departed` is permanently dead: once a peer learns of it, it removes the
matching entry from its resolved map and refuses to re-add it via
peer-list gossip. `departed` rides on every outgoing `Sync` and `Goodbye`,
so tombstones spread across the mesh exactly like state.

`Goodbye` is structurally `Sync` minus the `state` field. A leaving peer
emits one to a few survivors so the mesh learns of the departure
immediately instead of waiting for the K-consecutive-failure heuristic to
fire. The `T` parameter on `Goodbye` is phantom , the variant uses no
field of type `T`, so a sender can construct `GossipMessage::<()>::Goodbye
{...}` without parameterizing the engine over `T`.

**Wire layout of one frame:**

```
┌────────────┬──────────────────────────┐
│  u32 BE    │   JSON bytes (UTF-8)     │
│  length    │   serde_json::to_vec     │
│  (4 bytes) │   of GossipMessage<T>    │
└────────────┴──────────────────────────┘
```

- **`write_frame`** , serialise the message to JSON via `serde_json::to_vec`,
  refuse if larger than `MAX_FRAME`, write the 4-byte length, write the
  body, flush. Returns `io::Error::other` on serialisation failure (which
  shouldn't happen for any valid `T: Serialize`).
- **`read_frame`** , read 4 bytes, decode the length, refuse if larger than
  `MAX_FRAME` (which protects the listener from an attacker claiming a 4 GiB
  message and OOM-ing us), allocate exactly that many bytes, read them,
  deserialize.

The `MAX_FRAME` cap is deliberately generous , the full `CanvasDocument`
will be well under a megabyte even at full saturation , but small enough
that a single garbage peer can't allocate gigabytes on our heap.

### 3.4 `engine.rs` , the engine

The biggest source file. It is structured as:

- The internal **`PeerRegistry`** , peer state, keyed by UUID.
- The public **`GossipEngine`** struct + its methods.
- A `Drop` impl that signals shutdown.
- Two private free functions: **`spawn_listener`** and **`spawn_ticker`**,
  each of which `tokio::spawn`s exactly one task (plus mDNS tasks spawned
  by `discovery::spawn_mdns`).
- A private helper **`handle_connection`** that processes one inbound TCP
  connection.
- A private helper **`send_sync`** that opens one outbound TCP connection
  and writes one `Sync` frame.
- A private helper **`resolve_advertise_addr`** that turns the
  potentially-wildcard `gossip_addr` into a concrete address peers can dial.

**The peer registry** keeps four collections:

```rust
struct PeerRegistry {
    self_id: Uuid,
    self_addr: SocketAddr,
    resolved: Mutex<HashMap<Uuid, SocketAddr>>,  // peers whose UUID we know
    bootstraps: Mutex<HashSet<SocketAddr>>,      // addresses we've been told to try
    tombstones: Mutex<HashSet<Uuid>>,            // 2P-Set: UUIDs that have departed
    failure_counts: Mutex<HashMap<SocketAddr, u32>>, // per-address send-failure counter
}
```

The split between `resolved` and `bootstraps` is the key data-structural
choice. The `--bootstrap <addr>` CLI flag and the `add_bootstrap(addr)` API
both add to `bootstraps`. The ticker tries these along with `resolved`
addresses every tick. The first time one of them responds (via incoming
`Sync` with a `from` field), we learn its UUID and migrate it into
`resolved`. From then on it's a normal peer.

`tombstones` enforces 2P-Set semantics: once a UUID is tombstoned, it can
never be re-added to `resolved`. This blocks peer-list gossip from
reviving a peer that another node has already marked as departed. See §7
for the full lifecycle.

`failure_counts` powers the K-consecutive-failure eviction heuristic. Each
successful send to an address resets it to zero; each failure increments.
When it hits `FAILURE_THRESHOLD` (10), the corresponding UUID is
tombstoned (or, if the address is still an unresolved bootstrap, dropped
silently).

**Lock discipline.** All four mutexes are leaf locks. No method holds
more than one of them across an `await` or a nested method call. Critical
sections are short and the locks are released between them. This avoids
the kind of lock-ordering deadlock you can get when one method takes
`A → B` and another takes `B → A`.

**`GossipEngine` fields**:

```rust
pub struct GossipEngine {
    registry: Arc<PeerRegistry>,
    self_id: Uuid,
    local_addr: SocketAddr,       // what the OS actually bound
    advertise_addr: SocketAddr,   // what we tell others to reach us at
    shutdown: Arc<Notify>,
}
```

### 3.5 `discovery.rs` , mDNS announce + browse

Wraps the `mdns-sd` crate so the engine can publish itself on the local
subnet and learn about other nodes that did the same. Two responsibilities,
both inside one spawned task:

- **Announce**: register a `ServiceInfo` with service type
  `_crdt-net._tcp.local.`, instance name = our UUID, and TXT records
  carrying `node_id` and a protocol `version`. Other nodes resolve this and
  see exactly how to reach us.
- **Browse**: subscribe to mDNS events for the same service type. On
  `ServiceResolved`, we pull `node_id` from the TXT record (cross-check
  it's not ourselves), grab the IP/port, and call
  `registry.add_resolved(id, addr)`. On `ServiceRemoved`, we
  `registry.remove(id)`.

mDNS uses link-local IPv4 multicast (`224.0.0.251:5353`). Routers don't
forward it by default, so this only finds peers on the same broadcast
domain. For cross-subnet (NTNU VLANs, Tailscale, the internet) you fall
back to a manual `--bootstrap` peer and the rest comes from peer-list
gossip.

mDNS can be disabled via `config.enable_mdns = false`. Tests do this to
avoid cross-process pollution between parallel test runs on the same
machine.

---

## 4. The public API, method by method

### `GossipEngine::run`

```rust
pub async fn run<T>(
    config: GossipConfig,
    local: watch::Receiver<T>,
    merged: broadcast::Sender<T>,
) -> io::Result<Self>
where T: Crdt + Serialize + DeserializeOwned + Send + Sync + 'static
```

**What it does**, line by line:

1. **`TcpListener::bind(config.gossip_addr).await?`** , bind the TCP
   listener. If this fails (port in use, permission denied), `run` returns
   `Err` without spawning anything. This is the *only* failure path of
   `run`; once it returns `Ok`, nothing the engine does can fail to the
   caller , runtime errors are logged via `tracing` and swallowed.
2. **`listener.local_addr()?`** , capture the actual address.
3. **`resolve_advertise_addr(...)`** , compute the address peers should
   dial, given config + the real bound port.
4. Build the `PeerRegistry`, seed it with `config.peers` as bootstraps.
5. Build the shutdown `Notify`.
6. **`spawn_listener::<T>(...)`** , spawn the accept loop.
7. **`spawn_ticker::<T>(...)`** , spawn the periodic gossip loop.
8. **`discovery::spawn_mdns(...)`** if `enable_mdns` , announce + browse.
   mDNS init failure is non-fatal: it logs a warning and continues without
   auto-discovery.
9. Return the handle.

After `run` returns, two or three tokio tasks are alive and running
(listener, ticker, and mDNS browse if enabled).

### Accessors

- **`local_addr() -> SocketAddr`** , the actual bound socket address.
- **`advertise_addr() -> SocketAddr`** , what we put in outgoing `from`
  fields and mDNS records.
- **`node_id() -> Uuid`** , our identity.

### `GossipEngine::add_peer(node_id, addr)`

Add a peer whose UUID we already know , e.g., from an mDNS resolution or
from an explicit `--peer uuid@addr` config (not currently in the demo
CLI, but available programmatically). Goes straight into the resolved
map.

### `GossipEngine::add_bootstrap(addr)`

Add an address whose UUID we don't yet know. Goes into the bootstrap
set; the ticker tries to gossip to it each tick. When it responds, the
engine learns its UUID from the `from` field of the reply and migrates
it into the resolved map.

This is the API the demo CLI uses for `--bootstrap` and the runtime
`add IP:PORT` command. It's also what backs the spec's `POST /api/peers`
endpoint in `crdt-app`.

### `GossipEngine::remove_peer(node_id)`

Removes a peer from the resolved map by UUID **without** tombstoning it.
The peer can come back via mDNS, bootstrap, or peer-list gossip. Use this
for "I don't want to keep retrying this address right now"; use
`tombstone_peer` for "this peer is gone for good."

### `GossipEngine::tombstone_peer(node_id)`

Tombstones a peer by UUID. The UUID moves from `resolved` to `tombstones`
and cannot be re-added by any means (mDNS, bootstrap, peer-list gossip).
The tombstone propagates to the rest of the mesh via the next gossip
tick's `departed` field. See §7 for when each peer eventually learns it.

### `GossipEngine::known_peers() -> Vec<PeerEntry>`

Snapshot of currently-resolved peers. Used by the demo's `peers` command
and (in the future) by `crdt-app`'s `GET /api/peers` HTTP endpoint.

### `GossipEngine::known_tombstones() -> Vec<Uuid>`

Snapshot of currently-tombstoned UUIDs. Useful for diagnostics (the
demo's `tombstones` command prints it) and for verifying mesh convergence
in tests.

### `GossipEngine::graceful_shutdown().await`

Sends a `Goodbye` message with this node's UUID in `departed` to up to
`GOODBYE_FANOUT` (4) random peers, each with a `GOODBYE_TIMEOUT` (500ms)
write deadline. Then triggers the engine's tasks to exit. Calling this
before dropping the engine lets the rest of the mesh learn of the
departure immediately, instead of having to wait for the K-failure
heuristic to fire.

`Drop` only calls `shutdown.notify_waiters()` , it does **not** try to
send a Goodbye, because doing async work in a sync `Drop` is fragile.
Callers who want a clean exit must `await graceful_shutdown()` before the
engine falls out of scope.

### `GossipEngine::shutdown`

Calls `Notify::notify_waiters()`. All spawned tasks check the shutdown
notify on every iteration via `tokio::select!`, so they exit at the next
loop turn. Dropping `GossipEngine` also calls `shutdown` (via the `Drop`
impl) so leaking the handle never leaks tasks indefinitely.

> **Caveat:** `Notify::notify_waiters` only wakes futures that are
> *already polling* `notified()`. In practice the spawned tasks enter their
> `select!` essentially immediately after `run` returns, so calling
> `shutdown` after at least one tick interval is always safe. The 100ms or
> so right after `run` returns is a theoretical race window we don't bother
> closing.

---

## 5. The async tasks

Two or three tokio tasks run inside the engine (depending on whether mDNS is
enabled). All are spawned from `run` and live until `shutdown` fires.

### 5.1 The listener task (`spawn_listener` → accept loop)

```
loop {
    select {
        shutdown.notified() => return
        listener.accept() => {
            let permit = semaphore.try_acquire_owned()?;  // drop if at cap
            spawn(handle_connection(stream, ..., permit))
        }
    }
}
```

Pure accept loop. Each accepted TCP connection is handed off to a
short-lived task running `handle_connection`. The accept loop never blocks
on a slow peer , the slow peer just keeps its own dedicated handler task
busy.

**Connection cap.** The listener holds a `tokio::sync::Semaphore` with
`MAX_CONCURRENT_CONNECTIONS = 64` permits. Each accepted connection
acquires one permit; the spawned handler task holds it until it exits
(success, timeout, or error). If the cap is hit, the new connection is
dropped immediately (the kernel sends RST/FIN). This bounds resource use
under a connection flood , an adversary can't pile up unbounded handler
tasks or exhaust the process's file descriptors.

If `accept()` itself errors (rare , usually a fd-table problem), it logs
a warning and continues. The listener never dies on its own.

### 5.2 `handle_connection`

Per-connection task. Reads one frame within a deadline, processes it,
exits.

```rust
let read = time::timeout(READ_TIMEOUT, read_frame::<_, T>(&mut stream)).await;
match read {
    Err(_) => trace!(%peer, "connection idle past READ_TIMEOUT"),
    Ok(Ok(GossipMessage::Sync { from, state, known_peers, departed })) => {
        registry.absorb_tombstones(&departed);
        registry.add_resolved(from.node_id, from.addr);
        for entry in known_peers {
            registry.add_resolved(entry.node_id, entry.addr);
        }
        let merged_value = local.borrow().merge(&state);
        let _ = merged.send(merged_value);
    }
    Ok(Ok(GossipMessage::Goodbye { from, departed, known_peers })) => {
        registry.absorb_tombstones(&departed);
        for entry in known_peers {
            registry.add_resolved(entry.node_id, entry.addr);
        }
    }
    Ok(Err(e)) => trace!(error = %e, "discarding malformed frame"),
}
```

The flow on a normal `Sync`:

1. `time::timeout(READ_TIMEOUT, ...)` caps the handler's lifetime at 5
   seconds. A peer that opens TCP and never sends data (or sends only a
   partial 4-byte length header) used to hold this task open
   indefinitely; with the timeout, the task always exits.
2. Read one length-prefixed JSON frame. If the prefix is bogus, the body
   is truncated, the JSON is malformed, or the frame exceeds `MAX_FRAME`
   (4 MiB), `read_frame` returns an `io::Error`. We drop the frame and
   let the connection close.
3. **Absorb tombstones first.** Any UUID in `departed` is added to our
   tombstone set, and any matching `resolved` entry is dropped. Doing
   this before processing `known_peers` ensures a freshly-tombstoned
   UUID in the same message can't be re-added on the next line.
4. Add the sender (`from`) to the resolved peer map. `add_resolved`
   silently no-ops if the entry is self or already tombstoned.
5. Add every entry from `known_peers` to the resolved map , peer-list
   gossip.
6. Take `local.borrow()` , a read-locked view of the latest watch
   value.
7. Call `merge(&state)`. Because `Crdt::merge` produces a new `T`,
   nothing is mutated in place , `local` still holds the pre-merge
   value.
8. `merged.send(merged_value)` publishes the result on the broadcast.
   If nobody is subscribed yet, `send` returns `Err` and we ignore it.

`Goodbye` follows the same flow but skips state merging (the variant has
no state). It still propagates `departed` and `known_peers`.

**Why does the engine not write the merged value back to the watch?**
Because the engine doesn't own the watch `Sender` , the app does. The app
must do that itself via the forwarder pattern. The reason for that split
is the merge-vs-replace problem, explained in section 7.

### 5.3 The ticker task (`spawn_ticker`)

```
let mut ticker = time::interval(interval);
ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
ticker.tick().await;  // throw away the immediate first tick

loop {
    select {
        shutdown.notified() => return
        ticker.tick() => {
            let snapshot = local.borrow().clone();
            let (targets, known_peers, departed) = registry.gossip_snapshot();
            let chosen = targets.choose_multiple(rng, FANOUT);
            let from = PeerEntry { node_id: self_id, addr: advertise_addr };
            for addr in chosen {
                spawn(async move {
                    match send_sync(addr, from, snapshot, known_peers, departed).await {
                        Ok(()) => registry.mark_success(addr),
                        Err(e) => {
                            if let Some(id) = registry.mark_failure(addr) {
                                debug!(%addr, %id, "evicted after repeated failures");
                            }
                            warn!(%addr, error = %e, "gossip send failed");
                        }
                    }
                });
            }
        }
    }
}
```

The first `tick().await` outside the loop discards tokio's immediate-fire
on tick interval creation. Without it the engine would gossip as soon as
`run` returned, before the caller had a chance to add peers, leading to
flaky tests.

**Each interval:**

1. Take a snapshot of the local state.
2. Take a peer snapshot: a flat list of `targets` (resolved peers'
   addresses + still-unresolved bootstrap addresses) and a `known_peers`
   payload (resolved peers as `PeerEntry`, capped at 64). Bootstraps are
   *targets we send to* but not *peers we advertise* , we can only put
   resolved peers (with UUIDs) into `known_peers`.
3. Choose up to 2 random targets via `IteratorRandom::choose_multiple`.
   `FANOUT = 2` matches the spec.
4. For each chosen target, spawn a fire-and-forget task that builds the
   `Sync` envelope and runs `send_sync`. Spawning per-peer keeps a slow
   peer from delaying the others in the same tick.

**`MissedTickBehavior::Delay`** means: if the runtime is so loaded that a
tick deadline passes before we wake up, *don't* immediately fire a
catch-up tick. Just wait the full interval from now.

### 5.4 `send_sync`

```rust
async fn send_sync<T>(
    addr: SocketAddr,
    from: PeerEntry,
    state: T,
    known_peers: Vec<PeerEntry>,
) -> io::Result<()>
where T: Serialize + Send + Sync,
{
    let mut stream = time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;
    let msg = GossipMessage::Sync { from, state, known_peers };
    write_frame(&mut stream, &msg).await
}
```

- 2-second connect timeout. A node that's down doesn't stall the ticker for
  more than 2 seconds, and we don't pile up connection attempts.
- One frame, then we drop the stream (which closes the TCP connection).
- The ticker logs `warn!` on any error and moves on. The peer is retried
  next tick.

### 5.5 The mDNS task (when `enable_mdns = true`)

A single task spawned by `discovery::spawn_mdns` owns the
`mdns_sd::ServiceDaemon`. The task `tokio::select!`s between `shutdown.notified()`
and `daemon.browse(...)`'s receiver. On every `ServiceResolved` event it
extracts the peer's `node_id` from the TXT record and calls
`registry.add_resolved(...)`. On `ServiceRemoved` it parses the UUID from
the fullname and calls `registry.remove(...)`. The daemon is shut down
when the task exits.

---

## 6. Concurrency layout

After `run()` returns, here is everything that exists in memory:

| Owner | Lives in |
|---|---|
| `Arc<Mutex<HashSet<SocketAddr>>>` peer set | shared between `GossipEngine` handle and the ticker task |
| `Arc<Notify>` shutdown | shared between handle, listener task, ticker task |
| `watch::Receiver<T>` | cloned into the listener task and the ticker task; each acceptor clones again per connection |
| `broadcast::Sender<T>` | cloned into the listener task; cloned again per connection |

**Tasks alive per engine:**

- 1 listener task (long-lived, until shutdown)
- 1 ticker task (long-lived, until shutdown)
- 0..N handler tasks (one per in-flight inbound connection, very short-lived)
- 0..2 sender tasks per tick (one per outbound peer this interval, ≤2s each)

No mutex is held across `await` points. The only mutex (`peers`) is locked
exclusively for the time it takes to copy 2 addresses out of a `HashSet`.

---

## 7. The state contract (the one tricky thing)

Spec literal: *"On receive: merges incoming `CanvasDocument`, notifies
subscribers via broadcast channel."*

That sentence is correct but underspecifies one crucial detail: **how do
subscribers reinstall the merged value into the watch source so the *next*
gossip tick reflects it?** Two answers, only one of which is safe:

1. **Replace the watch value** with each broadcast frame: ❌ broken.
2. **Merge** the broadcast frame into the watch value: ✅ correct.

### Why replacing is broken

Imagine node X has watch value `{X:1}` and two peers A and C both send at
the same time. Both inbound connections are handled in parallel:

```
listener for A: reads local {X:1}, computes {X:1, A:3}, broadcasts {X:1, A:3}
listener for C: reads local {X:1}, computes {X:1, C:2}, broadcasts {X:1, C:2}
```

The forwarder receives both broadcasts. If it does
`watch_tx.send(value)` (replace), then whichever broadcast it processes
second wins, and the loser's contribution is lost forever , until the
next gossip round, if it happens at all.

### Why merging works

The forwarder does:

```rust
watch_tx.send_modify(|s| *s = s.merge(&incoming));
```

Now:

```
incoming {X:1, A:3} arrives first  → watch becomes {X:1, A:3}
incoming {X:1, C:2} arrives second → watch becomes {X:1, A:3, C:2}
```

Both contributions land. This is correct because state-based CRDT merge is
commutative, associative, and idempotent , applying `merge` to an
already-merged value is exactly the right thing.

### So why doesn't the engine just merge into the watch directly?

It can't , it only has a `watch::Receiver`, not a `Sender`. That was a
deliberate split:

- Keeps the engine's API surface small (no `watch::Sender` parameter).
- Keeps the app fully in control of the watch source, which it also writes
  to on every local edit. Two writers to the same watch would have to
  coordinate anyway.

Cost: the app must implement the small "forwarder" task. Benefit: the
engine has zero opinions about how the app's state container is
structured. The tests show the forwarder pattern in
[crdt-net/tests/gossip.rs](../crdt-net/tests/gossip.rs#L65-L72).

---

## 8. Three end-to-end scenarios

### 8.1 Local edit

```
app code:                            engine:                          peers:
─────────                            ───────                          ─────
ctx.canvas.bump_pixel(x,y,color)
state_tx.send_modify(|s| ...)
                                     watch sees new state
                                     (ticker hasn't fired yet)

tick fires                           snapshot = local.borrow().clone()
                                     choose 2 peers
                                     spawn send_sync(p1, snapshot)  ────►  p1: read_frame, merge, broadcast
                                     spawn send_sync(p2, snapshot)  ────►  p2: read_frame, merge, broadcast
```

### 8.2 Inbound from a peer

```
peer Y:                              engine on node X:               forwarder on node X:
───────                              ─────────────────               ────────────────────
write_frame(Sync(Y_state)) ────►     accept(), spawn handler
                                     handler: read_frame
                                     local.borrow().merge(&Y_state)
                                     merged.send(new_state)  ─────►   forward_rx.recv()
                                                                      state_tx.send_modify(|s| *s = s.merge(...))
                                                                      ↑ now the watch reflects Y's contribution
```

### 8.3 Partition + heal

```
A and B are peered, both bump locally → converge.
A.remove_peer(B.uuid); B.remove_peer(A.uuid) → partition.
A bumps, B bumps independently → states diverge.
A.add_peer(B.uuid, B.addr); B.add_peer(A.uuid, A.addr) → next tick(s) gossip in
both directions, both sides merge, both reach the union of edits.
```

(This is one of the integration tests.)

### 8.4 Peer-list propagation

```
A only knows B (via --bootstrap B). B knows A and C. C only knows B.
A and C have never been told about each other.

tick on B:
  snapshot known_peers = [A, C]
  send Sync{from: B, state, known_peers: [A, C]} to one or both of them

A receives B's Sync:
  registry.add_resolved(C.uuid, C.addr)   ← now A knows C
  …merge state as usual

next tick on A:
  targets include C
  send Sync to C directly

C receives A's Sync:
  registry.add_resolved(A.uuid, A.addr)   ← now C knows A
```

Within ~2 gossip intervals, A and C are directly connected. This is the
basis of how one bootstrap peer is enough to discover the entire mesh.

### 8.5 Tombstone propagation (graceful exit + crash)

```
A, B, C all peered with each other.

Graceful path (A presses Ctrl-C):
  A: graceful_shutdown.await
     ├ Goodbye{from: A, departed: [A], known_peers: [B, C]} → B  (500ms timeout)
     └ Goodbye{from: A, departed: [A], known_peers: [B, C]} → C  (500ms timeout)
  B: absorb_tombstones([A]) → drop A from resolved
  C: absorb_tombstones([A]) → drop A from resolved
  (next tick) B's Sync to C carries departed: [A] , idempotent reinforcement.

Crash path (A loses power):
  B: tick → send_sync(A) fails. failure_counts[A_addr] = 1.
  …repeat for ~10 ticks (≈10s at 1s interval)…
  B: failure_counts[A_addr] = 10 → tombstone(A) → drop from resolved.
  (next tick) B's Sync to C carries departed: [A] , C now knows too.
```

Both paths converge to the same end state: every live peer has A in its
`tombstones` set, no live peer attempts to gossip to A's address. The
graceful path is fast (one round trip). The crash path takes the
`FAILURE_THRESHOLD * interval` window first.

---

## 9. Error handling philosophy

The crate has exactly one fallible function visible to callers: `run`. It
returns `Err` if and only if the TCP bind fails. Everything else is
log-and-continue:

| Situation | What happens |
|---|---|
| Peer is down / connect refused | `warn!`, skip, retry next tick; mark_failure() increments counter |
| Peer connect times out (>2s) | same as above |
| 10 consecutive failures to one address | tombstone the UUID (if resolved) or drop the bootstrap; `debug!` log |
| Inbound TCP frame is malformed | `trace!`, drop connection |
| Inbound frame claims length > 4 MiB | `trace!`, drop connection |
| Inbound JSON fails to deserialize | `trace!`, drop connection |
| Inbound TCP connection idle > READ_TIMEOUT (5s) | `trace!`, drop connection |
| Accept exceeds MAX_CONCURRENT_CONNECTIONS (64) | `warn!`, drop the new connection immediately |
| Broadcast send fails (no subscribers) | silently ignored |
| Accept error | `warn!`, continue accept loop |
| Goodbye send fails / times out | silently ignored (best-effort) |

The spec phrasing for the basic failure modes ("warn + skip" / "discard
silently") is implemented literally; the additional rows above are
defensive limits that bound resource use under adversarial or accidental
abuse.

---

## 10. Testing strategy

There are nine integration tests across three files, all built on a
shared **`MockCrdt`** ([tests/common/mod.rs](../crdt-net/tests/common/mod.rs))
, a `BTreeMap<Uuid, u64>` with element-wise-max merge that trivially
satisfies the CRDT laws. Each test file declares `mod common;` at the
top to pull it in; Cargo doesn't compile the `common` subdirectory as a
test binary.

[tests/gossip.rs](../crdt-net/tests/gossip.rs) , 3 tests for the basic
gossip path:

1. **`converges_across_three_nodes`** , full mesh, each bumps; assert all
   three converge to the union total within a few intervals.
2. **`partition_then_heal`** , peer, mutate, converge, un-peer, mutate
   independently, re-peer, assert convergence.
3. **`garbage_does_not_kill_listener`** , bogus length prefixes /
   truncated frames; verify the listener stays up and still processes a
   valid frame afterwards.

[tests/discovery.rs](../crdt-net/tests/discovery.rs) , 2 tests for
peer-list propagation (mDNS itself is skipped , too flaky in CI):

1. **`peer_list_propagates_transitively`** , A↔B↔C, A and C learn each
   other through B's `known_peers` field.
2. **`bootstrap_gets_uuid_after_first_contact`** , confirms the
   bootstrap → resolved migration on first successful gossip.

[tests/tombstones.rs](../crdt-net/tests/tombstones.rs) , 4 tests for the
2P-Set tombstoning path:

1. **`graceful_shutdown_propagates_tombstone`** , A calls
   `graceful_shutdown`; within a few ticks B and C have A's UUID in
   `known_tombstones()`.
2. **`tombstoned_peer_is_not_revived_by_peer_list_gossip`** , A
   tombstones B; even though C still gossips B's address to A, A's
   resolved map stays empty for B.
3. **`consecutive_failures_evict_unreachable_bootstrap`** , bootstrap
   pointing at a bind-then-released port; engine evicts it after the
   threshold and still works for real peers added afterwards.
4. **`tombstone_propagates_transitively_via_known_peers_in_sync`** ,
   A→B→C topology, A tombstones C, B learns via A's `departed`, C
   correctly refuses to tombstone its own UUID.

All nine pass with `cargo test -p crdt-net`. The convergence and
tombstone tests were each run 10× in a loop to verify they aren't flaky.

---

## 11. How `crdt-net` plugs into the rest of the project

- **`crdt-core`** , provides the `Crdt` trait that `crdt-net` is generic
  over. Right now `crdt-core` only contains the trait; once student 1
  fills in `VectorClock`, `LWWRegister`, `ORSet`, ..., and the composite
  `CanvasDocument`, the latter will become the `T` that `crdt-app`
  instantiates the engine with. `crdt-net` itself doesn't need a single
  line of change.
- **`crdt-app`** , will own `Arc<RwLock<AppState>>`, the
  `watch::Sender<CanvasDocument>`, the `broadcast::Receiver<CanvasDocument>`,
  the forwarder task, the Axum HTTP/WS server, and CLI parsing. It calls
  `GossipEngine::run` once at startup, keeps the handle, and uses
  `add_peer` / `remove_peer` to back the `/api/peers` endpoints.

Roughly:

```rust
// In crdt-app startup, after parsing CLI args:
let (state_tx, state_rx) = watch::channel(CanvasDocument::default());
let (merged_tx, _) = broadcast::channel(32);

let engine = GossipEngine::run(
    GossipConfig::new(node_id, gossip_addr).with_peers(initial_peers),
    state_rx.clone(),
    merged_tx.clone(),
).await?;

// Forwarder
{
    let mut rx = merged_tx.subscribe();
    let tx = state_tx.clone();
    tokio::spawn(async move {
        while let Ok(incoming) = rx.recv().await {
            tx.send_modify(|s| *s = s.merge(&incoming));
        }
    });
}

// Hand `state_tx`, `merged_tx`, `engine` to the HTTP/WS layer.
```

That's the entire integration surface.

---

## 12. Quick reference , file → purpose

| File | Purpose |
|---|---|
| [Cargo.toml](../crdt-net/Cargo.toml) | Dependencies (tokio, serde, serde_json, uuid, tracing, rand, mdns-sd, local-ip-address, crdt-core) |
| [src/lib.rs](../crdt-net/src/lib.rs) | Module declarations + public re-exports |
| [src/config.rs](../crdt-net/src/config.rs) | `GossipConfig` struct + builders |
| [src/message.rs](../crdt-net/src/message.rs) | `PeerEntry`, `GossipMessage<T>` + length-prefixed JSON frame codec |
| [src/engine.rs](../crdt-net/src/engine.rs) | `GossipEngine`, `PeerRegistry`, listener + ticker tasks |
| [src/discovery.rs](../crdt-net/src/discovery.rs) | mDNS announce + browse |
| [tests/gossip.rs](../crdt-net/tests/gossip.rs) | Convergence / partition / garbage tests with `MockCrdt` |
| [tests/discovery.rs](../crdt-net/tests/discovery.rs) | Peer-list propagation + bootstrap resolution tests |
| [examples/two_node_demo.rs](../crdt-net/examples/two_node_demo.rs) | Hand-runnable demo with `--bootstrap` and `--mdns/--no-mdns` flags |

---

## 13. Glossary

- **State-based CRDT (CvRDT)** , a data type whose merge operation is
  commutative, associative, and idempotent. Two replicas exchanging full
  states and merging always converge, regardless of message order or
  duplication.
- **Gossip** , periodic, push-based, randomised peer-to-peer
  dissemination. Each tick a node sends its state to a small random subset
  of peers (fanout = 2 here). Anti-entropy: every node eventually sees
  every update.
- **`watch` channel** , single-producer, multi-consumer; consumers see
  only the latest value, not the history. Used here for "what is my
  current state".
- **`broadcast` channel** , multi-producer, multi-consumer; every
  subscriber receives every message (bounded buffer; if a subscriber
  lags, they get a `Lagged` error). Used here for "merged state event".
- **Forwarder** , small async task in `crdt-app` (and in the integration
  tests) that subscribes to the broadcast and merges values back into the
  watch source. Required for correctness; see section 7.
- **Fanout** , number of peers a node gossips to per tick. `FANOUT = 2`.
- **Anti-entropy interval** , `GossipConfig::interval`, default 5s.
- **mDNS (Multicast DNS)** , a zero-config service discovery protocol that
  uses link-local IPv4 multicast (`224.0.0.251:5353`). Nodes announce
  themselves and browse for others without any central registry. Doesn't
  cross broadcast domains, so it doesn't traverse NAT, VPNs, or routed
  subnets.
- **Bootstrap peer** , a peer address you supply explicitly because mDNS
  can't reach it (different subnet, Tailscale, internet). One bootstrap is
  enough; peer-list gossip propagates the rest of the mesh.
- **Peer-list gossip** , each `Sync` message carries the sender's resolved
  peer set, so a node connected to *any* peer transitively learns about
  the whole mesh within a few gossip intervals.
