# IDATT2104 Network Programming Project

A distributed collaborative pixel canvas using delta state-based conflict-free replicated data types and peer-to-peer gossip.

## Introduction

This project implements a distributed drawing application using CRDTs for conflict-free synchronization between peers. Each peer runs a standalone binary that includes the full application:
an HTTP server, a WebSocket endpoint for live browser updates, and a
TCP gossip engine for peer-to-peer state synchronization.

Peers discover each other automatically on the local network via mDNS,
or can be connected manually across subnets using bootstrap addresses.
The canvas, palette, user presence, and cursor positions all replicate
across the mesh without any central server or coordination.

The project includes a general-purpose CRDTs library, which has implementation of multiple state-based crdts. These have support for using time deltas to improve performance, where only the recently applied changes to the state are used. 
It also includes a networking library with a generic gossip engine that
works with any type implementing the DeltaCrdt trait.

The application ties these more general purpose libraries together to show off their functionality. It is a proof-of-concept application, to show off the use of the crdts. However, it does not use all of them. It is important to note we have focused on not tieng the libraries directly to the application, so they can easily be used for other types of applications


## Project structure
 
The project is a Cargo workspace with three Rust crates and a Vue
frontend:
 
```
crdt-core/     Pure CRDT library
crdt-net/      Generic gossip transport for delta Crdt's
crdt-app/      Canvas application. HTTP/WS server + embedded frontend
frontend/      Vue 3 single-page app
```
 
**`crdt-core`** is a general-purpose CRDT library. It knows nothing
about networking, canvases, or pixels. Any Rust project could use it.
All CRDTs implement a shared `Crdt` trait (`value`, `merge`, `compare`)
with the same contract: merge is commutative, associative, and
idempotent. CRDTs used in the canvas also implement a `DeltaCrdt` trait that 
extends `Crdt` with delta-state support (`delta_since`, `merge_delta`, `version`).
 
**`crdt-net`** is a generic gossip engine over TCP. It works with any type implementing
the `T: DeltaCrdt + Serialize`. It knows nothing about canvases,
it just discovers peers, sends state, and merges what comes back. It
supports full-state `Sync` for first contact and incremental `SyncDelta`
for established peers.
 
**`crdt-app`** is the application-specific crate. It composes CRDTs from
`crdt-core` into a `CanvasDocument`, wires it into `crdt-net` for
peer-to-peer sync, and serves the Vue frontend over HTTP/WebSocket.
 
**`frontend`** is a Vue 3 single-page app with a pixel canvas, color
picker, peer list, cursor rendering, and leaderboard. Built with Vite
and embedded into the Rust binary via `rust-embed`, so a single
executable ships the entire application.
 
Dependencies flow inward: `crdt-app` depends on both libraries, but the
libraries know nothing about canvases or pixels.They are separate layers,
that has its own responsibility and communicates through defined interfaces.
 
## Implemented functionality
 
| Feature | CRDT / mechanism |
|---|---|
| 64×64 shared pixel canvas | Per-pixel `LWWRegister<Rgba>` |
| Simultaneous multi-user editing | `VectorClock` -> LWW |
| Shared color palette (add/remove) | `ORSet<Rgba>`, add-wins semantics |
| Live peer presence | `ORSet<Uuid>`, add-wins |
| Pixel ownership leaderboard | Derived from LWW register winners |
| Total paint counter | `GCounter` (monotonic, per-node) |
| Live cursor positions | Per-user `LWWRegister<PixelCoord>` |
| Automatic LAN peer discovery | mDNS (`_crdt-net._tcp.local.`) |
| Cross-subnet peering | Bootstrap addresses + peer-list gossip |
| Graceful departure | `Goodbye` message + 2P-Set tombstones |
| Crash detection | K-consecutive-failure threshold → tombstone |
| Delta WebSocket push | `DeltaCrdt` → sparse updates per client |
| Delta peer gossip | Full `Sync` on first contact, `SyncDelta` after |
| Embedded frontend | `rust-embed` bundles Vue dist/ into binary |

### CRDTs implemented in `crdt-core`
 
Sstate-based CRDTs, all with tests ensuring
commutativity, associativity, and idempotency:
 
VectorClock, GCounter, PNCounter, GSet, TwoPSet, ORSet, LWWRegister,
MVRegister, LWWMap.
 
Five of these are used in the canvas document (VectorClock, GCounter,
LWWRegister, ORSet). The remaining are implemented and tested in
`crdt-core` as part of the library, available for other applications.
 

## Key design decisions

### The VectorClock
A important design desition is that our app uses one `VectorClock` on
`CanvasDocument`. This is the timestamp source for everything: LWW pixel
registers, ORSet tags, and delta computation.

An earlier design kept an `AtomicU64` clock counter on `AppState`
and passed timestamps into `CanvasDocument` from outside. This had two
problems:
 
1. **Manual syncing.** After every gossip merge, `AppState` had to call
   `advance_ts(max_seen)` to bump its counter past remote timestamps.
   This was a hand-rolled reimplementation of what `VectorClock::merge`
   already does.
2. **Race window.** Between `send_modify` (which merges the document)
   and `advance_ts` (which bumps the counter), another task could call
   `mutate` and get a timestamp lower than what was just merged.

Moving the clock into the document eliminated both. The clock merges
automatically with the rest of the state, and `increment` is called
inside the same `send_modify` closure as the mutation.

The clock uses  the Lamport rule. `VectorClock::increment` does not add 1 to the node's own
counter. It sets it to `max(own_counter, max_across_all_nodes) + 1`. 
Without this, a node that merges a peer's state and then paints can
generate a timestamp *lower* than what it just observed, and lose in LWW even though it painted later.

### Synchronous mutations
All canvas state lives inside a `tokio::sync::watch::Sender`. Every
mutation, either a paint, gossip merge, or user join,  goes through
`send_modify`, which is synchronous. The closure runs to completion
without yielding.
 
This has two benefits:
- **No lock-across-await risk.** An async `RwLock` can be held across
  suspension points, which can end up blocking other tasks. With
  `send_modify`, the critical section is a plain closure that cannot
  be interrupted.
- **Atomic mutation + delta.** The closure can mutate the document and
  compute what changed in one step. No concurrent write can slip in
  between.

The tradeoff: the closure blocks a tokio worker thread for its
duration. For our 256×256 canvas, merge and delta computation is 
really fast. If the document grew large enough for merge to take
up a significant time, moving to `spawn_blocking` would be warranted.
 
The `watch` channel was chosen over `RwLock` because it provides
change notification for free. WebSocket handlers and the gossip
engine subscribe and are woken on every change without polling.

### `mutate` as the single mutation interface
 
`AppState` exposes one generic method:
 
```rust
pub fn mutate<R>(&self, f: impl FnOnce(&mut CanvasDocument, Uuid) -> R) -> R
```
 
API handlers pass closures that call the appropriate `CanvasDocument`
method. Domain logic lives on `CanvasDocument`, so our `AppState` stays thin.
The state logic does not need to change if the application adds functionality.
 
### Symmetric clock increments on removals
 
All removal methods (`remove_user`, `remove_palette_color`) increment
the document clock, even though ORSet handles tombstoning internally.
Without this, `delta_since` returns an empty delta (the clock didn't
advance) and connected browsers never see the removal.
 
### Two gossip intervals
 
Browser updates are immediate. When `send_modify` fires, the `watch`
channel notifies all subscribers. WebSocket handlers wake up, compute a
delta, and push it. There is no timer involved.
 
Peer gossip runs on a configurable timer (default 200ms). This batches
rapid mutations (mouse-drag painting produces many) into
one network request per intervall, and provides anti-entropy. Even when
nothing changes, the tick carries `known_peers` and `departed`, which
is how tombstones propagate through the mesh.

### Peer lifecycle: discovery, gossip, departure
 
Peers discover each other two ways:
 
- **mDNS** for zero-config discovery on the same LAN. Each node
  announces a `_crdt-net._tcp.local.` service with its UUID and gossip
  address.
- **Bootstrap + peer-list gossip** for cross-subnet. Each `Sync`
  message includes the sender's list of known peers. One bootstrap address is 
  enough to discover the entire mesh.

Peers depart two ways:
 
- **Graceful (Ctrl-C):** the node sends a `Goodbye` to a few peers
  with its UUID in the `departed` field. Others learn immediately.
- **Crash:** no `Goodbye` is sent. Other nodes fail to
  reach the dead peer 10 times in a row and tombstone it
  automatically.

In both cases the UUID ends up in a tombstone set that propagates to
the whole mesh. 
 
## Future work / known limitations
 
- **Tombstone garbage collection.** ORSet tombstones and the `departed`
  set grow unboundedly. Garbage collection requires tracking a minimum
  version frontier across all peers.
- **JSON format.** Functional and good for debugging, but verbose.
  A binary format (would reduce bandwidth.
- **Cursor eviction is not a CRDT.** Cursors use a plain `HashMap`
  cleaned up on merge. An `ORMap` would give full CRDT guarantees at
  the cost of permanent tombstones per departed peer. Since we dont
  have GC, the use of `HashMap` lets us make sure the map doesn't grow 
  indefinitely.


## External dependencies
 
### `crdt-core`
 
| Crate | Purpose |
|---|---|
| `uuid` | Node identity (`NodeId = Uuid`) |
| `serde` | Optional serialization (`feature = "serde"`) |
| `proptest` (dev) | Property-based testing of CRDT laws |
 
### `crdt-net`
 
| Crate | Purpose |
|---|---|
| `tokio` | Async runtime — TCP, timers, task spawning |
| `serde` / `serde_json` | Wire format: length-prefixed JSON frames |
| `uuid` | Peer identity |
| `tracing` | Structured logging |
| `rand` | Random peer selection for gossip fanout |
| `mdns-sd` | mDNS service announcement and browsing |
| `local-ip-address` | Resolve non-loopback local IP for mDNS |
| `crdt-core` | `DeltaCrdt` trait bound on the engine |
 
### `crdt-app`
 
| Crate | Purpose |
|---|---|
| `axum` | HTTP server and WebSocket upgrades |
| `tokio` | Async runtime |
| `tower-http` | CORS middleware |
| `rust-embed` | Embed Vue dist/ into the binary |
| `clap` | CLI argument parsing |
| `tracing` / `tracing-subscriber` | Logging with env-based filter |
| `serde` / `serde_json` | Request/response serialization |
| `uuid` | Node and session identity |
| `http-body-util` (dev) | Response body reading in tests |
| `tower` (dev) | `ServiceExt::oneshot` for handler tests |


## Installation
 
### Prerequisites
 
- Rust toolchain (1.80+): https://rustup.rs
- Node.js (18+) and npm: https://nodejs.org

### Build
 
```bash
git clone https://github.com/christianremman/IDATT2104-NP-Project
cd IDATT2104-NP-Project
 
npm ci --prefix frontend
npm run build --prefix frontend
cargo build --release
```
 
Output: `target/release/crdt-app`
 
---
 
## Usage
 
### Development (with hot-reload)
 
```bash
npm run setup       # first time, installs dependencies
npm run dev         # starts backend (port 8080) + Vite dev server (port 3000)
```
 
Open http://localhost:3000. The dev server proxies `/api` and `/ws` to
the backend.
 
To run processes separately:
 
```bash
npm run dev:backend     # cargo run -p crdt-app (port 8080, gossip 9090)
npm run dev:frontend    # Vite dev server (port 3000)
```
 
### Two nodes on one machine
 
```bash
# Terminal 1
cargo run -p crdt-app -- --port 8080 --gossip-port 9090
 
# Terminal 2 (bootstrap to terminal 1's gossip port)
cargo run -p crdt-app -- --port 8081 --gossip-port 9091 \
    --peers 127.0.0.1:9090
```
 
Open http://localhost:8080 and http://localhost:8081 in separate tabs.
Paint in one, it appears in the other.
 
### Two nodes on the same LAN
 
```bash
# Machine A
cargo run -p crdt-app

# Machine B (mDNS discovers Machine A automatically)
cargo run -p crdt-app 
```
 
No `--peers` flag needed, mDNS handles discovery.
 
### CLI flags
 
| Flag | Default | Description |
|---|---|---|
| `--port` | `8080` | HTTP + WebSocket port for browsers |
| `--gossip-port` | `9090` | TCP port for peer-to-peer gossip (backend only) |
| `--peers` | _(empty)_ | Bootstrap peer addresses (comma-separated `IP:GOSSIP_PORT`) |
| `--gossip-interval-ms` | `200` | Gossip tick interval in milliseconds |
 
### Release build
 
```bash
npm run build    # builds frontend + cargo build --release
```
 
Or push a version tag to trigger CI:
 
```bash
git tag v1.0.0
git push --tags
```
 
### Debugging
 
```bash
# Gossip-level logging
RUST_LOG=info,crdt_net=debug cargo run -p crdt-app
 
# Everything
RUST_LOG=debug cargo run -p crdt-app
```
 
---
 
## Testing
 
```bash
# Full workspace
cargo test --workspace
 
# Individual crates
cargo test -p crdt-core     # CRDT property + unit tests
cargo test -p crdt-net      # Gossip integration tests
cargo test -p crdt-app      # HTTP endpoint tests
 
# Linting
cargo clippy --workspace -- -D warnings
 
# Formatting
cargo fmt --all --check
```
 
---
 
## API documentation
 
See [docs/api.md](docs/api.md) for full request/response documentation
with examples.
 
### REST endpoints
 
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/canvas` | Full canvas snapshot |
| `POST` | `/api/canvas/paint` | Paint a pixel |
| `POST` | `/api/canvas/cursor` | Update cursor position |
| `GET` | `/api/node` | This node's UUID |
| `GET` | `/api/palette` | Current palette colors |
| `POST` | `/api/palette` | Add a palette color |
| `DELETE` | `/api/palette` | Remove a palette color |
| `POST` | `/api/peers` | Add a bootstrap peer |
| `GET` | `/api/leaderboard` | Pixel ownership ranking |
