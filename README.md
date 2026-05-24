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

The project includes a generalt purpose CRDTs library, which has implementation of multiple state-based crdts. These have support for using time deltas to improve performance, where only the recently applied chages to the state are used. 
It also has a networking library, which has a general purpose gossiping engine supporting the use of delta crdts.

The application ties these more general purpose libraries together to show of their functionality. It is a proof-of-consept application, to show of the use of the crdts. However, it does not use all of them. It is importaint to note we have focused on not tieng the libraries directly to the application, so they can easily be used for other types of applications


- **crdt-app** — Axum HTTP + WebSocket server. Serves the frontend and exposes a REST API.
- **frontend** — Vue 3 single-page app. Pixel canvas, color picker, peer list, leaderboard. The build of the frontend is served by crdt-app.


## Implementation

### The crates + frontend
**`crdt-core`** is a general-purpose CRDT library. It knows nothing about
networking, canvases, or pixels. Any Rust project could use it.
**`crdt-net`** is a general-purpose gossip engine over TCP. It is generic over
any `T: DeltaCrdt + Serialize`. It knows nothing about canvases, it
just discovers peers, sends state, and merges what comes back.
**`crdt-app`** is the the application-specific crate. It is a Axum HTTP + WebSocket server. It composes CRDTs
from `crdt-core` into a `CanvasDocument`, wires it into `crdt-net` for
peer-to-peer sync, and serves a browser frontend over HTTP/WebSocket.
**`frontend`** — Vue 3 single-page app. Pixel canvas, color picker, peer list, leaderboard. The build of the frontend is served by crdt-app.

Explain each of the crates here, and the use of a cargo workspace.

## Key Design Desitions

### The VectorClock
A importaint design desition is that our app uses one `VectorClock` on
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

Moving the clock into the document eliminateed both. The clock merges
automatically with the rest of the state, and `increment` is called
inside the same `send_modify` closure as the mutation.

The clock uses  the Lamport rule. `VectorClock::increment` does not add 1 to the node's own
counter. It sets it to `max(own_counter, max_across_all_nodes) + 1`. 
Without this, a node that merges a peer's state and then paints can
generate a timestamp *lower* than what it just observed, and lose in LWW even though it painted later.

### Synchronus mutations

All mutations happens via `watch::Sender::send_modify`, meaning all canvas
mutations run to completion without yielding, eliminating
lock-across-await risks and enabling atomic mutation + delta
computation.

Explain more in detail why this is ggod, and the tradeoff


## Installation



## Development

### Prerequisites

- Rust toolchain (1.80+): https://rustup.rs
- Node.js (18+) and npm: https://nodejs.org

### Running
For easier development with the frontend (have auto reloading of files, not build required), 
the backend is started by npm.

```
npm run setup   # first time only — installs dependencies
npm run dev     # starts backend (port 8080) and frontend dev server (port 3000) together
```

Open http://localhost:3000. The canvas connects automatically.

To run processes separately:

```
npm run dev:backend    # cargo run -p crdt-app (port 8080, gossip port 9090)
npm run dev:frontend   # Vite dev server (port 3000, proxies /api and /ws to :8080)
```

For only running the backend (must have built frontend files).
```
cargo run
```
### Two nodes on one machine 

```
cargo run --port 8080 --gossip-port 9090 
cargo run --port 8081 --gossip-port 9091 --peers 127.0.0.1:9090
```

Open http://localhost:8080 and http://localhost:8081 in separate tabs. Both canvases sync.

### Building a release binary

```
npm run build
```

Builds the frontend, then compiles the Rust binary with the frontend embedded. Output: `target/release/crdt-app`.

### Debugging
 
```bash
# Verbose gossip logging
RUST_LOG=info,crdt_net=debug cargo run -p crdt-app -- --port 8080 --gossip-port 9090
 
# See everything
RUST_LOG=debug cargo run -p crdt-app -- --port 8080 --gossip-port 9090
```


### Releasing

Push a version tag to trigger the CI release workflow, which builds binaries for all platforms and uploads them to GitHub Releases:

```
git tag v1.0.0
git push --tags
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `--port` | 8080 | HTTP/WebSocket port |
| `--gossip-port` | 9090 | TCP port for peer-to-peer gossip |
| `--peers` | _(empty)_ | Comma-separated bootstrap peers, e.g. `127.0.0.1:9091` |
| `--gossip-interval-ms` | 200 |   Gossip tick interval |

## REST API

 
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/canvas` | Full canvas snapshot (JSON) |
| `POST` | `/api/canvas/paint` | Paint a pixel: `{"x":0,"y":0,"color":[255,0,0,255]}` |
| `POST` | `/api/canvas/cursor` | Update cursor: `{"user_id":"<uuid>","x":0,"y":0}` |
| `GET` | `/api/node` | This node's UUID |
| `GET` | `/api/palette` | Current palette colors |
| `POST` | `/api/palette` | Add color: `{"color":[255,0,0,255]}` |
| `DELETE` | `/api/palette` | Remove color: `{"color":[255,0,0,255]}` |
| `POST` | `/api/peers` | Add bootstrap peer: `{"addr":"192.168.1.10:9090"}` |
| `GET` | `/api/leaderboard` | Pixel ownership ranking |


## Testing

```
cargo test --workspace
cargo clippy --workspace -- -D warnings

# Individual crates
cargo test -p crdt-core    # CRDT unit + property tests
cargo test -p crdt-net     # Gossip integration tests
cargo test -p crdt-app     # API endpoint tests
 
# With output (useful for debugging)
cargo test -- --nocapture
```
