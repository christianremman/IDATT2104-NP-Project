# IDATT2104 Network Programming Project

A distributed collaborative pixel canvas using delta state-based conflict-free replicated data types and peer-to-peer gossip.

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
This is the Lamport clock rule applied to a vector clock.
 
Without this, a node that merges a peer's state and then paints can
generate a timestamp *lower* than what it just observed:


## Quick start (pre-built binary)

No Rust or Node.js required. Download the binary for your platform from [GitHub Releases](../../releases/latest):

| Platform | File |
|---|---|
| Linux x86_64 | `crdt-node-linux-x86_64` |
| macOS Apple Silicon | `crdt-node-macos-arm64` |
| Windows | `crdt-node-windows-x86_64.exe` |

Run it:

```
./crdt-node
```

Open http://localhost:8080. The canvas loads automatically.

On a LAN, each peer runs their own copy. Nodes discover each other via mDNS with no configuration. If mDNS is unavailable (e.g. university network), specify a peer manually:

```
./crdt-node --peers 192.168.x.x:9090
```

## Development

### Prerequisites

- Rust (stable) — https://rustup.rs
- Node.js 18+ and npm

### Running

```
npm run setup   # first time only — installs dependencies
npm run dev     # starts backend (port 8080) and frontend dev server (port 3000) together
```

Open http://localhost:3000. The canvas connects automatically. First compile takes ~30–60s.

To run processes separately:

```
npm run dev:backend    # cargo run -p crdt-app (port 8080, gossip port 9090)
npm run dev:frontend   # Vite dev server (port 3000, proxies /api and /ws to :8080)
```

### Two nodes on one machine (binary)

```
./crdt-node --port 8080 --gossip-port 9090 --peers 127.0.0.1:9091
./crdt-node --port 8081 --gossip-port 9091 --peers 127.0.0.1:9090
```

Open http://localhost:8080 and http://localhost:8081 in separate tabs. Both canvases sync.

### Building a release binary

```
npm run build
```

Builds the frontend, then compiles the Rust binary with the frontend embedded. Output: `target/release/crdt-app`.

## Releasing

Push a version tag to trigger the CI release workflow, which builds binaries for all platforms and uploads them to GitHub Releases:

```
git tag v1.0.0
git push --tags
```

## CLI flags

| Flag | Default | Description |
|---|---|---|
| `--port` | 8080 | HTTP/WebSocket port |
| `--gossip-port` | 9090 | TCP port for peer-to-peer gossip |
| `--peers` | _(empty)_ | Comma-separated bootstrap peers, e.g. `127.0.0.1:9091` |

## REST API

| Method | Path | Description |
|---|---|---|
| GET | `/api/canvas` | Full canvas snapshot |
| POST | `/api/canvas/paint` | Paint a pixel `{"x":0,"y":0,"color":[r,g,b,a]}` |
| GET | `/api/node` | Node ID and address |
| GET | `/api/palette` | Current shared palette |
| POST | `/api/palette` | Add color `{"color":[r,g,b,a]}` |
| DELETE | `/api/palette` | Remove color `{"color":[r,g,b,a]}` |
| GET | `/api/leaderboard` | Pixel ownership counts per node |
| GET | `/ws` | WebSocket — streams canvas snapshots on every change |

## Testing

```
cargo test --workspace
cargo clippy --workspace -- -D warnings
```
