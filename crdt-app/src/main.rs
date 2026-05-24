//! Entry point for the collaborative pixel canvas.
//!
//! This binary wires together the three layers of the application:
//!
//! 1. **State** (`state.rs`): holds the canvas in a `watch` channel.
//! 2. **Gossip** (`crdt-net`): syncs the canvas between peers over TCP.
//! 3. **HTTP/WS** (`api.rs`): serves the browser frontend and pushes
//!    live updates.
//!
//! The startup sequence: parse CLI args, create the shared
//! state, start the gossip engine, spawn background tasks, then serve
//! HTTP. On shutdown, axum drains connections (so WebSocket cleanup runs),
//! then the engine sends Goodbye to peers.
//!
//! **Two ports, two protocols**. Each node listens on ports:
//!
//! - `--port` (default 8080): HTTP + WebSocket for browsers. This is
//!   what you open in web browser.
//! - `--gossip-port` (default 9090): TCP for peer-to-peer CRDT gossip.
//!   Browsers never touch this: it's backend-to-backend only.
//!
//! **Peer discovery**. Nodes find each other two ways:
//!
//! - **Automatic (mDNS):** on the same LAN/WiFi, nodes discover each
//!   others automatiacally. Just start two nodes and they connect.
//! - **Manual (bootstrap):** across subnets or over the internet, pass
//!   `--peers <ip>:<gossip-port>` pointing at any node already in the
//!   mesh. Peer-list gossip propagates the rest: one bootstrap is
//!   enough to discover everyone connected.
//!
//! **Example: two nodes on localhost**
//!
//! ```bash
//! # Terminal 1
//! cargo run -p crdt-app -- --port 5000 --gossip-port 9999
//!
//! # Terminal 2 (different ports, bootstraps to terminal 1's gossip port)
//! cargo run -p crdt-app -- --port 8081 --gossip-port 9090 \
//!     --peers 127.0.0.1:9999
//!
//! # Open http://localhost:5000 and http://localhost:8081 in two browser tabs
//! ```
mod api;
mod canvas;
mod state;

use crdt_net::{GossipConfig, GossipEngine};
use state::AppState;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use uuid::Uuid;

/// Command-line arguments for the canvas node.
#[derive(clap::Parser)]
struct Args {
    /// Port for the HTTP server and WebSocket connections.
    /// This is what browsers connect to (e.g. http://localhost:8080).
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Port for peer-to-peer gossip.Bbackend-to-backend only.
    /// Must be different from --port and unique per node on the same machine.
    #[arg(long, default_value_t = 9090)]
    gossip_port: u16,

    /// Comma-separated addresses of other nodes' gossip ports to
    /// connect to on startup. Format: IP:GOSSIP_PORT.
    /// Not needed on the same LAN (mDNS handles discovery).
    /// One address is enough — peer-list gossip discovers the rest.
    ///
    /// Example: --peers 192.168.1.10:9090,192.168.1.11:9091
    #[arg(long, default_value = "")]
    peers: String,

    /// How often the gossip engine sends state to peers, in milliseconds.
    /// Lower = faster sync, more network traffic.
    ///
    /// Note: this only affects peer-to-peer gossip. Local browsers get
    /// updates immediately via the watch channel, no timer involved.
    /// The timer exists for gossip because it batches rapid mutations
    /// (e.g. mouse-drag painting) into one request per interval, and
    /// provides anti-entropy (retransmits even when nothing changed, so
    /// peers that missed a message eventually catch up).
    #[arg(long, default_value_t = 200)]
    gossip_interval_ms: u64,
}

#[tokio::main]
async fn main() {
    // Default filter: app + gossip at INFO, mDNS silenced. The mdns-sd
    // crate emits ERRORs for every network interface it can't use
    // (WSL virtual adapters, IPv6-only NICs) which is normal and not
    // actionable. Override with RUST_LOG when debugging discovery.
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,mdns_sd=off"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = <Args as clap::Parser>::parse();
    let node_id = Uuid::new_v4();
    let http_addr = format!("0.0.0.0:{}", args.port);

    let bootstrap: Vec<std::net::SocketAddr> = args
        .peers
        .split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|s| {
            s.parse()
                .map_err(|e| tracing::warn!("ignoring invalid peer address {s}: {e}"))
                .ok()
        })
        .collect();

    let (state, local_rx) = AppState::new(node_id);
    let (merged_tx, _) = broadcast::channel::<canvas::CanvasDocument>(64);

    let gossip_addr: std::net::SocketAddr =
        format!("0.0.0.0:{}", args.gossip_port).parse().unwrap();
    let config = GossipConfig::new(node_id, gossip_addr)
        .with_peers(bootstrap.clone())
        .with_interval(Duration::from_millis(args.gossip_interval_ms))
        .with_mdns(true);

    let engine = GossipEngine::run(config, local_rx, merged_tx.clone())
        .await
        .expect("gossip engine failed to start");

    state.set_engine(Arc::new(engine));

    tracing::info!(
        %node_id,
        http = %http_addr,
        gossip = %gossip_addr,
        bootstraps = ?bootstrap,
        interval_ms = args.gossip_interval_ms,
        "node started"
    );

    // The gossip engine's peer registry and the CanvasDocument's user
    // ORSet are independent: tombstoning a peer in the registry (on
    // Goodbye or repeated send failures) does not automatically remove
    // it from active_peers. This task bridges that gap by periodically
    // checking for tombstoned UUIDs that are still in the user set.
    {
        let state = Arc::clone(&state);
        let interval = Duration::from_millis(args.gossip_interval_ms);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(engine) = state.engine() else {
                    continue;
                };
                let tombstones: HashSet<Uuid> = engine.known_tombstones().into_iter().collect();
                if tombstones.is_empty() {
                    continue;
                }
                let departed: Vec<Uuid> = {
                    let active = state.canvas().active_users();
                    active.intersection(&tombstones).copied().collect()
                };
                if !departed.is_empty() {
                    tracing::debug!(
                        count = departed.len(),
                        "evicting departed peers from user set"
                    );
                    state.mutate(|doc, id| {
                        for uid in &departed {
                            doc.remove_user(uid, id);
                        }
                    });
                }
            }
        });
    }

    let state_clone = Arc::clone(&state);
    let mut merged_rx = merged_tx.subscribe();
    tokio::spawn(async move {
        while let Ok(incoming) = merged_rx.recv().await {
            tracing::debug!("applying incoming gossip merge");
            state_clone.apply_gossip(incoming);
        }
        tracing::warn!("gossip forwarder exited: broadcast channel closed");
    });

    let shutdown_signal = async {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("ctrl-c received, draining connections");
    };

    let listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .expect("failed to bind HTTP listener");

    axum::serve(listener, api::router(state.clone()))
        .with_graceful_shutdown(shutdown_signal)
        .await
        .expect("server error");

    // Axum is drained, all WS handlers have finished their cleanup
    tracing::info!("http server stopped, sending Goodbye to peers");
    if let Some(engine) = state.engine() {
        engine.graceful_shutdown().await;
    }
    tracing::info!("shutdown complete");
}
