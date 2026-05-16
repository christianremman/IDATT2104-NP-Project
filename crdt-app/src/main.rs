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

#[derive(clap::Parser)]
struct Args {
    #[arg(long, default_value_t = 8080)]
    port: u16,
    #[arg(long, default_value_t = 9090)]
    gossip_port: u16,
    /// Comma-separated bootstrap peers, e.g. 127.0.0.1:9091,127.0.0.1:9092
    #[arg(long, default_value = "")]
    peers: String,
    /// Gossip tick interval in milliseconds. Lower values give snappier
    /// convergence at the cost of more network chatter.
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

    // Polls the gossip engine's tombstone set and evicts departed peer UUIDs
    // from the CRDT user set. The engine's peer registry and the CanvasDocument
    // user ORSet are otherwise unconnected, so neither graceful Goodbye messages
    // nor crash evictions are reflected in active_peers without this.
    let state_reconcile = Arc::clone(&state);
    let reconcile_interval = Duration::from_millis(args.gossip_interval_ms);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(reconcile_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Some(engine) = state_reconcile.engine() {
                let tombstones: HashSet<Uuid> = engine.known_tombstones().into_iter().collect();
                state_reconcile.remove_departed_users(&tombstones);
            }
        }
    });

    tracing::info!(
        %node_id,
        http = %http_addr,
        gossip = %gossip_addr,
        bootstraps = ?bootstrap,
        interval_ms = args.gossip_interval_ms,
        "node started"
    );

    let state_clone = Arc::clone(&state);
    let mut merged_rx = merged_tx.subscribe();
    tokio::spawn(async move {
        while let Ok(incoming) = merged_rx.recv().await {
            tracing::debug!("applying incoming gossip merge");
            state_clone.apply_gossip(incoming);
        }
        tracing::warn!("gossip forwarder exited — broadcast channel closed");
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
