//! Tests for transitive peer-list propagation.
//!
//! mDNS itself is not tested here , multicast inside the CI / loopback
//! environment is restricted and flaky. We disable mDNS (`with_mdns(false)`)
//! so these tests exercise only the peer-list-in-Sync mechanism: a node
//! that knows peer B should learn about C from B's gossip.

use std::net::SocketAddr;
use std::time::Duration;

use crdt_core::Crdt;
use crdt_net::{GossipConfig, GossipEngine};
use tokio::sync::{broadcast, watch};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

mod common;
use common::MockCrdt;

struct Node {
    id: Uuid,
    engine: GossipEngine,
}

impl Node {
    async fn start(interval: Duration) -> Self {
        let id = Uuid::new_v4();
        let (state_tx, state_rx) = watch::channel(MockCrdt::default());
        let (merged_tx, _merged_rx) = broadcast::channel(32);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = GossipConfig::new(id, addr)
            .with_interval(interval)
            .with_mdns(false);
        let engine = GossipEngine::run(config, state_rx, merged_tx.clone())
            .await
            .expect("bind");

        // Need a forwarder so the engine's broadcast doesn't block; we don't
        // actually care about the state for these tests.
        let forward_tx = state_tx;
        let mut forward_rx = merged_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(incoming) = forward_rx.recv().await {
                forward_tx.send_modify(|s| s.merge(incoming));
            }
        });

        Self { id, engine }
    }
}

/// Wait until `node` reports at least `n` known peers, or timeout.
async fn wait_for_peers(engine: &GossipEngine, n: usize, deadline: Duration) -> usize {
    let res = timeout(deadline, async {
        loop {
            let len = engine.known_peers().len();
            if len >= n {
                return len;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match res {
        Ok(l) => l,
        Err(_) => engine.known_peers().len(),
    }
}

#[tokio::test]
async fn peer_list_propagates_transitively() {
    // Topology: A , B , C. A only knows B; C only knows B. After enough
    // ticks for B to gossip its known_peers to both, A and C should
    // discover each other.
    let interval = Duration::from_millis(60);
    let a = Node::start(interval).await;
    let b = Node::start(interval).await;
    let c = Node::start(interval).await;

    a.engine.add_peer(b.id, b.engine.local_addr());
    b.engine.add_peer(a.id, a.engine.local_addr());
    b.engine.add_peer(c.id, c.engine.local_addr());
    c.engine.add_peer(b.id, b.engine.local_addr());

    // After a handful of ticks (each ~60ms), A's known_peers should contain
    // C even though A was never told about it directly.
    let a_peers = wait_for_peers(&a.engine, 2, Duration::from_secs(3)).await;
    let c_peers = wait_for_peers(&c.engine, 2, Duration::from_secs(3)).await;

    assert!(a_peers >= 2, "A should know B and C, got {a_peers}");
    assert!(c_peers >= 2, "C should know A and B, got {c_peers}");

    let a_known: Vec<Uuid> = a.engine.known_peers().iter().map(|p| p.node_id).collect();
    assert!(a_known.contains(&c.id), "A should have learned C via B");

    let c_known: Vec<Uuid> = c.engine.known_peers().iter().map(|p| p.node_id).collect();
    assert!(c_known.contains(&a.id), "C should have learned A via B");
}

#[tokio::test]
async fn bootstrap_gets_uuid_after_first_contact() {
    let interval = Duration::from_millis(50);
    let a = Node::start(interval).await;

    // B is given A's address as a bootstrap (no UUID known yet).
    let b_id = Uuid::new_v4();
    let (state_tx, state_rx) = watch::channel(MockCrdt::default());
    let (merged_tx, _merged_rx) = broadcast::channel(32);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = GossipConfig::new(b_id, addr)
        .with_peers(vec![a.engine.local_addr()])
        .with_interval(interval)
        .with_mdns(false);
    let b_engine = GossipEngine::run(config, state_rx, merged_tx.clone())
        .await
        .expect("bind");
    // Forwarder.
    let mut forward_rx = merged_tx.subscribe();
    tokio::spawn(async move {
        while let Ok(incoming) = forward_rx.recv().await {
            state_tx.send_modify(|s| s.merge(incoming));
        }
    });

    // Also tell A about B by UUID so A gossips back; otherwise A doesn't
    // initiate connections to B (A has no peers).
    a.engine.add_peer(b_id, b_engine.local_addr());

    // Wait for B to resolve A from a bootstrap into a known peer.
    let _ = wait_for_peers(&b_engine, 1, Duration::from_secs(3)).await;
    let known: Vec<Uuid> = b_engine.known_peers().iter().map(|p| p.node_id).collect();
    assert!(
        known.contains(&a.id),
        "B should have resolved A's UUID after first contact, known = {known:?}"
    );
}
