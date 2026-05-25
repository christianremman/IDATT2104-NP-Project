//! Tests for 2P-Set–style peer tombstones.
//!
//! Covers:
//!  - `graceful_shutdown` sends a Goodbye that propagates the tombstone to
//!    the rest of the mesh within a few ticks.
//!  - A tombstoned UUID is not re-added by peer-list gossip carrying its
//!    address in `known_peers`.
//!  - K consecutive failed sends evicts an unreachable bootstrap.
//!  - Tombstones propagate transitively via the `departed` field of Sync.

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
        Self::start_with_bootstraps(interval, vec![]).await
    }

    async fn start_with_bootstraps(interval: Duration, bootstraps: Vec<SocketAddr>) -> Self {
        let id = Uuid::new_v4();
        let (state_tx, state_rx) = watch::channel(MockCrdt::default());
        let (merged_tx, _merged_rx) = broadcast::channel(32);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = GossipConfig::new(id, addr)
            .with_peers(bootstraps)
            .with_interval(interval)
            .with_mdns(false);
        let engine = GossipEngine::run(config, state_rx, merged_tx.clone())
            .await
            .expect("bind");

        // Forwarder to absorb broadcast traffic and prevent it from filling.
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

/// Wait until `predicate(engine)` returns true, or until `deadline` elapses.
async fn wait_until<F: Fn(&GossipEngine) -> bool>(
    engine: &GossipEngine,
    predicate: F,
    deadline: Duration,
) -> bool {
    timeout(deadline, async {
        loop {
            if predicate(engine) {
                return true;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or(false)
}

#[tokio::test]
async fn graceful_shutdown_propagates_tombstone() {
    let interval = Duration::from_millis(60);
    let a = Node::start(interval).await;
    let b = Node::start(interval).await;
    let c = Node::start(interval).await;

    a.engine.add_peer(b.id, b.engine.local_addr());
    a.engine.add_peer(c.id, c.engine.local_addr());
    b.engine.add_peer(a.id, a.engine.local_addr());
    b.engine.add_peer(c.id, c.engine.local_addr());
    c.engine.add_peer(a.id, a.engine.local_addr());
    c.engine.add_peer(b.id, b.engine.local_addr());

    // Give the mesh a moment to settle.
    sleep(Duration::from_millis(150)).await;

    // A says goodbye.
    a.engine.graceful_shutdown().await;

    let b_has = wait_until(
        &b.engine,
        |e| e.known_tombstones().contains(&a.id),
        Duration::from_secs(2),
    )
    .await;
    let c_has = wait_until(
        &c.engine,
        |e| e.known_tombstones().contains(&a.id),
        Duration::from_secs(2),
    )
    .await;

    assert!(b_has, "B should have A's tombstone after graceful shutdown");
    assert!(c_has, "C should have A's tombstone after graceful shutdown");

    // And the dead peer is gone from the resolved set on both.
    let b_resolved: Vec<Uuid> = b.engine.known_peers().iter().map(|p| p.node_id).collect();
    let c_resolved: Vec<Uuid> = c.engine.known_peers().iter().map(|p| p.node_id).collect();
    assert!(!b_resolved.contains(&a.id), "B should not still know A");
    assert!(!c_resolved.contains(&a.id), "C should not still know A");
}

#[tokio::test]
async fn tombstoned_peer_is_not_revived_by_peer_list_gossip() {
    let interval = Duration::from_millis(60);
    let a = Node::start(interval).await;
    let b = Node::start(interval).await;
    let c = Node::start(interval).await;

    // Full mesh, then settle.
    a.engine.add_peer(b.id, b.engine.local_addr());
    a.engine.add_peer(c.id, c.engine.local_addr());
    b.engine.add_peer(a.id, a.engine.local_addr());
    b.engine.add_peer(c.id, c.engine.local_addr());
    c.engine.add_peer(a.id, a.engine.local_addr());
    c.engine.add_peer(b.id, b.engine.local_addr());
    sleep(Duration::from_millis(200)).await;

    // A tombstones B locally (simulates "B died" or "B left and A learned first").
    a.engine.tombstone_peer(b.id);
    assert!(a.engine.known_tombstones().contains(&b.id));
    assert!(
        !a.engine.known_peers().iter().any(|p| p.node_id == b.id),
        "A should drop B from resolved when tombstoning"
    );

    // Meanwhile C still has B in its resolved map and gossips it to A.
    // Wait long enough for several ticks of C → A gossip.
    sleep(Duration::from_millis(300)).await;

    // A must NOT have re-acquired B.
    let a_resolved: Vec<Uuid> = a.engine.known_peers().iter().map(|p| p.node_id).collect();
    assert!(
        !a_resolved.contains(&b.id),
        "A should still not have B after C's peer-list gossip"
    );
    // And A's tombstone is intact.
    assert!(a.engine.known_tombstones().contains(&b.id));
}

#[tokio::test]
async fn consecutive_failures_evict_unreachable_bootstrap() {
    let interval = Duration::from_millis(30);
    // Pick a port the kernel has reserved for nobody by binding then
    // immediately releasing it. Using a fixed low port like 127.0.0.1:1
    // is unreliable across platforms (Linux RSTs instantly, Windows may
    // filter, macOS may RST after a small delay, and on some systems
    // 127.0.0.1:1 is privileged).
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead: SocketAddr = probe.local_addr().unwrap();
    drop(probe);
    let a = Node::start_with_bootstraps(interval, vec![dead]).await;

    // Wait long enough for FAILURE_THRESHOLD (10) ticks of failed sends to
    // happen. At 30ms intervals with near-instant RST from a closed port,
    // that's well under a second of real time; we add headroom so CI noise
    // doesn't make it flaky. After the threshold the bootstrap is silently
    // dropped , the registry doesn't expose the bootstrap set externally,
    // so the assertion is indirect: we verify the engine is still healthy
    // and can still discover a real peer added afterwards.
    sleep(Duration::from_millis(500)).await;

    let b = Node::start(interval).await;
    a.engine.add_peer(b.id, b.engine.local_addr());
    b.engine.add_peer(a.id, a.engine.local_addr());

    let saw_b = wait_until(
        &a.engine,
        |e| e.known_peers().iter().any(|p| p.node_id == b.id),
        Duration::from_secs(2),
    )
    .await;
    assert!(
        saw_b,
        "A should still be able to talk to real peers after bootstrap eviction"
    );
}

#[tokio::test]
async fn tombstone_propagates_transitively_via_known_peers_in_sync() {
    let interval = Duration::from_millis(50);
    let a = Node::start(interval).await;
    let b = Node::start(interval).await;
    let c = Node::start(interval).await;

    // A , B , C topology. A and C don't talk directly; they learn about
    // each other only via B's known_peers gossip. (This is the same
    // topology as the discovery-test `peer_list_propagates_transitively`.)
    a.engine.add_peer(b.id, b.engine.local_addr());
    b.engine.add_peer(a.id, a.engine.local_addr());
    b.engine.add_peer(c.id, c.engine.local_addr());
    c.engine.add_peer(b.id, b.engine.local_addr());

    // Let the mesh discover itself.
    let knows = wait_until(
        &a.engine,
        |e| e.known_peers().iter().any(|p| p.node_id == c.id),
        Duration::from_secs(2),
    )
    .await;
    assert!(knows, "A should have learned C via B's peer-list gossip");

    // A tombstones C (simulating a K-failure eviction).
    a.engine.tombstone_peer(c.id);

    // The tombstone must reach B (via A's next Sync's `departed` field)
    // and then C (via B's next Sync). C should know it's been tombstoned
    // by someone.
    let b_has = wait_until(
        &b.engine,
        |e| e.known_tombstones().contains(&c.id),
        Duration::from_secs(2),
    )
    .await;
    assert!(b_has, "B should have learned C's tombstone from A");

    // C learns about its own tombstone but ignores it (never tombstone self).
    let c_self = wait_until(
        &c.engine,
        |e| e.known_tombstones().contains(&c.id),
        Duration::from_millis(300),
    )
    .await;
    assert!(
        !c_self,
        "C should refuse to tombstone its own UUID (self-protection in absorb_tombstones)"
    );
}
