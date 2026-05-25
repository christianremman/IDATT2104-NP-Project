use std::net::SocketAddr;
use std::time::Duration;

use crdt_core::Crdt;
use crdt_net::{GossipConfig, GossipEngine};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, watch};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

mod common;
use common::MockCrdt;

struct Node {
    id: Uuid,
    state_tx: watch::Sender<MockCrdt>,
    state_rx: watch::Receiver<MockCrdt>,
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
        let engine = GossipEngine::run(config, state_rx.clone(), merged_tx.clone())
            .await
            .expect("bind");

        // Forwarder: merge each broadcast back into the watch source.
        let forward_tx = state_tx.clone();
        let mut forward_rx = merged_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(incoming) = forward_rx.recv().await {
                forward_tx.send_modify(|s| s.merge(incoming));
            }
        });

        Self {
            id,
            state_tx,
            state_rx,
            engine,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.engine.local_addr()
    }

    fn bump(&self) {
        self.state_tx.send_modify(|s| s.bump(self.id));
    }

    fn current(&self) -> MockCrdt {
        self.state_rx.borrow().clone()
    }
}

async fn await_total(node: &Node, expected: u64, deadline: Duration) -> u64 {
    let res = timeout(deadline, async {
        loop {
            let total = node.current().total();
            if total >= expected {
                return total;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match res {
        Ok(t) => t,
        Err(_) => node.current().total(),
    }
}

#[tokio::test]
async fn converges_across_three_nodes() {
    let interval = Duration::from_millis(80);
    let a = Node::start(interval).await;
    let b = Node::start(interval).await;
    let c = Node::start(interval).await;

    // Full mesh , wire each pair using known UUIDs and local addresses.
    a.engine.add_peer(b.id, b.addr());
    a.engine.add_peer(c.id, c.addr());
    b.engine.add_peer(a.id, a.addr());
    b.engine.add_peer(c.id, c.addr());
    c.engine.add_peer(a.id, a.addr());
    c.engine.add_peer(b.id, b.addr());

    for _ in 0..3 {
        a.bump();
    }
    b.bump();
    c.bump();
    c.bump();

    let deadline = Duration::from_secs(5);
    let ta = await_total(&a, 6, deadline).await;
    let tb = await_total(&b, 6, deadline).await;
    let tc = await_total(&c, 6, deadline).await;

    assert_eq!(ta, 6, "node a total");
    assert_eq!(tb, 6, "node b total");
    assert_eq!(tc, 6, "node c total");
    assert_eq!(a.current(), b.current());
    assert_eq!(b.current(), c.current());
}

#[tokio::test]
async fn partition_then_heal() {
    let interval = Duration::from_millis(60);
    let a = Node::start(interval).await;
    let b = Node::start(interval).await;

    a.engine.add_peer(b.id, b.addr());
    b.engine.add_peer(a.id, a.addr());

    a.bump();
    b.bump();
    let _ = await_total(&a, 2, Duration::from_secs(3)).await;
    let _ = await_total(&b, 2, Duration::from_secs(3)).await;
    assert_eq!(a.current(), b.current());

    a.engine.remove_peer(b.id);
    b.engine.remove_peer(a.id);

    a.bump();
    a.bump();
    b.bump();
    sleep(Duration::from_millis(150)).await;
    assert_ne!(a.current(), b.current(), "should have diverged");

    a.engine.add_peer(b.id, b.addr());
    b.engine.add_peer(a.id, a.addr());

    let ta = await_total(&a, 5, Duration::from_secs(5)).await;
    let tb = await_total(&b, 5, Duration::from_secs(5)).await;
    assert_eq!(ta, 5);
    assert_eq!(tb, 5);
    assert_eq!(a.current(), b.current());
}

#[tokio::test]
async fn garbage_does_not_kill_listener() {
    let interval = Duration::from_millis(60);
    let victim = Node::start(interval).await;
    let probe = Node::start(interval).await;

    let target = victim.addr();

    // Bogus length prefix claiming 32 MiB, then close.
    let mut s = TcpStream::connect(target).await.unwrap();
    s.write_all(&(32u32 * 1024 * 1024).to_be_bytes())
        .await
        .unwrap();
    drop(s);

    // Valid length, truncated body.
    let mut s = TcpStream::connect(target).await.unwrap();
    s.write_all(&100u32.to_be_bytes()).await.unwrap();
    s.write_all(b"not enough bytes").await.unwrap();
    drop(s);

    // Half a length prefix.
    let mut s = TcpStream::connect(target).await.unwrap();
    s.write_all(&[0xff, 0xff]).await.unwrap();
    drop(s);

    // A real peer should still be able to reach the victim.
    victim.engine.add_peer(probe.id, probe.addr());
    probe.engine.add_peer(victim.id, victim.addr());
    probe.bump();
    probe.bump();
    victim.bump();

    let t = await_total(&victim, 3, Duration::from_secs(5)).await;
    assert_eq!(t, 3, "victim should still receive after garbage");
}
