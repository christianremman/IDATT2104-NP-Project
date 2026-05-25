//! Run two (or more) nodes in separate terminals and watch them converge.
//!
//! Each node owns a counter map keyed by node UUID with element-wise max
//! merge (a valid state-based CRDT). Type `bump` on stdin to increment
//! this node's counter. State changes , local or merged , print one
//! deduped line at a time.
//!
//! With mDNS on (the default), nodes on the same local subnet discover
//! each other automatically with zero `--bootstrap` flags. Across
//! subnets / over Tailscale, supply one `--bootstrap IP:PORT` and peer-list
//! gossip will discover the rest of the mesh.
//!
//! Examples:
//!
//!   # zero-config on shared Wi-Fi
//!   cargo run -p crdt-net --example two_node_demo -- --port 9090
//!
//!   # explicit bootstrap (across subnets / Tailscale / WAN)
//!   cargo run -p crdt-net --example two_node_demo -- --port 9090 \
//!       --bootstrap 192.168.1.57:9090
//!
//! Commands on stdin:
//!   bump          , increment this node's counter
//!   peers         , list discovered peers
//!   add  IP:PORT  , add a bootstrap peer at runtime
//!   rm   UUID     , remove a peer by node UUID (use `peers` to find it)
//!   quit          , exit

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use crdt_core::{Crdt, DeltaCrdt};
use crdt_net::{GossipConfig, GossipEngine};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, watch};
use uuid::Uuid;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Counter {
    counts: BTreeMap<Uuid, u64>,
}

impl Counter {
    fn bump(&mut self, who: Uuid) {
        *self.counts.entry(who).or_default() += 1;
    }
    fn total(&self) -> u64 {
        self.counts.values().sum()
    }
}

impl Crdt for Counter {
    type Value = u64;
    fn value(&self) -> u64 {
        self.total()
    }
    fn merge(&mut self, other: Self) {
        for (k, v) in other.counts {
            let slot = self.counts.entry(k).or_default();
            if v > *slot {
                *slot = v;
            }
        }
    }
    fn compare(&self, other: &Self) -> bool {
        self.counts
            .iter()
            .all(|(k, v)| other.counts.get(k).is_some_and(|ov| v <= ov))
    }
}

impl DeltaCrdt for Counter {
    type Delta = BTreeMap<Uuid, u64>;
    type Version = BTreeMap<Uuid, u64>;

    fn version(&self) -> Self::Version {
        self.counts.clone()
    }

    fn delta_since(&self, since: &Self::Version) -> Self::Delta {
        self.counts
            .iter()
            .filter_map(|(k, &v)| {
                let known = since.get(k).copied().unwrap_or(0);
                (v > known).then_some((*k, v))
            })
            .collect()
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        for (k, v) in delta {
            let slot = self.counts.entry(k).or_default();
            if v > *slot {
                *slot = v;
            }
        }
    }

    fn is_empty_delta(delta: &Self::Delta) -> bool {
        delta.is_empty()
    }

    fn version_includes(current: &Self::Version, other: &Self::Version) -> bool {
        other
            .iter()
            .all(|(k, v)| current.get(k).copied().unwrap_or(0) >= *v)
    }
}

struct Args {
    bind: String,
    port: u16,
    bootstraps: Vec<SocketAddr>,
    mdns: bool,
}

fn parse_args() -> Args {
    fn die(msg: impl std::fmt::Display) -> ! {
        eprintln!("error: {msg}");
        eprintln!(
            "usage: two_node_demo --port <P> [--bind <IP>] \\\n         \
             [--bootstrap IP:PORT]... [--no-mdns]\n\
             example: two_node_demo --port 9090 --bootstrap 192.168.1.57:9090"
        );
        std::process::exit(2);
    }

    let mut bind: String = "0.0.0.0".to_string();
    let mut port: Option<u16> = None;
    let mut bootstraps = Vec::new();
    let mut mdns = true;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--bind" => bind = it.next().unwrap_or_else(|| die("--bind needs value")),
            "--port" => {
                let raw = it.next().unwrap_or_else(|| die("--port needs value"));
                port = Some(
                    raw.parse()
                        .unwrap_or_else(|e| die(format!("--port {raw}: {e}"))),
                );
            }
            // `--peer` kept as an alias for backwards compatibility with older notes.
            "--bootstrap" | "--peer" => {
                let raw = it.next().unwrap_or_else(|| die("--bootstrap needs value"));
                let addr: SocketAddr = raw
                    .parse()
                    .unwrap_or_else(|e| die(format!("--bootstrap {raw}: {e} (expected IP:PORT)")));
                bootstraps.push(addr);
            }
            "--no-mdns" => mdns = false,
            "--mdns" => mdns = true,
            "-h" | "--help" => {
                eprintln!(
                    "usage: two_node_demo --port <P> [--bind <IP>] \\\n         \
                     [--bootstrap IP:PORT]... [--no-mdns]\n\
                     defaults: --bind 0.0.0.0, mDNS ON\n\
                     example: two_node_demo --port 9090 --bootstrap 192.168.1.57:9090"
                );
                std::process::exit(0);
            }
            other => die(format!("unknown arg: {other}")),
        }
    }
    Args {
        bind,
        port: port.unwrap_or_else(|| die("--port is required")),
        bootstraps,
        mdns,
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crdt_net=info,warn")),
        )
        .with_target(false)
        .init();

    let args = parse_args();
    let node_id = Uuid::new_v4();
    let gossip_addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse().unwrap();

    let (state_tx, state_rx) = watch::channel(Counter::default());
    let (merged_tx, _) = broadcast::channel::<Counter>(64);

    let engine = GossipEngine::run(
        GossipConfig::new(node_id, gossip_addr)
            .with_peers(args.bootstraps.clone())
            .with_interval(Duration::from_secs(1))
            .with_mdns(args.mdns),
        state_rx.clone(),
        merged_tx.clone(),
    )
    .await?;

    println!(
        "node {} listening on {} (advertise={})",
        node_id,
        engine.local_addr(),
        engine.advertise_addr()
    );
    println!(
        "initial bootstraps: {:?}  |  mDNS: {}",
        args.bootstraps,
        if args.mdns { "on" } else { "off" }
    );
    println!(
        "commands: bump | peers | tombstones | add IP:PORT | rm UUID | quit  (Ctrl-C also exits cleanly)"
    );

    // Self-peer guard: if a bootstrap matches our advertise addr, warn loudly.
    let advertise = engine.advertise_addr();
    for b in &args.bootstraps {
        if b == &advertise {
            eprintln!(
                "WARNING: --bootstrap {b} is this node's own advertise address. \
                 Did you mean a peer's IP? Self-peering does nothing useful."
            );
        }
    }

    // Forwarder.
    {
        let state_tx = state_tx.clone();
        let mut rx = merged_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(incoming) = rx.recv().await {
                state_tx.send_modify(|s| s.merge(incoming));
            }
        });
    }

    // Deduped STATE printer.
    {
        let mut rx = state_rx.clone();
        let mut last = rx.borrow().clone();
        println!("STATE total={} counts={:?}", last.total(), last.counts);
        tokio::spawn(async move {
            loop {
                if rx.changed().await.is_err() {
                    return;
                }
                let s = rx.borrow().clone();
                if s != last {
                    println!("STATE total={} counts={:?}", s.total(), s.counts);
                    last = s;
                }
            }
        });
    }

    // stdin loop, racing against Ctrl-C so we can shut down gracefully.
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let line = match line? {
                    Some(l) => l,
                    None => break, // stdin closed
                };
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let (cmd, rest) = line.split_once(' ').unwrap_or((line, ""));
                match cmd {
                    "bump" => state_tx.send_modify(|s| s.bump(node_id)),
                    "peers" => {
                        let peers = engine.known_peers();
                        if peers.is_empty() {
                            println!("(no known peers yet)");
                        } else {
                            for p in peers {
                                println!("  {}  {}", p.node_id, p.addr);
                            }
                        }
                    }
                    "tombstones" => {
                        let tombs = engine.known_tombstones();
                        if tombs.is_empty() {
                            println!("(no tombstones)");
                        } else {
                            for id in tombs {
                                println!("  {id}");
                            }
                        }
                    }
                    "add" => match rest.parse::<SocketAddr>() {
                        Ok(addr) => {
                            engine.add_bootstrap(addr);
                            println!("added bootstrap {addr}");
                        }
                        Err(e) => println!("bad addr: {e}"),
                    },
                    "rm" => match Uuid::parse_str(rest) {
                        Ok(id) => {
                            engine.remove_peer(id);
                            println!("removed {id}");
                        }
                        Err(e) => println!("bad UUID: {e}"),
                    },
                    "quit" | "exit" => break,
                    _ => println!("unknown command: {cmd}"),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nCtrl-C received , sending Goodbye to peers...");
                break;
            }
        }
    }

    eprintln!("graceful shutdown...");
    engine.graceful_shutdown().await;
    Ok(())
}
