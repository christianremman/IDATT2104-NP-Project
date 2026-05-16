//! mDNS service discovery for crdt-net peers.
//!
//! On the same local subnet, each node announces a
//! `_crdt-net._tcp.local.` service with its UUID as the instance name and
//! its advertised gossip address as the service location. Each node also
//! browses for the same service type; resolved entries are inserted into
//! the engine's peer registry.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tokio::sync::Notify;
use tracing::debug;
use uuid::Uuid;

use crate::engine::PeerRegistry;

const SERVICE_TYPE: &str = "_crdt-net._tcp.local.";

pub(crate) fn spawn_mdns(
    self_id: Uuid,
    advertise_addr: SocketAddr,
    registry: Arc<PeerRegistry>,
    shutdown: Arc<Notify>,
) -> io::Result<()> {
    let daemon = ServiceDaemon::new().map_err(io_other)?;

    let instance_name = self_id.to_string();
    let host = format!("{instance_name}.local.");
    let addr_str = advertise_addr.ip().to_string();

    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("node_id".to_string(), self_id.to_string());
    props.insert("version".to_string(), "1".to_string());

    let service = ServiceInfo::new(
        SERVICE_TYPE,
        &instance_name,
        &host,
        addr_str.as_str(),
        advertise_addr.port(),
        Some(props),
    )
    .map_err(io_other)?;

    daemon.register(service).map_err(io_other)?;
    let receiver = daemon.browse(SERVICE_TYPE).map_err(io_other)?;

    // The daemon must live for the lifetime of the engine; move it into the
    // browse task and shut it down via the shared `shutdown` Notify.
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    debug!("mdns shutdown");
                    let _ = daemon.shutdown();
                    return;
                }
                event = receiver.recv_async() => {
                    let Ok(event) = event else {
                        debug!("mdns receiver closed");
                        return;
                    };
                handle_event(self_id, &registry, event);
                }
            }
        }
    });

    Ok(())
}

fn handle_event(self_id: Uuid, registry: &PeerRegistry, event: ServiceEvent) {
    match event {
        ServiceEvent::ServiceResolved(info) => {
            if let Some((id, addr)) = parse_peer(self_id, &info) {
                debug!(%id, %addr, "mdns resolved peer");
                registry.add_resolved(id, addr);
            }
        }
        ServiceEvent::ServiceRemoved(_ty, fullname) => {
            if let Some(uuid_str) = fullname.split('.').next()
                && let Ok(id) = Uuid::parse_str(uuid_str)
                && id != self_id
            {
                debug!(%id, "mdns removed peer");
                registry.remove(id);
            }
        }
        _ => {}
    }
}

fn parse_peer(self_id: Uuid, info: &ServiceInfo) -> Option<(Uuid, SocketAddr)> {
    let id_prop = info
        .get_property("node_id")
        .and_then(|p| p.val_str().to_string().into());
    let id_str: String = id_prop?;
    let id = Uuid::parse_str(&id_str).ok()?;
    if id == self_id {
        return None;
    }
    let port = info.get_port();
    // `get_addresses()` returns a `HashSet<IpAddr>`. Iteration order is
    // unspecified, so we explicitly prefer IPv4: if a service announces
    // both, picking randomly between v4 and v6 would make peer addresses
    // non-deterministic across runs. Fall back to whatever the set has if
    // it's v6-only.
    let addresses = info.get_addresses();
    let ip = addresses
        .iter()
        .find(|a| a.is_ipv4())
        .or_else(|| addresses.iter().next())
        .copied()?;
    Some((id, SocketAddr::new(ip, port)))
}

fn io_other<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(format!("{e}"))
}
