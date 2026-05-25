//! HTTP and WebSocket API for the canvas application.
//!
//! This file is the boundary between the browser and the application
//! state. It parses HTTP requests, calls [`AppState::mutate`] with the
//! appropriate [`CanvasDocument`] method, and serializes the response.
//! No domain logic lives here: if you want to understand *what*
//! happens to the canvas, look at `canvas.rs`. If you want to
//! understand *how* it's coordinated, look at `state.rs`. This file
//! is just plumbing.
//!
//! **WebSocket session lifecycle**
//!
//! Each browser tab opens a WebSocket to this node's HTTP server.
//! These are local connections. Peer-to-peer gossip runs on separate
//! TCP connections managed by `crdt-net`.
//!
//! The `active_peers` list in the frontend shows which nodes
//! someone is actively using. A [`AtomicUsize`] counts
//! how many browser tabs are connected to this node via WebSocket:
//!
//! - First browser connects (0 -> 1): this node's UUID is added to
//!   the users ORSet. Gossip transimits it to other nodes.
//! - Last browser disconnects (1 -> 0): the UUID is removed.
//!   Gossip transmits that too.
//!
//! ** Notes**
//!
//! A node running with zero browsers is still a gossip peer,
//! it just doesn't appear in `active_peers`.
//!
//! Cursors are tracked per browser session, not per node. Each
//! tab has its own cursor position, cleaned up individually when
//! that tab's WebSocket closes.
use crate::canvas::{
    CanvasDeltaView, CanvasDocument, CanvasVersion, CanvasView, LeaderboardEntry, Rgba,
};
use crate::state::AppState;
use axum::extract::ws;
use axum::{
    body::Body,
    extract::{ws::WebSocket, ws::WebSocketUpgrade, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use crdt_core::DeltaCrdt;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

/// Tracks how many browser tabs are connected to this node.
///
/// Module-level because it's a WS-layer concern, not application state.
/// The counter drives add/remove of this node's UUID in the users ORSet:
/// first tab in adds, last tab out removes.
static WS_SESSIONS: AtomicUsize = AtomicUsize::new(0);

/// Embeds the built Vue frontend (`frontend/dist/`) into the binary at
/// compile time. `static_handler` serves these assets from `/`, so a
/// single binary ships the app end-to-end. Run `npm run build --prefix
/// frontend` before `cargo build` to populate `dist/`.
#[derive(RustEmbed)]
#[folder = "../frontend/dist/"]
struct Frontend;

#[derive(Deserialize)]
struct WsQuery {
    id: Option<String>,
}

/// Envelope for messages pushed to the browser over the WebSocket.
///
/// `Snapshot` carries the full [`CanvasView`] and is sent once when a
/// client connects (or after a server-side reset). `Delta` carries a
/// sparse [`CanvasDeltaView`] computed against that client's last-known
/// vector clock.
#[derive(Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
enum WsMessage {
    Snapshot(CanvasView),
    Delta(CanvasDeltaView),
}
/// Body for `POST /api/canvas/paint`.
#[derive(Deserialize)]
pub struct PaintRequest {
    pub x: u8,
    pub y: u8,
    pub color: [u8; 4],
}

/// Response for `GET /api/node`: identifies this peer on the network.
#[derive(Serialize)]
pub struct NodeInfo {
    /// UUID of this node, assigned at startup.
    pub id: String,
}

/// Body for `POST /api/palette` and `DELETE /api/palette`.
#[derive(Deserialize)]
pub struct PaletteRequest {
    pub color: [u8; 4],
}

/// Body for `POST /api/peers`: adds a runtime bootstrap peer to the gossip engine.
#[derive(Deserialize)]
pub struct BootstrapRequest {
    pub addr: String,
}

/// Body for `POST /api/canvas/cursor`.
#[derive(Deserialize)]
pub struct CursorRequest {
    /// UUID of the user whose cursor is being updated.
    pub user_id: String,
    pub x: u8,
    pub y: u8,
}

/// Build the application router with all API routes, the WebSocket endpoint,
/// and the static file fallback for the embedded Vue frontend.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/canvas", get(get_canvas))
        .route("/api/canvas/paint", post(paint))
        .route("/api/canvas/cursor", post(cursor))
        .route("/api/node", get(node_info))
        .route(
            "/api/palette",
            get(get_palette).post(add_palette).delete(remove_palette),
        )
        .route("/api/peers", post(add_peer))
        .route("/api/leaderboard", get(get_leaderboard))
        .route("/ws", get(ws_handler))
        .fallback(static_handler)
        .with_state(state)
        .layer(CorsLayer::permissive())
}

/// `GET /api/canvas`: returns the full canvas as a [`CanvasView`] JSON snapshot.
async fn get_canvas(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(CanvasView::from(&*s.canvas()))
}

/// `POST /api/canvas/paint`: paint a single pixel; always returns `{ ok: true }`.
async fn paint(State(s): State<Arc<AppState>>, Json(req): Json<PaintRequest>) -> impl IntoResponse {
    let color: Rgba = (req.color[0], req.color[1], req.color[2], req.color[3]);
    s.mutate(|doc, id| doc.paint(req.x, req.y, color, id));
    tracing::debug!(x = req.x, y = req.y, "pixel painted");
    Json(serde_json::json!({ "ok": true }))
}

/// `GET /api/node`: returns this node's UUID
async fn node_info(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(NodeInfo {
        id: s.node_id().to_string(),
    })
}

/// `POST /api/canvas/cursor`: update the cursor position for a user.
///
/// `user_id` is taken from the request body without authentication; any client
/// can move any cursor. Acceptable for this project scope (no auth layer).
async fn cursor(
    State(s): State<Arc<AppState>>,
    Json(req): Json<CursorRequest>,
) -> impl IntoResponse {
    match Uuid::parse_str(&req.user_id) {
        Ok(user_id) => {
            s.mutate(|doc, id| doc.update_cursor(user_id, req.x, req.y, id));
            StatusCode::NO_CONTENT
        }
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

/// `GET /api/palette`: returns the current shared palette as a JSON array of RGBA arrays.
async fn get_palette(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let colors: Vec<[u8; 4]> = s
        .canvas()
        .palette_colors()
        .into_iter()
        .map(|(r, g, b, a)| [r, g, b, a])
        .collect();
    Json(colors)
}

/// `POST /api/palette`: add a color to the shared palette; returns 201 Created.
async fn add_palette(
    State(s): State<Arc<AppState>>,
    Json(req): Json<PaletteRequest>,
) -> impl IntoResponse {
    let color = (req.color[0], req.color[1], req.color[2], req.color[3]);
    s.mutate(|doc, id| doc.add_palette_color(color, &id));
    tracing::debug!(?color, "palette color added");
    StatusCode::CREATED
}

/// `DELETE /api/palette`: remove a color from the shared palette.
///
/// Returns 204 No Content on success, 404 Not Found if the color was not in the palette.
async fn remove_palette(
    State(s): State<Arc<AppState>>,
    Json(req): Json<PaletteRequest>,
) -> impl IntoResponse {
    let color = (req.color[0], req.color[1], req.color[2], req.color[3]);
    let removed = s.mutate(|doc, id| doc.remove_palette_color(&color, id));
    if removed {
        tracing::debug!(?color, "palette color removed");
        StatusCode::NO_CONTENT
    } else {
        tracing::debug!(?color, "color not in palette");
        StatusCode::NOT_FOUND
    }
}

/// `GET /api/leaderboard`: returns pixel ownership counts sorted descending.
async fn get_leaderboard(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let board: Vec<LeaderboardEntry> = s
        .canvas()
        .ownership_leaderboard()
        .into_iter()
        .map(|(id, n)| LeaderboardEntry {
            peer_id: id.to_string(),
            pixels: n,
        })
        .collect();
    Json(board)
}

/// `POST /api/peers`: add a bootstrap peer to the gossip engine at runtime.
///
/// Body: `{"addr": "192.168.1.10:9090"}`. Returns 204 on success, 400 if the
/// address cannot be parsed as a `SocketAddr`.
async fn add_peer(
    State(s): State<Arc<AppState>>,
    Json(req): Json<BootstrapRequest>,
) -> impl IntoResponse {
    match req.addr.parse::<std::net::SocketAddr>() {
        Ok(addr) => {
            s.add_bootstrap(addr);
            StatusCode::NO_CONTENT
        }
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

/// Serves the static frontend files
async fn static_handler(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Frontend::get(path) {
        Some(content) => Response::builder()
            .header(header::CONTENT_TYPE, content.metadata.mimetype())
            .body(Body::from(content.data.into_owned()))
            .unwrap(),
        None => match Frontend::get("index.html") {
            Some(index) => Response::builder()
                .header(header::CONTENT_TYPE, "text/html")
                .body(Body::from(index.data.into_owned()))
                .unwrap(),
            None => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(
                    "Frontend not embedded. Run `npm run build --prefix frontend` before `cargo build`.",
                ))
                .unwrap(),
        },
    }
}

/// `GET /ws`: upgrade to a WebSocket connection and hand off to [`handle_ws`].
///
/// Accepts an optional `?id=<uuid>` query parameter. The frontend passes its
/// stable `sessionStorage` UUID for cursor ownership tracking. Falls back to a
/// fresh UUID when absent or invalid.
async fn ws_handler(
    State(s): State<Arc<AppState>>,
    Query(q): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let user_id =
        q.id.as_deref()
            .and_then(|s| Uuid::parse_str(s).ok())
            .unwrap_or_else(Uuid::new_v4);
    ws.on_upgrade(move |socket| handle_ws(socket, s, user_id))
}

/// Per-client WebSocket session.
///
/// Manages user lifecycle via the [`WS_SESSIONS`] counter:
/// - First tab: adds this node's UUID to the users ORSet.
/// - Last tab: removes it and cleans up the cursor.
/// - Middle tabs: only clean up their own cursor on disconnect.
///
/// The three phases (register - snapshot - stream - cleanup) are split
/// into separate functions for readability.
async fn handle_ws(mut socket: WebSocket, state: Arc<AppState>, user_id: Uuid) {
    // Register this session. If it's the first browser tab on this node,
    // make the node visible in active_peers.
    let is_first = WS_SESSIONS.fetch_add(1, Ordering::Relaxed) == 0;
    if is_first {
        state.mutate(|doc, id| doc.add_user(id, &id));
    }
    tracing::info!(%user_id, sessions = WS_SESSIONS.load(Ordering::Relaxed), "ws client connected");

    // Send initial full snapshot, then stream deltas.
    let last_seen = match send_snapshot(&mut socket, &state).await {
        Some(version) => version,
        None => {
            cleanup_session(&state, &user_id);
            return;
        }
    };

    stream_deltas(&mut socket, &state, last_seen).await;

    cleanup_session(&state, &user_id);
}

/// Clean up when a WebSocket session ends.
///
/// Always removes this session's cursor. If this was the last tab on
/// this node, also removes the node from the users ORSet.
fn cleanup_session(state: &AppState, user_id: &Uuid) {
    let was_last = WS_SESSIONS.fetch_sub(1, Ordering::Relaxed) == 1;
    state.mutate(|doc, id| {
        doc.remove_cursor_entry(user_id, id);
        if was_last {
            doc.remove_user(&id, id);
        }
    });
    tracing::info!(
        %user_id,
        was_last,
        sessions = WS_SESSIONS.load(Ordering::Relaxed),
        "ws client disconnected"
    );
}

/// Send the initial full-state snapshot. Returns the version it covers
/// so the delta loop knows where to start, or `None` if the send fails.
async fn send_snapshot(socket: &mut WebSocket, state: &AppState) -> Option<CanvasVersion> {
    let (msg, version) = {
        let doc = state.canvas();
        let version = doc.version();
        let envelope = WsMessage::Snapshot(CanvasView::from(&*doc));
        (serde_json::to_string(&envelope), version)
    }; // borrow guard dropped before await
    let msg = match msg {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to serialize canvas snapshot");
            return None;
        }
    };
    if socket.send(ws::Message::Text(msg)).await.is_err() {
        return None;
    }
    Some(version)
}

/// Stream deltas to the client until it disconnects or the watch closes.
///
/// Borrows the document just long enough to compute the delta and
/// serialize: the guard is always dropped before the `.await` on
/// `socket.send`.
async fn stream_deltas(socket: &mut WebSocket, state: &AppState, mut last_seen: CanvasVersion) {
    let mut rx = state.subscribe();
    loop {
        tokio::select! {
            result = rx.changed() => {
                if result.is_err() {
                    break;
                }
                // Borrow, compute delta, serialize, drop guard — all sync.
                let msg = {
                    let doc = rx.borrow_and_update();
                    let delta = doc.delta_since(&last_seen);
                    if CanvasDocument::is_empty_delta(&delta) {
                        continue;
                    }
                    let view = CanvasDeltaView::project(&delta, &doc);
                    let envelope = WsMessage::Delta(view);
                    last_seen = doc.version();
                    serde_json::to_string(&envelope)
                }; // guard dropped
                let Ok(msg) = msg else {
                    tracing::error!("failed to serialize canvas delta");
                    break;
                };
                if socket.send(ws::Message::Text(msg)).await.is_err() {
                    break;
                }
            }
            // Detect client disconnect without waiting for a state change.
            msg = socket.recv() => {
                match msg {
                    Some(Ok(_)) => {} // ignore client → server messages
                    _ => break,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn make_app() -> Router {
        let (state, _rx) = crate::state::AppState::new(Uuid::new_v4());
        router(state)
    }
    #[test]
    fn session_counter_first_register() {
        let counter = AtomicUsize::new(0);
        assert_eq!(counter.fetch_add(1, Ordering::Relaxed), 0); // first
    }

    #[test]
    fn session_counter_last_deregister() {
        let counter = AtomicUsize::new(2);
        assert_ne!(counter.fetch_sub(1, Ordering::Relaxed), 1); // not last
        assert_eq!(counter.fetch_sub(1, Ordering::Relaxed), 1); // last
    }

    #[tokio::test]
    async fn get_canvas_returns_empty_snapshot() {
        let app = make_app();
        let req = Request::get("/api/canvas").body(Body::empty()).unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let view: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(view["pixels"].as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn paint_returns_ok() {
        let app = make_app();
        let req = Request::post("/api/canvas/paint")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"x":1,"y":2,"color":[255,0,0,255]}"#))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn paint_then_get_shows_pixel() {
        let (state, _rx) = crate::state::AppState::new(Uuid::new_v4());
        let app = router(state.clone());

        let paint = Request::post("/api/canvas/paint")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"x":3,"y":4,"color":[10,20,30,40]}"#))
            .unwrap();
        let _ = app.clone().oneshot(paint).await.unwrap();

        let get = Request::get("/api/canvas").body(Body::empty()).unwrap();
        let res = app.oneshot(get).await.unwrap();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let view: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(view["pixels"]["3,4"], serde_json::json!([10, 20, 30, 40]));
    }

    #[tokio::test]
    async fn node_info_returns_uuid() {
        let app = make_app();
        let req = Request::get("/api/node").body(Body::empty()).unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let info: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(Uuid::parse_str(info["id"].as_str().unwrap()).is_ok());
    }

    #[tokio::test]
    async fn add_peer_valid_returns_no_content() {
        let app = make_app();
        let req = Request::post("/api/peers")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"addr":"127.0.0.1:9090"}"#))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn add_peer_invalid_returns_bad_request() {
        let app = make_app();
        let req = Request::post("/api/peers")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"addr":"not-an-address"}"#))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn palette_add_and_get() {
        let (state, _rx) = crate::state::AppState::new(Uuid::new_v4());
        let app = router(state.clone());

        let add = Request::post("/api/palette")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"color":[255,0,0,255]}"#))
            .unwrap();
        let res = app.clone().oneshot(add).await.unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let get = Request::get("/api/palette").body(Body::empty()).unwrap();
        let res = app.oneshot(get).await.unwrap();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let colors: Vec<[u8; 4]> = serde_json::from_slice(&body).unwrap();
        assert!(colors.contains(&[255, 0, 0, 255]));
    }

    #[tokio::test]
    async fn remove_nonexistent_palette_returns_not_found() {
        let app = make_app();
        let req = Request::delete("/api/palette")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"color":[1,2,3,4]}"#))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cursor_valid_uuid_returns_no_content() {
        let app = make_app();
        let user_id = Uuid::new_v4();
        let body = serde_json::json!({"user_id": user_id.to_string(), "x": 10, "y": 20});
        let req = Request::post("/api/canvas/cursor")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn cursor_invalid_uuid_returns_bad_request() {
        let app = make_app();
        let req = Request::post("/api/canvas/cursor")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"user_id":"not-a-uuid","x":0,"y":0}"#))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }
}
