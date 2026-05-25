# API Documentation

All endpoints are served by the `crdt-app` binary. The base URL depends
on the `--port` flag.

## REST API

### `GET /api/canvas`

Returns the full canvas state as a JSON snapshot.

**Response** `200 OK`

```json
{
  "pixels": {
    "0,0": [255, 0, 0, 255],
    "3,4": [0, 255, 0, 255]
  },
  "active_peers": [
    "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  ],
  "palette": [
    [255, 0, 0, 255],
    [0, 0, 255, 255]
  ],
  "paint_total": 42,
  "leaderboard": [
    { "peer_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "pixels": 30 }
  ],
  "cursors": {
    "a1b2c3d4-e5f6-7890-abcd-ef1234567890": [10, 20]
  }
}
```

| Field | Type | Description |
|---|---|---|
| `pixels` | `Object<string, [u8; 4]>` | Painted pixels keyed as `"x,y"`. Unpainted pixels are absent (default white). |
| `active_peers` | `string[]` | UUIDs of nodes with active browser connections, sorted. |
| `palette` | `[u8; 4][]` | Shared palette colors `[r, g, b, a]`, sorted. |
| `paint_total` | `number` | Total paint operations across all peers (monotonic). |
| `leaderboard` | `LeaderboardEntry[]` | Pixel ownership counts, sorted descending. |
| `cursors` | `Object<string, [u8; 2]>` | Cursor positions `[x, y]` per peer UUID. |

---

### `POST /api/canvas/paint`

Paint a single pixel. The CRDT assigns a Lamport timestamp internally,
concurrent paints to the same coordinate are resolved by last-writer-wins.

**Request body**

```json
{
  "x": 3,
  "y": 4,
  "color": [255, 0, 0, 255]
}
```

| Field | Type | Description |
|---|---|---|
| `x` | `u8` | X coordinate (0–255) |
| `y` | `u8` | Y coordinate (0–255) |
| `color` | `[u8; 4]` | RGBA color |

**Response** `200 OK`

```json
{ "ok": true }
```

---

### `POST /api/canvas/cursor`

Update a browser session's cursor position. Used by the frontend to
show other users' cursors on the canvas.

**Request body**

```json
{
  "user_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "x": 10,
  "y": 20
}
```

| Field | Type | Description |
|---|---|---|
| `user_id` | `string` | UUID of the browser session (from `sessionStorage`) |
| `x` | `u8` | X coordinate |
| `y` | `u8` | Y coordinate |

**Response** `204 No Content` on success, `400 Bad Request` if
`user_id` is not a valid UUID.

---

### `GET /api/node`

Returns this node's identity.

**Response** `200 OK`

```json
{
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

| Field | Type | Description |
|---|---|---|
| `id` | `string` | UUID assigned to this node at startup |

---

### `GET /api/palette`

Returns the current shared palette.

**Response** `200 OK`

```json
[
  [255, 0, 0, 255],
  [0, 255, 0, 255],
  [0, 0, 255, 255]
]
```

An array of `[r, g, b, a]` colors.

---

### `POST /api/palette`

Add a color to the shared palette. Uses ORSet add-wins. If
two peers concurrently add and remove the same color, the add wins.

**Request body**

```json
{
  "color": [255, 128, 0, 255]
}
```

| Field | Type | Description |
|---|---|---|
| `color` | `[u8; 4]` | RGBA color to add |

**Response** `201 Created`

---

### `DELETE /api/palette`

Remove a color from the shared palette.

**Request body**

```json
{
  "color": [255, 128, 0, 255]
}
```

| Field | Type | Description |
|---|---|---|
| `color` | `[u8; 4]` | RGBA color to remove |

**Response** `204 No Content` on success, `404 Not Found` if the color
is not in the palette.

---

### `POST /api/peers`

Add a bootstrap peer to the gossip engine at runtime. The engine
attempts to gossip to this address until it responds, at which point
the peer's UUID is learned and it migrates into the resolved peer map.

**Request body**

```json
{
  "addr": "192.168.1.10:9090"
}
```

| Field | Type | Description |
|---|---|---|
| `addr` | `string` | Socket address (`IP:PORT`) of the peer's gossip port |

**Response** `204 No Content` on success, `400 Bad Request` if the
address cannot be parsed as a `SocketAddr`.

---

### `GET /api/leaderboard`

Returns pixel ownership counts — how many pixels each peer currently
"owns" (is the last writer of).

**Response** `200 OK`

```json
[
  { "peer_id": "a1b2c3d4-...", "pixels": 120 },
  { "peer_id": "e5f67890-...", "pixels": 45 }
]
```

| Field | Type | Description |
|---|---|---|
| `peer_id` | `string` | UUID of the peer |
| `pixels` | `number` | Number of pixels this peer last painted |

Sorted descending by `pixels`.

---

## WebSocket protocol

### Connecting

```
GET /ws?id=<uuid>
```

The `id` parameter is optional. The frontend passes its stable
`sessionStorage` UUID so cursor positions are attributed to the correct
browser session across page reloads. If absent or invalid, the server
generates a fresh UUID.


#### Snapshot

Sent once immediately after the WebSocket connects. Contains the full
canvas state. The same data as `GET /api/canvas`.

```json
{
  "type": "snapshot",
  "payload": {
    "pixels": { "0,0": [255, 0, 0, 255] },
    "active_peers": ["a1b2c3d4-e5f6-7890-abcd-ef1234567890"],
    "palette": [[255, 0, 0, 255]],
    "paint_total": 42,
    "leaderboard": [{ "peer_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "pixels": 30 }],
    "cursors": { "aa1b2c3d4-e5f6-7890-abcd-ef1234567890": [10, 20] }
  }
}
```

#### Delta

Sent on every subsequent state change. Contains only what changed
since the client's last update:

```json
{
  "type": "delta",
  "payload": {
    "pixels": { "3,4": [255, 255, 0, 255] },
    "active_peers": ["a1b2c3d4-e5f6-7890-abcd-ef1234567890.", "e5f67890-e5f6-7890-abcd-ef1234567890"],
    "paint_total": 43
  }
}
```

**Fields present only when changed.** If `palette` is absent from a
delta, the palette has not changed. the frontend keeps its previous
value. `pixels` is always present (may be empty `{}`). `active_peers`
and `paint_total` are always present for consistency.

| Field | Presence | Description |
|---|---|---|
| `pixels` | Always | Changed pixel coordinates and their new colors |
| `active_peers` | Always | Full list of active peer UUIDs |
| `paint_total` | Always | Current total paint count |
| `palette` | Only when changed | Full palette (replaced, not patched) |
| `leaderboard` | Only when pixels changed | Full leaderboard (re-derived) |
| `cursors` | Only when changed | Changed cursor positions (patched into existing map) |

### Reconnection

The frontend reconnects automatically after 3 seconds on disconnect.
Each reconnection receives a fresh snapshot, so no state is lost.