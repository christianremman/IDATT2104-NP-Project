// Pinia store for the shared CRDT canvas.
//
// Manages the WebSocket connection to the backend, applies snapshot and delta
// messages to local state, and exposes actions for painting, cursor updates,
// and palette management.
import { defineStore } from 'pinia'

// WebSocket instance lives outside Pinia state — Vue's Proxy wrapping breaks
// the WebSocket internal `this instanceof WebSocket` checks.
let _ws = null
// Base URL for API and WS calls. Empty string = relative (embedded mode).
// Set to 'http://host:port' when ?port= is provided (Vite dev server targeting a specific node).
let _apiBase = ''

// Stable per-tab identity used for cursor ownership. Survives page refresh
// within the same tab via sessionStorage.
const _storedClientId = sessionStorage.getItem('canvas-client-id') ?? crypto.randomUUID()
sessionStorage.setItem('canvas-client-id', _storedClientId)

export const useCanvasStore = defineStore('canvas', {
  state: () => ({
    pixels: new Map(),          // "x,y" → [r,g,b,a]
    palette: new Set(),         // JSON.stringify([r,g,b,a]) strings — value equality
    cursors: new Map(),         // userId → { x, y }
    activePeers: new Set(),     // uuid strings
    paintTotal: 0,
    leaderboard: [],            // [{ peer_id, pixels }] sorted desc
    connected: false,
    nodeId: null,
    nodeAddr: null,
    selectedColor: [0, 0, 0, 255],
    clientId: _storedClientId,
  }),

  getters: {
    paletteColors: (state) => [...state.palette].map(s => JSON.parse(s)),
  },

  actions: {
    // Entry point called from App.vue on mount.
    init(port) {
      if (port) _apiBase = `${location.protocol}//${location.hostname}:${port}`
      this.connect()
    },

    // Fetch this node's UUID and address from the backend; retries on failure.
    async fetchNodeInfo() {
      try {
        const r = await fetch(`${_apiBase}/api/node`)
        const d = await r.json()
        this.nodeId = d.id
        this.nodeAddr = d.addr
      } catch {
        setTimeout(() => this.fetchNodeInfo(), 3000)
      }
    },

    // Open the WebSocket connection; skips if one is already open or connecting.
    // Reconnects automatically after 3 s on close.
    connect() {
      if (_ws && _ws.readyState <= WebSocket.OPEN) return
      _ws = null
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
      const host = _apiBase ? new URL(_apiBase).host : location.host
      _ws = new WebSocket(`${proto}//${host}/ws?id=${_storedClientId}`)

      _ws.onopen = () => {
        this.connected = true
        if (!this.nodeId) this.fetchNodeInfo()
      }

      _ws.onmessage = (evt) => {
        const msg = JSON.parse(evt.data)
        // Backend wraps state in `{type, payload}`.
        const data = msg.payload ?? msg
        if (msg.type === 'snapshot') {
          this._applySnapshot(data)
        } else if (msg.type === 'delta') {
          this._applyDelta(data)
        } else {
          // Unknown type: don't silently apply as a snapshot — that
          // masked decode errors and stale frames in earlier versions.
          console.warn('[canvas] dropped WS message with unknown type:', msg.type)
        }
      }

      _ws.onclose = () => {
        this.connected = false
        _ws = null
        setTimeout(() => this.connect(), 3000)
      }

      _ws.onerror = () => _ws && _ws.close()
    },

    // Full canvas state. Replaces every collection wholesale.
    _applySnapshot(data) {
      this.pixels.clear()
      for (const [key, color] of Object.entries(data.pixels ?? {})) {
        this.pixels.set(key, color)
      }
      this.palette.clear()
      for (const color of (data.palette ?? [])) {
        this.palette.add(JSON.stringify(color))
      }
      this.activePeers.clear()
      for (const peer of (data.active_peers ?? [])) {
        this.activePeers.add(peer)
      }
      this.paintTotal = data.paint_total ?? 0
      this.leaderboard = data.leaderboard ?? []
      this.cursors.clear()
      for (const [userId, pos] of Object.entries(data.cursors ?? {})) {
        this.cursors.set(userId, { x: pos[0], y: pos[1] })
      }
    },

    // Sparse update from the backend's `CanvasDeltaView`.
    //
    // `pixels` carries only the cells that changed and is patched into
    // the existing Map (no clear). The other fields are present only
    // when their underlying CRDT changed — when present, replace the
    // whole collection (the backend ships the full derived view for
    // these to keep the projection simple).
    _applyDelta(data) {
      for (const [key, color] of Object.entries(data.pixels ?? {})) {
        this.pixels.set(key, color)
      }
      if (data.active_peers !== undefined) {
        this.activePeers.clear()
        for (const peer of data.active_peers) {
          this.activePeers.add(peer)
        }
      }
      if (data.palette !== undefined) {
        this.palette.clear()
        for (const color of data.palette) {
          this.palette.add(JSON.stringify(color))
        }
      }
      if (data.paint_total !== undefined) {
        this.paintTotal = data.paint_total
      }
      if (data.leaderboard !== undefined) {
        this.leaderboard = data.leaderboard
      }
      if (data.cursors !== undefined) {
        for (const [userId, pos] of Object.entries(data.cursors)) {
          this.cursors.set(userId, { x: pos[0], y: pos[1] })
        }
      }
    },

    // POST a paint operation to the backend. Errors are swallowed because the
    // optimistic local update in PixelCanvas already reflects the change.
    async paint(x, y, color) {
      await fetch(`${_apiBase}/api/canvas/paint`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ x, y, color }),
      }).catch(() => {})
    },

    // POST the local client's current cursor cell to the backend for broadcast.
    async updateCursor(x, y) {
      await fetch(`${_apiBase}/api/canvas/cursor`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: this.clientId, x, y }),
      }).catch(() => {})
    },

    // Add a color to the shared palette (ORSet add-wins).
    async addColor(color) {
      await fetch(`${_apiBase}/api/palette`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ color }),
      }).catch(() => {})
    },

    // Add a bootstrap peer to the gossip engine at runtime.
    // Returns 'ok', 'bad-address', or 'network-error'.
    async bootstrap(addr) {
      try {
        const res = await fetch(`${_apiBase}/api/peers`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ addr }),
        })
        return res.ok ? 'ok' : 'bad-address'
      } catch {
        return 'network-error'
      }
    },

    // Remove a color from the shared palette.
    async removeColor(color) {
      await fetch(`${_apiBase}/api/palette`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ color }),
      }).catch(() => {})
    },
  },
})
