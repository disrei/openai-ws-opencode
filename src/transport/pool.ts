import crypto from "node:crypto"
import WebSocket from "ws"
import type { TransportContext } from "./headers.js"

type WebSocketLike = {
  readyState: number
  send(data: string): void
  close(code?: number, reason?: string): void
  terminate?: () => void
  ping?: () => void
  on?(event: string, listener: (...args: any[]) => void): unknown
  off?(event: string, listener: (...args: any[]) => void): unknown
  addEventListener?(event: string, listener: (...args: any[]) => void): unknown
  removeEventListener?(event: string, listener: (...args: any[]) => void): unknown
}

type WebSocketConstructor = new (url: string, options: { headers: Record<string, string> }) => WebSocketLike

const DEFAULT_CONNECT_TIMEOUT_MS = 10_000
const MAX_RECONNECT_ATTEMPTS = 3
const RECONNECT_DELAY_MS = 100
const IDLE_EVICT_MS = 120_000

let WebSocketImpl: WebSocketConstructor = WebSocket as unknown as WebSocketConstructor

export interface PendingRequest {
  body: Record<string, unknown>
  controller: ReadableStreamDefaultController<Uint8Array>
  done: boolean
  sent: boolean
  forwarded: boolean
}

export interface PooledConnection {
  ws: WebSocketLike | null
  wsUrl: string
  headers: Record<string, string>
  scopeKey: string
  busy: boolean
  pending: PendingRequest | null
  activeSessionID?: string
  lastSessionID?: string
  activeAgent?: string
  lastAgent?: string
  lastModelID?: string
  lastStablePrefixHash?: string
  generation: number
  reconnectAttempts: number
  idleTimer: ReturnType<typeof setTimeout> | null
  connectTimer: ReturnType<typeof setTimeout> | null
  retryTimer: ReturnType<typeof setTimeout> | null
  detach: (() => void) | null
}

export const connectionPool: PooledConnection[] = []

export const readyState = {
  CONNECTING: 0,
  OPEN: 1,
  CLOSING: 2,
  CLOSED: 3,
} as const

function on(ws: WebSocketLike, event: string, listener: (...args: any[]) => void) {
  if (ws.on) ws.on(event, listener)
  else ws.addEventListener?.(event, listener)
}

function off(ws: WebSocketLike, event: string, listener: (...args: any[]) => void) {
  if (ws.off) ws.off(event, listener)
  else ws.removeEventListener?.(event, listener)
}

function scopeKey(wsUrl: string, headers: Record<string, string>): string {
  const auth = Object.entries(headers)
    .filter(([key]) => ["authorization", "chatgpt-account-id", "originator", "openai-beta"].includes(key.toLowerCase()))
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}:${value}`)
    .join("\n")
  return `${wsUrl}::${crypto.createHash("sha256").update(auth).digest("hex")}`
}

function clearTimer(timer: ReturnType<typeof setTimeout> | null) {
  if (timer) clearTimeout(timer)
}

function detach(conn: PooledConnection) {
  if (conn.detach) {
    conn.detach()
    conn.detach = null
  }
  clearTimer(conn.connectTimer)
  clearTimer(conn.idleTimer)
  clearTimer(conn.retryTimer)
  conn.connectTimer = null
  conn.idleTimer = null
  conn.retryTimer = null
}

function remove(conn: PooledConnection) {
  const index = connectionPool.indexOf(conn)
  if (index >= 0) connectionPool.splice(index, 1)
  detach(conn)
}

function release(conn: PooledConnection) {
  conn.lastSessionID = conn.activeSessionID
  conn.lastAgent = conn.activeAgent
  conn.pending = null
  conn.activeSessionID = undefined
  conn.activeAgent = undefined
  conn.busy = false
  conn.reconnectAttempts = 0
  const timer = setTimeout(() => {
    if (!conn.busy) {
      remove(conn)
      conn.ws?.close(1000, "idle eviction")
    }
  }, IDLE_EVICT_MS)
  if (typeof timer === "object" && "unref" in timer) timer.unref()
  conn.idleTimer = timer
}

function fail(conn: PooledConnection, error: Error, closeSocket: boolean) {
  const pending = conn.pending
  if (pending && !pending.done) {
    pending.done = true
    try {
      pending.controller.error(error)
    } catch {}
  }
  conn.pending = null
  conn.busy = false
  conn.activeSessionID = undefined
  conn.activeAgent = undefined
  if (closeSocket) {
    remove(conn)
    try {
      conn.ws?.terminate?.()
      conn.ws?.close(1000, "aborted")
    } catch {}
  }
}

function parseFrames(data: unknown): Array<Record<string, unknown>> {
  const text = typeof data === "string" ? data : Buffer.isBuffer(data) ? data.toString("utf8") : String(data)
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
  const candidates = lines.length ? lines : [text]
  const frames: Array<Record<string, unknown>> = []
  for (const candidate of candidates) {
    try {
      const parsed = JSON.parse(candidate)
      if (parsed && typeof parsed === "object") frames.push(parsed as Record<string, unknown>)
    } catch {}
  }
  return frames
}

function enqueueSSE(conn: PooledConnection, frame: Record<string, unknown>) {
  const pending = conn.pending
  if (!pending || pending.done) return
  const eventType = typeof frame.type === "string" ? frame.type : "message"
  const encoded = new TextEncoder().encode(`event: ${eventType}\ndata: ${JSON.stringify(frame)}\n\n`)
  pending.forwarded = true
  pending.controller.enqueue(encoded)
  if (["response.completed", "response.failed", "response.incomplete", "error"].includes(eventType)) {
    pending.done = true
    pending.controller.close()
    release(conn)
  }
}

function connect(conn: PooledConnection) {
  const ws = new WebSocketImpl(conn.wsUrl, { headers: conn.headers })
  const generation = ++conn.generation
  conn.ws = ws
  clearTimer(conn.retryTimer)
  conn.retryTimer = null
  clearTimer(conn.connectTimer)
  const connectTimer = setTimeout(() => {
    if (generation !== conn.generation || ws.readyState === readyState.OPEN) return
    fail(conn, new Error("WebSocket connection timed out"), true)
  }, DEFAULT_CONNECT_TIMEOUT_MS)
  if (typeof connectTimer === "object" && "unref" in connectTimer) connectTimer.unref()
  conn.connectTimer = connectTimer

  const handleOpen = () => {
    if (generation !== conn.generation) return
    clearTimer(conn.connectTimer)
    conn.connectTimer = null
    if (conn.pending && !conn.pending.done) {
      ws.send(JSON.stringify({ type: "response.create", ...conn.pending.body }))
      conn.pending.sent = true
    }
  }

  const handleMessage = (data: unknown) => {
    if (generation !== conn.generation) return
    for (const frame of parseFrames(data)) enqueueSSE(conn, frame)
  }

  const handleError = () => {
    if (generation !== conn.generation) return
    handleSocketLoss(conn)
  }

  const handleClose = () => {
    if (generation !== conn.generation) return
    handleSocketLoss(conn)
  }

  on(ws, "open", handleOpen)
  on(ws, "message", handleMessage)
  on(ws, "error", handleError)
  on(ws, "close", handleClose)

  conn.detach = () => {
    off(ws, "open", handleOpen)
    off(ws, "message", handleMessage)
    off(ws, "error", handleError)
    off(ws, "close", handleClose)
  }
}

function handleSocketLoss(conn: PooledConnection) {
  clearTimer(conn.connectTimer)
  conn.connectTimer = null
  const pending = conn.pending
  if (!pending || pending.done) {
    remove(conn)
    return
  }
  if (!pending.sent && conn.reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
    conn.reconnectAttempts++
    detach(conn)
    const timer = setTimeout(() => {
      conn.retryTimer = null
      if (!connectionPool.includes(conn) || !conn.pending || conn.pending.done || conn.pending.sent) return
      connect(conn)
    }, RECONNECT_DELAY_MS)
    if (typeof timer === "object" && "unref" in timer) timer.unref()
    conn.retryTimer = timer
    return
  }
  fail(conn, new Error("WebSocket closed after stream started"), true)
}

function create(wsUrl: string, headers: Record<string, string>): PooledConnection {
  const conn: PooledConnection = {
    ws: null,
    wsUrl,
    headers,
    scopeKey: scopeKey(wsUrl, headers),
    busy: false,
    pending: null,
    generation: 0,
    reconnectAttempts: 0,
    idleTimer: null,
    connectTimer: null,
    retryTimer: null,
    detach: null,
  }
  connectionPool.push(conn)
  connect(conn)
  return conn
}

export function acquireConnection(wsUrl: string, headers: Record<string, string>, context: TransportContext): PooledConnection {
  const key = scopeKey(wsUrl, headers)
  const reusable = connectionPool.find(
    (conn) =>
      !conn.busy &&
      conn.scopeKey === key &&
      conn.ws?.readyState === readyState.OPEN &&
      (!context.sessionID || !conn.lastSessionID || conn.lastSessionID === context.sessionID) &&
      (!context.agent || !conn.lastAgent || conn.lastAgent === context.agent),
  )
  const conn = reusable ?? create(wsUrl, headers)
  clearTimer(conn.idleTimer)
  conn.idleTimer = null
  conn.busy = true
  return conn
}

export function closeConnections(predicate: (conn: PooledConnection) => boolean = () => true) {
  for (const conn of [...connectionPool]) {
    if (!predicate(conn)) continue
    fail(conn, new Error("Session disposed"), true)
  }
}

export function resetPoolForTesting() {
  for (const conn of [...connectionPool]) {
    try {
      conn.ws?.close(1000, "test reset")
    } catch {}
    remove(conn)
  }
}

export function setWebSocketConstructorForTesting(ctor: WebSocketConstructor) {
  WebSocketImpl = ctor
}

export function resetWebSocketConstructorForTesting() {
  WebSocketImpl = WebSocket as unknown as WebSocketConstructor
}
