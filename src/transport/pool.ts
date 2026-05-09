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
const STALE_REUSE_MS = 60_000
const HEARTBEAT_INTERVAL_MS = 30_000
const PONG_TIMEOUT_MS = 10_000

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
  lastActivityAt: number
  lastCloseCode?: number
  lastCloseReason?: string
  lastErrorMessage?: string
  idleTimer: ReturnType<typeof setTimeout> | null
  connectTimer: ReturnType<typeof setTimeout> | null
  retryTimer: ReturnType<typeof setTimeout> | null
  heartbeatTimer: ReturnType<typeof setTimeout> | null
  pongTimer: ReturnType<typeof setTimeout> | null
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

function unrefTimer(timer: ReturnType<typeof setTimeout>): ReturnType<typeof setTimeout> {
  if (typeof timer === "object" && timer !== null && "unref" in timer) {
    ;(timer as { unref?: () => void }).unref?.()
  }
  return timer
}

function clearHeartbeat(conn: PooledConnection) {
  clearTimer(conn.heartbeatTimer)
  clearTimer(conn.pongTimer)
  conn.heartbeatTimer = null
  conn.pongTimer = null
}

function detach(conn: PooledConnection) {
  if (conn.detach) {
    conn.detach()
    conn.detach = null
  }
  clearTimer(conn.connectTimer)
  clearTimer(conn.idleTimer)
  clearTimer(conn.retryTimer)
  clearHeartbeat(conn)
  conn.connectTimer = null
  conn.idleTimer = null
  conn.retryTimer = null
}

function remove(conn: PooledConnection) {
  const index = connectionPool.indexOf(conn)
  if (index >= 0) connectionPool.splice(index, 1)
  detach(conn)
}

function scheduleHeartbeat(conn: PooledConnection) {
  clearHeartbeat(conn)
  if (conn.busy || conn.pending) return
  const ws = conn.ws
  if (!ws || ws.readyState !== readyState.OPEN) return
  if (typeof ws.ping !== "function") return
  const generation = conn.generation
  conn.heartbeatTimer = unrefTimer(
    setTimeout(() => {
      conn.heartbeatTimer = null
      if (generation !== conn.generation || conn.busy || !conn.ws) return
      if (conn.ws.readyState !== readyState.OPEN) return
      try {
        conn.ws.ping?.()
      } catch {
        remove(conn)
        try {
          conn.ws?.terminate?.()
        } catch {}
        return
      }
      conn.pongTimer = unrefTimer(
        setTimeout(() => {
          conn.pongTimer = null
          if (generation !== conn.generation || conn.busy) return
          remove(conn)
          try {
            conn.ws?.terminate?.()
          } catch {}
          try {
            conn.ws?.close(1001, "pong timeout")
          } catch {}
        }, PONG_TIMEOUT_MS),
      )
    }, HEARTBEAT_INTERVAL_MS),
  )
}

function release(conn: PooledConnection) {
  conn.lastSessionID = conn.activeSessionID
  conn.lastAgent = conn.activeAgent
  conn.pending = null
  conn.activeSessionID = undefined
  conn.activeAgent = undefined
  conn.busy = false
  conn.reconnectAttempts = 0
  conn.idleTimer = unrefTimer(
    setTimeout(() => {
      conn.idleTimer = null
      if (!conn.busy) {
        remove(conn)
        try {
          conn.ws?.close(1000, "idle eviction")
        } catch {}
      }
    }, IDLE_EVICT_MS),
  )
  scheduleHeartbeat(conn)
}

function formatFailureMessage(conn: PooledConnection, reason: string): string {
  const code = conn.lastCloseCode !== undefined ? conn.lastCloseCode : "unknown"
  const closeReason = JSON.stringify(conn.lastCloseReason ?? "")
  const lastError = conn.lastErrorMessage ? JSON.stringify(conn.lastErrorMessage) : "none"
  return (
    `WebSocket closed before response completed; cannot retry because ${reason}; ` +
    `reconnectAttempts=${conn.reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}; ` +
    `closeCode=${code}; closeReason=${closeReason}; lastError=${lastError}`
  )
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
    } catch {}
    try {
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
  try {
    pending.controller.enqueue(encoded)
  } catch {
    return
  }
  pending.forwarded = true
  if (["response.completed", "response.failed", "response.incomplete", "error"].includes(eventType)) {
    pending.done = true
    try {
      pending.controller.close()
    } catch {}
    release(conn)
  }
}

export function sendPending(conn: PooledConnection): boolean {
  const pending = conn.pending
  if (!pending || pending.done || pending.sent) return false
  const ws = conn.ws
  if (!ws || ws.readyState !== readyState.OPEN) return false
  try {
    ws.send(JSON.stringify({ type: "response.create", ...pending.body }))
  } catch (error) {
    conn.lastErrorMessage = error instanceof Error ? error.message : String(error)
    queueMicrotask(() => handleSocketLoss(conn))
    return false
  }
  pending.sent = true
  return true
}

function normalizeCloseReason(reason: unknown): string | undefined {
  if (reason === undefined || reason === null) return undefined
  if (typeof reason === "string") return reason
  if (Buffer.isBuffer(reason)) return reason.toString("utf8")
  if (reason instanceof Uint8Array) return Buffer.from(reason).toString("utf8")
  return String(reason)
}

function connect(conn: PooledConnection) {
  clearHeartbeat(conn)
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
  conn.connectTimer = unrefTimer(connectTimer)

  const handleOpen = () => {
    if (generation !== conn.generation) return
    clearTimer(conn.connectTimer)
    conn.connectTimer = null
    conn.lastActivityAt = Date.now()
    sendPending(conn)
  }

  const handleMessage = (data: unknown) => {
    if (generation !== conn.generation) return
    conn.lastActivityAt = Date.now()
    for (const frame of parseFrames(data)) enqueueSSE(conn, frame)
  }

  const handlePong = () => {
    if (generation !== conn.generation) return
    conn.lastActivityAt = Date.now()
    clearTimer(conn.pongTimer)
    conn.pongTimer = null
    if (!conn.busy && !conn.pending) scheduleHeartbeat(conn)
  }

  const handleError = (error: unknown) => {
    if (generation !== conn.generation) return
    conn.lastErrorMessage =
      error instanceof Error ? error.message : error !== undefined && error !== null ? String(error) : undefined
    handleSocketLoss(conn)
  }

  const handleClose = (code?: number, reason?: unknown) => {
    if (generation !== conn.generation) return
    if (typeof code === "number") conn.lastCloseCode = code
    const normalizedReason = normalizeCloseReason(reason)
    if (normalizedReason !== undefined) conn.lastCloseReason = normalizedReason
    handleSocketLoss(conn)
  }

  on(ws, "open", handleOpen)
  on(ws, "message", handleMessage)
  on(ws, "pong", handlePong)
  on(ws, "error", handleError)
  on(ws, "close", handleClose)

  conn.detach = () => {
    off(ws, "open", handleOpen)
    off(ws, "message", handleMessage)
    off(ws, "pong", handlePong)
    off(ws, "error", handleError)
    off(ws, "close", handleClose)
  }
}

function handleSocketLoss(conn: PooledConnection) {
  clearTimer(conn.connectTimer)
  conn.connectTimer = null
  clearHeartbeat(conn)
  const pending = conn.pending
  if (!pending || pending.done) {
    remove(conn)
    try {
      conn.ws?.terminate?.()
    } catch {}
    return
  }
  const canRetry = !pending.forwarded && conn.reconnectAttempts < MAX_RECONNECT_ATTEMPTS
  if (canRetry) {
    conn.reconnectAttempts++
    pending.sent = false
    const priorWs = conn.ws
    detach(conn)
    try {
      priorWs?.terminate?.()
    } catch {}
    try {
      priorWs?.close(1000, "retrying")
    } catch {}
    conn.ws = null
    conn.retryTimer = unrefTimer(
      setTimeout(() => {
        conn.retryTimer = null
        if (!connectionPool.includes(conn) || !conn.pending || conn.pending.done || conn.pending.forwarded) return
        connect(conn)
      }, RECONNECT_DELAY_MS),
    )
    return
  }
  const reason = pending.forwarded
    ? "stream already forwarded data"
    : "retry limit reached before any frame was forwarded"
  fail(conn, new Error(formatFailureMessage(conn, reason)), true)
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
    lastActivityAt: Date.now(),
    idleTimer: null,
    connectTimer: null,
    retryTimer: null,
    heartbeatTimer: null,
    pongTimer: null,
    detach: null,
  }
  connectionPool.push(conn)
  connect(conn)
  return conn
}

function isReusable(conn: PooledConnection, key: string, context: TransportContext, now: number): boolean {
  if (conn.busy) return false
  if (conn.scopeKey !== key) return false
  if (conn.ws?.readyState !== readyState.OPEN) return false
  if (now - conn.lastActivityAt > STALE_REUSE_MS) return false
  if (context.sessionID && conn.lastSessionID && conn.lastSessionID !== context.sessionID) return false
  if (context.agent && conn.lastAgent && conn.lastAgent !== context.agent) return false
  return true
}

export function acquireConnection(
  wsUrl: string,
  headers: Record<string, string>,
  context: TransportContext,
): PooledConnection {
  const key = scopeKey(wsUrl, headers)
  const now = Date.now()
  for (const candidate of [...connectionPool]) {
    if (candidate.busy || candidate.scopeKey !== key) continue
    if (candidate.ws?.readyState === readyState.OPEN && now - candidate.lastActivityAt > STALE_REUSE_MS) {
      remove(candidate)
      try {
        candidate.ws?.close(1000, "stale idle")
      } catch {}
    }
  }
  const reusable = connectionPool.find((conn) => isReusable(conn, key, context, now))
  const conn = reusable ?? create(wsUrl, headers)
  clearTimer(conn.idleTimer)
  conn.idleTimer = null
  clearHeartbeat(conn)
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
