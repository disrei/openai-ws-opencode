import crypto from "node:crypto"
import WebSocket from "ws"
import { transportConfig } from "./config.js"
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

let WebSocketImpl: WebSocketConstructor = WebSocket as unknown as WebSocketConstructor

type PendingMetadata = {
  responseId?: string
  model?: string
  createdAt?: number
  serviceTier?: unknown
  usage?: unknown
}

export interface PendingRequest {
  body: Record<string, unknown>
  controller: ReadableStreamDefaultController<Uint8Array>
  done: boolean
  sent: boolean
  forwarded: boolean
  replayUnsafeForwarded: boolean
  frameCount: number
  lastEventType?: string
  lastSequenceNumber?: number
  metadata: PendingMetadata
}

export interface PooledConnection {
  id: string
  ws: WebSocketLike | null
  wsUrl: string
  headers: Record<string, string>
  scopeKey: string
  scopeHash: string
  busy: boolean
  warm: boolean
  pending: PendingRequest | null
  activeSessionID?: string
  lastSessionID?: string
  activeAgent?: string
  lastAgent?: string
  lastModelID?: string
  lastStablePrefixHash?: string
  lastResponseID?: string
  generation: number
  reconnectAttempts: number
  createdAt: number
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

type QueueEntry = {
  wsUrl: string
  headers: Record<string, string>
  context: TransportContext
  resolve: (conn: PooledConnection) => void
  reject: (error: Error) => void
  signal?: AbortSignal
  onAbort: () => void
}

const acquisitionQueues = new Map<string, QueueEntry[]>()
let nextConnectionID = 1

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

function authScopeHash(headers: Record<string, string>): string {
  const auth = Object.entries(headers)
    .filter(([key]) => ["authorization", "chatgpt-account-id", "originator", "openai-beta"].includes(key.toLowerCase()))
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}:${value}`)
    .join("\n")
  return crypto.createHash("sha256").update(auth).digest("hex")
}

function scopeKey(wsUrl: string, headers: Record<string, string>): string {
  return `${wsUrl}::${authScopeHash(headers)}`
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

function retryDelay(attempt: number): number {
  const exponential = Math.min(
    transportConfig.reconnectMaxDelayMs,
    transportConfig.reconnectBaseDelayMs * 2 ** Math.max(0, attempt - 1),
  )
  return exponential
}

function closeSocket(conn: PooledConnection, code: number, reason: string) {
  try {
    conn.ws?.terminate?.()
  } catch {}
  try {
    conn.ws?.close(code, reason)
  } catch {}
}

function finishConnection(conn: PooledConnection) {
  conn.pending = null
  conn.activeSessionID = undefined
  conn.activeAgent = undefined
  conn.busy = false
  drainQueue(conn.scopeKey)
}

function terminalRelease(conn: PooledConnection) {
  release(conn)
  drainQueue(conn.scopeKey)
}

function removeAndDrain(conn: PooledConnection) {
  remove(conn)
  drainQueue(conn.scopeKey)
}

function transportDiagnostics(conn: PooledConnection, pending?: PendingRequest) {
  return {
    message: "WebSocket closed before response completed",
    connectionId: conn.id,
    scopeHash: conn.scopeHash.slice(0, 12),
    sessionId: conn.activeSessionID,
    agent: conn.activeAgent,
    reconnectAttempts: conn.reconnectAttempts,
    closeCode: conn.lastCloseCode ?? null,
    closeReason: conn.lastCloseReason ?? "",
    lastError: conn.lastErrorMessage ?? null,
    frameCount: pending?.frameCount ?? 0,
    lastEventType: pending?.lastEventType,
  }
}

function scheduleHeartbeat(conn: PooledConnection) {
  clearHeartbeat(conn)
  const ws = conn.ws
  if (!ws || ws.readyState !== readyState.OPEN) return
  if (typeof ws.ping !== "function") return
  const generation = conn.generation
  conn.heartbeatTimer = unrefTimer(
    setTimeout(() => {
      conn.heartbeatTimer = null
      if (generation !== conn.generation || !conn.ws) return
      if (conn.ws.readyState !== readyState.OPEN) return
      try {
        conn.ws.ping?.()
      } catch {
        handleSocketLoss(conn)
        return
      }
      conn.pongTimer = unrefTimer(
        setTimeout(() => {
          conn.pongTimer = null
          if (generation !== conn.generation) return
          conn.lastCloseCode = 1006
          conn.lastCloseReason = "pong timeout"
          handleSocketLoss(conn)
        }, transportConfig.pongTimeoutMs),
      )
    }, transportConfig.heartbeatIntervalMs),
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
  clearTimer(conn.idleTimer)
  conn.idleTimer = null
  if (!conn.warm) {
    conn.idleTimer = unrefTimer(
      setTimeout(() => {
        conn.idleTimer = null
        if (!conn.busy) {
          removeAndDrain(conn)
          try {
            conn.ws?.close(1000, "idle eviction")
          } catch {}
        }
      }, transportConfig.idleEvictMs),
    )
  }
  scheduleHeartbeat(conn)
}

function formatFailureMessage(conn: PooledConnection, reason: string): string {
  const code = conn.lastCloseCode !== undefined ? conn.lastCloseCode : "unknown"
  const closeReason = JSON.stringify(conn.lastCloseReason ?? "")
  const lastError = conn.lastErrorMessage ? JSON.stringify(conn.lastErrorMessage) : "none"
  return (
    `WebSocket closed before response completed; cannot retry because ${reason}; ` +
    `reconnectAttempts=${conn.reconnectAttempts}/${transportConfig.maxReconnectAttempts}; ` +
    `closeCode=${code}; closeReason=${closeReason}; lastError=${lastError}`
  )
}

function fail(conn: PooledConnection, error: Error, shouldCloseSocket: boolean) {
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
  if (shouldCloseSocket) {
    removeAndDrain(conn)
    closeSocket(conn, 1000, "aborted")
  } else {
    drainQueue(conn.scopeKey)
  }
}

function cacheResponseMetadata(pending: PendingRequest, frame: Record<string, unknown>) {
  const sequenceNumber = frame.sequence_number
  if (typeof sequenceNumber === "number") pending.lastSequenceNumber = sequenceNumber
  const response = frame.response
  if (response && typeof response === "object") {
    const value = response as Record<string, unknown>
    if (typeof value.id === "string") pending.metadata.responseId = value.id
    if (typeof value.model === "string") pending.metadata.model = value.model
    if (typeof value.created_at === "number") pending.metadata.createdAt = value.created_at
    if ("service_tier" in value) pending.metadata.serviceTier = value.service_tier
    if ("usage" in value) pending.metadata.usage = value.usage
  }
  if (typeof frame.response_id === "string") pending.metadata.responseId = frame.response_id
}

function isReplayUnsafeFrame(frame: Record<string, unknown>): boolean {
  const eventType = typeof frame.type === "string" ? frame.type : "message"
  if (["response.created", "response.in_progress"].includes(eventType)) return false
  if (["response.completed", "response.failed", "response.incomplete", "error"].includes(eventType)) return true
  if (eventType.includes(".delta")) return true
  if (eventType.includes("output_item") || eventType.includes("content_part")) return true
  if (eventType.includes("function_call") || eventType.includes("tool")) return true
  if (eventType.includes("reasoning") || eventType.includes("annotation")) return true
  return eventType !== "message" || Object.keys(frame).some((key) => ["delta", "output", "item", "content", "arguments"].includes(key))
}

function fallbackUsage() {
  return {
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    input_tokens_details: { cached_tokens: 0 },
    output_tokens_details: { reasoning_tokens: 0 },
  }
}

function syntheticIncompleteFrame(conn: PooledConnection, pending: PendingRequest): Record<string, unknown> {
  const metadata = pending.metadata
  return {
    type: "response.incomplete",
    ...(pending.lastSequenceNumber !== undefined ? { sequence_number: pending.lastSequenceNumber + 1 } : {}),
    response: {
      id: metadata.responseId ?? "resp_unknown",
      object: "response",
      status: "incomplete",
      ...(metadata.model ? { model: metadata.model } : {}),
      ...(metadata.createdAt !== undefined ? { created_at: metadata.createdAt } : {}),
      incomplete_details: { reason: "transport_error" },
      usage: metadata.usage ?? fallbackUsage(),
      service_tier: metadata.serviceTier ?? null,
    },
    openai_ws_transport_error: transportDiagnostics(conn, pending),
  }
}

function synthesizeIncomplete(conn: PooledConnection) {
  const pending = conn.pending
  if (!pending || pending.done) return
  const frame = syntheticIncompleteFrame(conn, pending)
  const encoded = new TextEncoder().encode(`event: response.incomplete\ndata: ${JSON.stringify(frame)}\n\n`)
  try {
    pending.controller.enqueue(encoded)
  } catch {}
  pending.done = true
  try {
    pending.controller.close()
  } catch {}
  finishConnection(conn)
  removeAndDrain(conn)
  closeSocket(conn, 1000, "transport incomplete")
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
  cacheResponseMetadata(pending, frame)
  const encoded = new TextEncoder().encode(`event: ${eventType}\ndata: ${JSON.stringify(frame)}\n\n`)
  try {
    pending.controller.enqueue(encoded)
  } catch {
    return
  }
  pending.forwarded = true
  pending.frameCount++
  pending.lastEventType = eventType
  if (isReplayUnsafeFrame(frame)) pending.replayUnsafeForwarded = true
  if (["response.completed", "response.failed", "response.incomplete", "error"].includes(eventType)) {
    pending.done = true
    try {
      pending.controller.close()
    } catch {}
    if (pending.metadata.responseId) conn.lastResponseID = pending.metadata.responseId
    terminalRelease(conn)
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
    conn.lastCloseCode = 1006
    conn.lastCloseReason = "connection timed out"
    handleSocketLoss(conn)
  }, transportConfig.connectTimeoutMs)
  conn.connectTimer = unrefTimer(connectTimer)

  const handleOpen = () => {
    if (generation !== conn.generation) return
    clearTimer(conn.connectTimer)
    conn.connectTimer = null
    conn.lastActivityAt = Date.now()
    scheduleHeartbeat(conn)
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
    scheduleHeartbeat(conn)
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
    removeAndDrain(conn)
    closeSocket(conn, 1000, "socket lost")
    return
  }
  if (pending.replayUnsafeForwarded) {
    synthesizeIncomplete(conn)
    return
  }
  const canRetry = conn.reconnectAttempts < transportConfig.maxReconnectAttempts
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
        if (!connectionPool.includes(conn) || !conn.pending || conn.pending.done || conn.pending.replayUnsafeForwarded) return
        connect(conn)
      }, retryDelay(conn.reconnectAttempts)),
    )
    return
  }
  const reason = pending.forwarded
    ? "retry limit reached after replay-safe frames only"
    : "retry limit reached before any frame was forwarded"
  fail(conn, new Error(formatFailureMessage(conn, reason)), true)
}

function create(wsUrl: string, headers: Record<string, string>, warm = false): PooledConnection {
  const hash = authScopeHash(headers)
  const conn: PooledConnection = {
    id: `ws-${nextConnectionID++}`,
    ws: null,
    wsUrl,
    headers,
    scopeKey: `${wsUrl}::${hash}`,
    scopeHash: hash,
    busy: false,
    warm,
    pending: null,
    generation: 0,
    reconnectAttempts: 0,
    createdAt: Date.now(),
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
  if (conn.ws?.readyState !== readyState.OPEN && conn.ws?.readyState !== readyState.CONNECTING) return false
  if (now - conn.lastActivityAt > transportConfig.staleReuseMs) return false
  if (now - conn.createdAt > transportConfig.connectionMaxAgeMs) return false
  if (context.sessionID && conn.lastSessionID && conn.lastSessionID !== context.sessionID) return false
  if (context.agent && conn.lastAgent && conn.lastAgent !== context.agent) return false
  return true
}

function activeCount(key: string): number {
  return connectionPool.filter((conn) => conn.scopeKey === key && (conn.busy || conn.ws?.readyState === readyState.CONNECTING)).length
}

function cleanupStaleConnections(key: string, now: number) {
  for (const candidate of [...connectionPool]) {
    if (candidate.busy || candidate.scopeKey !== key) continue
    if (
      candidate.ws?.readyState === readyState.OPEN &&
      (now - candidate.lastActivityAt > transportConfig.staleReuseMs ||
        now - candidate.createdAt > transportConfig.connectionMaxAgeMs)
    ) {
      remove(candidate)
      try {
        candidate.ws?.close(1000, "stale idle")
      } catch {}
    }
  }
}

function reserveConnection(wsUrl: string, headers: Record<string, string>, context: TransportContext): PooledConnection | null {
  const key = scopeKey(wsUrl, headers)
  const now = Date.now()
  cleanupStaleConnections(key, now)
  const reusable = connectionPool.find((conn) => isReusable(conn, key, context, now))
  if (reusable) {
    clearTimer(reusable.idleTimer)
    reusable.idleTimer = null
    clearHeartbeat(reusable)
    reusable.busy = true
    reusable.warm = false
    return reusable
  }
  if (activeCount(key) >= transportConfig.maxConnectionsPerScope) return null
  const conn = create(wsUrl, headers)
  conn.busy = true
  return conn
}

function queueAcquire(
  wsUrl: string,
  headers: Record<string, string>,
  context: TransportContext,
  signal?: AbortSignal,
): Promise<PooledConnection> {
  const key = scopeKey(wsUrl, headers)
  return new Promise((resolve, reject) => {
    const entry: QueueEntry = {
      wsUrl,
      headers,
      context,
      resolve,
      reject,
      signal,
      onAbort: () => {
        const queue = acquisitionQueues.get(key)
        if (queue) {
          const index = queue.indexOf(entry)
          if (index >= 0) queue.splice(index, 1)
          if (queue.length === 0) acquisitionQueues.delete(key)
        }
        reject(signal?.reason instanceof Error ? signal.reason : new DOMException("Aborted", "AbortError"))
      },
    }
    if (signal?.aborted) {
      entry.onAbort()
      return
    }
    signal?.addEventListener("abort", entry.onAbort, { once: true })
    const queue = acquisitionQueues.get(key) ?? []
    queue.push(entry)
    acquisitionQueues.set(key, queue)
  })
}

function drainQueue(key: string) {
  const queue = acquisitionQueues.get(key)
  if (!queue?.length) return
  while (queue.length) {
    const entry = queue[0]
    if (entry.signal?.aborted) {
      queue.shift()
      entry.onAbort()
      continue
    }
    const conn = reserveConnection(entry.wsUrl, entry.headers, entry.context)
    if (!conn) break
    queue.shift()
    entry.signal?.removeEventListener("abort", entry.onAbort)
    entry.resolve(conn)
  }
  if (queue.length === 0) acquisitionQueues.delete(key)
}

export function acquireConnection(
  wsUrl: string,
  headers: Record<string, string>,
  context: TransportContext,
  signal?: AbortSignal,
): PooledConnection | Promise<PooledConnection> {
  const conn = reserveConnection(wsUrl, headers, context)
  if (conn) return conn
  return queueAcquire(wsUrl, headers, context, signal)
}

export function ensureWarmConnection(wsUrl: string, headers: Record<string, string>) {
  const key = scopeKey(wsUrl, headers)
  const now = Date.now()
  cleanupStaleConnections(key, now)
  const existing = connectionPool.find((conn) => conn.scopeKey === key && !conn.busy && conn.ws?.readyState === readyState.OPEN)
  if (existing) {
    existing.warm = true
    clearTimer(existing.idleTimer)
    existing.idleTimer = null
    scheduleHeartbeat(existing)
    return existing
  }
  if (activeCount(key) >= transportConfig.maxConnectionsPerScope) return undefined
  return create(wsUrl, headers, true)
}

export function closeConnections(predicate: (conn: PooledConnection) => boolean = () => true, message = "Session disposed") {
  for (const conn of [...connectionPool]) {
    if (!predicate(conn)) continue
    fail(conn, new Error(message), true)
  }
}

export function resetPoolForTesting() {
  for (const conn of [...connectionPool]) {
    try {
      conn.ws?.close(1000, "test reset")
    } catch {}
    remove(conn)
  }
  for (const queue of acquisitionQueues.values()) {
    for (const entry of queue) {
      entry.signal?.removeEventListener("abort", entry.onAbort)
      entry.reject(new Error("Pool reset"))
    }
  }
  acquisitionQueues.clear()
}

export function setWebSocketConstructorForTesting(ctor: WebSocketConstructor) {
  WebSocketImpl = ctor
}

export function resetWebSocketConstructorForTesting() {
  WebSocketImpl = WebSocket as unknown as WebSocketConstructor
}
