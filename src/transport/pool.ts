import crypto from "node:crypto"
import {
  CODEX_WS_URL,
  OPENAI_MODEL_HEADER,
  OPENAI_WS_INSTALLATION_ID_ENV,
  RESPONSE_PROCESSED_DISABLE_ENV,
  RESPONSE_PROCESSED_ENV,
  X_CODEX_INSTALLATION_ID_HEADER,
  X_CODEX_TURN_STATE_HEADER,
  X_CODEX_WINDOW_ID_HEADER,
  X_MODELS_ETAG_HEADER,
  X_OPENAI_SUBAGENT_HEADER,
  X_REASONING_INCLUDED_HEADER,
} from "../constants.js"
import { loadDefaultWebSocketConstructor, type WebSocketConstructor, type WebSocketLike } from "./bun-websocket.js"
import { transportConfig } from "./config.js"
import type { TransportContext } from "./headers.js"

let WebSocketImpl: WebSocketConstructor = loadDefaultWebSocketConstructor()

type PendingMetadata = {
  responseId?: string
}

export interface PendingRequest {
  body: Record<string, unknown>
  controller: ReadableStreamDefaultController<Uint8Array>
  onFinalize?: () => void
  onError?: (error: Error, pending: PendingRequest) => boolean
  done: boolean
  sent: boolean
  writeCommitted: boolean
  framesReceived: boolean
  finalMessageOutputReceived: boolean
  processedAckSent: boolean
  explicitPreviousResponseID: boolean
  previousResponseNotFoundRetried: boolean
  idleTimer: ReturnType<typeof setTimeout> | null
  metadata: PendingMetadata
}

export class WebSocketPreStreamTransportError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WebSocketPreStreamTransportError"
  }
}

export function isWebSocketPreStreamTransportError(error: unknown): error is WebSocketPreStreamTransportError {
  return error instanceof WebSocketPreStreamTransportError || (error instanceof Error && error.name === "WebSocketPreStreamTransportError")
}

export interface PooledConnection {
  id: string
  ws: WebSocketLike | null
  wsUrl: string
  headers: Record<string, string>
  scopeKey: string
  scopeHash: string
  contextKey: string
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
  turnState?: string
  serverModel?: string
  modelsEtag?: string
  serverReasoningIncluded?: boolean
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
const turnStateByContext = new Map<string, string>()
const lastResponseIDByContext = new Map<string, string>()
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

function contextScopeKey(wsUrl: string, headers: Record<string, string>, context: TransportContext): string {
  return `${scopeKey(wsUrl, headers)}::session:${context.sessionID ?? ""}::agent:${context.agent ?? ""}`
}

function headersForConnection(
  wsUrl: string,
  baseHeaders: Record<string, string>,
  context: TransportContext,
  key: string,
): Record<string, string> {
  const headers = { ...baseHeaders }
  if (context.sessionID) {
    headers.session_id = context.sessionID
    headers["session-id"] = context.sessionID
    headers.thread_id = context.sessionID
    headers["thread-id"] = context.sessionID
    headers["x-client-request-id"] = crypto.createHash("sha256").update(`${wsUrl}:${context.sessionID}`).digest("hex").slice(0, 32)
    headers[X_CODEX_WINDOW_ID_HEADER] = context.sessionID
  }
  if (context.agent && context.agent !== "primary") headers[X_OPENAI_SUBAGENT_HEADER] = context.agent
  const installationID = process.env[OPENAI_WS_INSTALLATION_ID_ENV]
  if (installationID) headers[X_CODEX_INSTALLATION_ID_HEADER] = installationID
  const turnState = turnStateByContext.get(key)
  if (turnState) headers[X_CODEX_TURN_STATE_HEADER] = turnState
  return headers
}

function headerValue(source: unknown, name: string): string | undefined {
  const headers = (source as { headers?: unknown } | undefined)?.headers
  if (!headers) return undefined
  if (headers instanceof Headers) return headers.get(name) ?? undefined
  if (typeof (headers as { get?: unknown }).get === "function") {
    const value = (headers as { get: (key: string) => unknown }).get(name)
    return Array.isArray(value) ? String(value[0]) : value === undefined || value === null ? undefined : String(value)
  }
  const record = headers as Record<string, unknown>
  const value = record[name] ?? record[name.toLowerCase()]
  return Array.isArray(value) ? String(value[0]) : value === undefined || value === null ? undefined : String(value)
}

function supportsUpgradeEvent(constructor: WebSocketConstructor): boolean {
  return process.versions.bun === undefined || constructor.name !== "WebSocket"
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

function clearPendingTimer(pending: PendingRequest | null | undefined) {
  if (!pending) return
  clearTimer(pending.idleTimer)
  pending.idleTimer = null
}

function finalizePending(pending: PendingRequest | null | undefined) {
  if (!pending) return
  clearPendingTimer(pending)
  pending.onFinalize?.()
  pending.onFinalize = undefined
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

function terminalRelease(conn: PooledConnection) {
  release(conn)
  drainQueue(conn.scopeKey)
}

function removeAndDrain(conn: PooledConnection) {
  remove(conn)
  drainQueue(conn.scopeKey)
}

function release(conn: PooledConnection) {
  conn.lastSessionID = conn.activeSessionID
  conn.lastAgent = conn.activeAgent
  finalizePending(conn.pending)
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
}

function closeFailureSummary(conn: PooledConnection): string | undefined {
  const reason = conn.lastCloseReason ?? ""
  if (conn.lastCloseCode === 1008) {
    if (reason.includes("usage_limit_reached")) return `OpenAI WebSocket error 429 usage_limit_reached${reason ? `: ${reason}` : ""}`
    return `OpenAI WebSocket policy close 1008${reason ? `: ${reason}` : ""}`
  }
  return undefined
}

function formatFailureMessage(conn: PooledConnection, reason: string): string {
  const code = conn.lastCloseCode !== undefined ? conn.lastCloseCode : "unknown"
  const closeReason = JSON.stringify(conn.lastCloseReason ?? "")
  const lastError = conn.lastErrorMessage ? JSON.stringify(conn.lastErrorMessage) : "none"
  const closeSummary = closeFailureSummary(conn)
  const pending = conn.pending
  const responseCreateState = pending
    ? `; responseCreateSent=${pending.sent}; responseCreateWriteCommitted=${pending.writeCommitted}; responseFramesReceived=${pending.framesReceived}`
    : ""
  return (
    `${closeSummary ? `${closeSummary}; ` : ""}WebSocket closed before response completed; cannot retry because ${reason}; ` +
    `reconnectAttempts=${conn.reconnectAttempts}/${transportConfig.maxReconnectAttempts}; ` +
    `closeCode=${code}; closeReason=${closeReason}; lastError=${lastError}${responseCreateState}`
  )
}

function fail(conn: PooledConnection, error: Error, shouldCloseSocket: boolean) {
  const pending = conn.pending
  if (pending && !pending.done) {
    const handled = pending.onError?.(error, pending) ?? false
    pending.done = true
    if (handled) {
      clearPendingTimer(pending)
      pending.onFinalize = undefined
      pending.onError = undefined
    } else {
      finalizePending(pending)
      try {
        pending.controller.error(error)
      } catch {}
    }
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
  const response = frame.response
  if (response && typeof response === "object") {
    const value = response as Record<string, unknown>
    if (typeof value.id === "string") pending.metadata.responseId = value.id
  }
  if (typeof frame.response_id === "string") pending.metadata.responseId = frame.response_id
}

function isTerminalEvent(eventType: string): boolean {
  return ["response.completed", "response.failed", "response.incomplete"].includes(eventType)
}

function isFinalMessageOutputItem(frame: Record<string, unknown>): boolean {
  if (frame.type !== "response.output_item.done") return false
  const item = frame.item
  if (!item || typeof item !== "object" || Array.isArray(item)) return false
  const value = item as Record<string, unknown>
  if (value.type !== "message") return false
  return value.status === undefined || value.status === "completed"
}

function schedulePendingIdleTimeout(conn: PooledConnection, reason: string) {
  const pending = conn.pending
  if (!pending || pending.done) return
  clearPendingTimer(pending)
  const generation = conn.generation
  pending.idleTimer = unrefTimer(
    setTimeout(() => {
      if (generation !== conn.generation) return
      const current = conn.pending
      if (!current || current !== pending || current.done) return
      current.idleTimer = null
      conn.lastCloseCode = 1006
      conn.lastCloseReason = "idle timeout waiting for websocket"
      fail(conn, new Error(formatFailureMessage(conn, `idle timeout waiting for websocket after ${reason}`)), true)
    }, transportConfig.streamIdleTimeoutMs),
  )
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

function numberFrom(value: unknown): number | undefined {
  if (typeof value === "number") return value
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) return parsed
  }
  return undefined
}

function messageFromError(error: unknown): string | undefined {
  if (error instanceof Error) return error.message
  if (error && typeof error === "object" && "message" in error) {
    const message = (error as { message?: unknown }).message
    if (typeof message === "string" && message) return message
  }
  return error !== undefined && error !== null ? String(error) : undefined
}

function wrappedWebSocketError(frame: Record<string, unknown>): Error | undefined {
  if (frame.type !== "error") return undefined
  const error = frame.error && typeof frame.error === "object" ? (frame.error as Record<string, unknown>) : {}
  const code = typeof error.code === "string" ? error.code : typeof frame.code === "string" ? frame.code : undefined
  const message =
    typeof error.message === "string" ? error.message : typeof frame.message === "string" ? frame.message : code
  if (code === "websocket_connection_limit_reached") {
    return new Error(message ?? "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue.")
  }
  const status =
    numberFrom(frame.status) ??
    numberFrom(frame.status_code) ??
    numberFrom(error.status) ??
    numberFrom(error.status_code) ??
    (code === "usage_limit_reached" ? 429 : undefined)
  if (status !== undefined && status >= 200 && status < 300) return undefined
  return new Error(
    `OpenAI WebSocket error${status !== undefined ? ` ${status}` : ""}${code ? ` ${code}` : ""}${message ? `: ${message}` : ""}`,
  )
}

function isCodexConnection(conn: PooledConnection): boolean {
  return conn.wsUrl === CODEX_WS_URL || conn.wsUrl.includes("chatgpt.com/backend-api/codex")
}

function shouldSendResponseProcessed(conn: PooledConnection): boolean {
  if (process.env[RESPONSE_PROCESSED_DISABLE_ENV] === "1") return false
  if (process.env[RESPONSE_PROCESSED_ENV] === "1") return true
  return isCodexConnection(conn)
}

function sendResponseProcessed(conn: PooledConnection, pending: PendingRequest, eventType: string) {
  if (eventType !== "response.completed") return
  if (!shouldSendResponseProcessed(conn)) return
  if (pending.processedAckSent || !pending.metadata.responseId) return
  try {
    conn.ws?.send(JSON.stringify({ type: "response.processed", response_id: pending.metadata.responseId }))
    pending.processedAckSent = true
  } catch (error) {
    conn.lastErrorMessage = error instanceof Error ? error.message : String(error)
  }
}

function websocketErrorCode(frame: Record<string, unknown>): string | undefined {
  const error = frame.error && typeof frame.error === "object" ? (frame.error as Record<string, unknown>) : {}
  return typeof error.code === "string" ? error.code : typeof frame.code === "string" ? frame.code : undefined
}

function hasReplaySafeInput(body: Record<string, unknown>): boolean {
  const input = body.input
  if (typeof input === "string") return input.length > 0
  if (!Array.isArray(input)) return false
  return input.length > 0 && input.every((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return false
    const value = item as Record<string, unknown>
    return value.type === "message"
  })
}

function clearLastResponseID(conn: PooledConnection) {
  conn.lastResponseID = undefined
  lastResponseIDByContext.delete(conn.contextKey)
}

function handlePreviousResponseNotFound(conn: PooledConnection, pending: PendingRequest): boolean {
  clearLastResponseID(conn)
  pending.metadata.responseId = undefined
  if (!pending.explicitPreviousResponseID) {
    fail(conn, new Error("OpenAI WebSocket previous_response_not_found for a request without explicit previous_response_id"), true)
    return true
  }
  if (pending.previousResponseNotFoundRetried) {
    fail(conn, new Error("OpenAI WebSocket previous_response_not_found after retrying without previous_response_id"), true)
    return true
  }
  if (!hasReplaySafeInput(pending.body)) {
    fail(
      conn,
      new Error(
        "OpenAI WebSocket previous_response_not_found; cannot safely retry without previous_response_id because request input does not contain replayable message context",
      ),
      true,
    )
    return true
  }
  pending.previousResponseNotFoundRetried = true
  pending.body.previous_response_id = null
  pending.sent = false
  pending.writeCommitted = false
  pending.framesReceived = false
  pending.finalMessageOutputReceived = false
  pending.processedAckSent = false
  clearPendingTimer(pending)
  if (conn.ws?.readyState !== readyState.OPEN || !sendPending(conn)) {
    fail(conn, new Error("OpenAI WebSocket previous_response_not_found; retry without previous_response_id could not be sent"), true)
  }
  return true
}

function enqueueSSE(conn: PooledConnection, frame: Record<string, unknown>) {
  const pending = conn.pending
  if (!pending || pending.done) return
  pending.writeCommitted = true
  pending.framesReceived = true
  const eventType = typeof frame.type === "string" ? frame.type : "message"
  cacheResponseMetadata(pending, frame)
  if (isFinalMessageOutputItem(frame)) pending.finalMessageOutputReceived = true
  const mappedError = wrappedWebSocketError(frame)
  if (mappedError) {
    if (websocketErrorCode(frame) === "previous_response_not_found" && handlePreviousResponseNotFound(conn, pending)) return
    clearLastResponseID(conn)
    fail(conn, mappedError, true)
    return
  }
  const encoded = new TextEncoder().encode(`event: ${eventType}\ndata: ${JSON.stringify(frame)}\n\n`)
  try {
    pending.controller.enqueue(encoded)
  } catch {
    return
  }
  if (!isTerminalEvent(eventType)) schedulePendingIdleTimeout(conn, eventType)
  if (isTerminalEvent(eventType)) {
    finalizePending(pending)
    pending.done = true
    sendResponseProcessed(conn, pending, eventType)
    try {
      pending.controller.close()
    } catch {}
    if (eventType === "response.completed" && pending.metadata.responseId) {
      conn.lastResponseID = pending.metadata.responseId
      lastResponseIDByContext.set(conn.contextKey, pending.metadata.responseId)
    } else {
      clearLastResponseID(conn)
      pending.metadata.responseId = undefined
    }
    terminalRelease(conn)
  }
}

function finishAfterFinalOutputSocketClose(conn: PooledConnection, pending: PendingRequest) {
  finalizePending(pending)
  pending.done = true
  try {
    pending.controller.close()
  } catch {}
  clearLastResponseID(conn)
  conn.pending = null
  conn.busy = false
  conn.activeSessionID = undefined
  conn.activeAgent = undefined
  removeAndDrain(conn)
}

export function sendPending(conn: PooledConnection): boolean {
  const pending = conn.pending
  if (!pending || pending.done || pending.sent) return false
  const ws = conn.ws
  if (!ws || ws.readyState !== readyState.OPEN) return false
  const generation = conn.generation
  try {
    const body = { ...pending.body }
    const payload = JSON.stringify({ ...body, type: "response.create" })
    pending.sent = true
    ws.send(payload, (error?: Error) => {
      if (generation !== conn.generation || conn.pending !== pending || pending.done) return
      if (error) {
        pending.sent = false
        pending.writeCommitted = false
        conn.lastErrorMessage = messageFromError(error) ?? "websocket send failed"
        queueMicrotask(() => handleSocketLoss(conn))
        return
      }
      pending.writeCommitted = true
      schedulePendingIdleTimeout(conn, "response.create")
    })
  } catch (error) {
    pending.sent = false
    pending.writeCommitted = false
    conn.lastErrorMessage = messageFromError(error) ?? "websocket send failed"
    queueMicrotask(() => handleSocketLoss(conn))
    return false
  }
  return true
}

function normalizeCloseReason(reason: unknown): string | undefined {
  if (reason === undefined || reason === null) return undefined
  if (typeof reason === "string") return reason
  if (Buffer.isBuffer(reason)) return reason.toString("utf8")
  if (reason instanceof Uint8Array) return Buffer.from(reason).toString("utf8")
  return String(reason)
}

function closeCodeFrom(value: unknown): number | undefined {
  if (typeof value === "number") return value
  if (value && typeof value === "object") {
    const code = (value as { code?: unknown }).code
    if (typeof code === "number") return code
  }
  return undefined
}

function closeReasonFrom(codeOrEvent: unknown, reason: unknown): string | undefined {
  if (reason !== undefined) return normalizeCloseReason(reason)
  if (codeOrEvent && typeof codeOrEvent === "object" && "reason" in codeOrEvent) {
    return normalizeCloseReason((codeOrEvent as { reason?: unknown }).reason)
  }
  return undefined
}

function connect(conn: PooledConnection) {
  const generation = ++conn.generation
  clearTimer(conn.retryTimer)
  conn.retryTimer = null
  clearTimer(conn.connectTimer)
  const connectTimer = setTimeout(() => {
    if (generation !== conn.generation || conn.ws?.readyState === readyState.OPEN) return
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
    sendPending(conn)
  }

  const handleUpgrade = (response: unknown) => {
    if (generation !== conn.generation) return
    const turnState = headerValue(response, X_CODEX_TURN_STATE_HEADER)
    if (turnState) {
      conn.turnState = turnState
      turnStateByContext.set(conn.contextKey, turnState)
    }
    conn.serverReasoningIncluded = headerValue(response, X_REASONING_INCLUDED_HEADER) !== undefined
    conn.modelsEtag = headerValue(response, X_MODELS_ETAG_HEADER)
    conn.serverModel = headerValue(response, OPENAI_MODEL_HEADER)
  }

  const handleMessage = (data: unknown, isBinary?: boolean) => {
    if (generation !== conn.generation) return
    conn.lastActivityAt = Date.now()
    if (isBinary) {
      fail(conn, new Error("unexpected binary websocket event"), true)
      return
    }
    for (const frame of parseFrames(data)) enqueueSSE(conn, frame)
  }

  const handleError = (error: unknown) => {
    if (generation !== conn.generation) return
    conn.lastErrorMessage = messageFromError(error) ?? "unknown websocket error"
    handleSocketLoss(conn)
  }

  const handleClose = (codeOrEvent?: unknown, reason?: unknown) => {
    if (generation !== conn.generation) return
    conn.lastCloseCode = closeCodeFrom(codeOrEvent) ?? conn.lastCloseCode ?? 1006
    const normalizedReason = closeReasonFrom(codeOrEvent, reason)
    if (normalizedReason !== undefined) conn.lastCloseReason = normalizedReason
    else if (conn.lastCloseReason === undefined && closeCodeFrom(codeOrEvent) === undefined) conn.lastCloseReason = "socket closed without close code"
    handleSocketLoss(conn)
  }

  const handleUnexpectedResponse = (_request: unknown, response: unknown) => {
    if (generation !== conn.generation) return
    const status = numberFrom((response as { statusCode?: unknown } | undefined)?.statusCode)
    if (status !== undefined) conn.lastCloseCode = status
    conn.lastCloseReason = `websocket handshake failed${status !== undefined ? ` with status ${status}` : ""}`
    handleSocketLoss(conn)
  }

  const attachUpgrade = supportsUpgradeEvent(WebSocketImpl)
  let ws: WebSocketLike
  try {
    ws = new WebSocketImpl(conn.wsUrl, {
      headers: conn.headers,
      perMessageDeflate: true,
      finishRequest(request) {
        if (attachUpgrade) request.on?.("upgrade", handleUpgrade)
        request.end?.()
      },
    })
  } catch (error) {
    conn.lastErrorMessage = messageFromError(error) ?? "websocket constructor failed"
    queueMicrotask(() => handleSocketLoss(conn))
    return
  }
  conn.ws = ws
  on(ws, "open", handleOpen)
  if (attachUpgrade) on(ws, "upgrade", handleUpgrade)
  on(ws, "message", handleMessage)
  on(ws, "error", handleError)
  on(ws, "close", handleClose)
  if (attachUpgrade) on(ws, "unexpected-response", handleUnexpectedResponse)

  conn.detach = () => {
    off(ws, "open", handleOpen)
    if (attachUpgrade) off(ws, "upgrade", handleUpgrade)
    off(ws, "message", handleMessage)
    off(ws, "error", handleError)
    off(ws, "close", handleClose)
    if (attachUpgrade) off(ws, "unexpected-response", handleUnexpectedResponse)
  }
  if (ws.readyState === readyState.OPEN) queueMicrotask(handleOpen)
}

function handleSocketLoss(conn: PooledConnection) {
  clearTimer(conn.connectTimer)
  conn.connectTimer = null
  const pending = conn.pending
  if (!pending || pending.done) {
    finalizePending(pending)
    removeAndDrain(conn)
    closeSocket(conn, 1000, "socket lost")
    return
  }
  if (pending.finalMessageOutputReceived) {
    finishAfterFinalOutputSocketClose(conn, pending)
    return
  }
  if (pending.writeCommitted) {
    fail(conn, new Error(formatFailureMessage(conn, "response.create was already sent")), true)
    return
  }
  const canRetry = conn.reconnectAttempts < transportConfig.maxReconnectAttempts
  if (canRetry) {
    conn.reconnectAttempts++
    conn.generation++
    pending.sent = false
    pending.writeCommitted = false
    clearPendingTimer(pending)
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
        if (!connectionPool.includes(conn) || !conn.pending || conn.pending.done || conn.pending.sent || conn.pending.writeCommitted) return
        connect(conn)
      }, retryDelay(conn.reconnectAttempts)),
    )
    return
  }
  fail(conn, new WebSocketPreStreamTransportError(formatFailureMessage(conn, "retry limit reached before response.create was sent")), true)
}

function create(wsUrl: string, headers: Record<string, string>, context: TransportContext = {}, warm = false): PooledConnection {
  const hash = authScopeHash(headers)
  const key = contextScopeKey(wsUrl, headers, context)
  const conn: PooledConnection = {
    id: `ws-${nextConnectionID++}`,
    ws: null,
    wsUrl,
    headers: headersForConnection(wsUrl, headers, context, key),
    scopeKey: key,
    scopeHash: hash,
    contextKey: key,
    busy: false,
    warm,
    pending: null,
    lastResponseID: lastResponseIDByContext.get(key),
    generation: 0,
    reconnectAttempts: 0,
    createdAt: Date.now(),
    lastActivityAt: Date.now(),
    idleTimer: null,
    connectTimer: null,
    retryTimer: null,
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
  const key = contextScopeKey(wsUrl, headers, context)
  const now = Date.now()
  cleanupStaleConnections(key, now)
  const reusable = connectionPool.find((conn) => isReusable(conn, key, context, now))
  if (reusable) {
    clearTimer(reusable.idleTimer)
    reusable.idleTimer = null
    reusable.busy = true
    reusable.warm = false
    return reusable
  }
  if (activeCount(key) >= transportConfig.maxConnectionsPerScope) return null
  const conn = create(wsUrl, headers, context)
  conn.busy = true
  return conn
}

function queueAcquire(
  wsUrl: string,
  headers: Record<string, string>,
  context: TransportContext,
  signal?: AbortSignal,
): Promise<PooledConnection> {
  const key = contextScopeKey(wsUrl, headers, context)
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
  const key = contextScopeKey(wsUrl, headers, {})
  const now = Date.now()
  cleanupStaleConnections(key, now)
  const existing = connectionPool.find((conn) => conn.scopeKey === key && !conn.busy && conn.ws?.readyState === readyState.OPEN)
  if (existing) {
    existing.warm = true
    clearTimer(existing.idleTimer)
    existing.idleTimer = null
    return existing
  }
  if (activeCount(key) >= transportConfig.maxConnectionsPerScope) return undefined
  return create(wsUrl, headers, {}, true)
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
      conn.ws?.terminate?.()
    } catch {}
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
  turnStateByContext.clear()
  lastResponseIDByContext.clear()
}

export function setWebSocketConstructorForTesting(ctor: WebSocketConstructor) {
  WebSocketImpl = ctor
}

export function resetWebSocketConstructorForTesting() {
  WebSocketImpl = loadDefaultWebSocketConstructor()
}
