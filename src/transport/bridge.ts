import { prepareBody } from "./body.js"
import { acquireConnection, closeConnections, sendPending, type PooledConnection } from "./pool.js"
import type { TransportContext } from "./headers.js"

function abortPending(conn: PooledConnection, error: Error) {
  const pending = conn.pending
  if (pending && !pending.done && conn.ws?.readyState === 1) {
    try {
      conn.ws.send(JSON.stringify({ type: "response.cancel" }))
    } catch {}
  }
  if (pending && !pending.done) {
    pending.done = true
    try {
      pending.controller.error(error)
    } catch {}
  }
  conn.pending = null
  conn.busy = false
  closeConnections((candidate) => candidate === conn, "Client aborted")
}

function abortError(signal?: AbortSignal): Error {
  return signal?.reason instanceof Error ? signal.reason : new DOMException("Aborted", "AbortError")
}

export function bridgeWebSocket(
  wsUrl: string,
  headers: Record<string, string>,
  requestBody: Record<string, unknown>,
  isOAuth: boolean,
  context: TransportContext = {},
  signal?: AbortSignal,
): Response {
  const wsBody = prepareBody(requestBody, isOAuth, context)
  const acquisitionController = new AbortController()
  let conn: PooledConnection | undefined
  let finalized = false
  let streamController: ReadableStreamDefaultController<Uint8Array> | undefined

  const cleanupAbort = () => {
    if (signal) signal.removeEventListener("abort", onAbort)
  }

  const finalize = () => {
    finalized = true
    cleanupAbort()
    acquisitionController.abort(new DOMException("Stream finalized", "AbortError"))
  }

  const settleQueued = (error: Error) => {
    try {
      streamController?.error(error)
    } catch {}
  }

  const onAbort = () => {
    if (finalized) return
    finalized = true
    cleanupAbort()
    const error = abortError(signal)
    acquisitionController.abort(error)
    if (conn) abortPending(conn, error)
    else settleQueued(error)
  }

  const startStream = (controller: ReadableStreamDefaultController<Uint8Array>, acquired: PooledConnection) => {
    conn = acquired
    if (finalized || signal?.aborted) {
      abortPending(conn, abortError(signal))
      return
    }
    conn.activeSessionID = context.sessionID
    conn.activeAgent = context.agent
    conn.lastModelID = context.modelID ?? conn.lastModelID
    conn.lastStablePrefixHash = context.stablePrefixHash ?? conn.lastStablePrefixHash
    conn.pending = {
      body: wsBody,
      controller,
      onFinalize: finalize,
      done: false,
      sent: false,
      writeCommitted: false,
      framesReceived: false,
      finalMessageOutputReceived: false,
      processedAckSent: false,
      idleTimer: null,
      metadata: {},
    }
    sendPending(conn)
  }

  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      streamController = controller
      if (signal?.aborted) {
        finalized = true
        controller.error(abortError(signal))
        return
      }
      if (signal) signal.addEventListener("abort", onAbort, { once: true })
      const acquired = acquireConnection(wsUrl, headers, context, acquisitionController.signal)
      if (acquired instanceof Promise) {
        acquired.then((next) => startStream(controller, next)).catch((error) => {
          if (finalized) return
          finalized = true
          cleanupAbort()
          controller.error(error instanceof Error ? error : new Error(String(error)))
        })
        return
      }
      startStream(controller, acquired)
    },
    cancel() {
      if (finalized) return
      finalized = true
      cleanupAbort()
      acquisitionController.abort(new DOMException("Aborted", "AbortError"))
      if (conn) abortPending(conn, new DOMException("Aborted", "AbortError"))
      else settleQueued(new DOMException("Aborted", "AbortError"))
    },
  })

  return new Response(readable, {
    status: 200,
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-request-id": `ws-${Date.now()}`,
    },
  })
}
