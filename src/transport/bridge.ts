import { prepareBody } from "./body.js"
import { acquireConnection, closeConnections, sendPending, type PooledConnection } from "./pool.js"
import type { TransportContext } from "./headers.js"

function abortPending(conn: PooledConnection, error: Error) {
  const pending = conn.pending
  if (!pending || pending.done) return
  pending.done = true
  if (conn.ws?.readyState === 1) {
    try {
      conn.ws.send(JSON.stringify({ type: "response.cancel" }))
    } catch {}
  }
  try {
    pending.controller.error(error)
  } catch {}
  conn.pending = null
  conn.busy = false
  closeConnections((candidate) => candidate === conn, "Client aborted")
}

export function bridgeWebSocket(
  wsUrl: string,
  headers: Record<string, string>,
  requestBody: Record<string, unknown>,
  isOAuth: boolean,
  context: TransportContext = {},
  signal?: AbortSignal,
): Response {
  const wsBody = prepareBody(requestBody, isOAuth)
  let conn: PooledConnection | undefined
  let finalized = false

  const cleanupAbort = () => {
    if (signal) signal.removeEventListener("abort", onAbort)
  }

  const onAbort = () => {
    if (finalized) return
    finalized = true
    cleanupAbort()
    if (conn) abortPending(conn, signal?.reason instanceof Error ? signal.reason : new DOMException("Aborted", "AbortError"))
  }

  const startStream = (controller: ReadableStreamDefaultController<Uint8Array>, acquired: PooledConnection) => {
    conn = acquired
    if (finalized || signal?.aborted) {
      abortPending(conn, signal?.reason instanceof Error ? signal.reason : new DOMException("Aborted", "AbortError"))
      return
    }
    conn.activeSessionID = context.sessionID
    conn.activeAgent = context.agent
    conn.lastModelID = context.modelID ?? conn.lastModelID
    conn.lastStablePrefixHash = context.stablePrefixHash ?? conn.lastStablePrefixHash
    conn.pending = {
      body: wsBody,
      controller,
      done: false,
      sent: false,
      forwarded: false,
      replayUnsafeForwarded: false,
      frameCount: 0,
      metadata: {},
    }
    sendPending(conn)
  }

  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      if (signal?.aborted) {
        controller.error(signal.reason instanceof Error ? signal.reason : new DOMException("Aborted", "AbortError"))
        return
      }
      if (signal) signal.addEventListener("abort", onAbort, { once: true })
      const acquired = acquireConnection(wsUrl, headers, context, signal)
      if (acquired instanceof Promise) {
        acquired.then((next) => startStream(controller, next)).catch((error) => {
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
      if (conn) abortPending(conn, new DOMException("Aborted", "AbortError"))
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
