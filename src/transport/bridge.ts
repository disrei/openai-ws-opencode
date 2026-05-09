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
  closeConnections((candidate) => candidate === conn)
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
  const conn = acquireConnection(wsUrl, headers, context)
  let finalized = false

  const cleanupAbort = () => {
    if (signal) signal.removeEventListener("abort", onAbort)
  }

  const onAbort = () => {
    if (finalized) return
    finalized = true
    cleanupAbort()
    abortPending(conn, signal?.reason instanceof Error ? signal.reason : new DOMException("Aborted", "AbortError"))
  }

  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
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
      }

      if (signal?.aborted) {
        onAbort()
        return
      }
      if (signal) signal.addEventListener("abort", onAbort, { once: true })
      sendPending(conn)
    },
    cancel() {
      if (finalized) return
      finalized = true
      cleanupAbort()
      abortPending(conn, new DOMException("Aborted", "AbortError"))
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
