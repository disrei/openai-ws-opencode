import { prepareBody } from "./body.js"
import {
  acquireConnection,
  closeConnection,
  closeConnections,
  isWebSocketPreStreamTransportError,
  sendPending,
  type PendingRequest,
  type PooledConnection,
} from "./pool.js"
import { transportConfig } from "./config.js"
import type { TransportContext } from "./headers.js"

type FallbackFetch = (signal: AbortSignal) => Promise<Response>

const encoder = new TextEncoder()

function abortPending(conn: PooledConnection, error: Error) {
  const pending = conn.pending
  let closeAfterCancel: (() => void) | undefined
  if (pending && !pending.done && conn.ws?.readyState === 1) {
    const cancel = {
      type: "response.cancel",
      ...(pending.metadata.responseId ? { response_id: pending.metadata.responseId } : {}),
    }
    let closed = false
    closeAfterCancel = () => {
      if (closed) return
      closed = true
      closeConnection(conn, "Client aborted", Boolean(pending.metadata.responseId))
    }
    try {
      conn.ws.send(JSON.stringify(cancel), closeAfterCancel)
      setTimeout(closeAfterCancel, 250).unref?.()
    } catch {
      closeAfterCancel()
    }
  }
  if (pending && !pending.done) {
    pending.done = true
    try {
      pending.controller.error(error)
    } catch {}
  }
  conn.pending = null
  conn.busy = false
  if (!closeAfterCancel) closeConnections((candidate) => candidate === conn, "Client aborted")
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
  fallbackFetch?: FallbackFetch,
): Response {
  const wsBody = prepareBody(requestBody, isOAuth, context)
  const acquisitionController = new AbortController()
  const fallbackController = new AbortController()
  let conn: PooledConnection | undefined
  let finalized = false
  let fallbackStarted = false
  let streamController: ReadableStreamDefaultController<Uint8Array> | undefined

  const enqueuePreFirstBytePing = () => {
    const ping = transportConfig.preFirstBytePing
    if (!ping || !streamController) return
    queueMicrotask(() => {
      if (finalized || signal?.aborted || !streamController || conn?.pending?.framesReceived) return
      try {
        streamController.enqueue(encoder.encode(ping))
      } catch {}
    })
  }

  const cleanupAbort = () => {
    if (signal) signal.removeEventListener("abort", onAbort)
  }

  const finalize = () => {
    finalized = true
    cleanupAbort()
    acquisitionController.abort(new DOMException("Stream finalized", "AbortError"))
    fallbackController.abort(new DOMException("Stream finalized", "AbortError"))
  }

  const settleQueued = (error: Error) => {
    try {
      streamController?.error(error)
    } catch {}
  }

  const finishFallback = () => {
    finalized = true
    cleanupAbort()
    acquisitionController.abort(new DOMException("Fallback finalized", "AbortError"))
    fallbackController.abort(new DOMException("Fallback finalized", "AbortError"))
  }

  const pipeFallback = async () => {
    if (!streamController || !fallbackFetch) return
    try {
      const response = await fallbackFetch(fallbackController.signal)
      if (finalized) return
      if (!response.body) {
        streamController.close()
        finishFallback()
        return
      }
      const reader = response.body.getReader()
      try {
        while (!finalized) {
          const { value, done } = await reader.read()
          if (done) break
          if (value) streamController.enqueue(value)
        }
      } finally {
        reader.releaseLock()
      }
      if (!finalized) {
        streamController.close()
        finishFallback()
      }
    } catch (error) {
      if (finalized) return
      finalized = true
      cleanupAbort()
      try {
        streamController.error(error instanceof Error ? error : new Error(String(error)))
      } catch {}
    }
  }

  const tryStartFallback = (error: Error, pending?: PendingRequest): boolean => {
    if (!fallbackFetch || fallbackStarted || finalized || signal?.aborted) return false
    if (pending && (pending.framesReceived || pending.writeCommitted)) return false
    if (!isWebSocketPreStreamTransportError(error)) return false
    fallbackStarted = true
    acquisitionController.abort(error)
    void pipeFallback()
    return true
  }

  const onAbort = () => {
    if (finalized) return
    finalized = true
    cleanupAbort()
    const error = abortError(signal)
    acquisitionController.abort(error)
    fallbackController.abort(error)
    if (fallbackStarted) settleQueued(error)
    else if (conn) abortPending(conn, error)
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
      previousResponseNotFoundRetried: false,
      idleTimer: null,
      metadata: { resetPreviousResponseID: context.resetPreviousResponseID === true },
      onError: (error, pending) => tryStartFallback(error, pending),
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
      enqueuePreFirstBytePing()
      if (signal) signal.addEventListener("abort", onAbort, { once: true })
      const acquired = acquireConnection(wsUrl, headers, context, acquisitionController.signal)
      if (acquired instanceof Promise) {
        acquired.then((next) => startStream(controller, next)).catch((error) => {
          if (finalized) return
          const nextError = error instanceof Error ? error : new Error(String(error))
          if (tryStartFallback(nextError)) return
          finalized = true
          cleanupAbort()
          fallbackController.abort(nextError)
          controller.error(nextError)
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
      fallbackController.abort(new DOMException("Aborted", "AbortError"))
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
