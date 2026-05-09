import WebSocket from "ws"

export type WebSocketLike = {
  readyState: number
  send(data: string, callback?: (error?: Error) => void): void
  close(code?: number, reason?: string): void
  terminate?: () => void
  ping?: () => void
  on?(event: string, listener: (...args: any[]) => void): unknown
  off?(event: string, listener: (...args: any[]) => void): unknown
  addEventListener?(event: string, listener: (...args: any[]) => void): unknown
  removeEventListener?(event: string, listener: (...args: any[]) => void): unknown
}

export type WebSocketConstructor = new (
  url: string,
  options: {
    headers: Record<string, string>
    perMessageDeflate?: boolean
    finishRequest?: (request: { on?: (event: string, listener: (...args: unknown[]) => void) => unknown; end?: () => void }) => void
  },
) => WebSocketLike

export function loadDefaultWebSocketConstructor(): WebSocketConstructor {
  return WebSocket as unknown as WebSocketConstructor
}
