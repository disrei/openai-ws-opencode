import crypto from "node:crypto"
import { EventEmitter } from "node:events"
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

const CONNECTING = 0
const OPEN = 1
const CLOSING = 2
const CLOSED = 3

type BunConnect = (options: {
  hostname: string
  port: number
  tls?: boolean
  socket: {
    open?: (socket: BunSocket) => void
    data?: (socket: BunSocket, data: Uint8Array) => void
    close?: (socket: BunSocket, error?: Error) => void
    error?: (socket: BunSocket, error: Error) => void
    connectError?: (socket: BunSocket, error: Error) => void
    end?: (socket: BunSocket) => void
    timeout?: (socket: BunSocket) => void
  }
}) => Promise<BunSocket>

type BunSocket = {
  write(data: string | Uint8Array): unknown
  end?: () => void
  close?: () => void
}

type BunGlobal = {
  Bun?: {
    connect?: BunConnect
  }
}

export function loadDefaultWebSocketConstructor(): WebSocketConstructor {
  const connect = (globalThis as BunGlobal).Bun?.connect
  return typeof connect === "function" ? (BunWebSocket as unknown as WebSocketConstructor) : (WebSocket as unknown as WebSocketConstructor)
}

class BunWebSocket extends EventEmitter implements WebSocketLike {
  readyState = CONNECTING
  #socket: BunSocket | null = null
  #handshakeBuffer = Buffer.alloc(0)
  #frameBuffer = Buffer.alloc(0)
  #fragments: Buffer[] = []
  #fragmentOpcode: number | null = null
  #closed = false

  constructor(
    private readonly url: string,
    private readonly options: { headers: Record<string, string> },
  ) {
    super()
    void this.#connect()
  }

  send(data: string, callback?: (error?: Error) => void) {
    if (this.readyState !== OPEN || !this.#socket) throw new Error("WebSocket is not open")
    try {
      this.#socket.write(encodeClientFrame(0x1, Buffer.from(data, "utf8")))
      if (callback) queueMicrotask(() => callback())
    } catch (error) {
      if (callback) callback(error instanceof Error ? error : new Error(String(error)))
      else throw error
    }
  }

  ping() {
    if (this.readyState !== OPEN || !this.#socket) return
    this.#socket.write(encodeClientFrame(0x9))
  }

  close(code = 1000, reason = "") {
    if (this.readyState === CLOSED || this.readyState === CLOSING) return
    this.readyState = CLOSING
    if (this.#socket) {
      const payload = Buffer.alloc(2 + Buffer.byteLength(reason))
      payload.writeUInt16BE(code, 0)
      payload.write(reason, 2)
      try {
        this.#socket.write(encodeClientFrame(0x8, payload))
      } catch {}
      this.#endSocket()
    } else {
      this.#finishClose(code, reason)
    }
  }

  terminate() {
    this.#endSocket()
    this.readyState = CLOSED
  }

  async #connect() {
    const parsed = new URL(this.url)
    const connect = (globalThis as BunGlobal).Bun?.connect
    if (typeof connect !== "function") {
      this.#emitError(new Error("Bun.connect is unavailable"))
      return
    }
    try {
      this.#socket = await connect({
        hostname: parsed.hostname,
        port: Number(parsed.port || (parsed.protocol === "wss:" ? 443 : 80)),
        tls: parsed.protocol === "wss:",
        socket: {
          open: (socket) => {
            this.#socket = socket
            queueMicrotask(() => socket.write(this.#handshakeRequest(parsed)))
          },
          data: (_socket, data) => this.#handleData(Buffer.from(data)),
          close: (_socket, error) => {
            if (error) this.#emitError(error)
            this.#finishClose()
          },
          error: (_socket, error) => this.#emitError(error),
          connectError: (_socket, error) => this.#emitError(error),
          end: () => this.#finishClose(),
          timeout: () => this.#emitError(new Error("WebSocket socket timed out")),
        },
      })
    } catch (error) {
      this.#emitError(error instanceof Error ? error : new Error(String(error)))
    }
  }

  #handshakeRequest(parsed: URL): string {
    const key = crypto.randomBytes(16).toString("base64")
    const path = `${parsed.pathname || "/"}${parsed.search}`
    const host = parsed.port ? `${parsed.hostname}:${parsed.port}` : parsed.hostname
    const headers: Record<string, string> = {
      Host: host,
      Upgrade: "websocket",
      Connection: "Upgrade",
      "Sec-WebSocket-Key": key,
      "Sec-WebSocket-Version": "13",
      ...this.options.headers,
    }
    return `GET ${path} HTTP/1.1\r\n${Object.entries(headers)
      .map(([name, value]) => `${name}: ${value}`)
      .join("\r\n")}\r\n\r\n`
  }

  #handleData(data: Buffer) {
    if (this.readyState === CONNECTING) {
      this.#handshakeBuffer = Buffer.concat([this.#handshakeBuffer, data])
      const split = this.#handshakeBuffer.indexOf("\r\n\r\n")
      if (split < 0) return
      const head = this.#handshakeBuffer.subarray(0, split).toString("utf8")
      const rest = this.#handshakeBuffer.subarray(split + 4)
      this.#handshakeBuffer = Buffer.alloc(0)
      const response = parseHandshake(head)
      if (response.status !== 101) {
        this.#emitError(new Error(`WebSocket upgrade failed with HTTP ${response.status}`))
        this.#endSocket()
        return
      }
      this.readyState = OPEN
      this.emit("upgrade", { headers: response.headers })
      this.emit("open")
      if (rest.length) this.#handleFrames(rest)
      return
    }
    this.#handleFrames(data)
  }

  #handleFrames(data: Buffer) {
    this.#frameBuffer = Buffer.concat([this.#frameBuffer, data])
    while (this.#frameBuffer.length >= 2) {
      const first = this.#frameBuffer[0]
      const second = this.#frameBuffer[1]
      const fin = (first & 0x80) !== 0
      let length = second & 0x7f
      let offset = 2
      if (length === 126) {
        if (this.#frameBuffer.length < offset + 2) return
        length = this.#frameBuffer.readUInt16BE(offset)
        offset += 2
      } else if (length === 127) {
        if (this.#frameBuffer.length < offset + 8) return
        const bigLength = this.#frameBuffer.readBigUInt64BE(offset)
        if (bigLength > BigInt(Number.MAX_SAFE_INTEGER)) {
          this.#emitError(new Error("WebSocket frame is too large"))
          this.#endSocket()
          return
        }
        length = Number(bigLength)
        offset += 8
      }
      const masked = (second & 0x80) !== 0
      const maskOffset = offset
      if (masked) offset += 4
      if (this.#frameBuffer.length < offset + length) return
      let payload: Buffer = Buffer.from(this.#frameBuffer.subarray(offset, offset + length))
      if (masked) payload = unmask(payload, this.#frameBuffer.subarray(maskOffset, maskOffset + 4))
      this.#frameBuffer = this.#frameBuffer.subarray(offset + length)
      this.#handleFrame(first & 0x0f, fin, payload)
    }
  }

  #handleFrame(opcode: number, fin: boolean, payload: Buffer) {
    if (opcode === 0x8) {
      const code = payload.length >= 2 ? payload.readUInt16BE(0) : undefined
      const reason = payload.length > 2 ? payload.subarray(2).toString("utf8") : undefined
      this.#endSocket()
      this.#finishClose(code, reason)
      return
    }
    if (opcode === 0x9) {
      this.#socket?.write(encodeClientFrame(0xa, payload))
      return
    }
    if (opcode === 0xa) {
      this.emit("pong")
      return
    }
    if (opcode === 0x0) {
      this.#fragments.push(payload)
      if (fin) this.#emitMessage(this.#fragmentOpcode ?? 0x1, Buffer.concat(this.#fragments))
      if (fin) this.#resetFragments()
      return
    }
    if (opcode !== 0x1 && opcode !== 0x2) return
    if (!fin) {
      this.#fragmentOpcode = opcode
      this.#fragments = [payload]
      return
    }
    this.#emitMessage(opcode, payload)
  }

  #emitMessage(opcode: number, payload: Buffer) {
    if (opcode === 0x1) this.emit("message", payload.toString("utf8"), false)
    else this.emit("message", payload, true)
  }

  #resetFragments() {
    this.#fragmentOpcode = null
    this.#fragments = []
  }

  #emitError(error: Error) {
    queueMicrotask(() => this.emit("error", error))
  }

  #endSocket() {
    try {
      this.#socket?.end?.()
    } catch {
      try {
        this.#socket?.close?.()
      } catch {}
    }
  }

  #finishClose(code?: number, reason?: string) {
    if (this.#closed) return
    this.#closed = true
    this.readyState = CLOSED
    this.emit("close", code, reason)
  }
}

function parseHandshake(text: string): { status: number; headers: Headers } {
  const lines = text.split(/\r?\n/)
  const status = Number(lines[0]?.match(/\s(\d{3})\s/)?.[1] ?? 0)
  const headers = new Headers()
  for (const line of lines.slice(1)) {
    const index = line.indexOf(":")
    if (index <= 0) continue
    headers.append(line.slice(0, index).trim(), line.slice(index + 1).trim())
  }
  return { status, headers }
}

function encodeClientFrame(opcode: number, payload: Uint8Array = Buffer.alloc(0)): Buffer {
  const length = payload.length
  const lengthBytes = length < 126 ? 0 : length <= 0xffff ? 2 : 8
  const frame = Buffer.alloc(2 + lengthBytes + 4 + length)
  frame[0] = 0x80 | opcode
  if (length < 126) {
    frame[1] = 0x80 | length
  } else if (length <= 0xffff) {
    frame[1] = 0x80 | 126
    frame.writeUInt16BE(length, 2)
  } else {
    frame[1] = 0x80 | 127
    frame.writeBigUInt64BE(BigInt(length), 2)
  }
  const maskOffset = 2 + lengthBytes
  const dataOffset = maskOffset + 4
  const mask = crypto.randomBytes(4)
  mask.copy(frame, maskOffset)
  for (let index = 0; index < payload.length; index++) {
    frame[dataOffset + index] = payload[index] ^ mask[index % 4]
  }
  return frame
}

function unmask(payload: Uint8Array, mask: Uint8Array): Buffer {
  const output = Buffer.alloc(payload.length)
  for (let index = 0; index < payload.length; index++) output[index] = payload[index] ^ mask[index % 4]
  return output
}
