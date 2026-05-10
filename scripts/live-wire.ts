import WebSocket from "ws"
import { OPENAI_WS_URL } from "../src/constants.js"
import { apiKeyWebSocketHeaders, bridgeWebSocket, resetPoolForTesting, resetWebSocketConstructorForTesting, setWebSocketConstructorForTesting } from "../src/testing.js"

const apiKey = process.env.OPENAI_API_KEY
const model = (process.env.OPENAI_WS_LIVE_WIRE_MODEL ?? process.env.OPENAI_WS_LIVE_MODEL ?? "gpt-5.5").replace(/^openai-ws\//, "")
const timeoutMs = Number(process.env.OPENAI_WS_LIVE_WIRE_TIMEOUT_MS ?? 60_000)

type EventFrame = {
  event: string
  data: Record<string, unknown>
}

class CapturingWebSocket extends WebSocket {
  static instances: CapturingWebSocket[] = []
  sent: string[] = []

  constructor(url: string, options: WebSocket.ClientOptions) {
    super(url, options)
    CapturingWebSocket.instances.push(this)
  }

  send(data: WebSocket.RawData, callback?: (err?: Error) => void): void {
    this.sent.push(typeof data === "string" ? data : data.toString())
    super.send(data, callback)
  }
}

function nonce(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`
}

function sentFrames(): Array<Record<string, unknown>> {
  return CapturingWebSocket.instances.flatMap((socket) =>
    socket.sent.flatMap((value) => {
      try {
        return [JSON.parse(value) as Record<string, unknown>]
      } catch {
        return []
      }
    }),
  )
}

function responseID(frame: EventFrame): string | undefined {
  const response = frame.data.response
  if (response && typeof response === "object" && !Array.isArray(response)) {
    const id = (response as Record<string, unknown>).id
    if (typeof id === "string") return id
  }
  const id = frame.data.response_id
  return typeof id === "string" ? id : undefined
}

function outputDelta(frame: EventFrame): string {
  if (typeof frame.data.delta === "string") return frame.data.delta
  if (typeof frame.data.text === "string" && /output_text/.test(frame.event)) return frame.data.text
  return ""
}

async function readChunk(reader: ReadableStreamDefaultReader<Uint8Array>, label: string): Promise<ReadableStreamReadResult<Uint8Array>> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      reader.read(),
      new Promise<ReadableStreamReadResult<Uint8Array>>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

async function* sseFrames(response: Response, label: string): AsyncGenerator<EventFrame> {
  const reader = response.body?.getReader()
  if (!reader) throw new Error(`${label} did not have a response body`)
  const decoder = new TextDecoder()
  let buffer = ""
  try {
    while (true) {
      const { value, done } = await readChunk(reader, label)
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const parts = buffer.split(/\n\n/)
      buffer = parts.pop() ?? ""
      for (const part of parts) {
        const event = part.match(/^event: (.*)$/m)?.[1] ?? "message"
        const dataText = part.match(/^data: (.*)$/m)?.[1]
        if (!dataText) continue
        const data = JSON.parse(dataText) as Record<string, unknown>
        yield { event, data }
      }
    }
  } finally {
    reader.releaseLock()
  }
}

async function waitForCreated(response: Response): Promise<{ id: string; text: string }> {
  let text = ""
  for await (const frame of sseFrames(response, "first live-wire stream")) {
    if (frame.event === "error" || frame.data.type === "error") throw new Error(`first stream error: ${JSON.stringify(frame.data)}`)
    text += outputDelta(frame)
    const id = responseID(frame)
    if (id) return { id, text }
  }
  throw new Error("first stream ended before any response id was emitted")
}

async function readUntilTerminal(response: Response, label: string, expected?: string): Promise<{ id: string; text: string }> {
  let id: string | undefined
  let text = ""
  for await (const frame of sseFrames(response, label)) {
    if (frame.event === "error" || frame.data.type === "error") throw new Error(`${label} error: ${JSON.stringify(frame.data)}`)
    id ??= responseID(frame)
    text += outputDelta(frame)
    if (frame.event === "response.completed" || frame.data.type === "response.completed") break
  }
  if (!id) throw new Error(`${label} ended before any response id was emitted`)
  if (expected && !text.includes(expected)) throw new Error(`${label} did not include expected nonce ${expected}; got ${JSON.stringify(text)}`)
  return { id, text }
}

async function readToCompletion(response: Response, expected: string): Promise<string> {
  let text = ""
  for await (const frame of sseFrames(response, "second live-wire stream")) {
    if (frame.event === "error" || frame.data.type === "error") throw new Error(`second stream error: ${JSON.stringify(frame.data)}`)
    text += outputDelta(frame)
    if (frame.event === "response.completed" || frame.data.type === "response.completed") break
  }
  if (!text.includes(expected)) throw new Error(`second response did not include expected nonce ${expected}; got ${JSON.stringify(text)}`)
  return text
}

async function main() {
  if (!apiKey) throw new Error("OPENAI_API_KEY is required for live-wire WSS testing")

  resetPoolForTesting()
  setWebSocketConstructorForTesting(CapturingWebSocket as any)

  const sessionID = nonce("live_wire_session")
  const headers = apiKeyWebSocketHeaders(apiKey)
  const context = { sessionID, agent: "live-wire", stablePrefixHash: nonce("prefix") }

  try {
    const first = bridgeWebSocket(
      OPENAI_WS_URL,
      headers,
      {
        model,
        stream: true,
        generate: false,
        input: "Warm up this live-wire continuation context. Do not generate output.",
      },
      false,
      context,
    )

    const { id } = await readUntilTerminal(first, "first live-wire warmup")

    const expected = nonce("LIVE_WIRE_OK")
    const second = bridgeWebSocket(
      OPENAI_WS_URL,
      headers,
      {
        model,
        stream: true,
        input: `Reply exactly ${expected} and nothing else.`,
        max_output_tokens: 64,
      },
      false,
      context,
    )

    let text: string
    try {
      text = await readToCompletion(second, expected)
    } catch (error) {
      console.error(
        JSON.stringify({
          model,
          capturedResponseID: id,
          sentFrames: sentFrames().filter((frame) => frame.type === "response.create" || frame.type === "response.cancel"),
          socketStates: CapturingWebSocket.instances.map((socket) => socket.readyState),
        }),
      )
      throw error
    }
    const createFrames = sentFrames().filter((frame) => frame.type === "response.create")
    const secondCreate = createFrames.at(-1)
    if (!secondCreate) throw new Error("second response.create was not sent")
    if (secondCreate.previous_response_id !== id) {
      throw new Error(`second response.create did not reuse captured id; expected ${id}, got ${String(secondCreate.previous_response_id)}`)
    }

    console.log(
      JSON.stringify({
        ok: true,
        model,
        capturedResponseID: id,
        secondPreviousResponseID: secondCreate.previous_response_id,
        secondText: text,
      }),
    )
  } finally {
    resetPoolForTesting()
    resetWebSocketConstructorForTesting()
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : String(error))
  process.exit(1)
})
