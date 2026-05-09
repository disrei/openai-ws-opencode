import crypto from "node:crypto"
import { EventEmitter } from "node:events"
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { describe, expect, test, afterEach, vi } from "vitest"
import plugin from "../src/index.js"
import { createBrowserAuthorization } from "../src/auth/oauth.js"
import {
  CLIENT_ID,
  CODEX_ORIGINATOR,
  OPENAI_WS_INSTALLATION_ID_ENV,
  RESPONSE_PROCESSED_ENV,
  USER_AGENT,
} from "../src/constants.js"
import {
  apiKeyWebSocketHeaders,
  bridgeWebSocket,
  connectionPool,
  oauthTesting,
  oauthWebSocketHeaders,
  prepareBody,
  resetPoolForTesting,
  resetCatalogCacheForTesting,
  resetWebSocketConstructorForTesting,
  fallbackCodexClientVersion,
  fetchCodexCatalog,
  fetchOpenAIModelIds,
  resolveCodexClientVersion,
  resolveModels,
  resolveModelsForApiKey,
  resolveModelsForOAuth,
  setWebSocketConstructorForTesting,
} from "../src/testing.js"
import { isDirectExecution, patchConfigText, setupOpenCodeConfig } from "../bin/setup.ts"

class MockWebSocket extends EventEmitter {
  static instances: MockWebSocket[] = []
  readyState = 0
  sent: string[] = []
  pingCount = 0
  terminateCount = 0
  url: string
  options: { headers: Record<string, string>; perMessageDeflate?: boolean }

  constructor(url: string, options: { headers: Record<string, string>; perMessageDeflate?: boolean }) {
    super()
    this.url = url
    this.options = options
    MockWebSocket.instances.push(this)
  }

  send(data: string) {
    this.sent.push(data)
  }

  close() {
    this.readyState = 3
    this.emit("close")
  }

  terminate() {
    this.terminateCount++
    this.readyState = 3
  }

  ping() {
    this.pingCount++
  }

  pong() {
    this.emit("pong")
  }

  open() {
    this.readyState = 1
    this.emit("open")
  }

  serverMessage(frame: unknown) {
    this.emit("message", JSON.stringify(frame))
  }

  serverBinary(data = Buffer.from([1])) {
    this.emit("message", data, true)
  }

  upgrade(headers: Record<string, string>) {
    this.emit("upgrade", { headers })
  }
}

type WireServerSocket = {
  write(data: string | Uint8Array): unknown
  end?: () => void
  close?: () => void
}

type WireTurn = { headers: Record<string, string>; body?: Record<string, unknown> }

function listenWireWebSocket(received: WireTurn[]) {
  const listen = (globalThis as { Bun?: { listen?: (options: unknown) => { port: number; stop(force?: boolean): void } } }).Bun?.listen
  if (typeof listen !== "function") throw new Error("Bun.listen is unavailable")
  const states = new WeakMap<object, { handshake: Buffer; frames: Buffer; turn?: WireTurn }>()
  const getState = (socket: object) => {
    const existing = states.get(socket)
    if (existing) return existing
    const next = { handshake: Buffer.alloc(0), frames: Buffer.alloc(0), turn: undefined as WireTurn | undefined }
    states.set(socket, next)
    return next
  }

  const server = listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      data(socket: WireServerSocket, data: Uint8Array) {
        const state = getState(socket)
        let rest = Buffer.from(data)
        if (!state.turn) {
          state.handshake = Buffer.concat([state.handshake, rest])
          const split = state.handshake.indexOf("\r\n\r\n")
          if (split < 0) return
          const requestText = state.handshake.subarray(0, split).toString("utf8")
          rest = Buffer.from(state.handshake.subarray(split + 4))
          const headers = parseWireHeaders(requestText)
          const turn: WireTurn = { headers }
          received.push(turn)
          state.turn = turn
          socket.write(wireUpgradeResponse(headers["sec-websocket-key"]))
        }
        if (rest.length) receiveWireFrames(socket, state, rest)
      },
    },
  })
  return server
}

function parseWireHeaders(text: string): Record<string, string> {
  const headers: Record<string, string> = {}
  for (const line of text.split(/\r?\n/).slice(1)) {
    const index = line.indexOf(":")
    if (index <= 0) continue
    headers[line.slice(0, index).trim().toLowerCase()] = line.slice(index + 1).trim()
  }
  return headers
}

function wireUpgradeResponse(key: string | undefined): string {
  const accept = crypto
    .createHash("sha1")
    .update(`${key ?? ""}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64")
  return [
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${accept}`,
    "x-codex-turn-state: turn_live",
    "",
    "",
  ].join("\r\n")
}

function receiveWireFrames(
  socket: WireServerSocket,
  state: { frames: Buffer; turn?: WireTurn },
  data: Buffer,
) {
  state.frames = Buffer.concat([state.frames, data])
  while (state.frames.length >= 2) {
    const first = state.frames[0]
    const second = state.frames[1]
    const opcode = first & 0x0f
    let length = second & 0x7f
    let offset = 2
    if (length === 126) {
      if (state.frames.length < offset + 2) return
      length = state.frames.readUInt16BE(offset)
      offset += 2
    } else if (length === 127) {
      if (state.frames.length < offset + 8) return
      length = Number(state.frames.readBigUInt64BE(offset))
      offset += 8
    }
    const masked = (second & 0x80) !== 0
    const maskOffset = offset
    if (masked) offset += 4
    if (state.frames.length < offset + length) return
    let payload = state.frames.subarray(offset, offset + length)
    if (masked) payload = unmaskWirePayload(payload, state.frames.subarray(maskOffset, maskOffset + 4))
    state.frames = state.frames.subarray(offset + length)
    if (opcode === 0x8) {
      socket.end?.()
      return
    }
    if (opcode !== 0x1 || !state.turn) continue
    const body = JSON.parse(payload.toString("utf8")) as Record<string, unknown>
    state.turn.body = body
    socket.write(encodeWireTextFrame(JSON.stringify({ type: "response.completed", response: { id: `resp_live_${body.input}` } })))
  }
}

function encodeWireTextFrame(text: string): Buffer {
  const payload = Buffer.from(text, "utf8")
  const lengthBytes = payload.length < 126 ? 0 : payload.length <= 0xffff ? 2 : 8
  const frame = Buffer.alloc(2 + lengthBytes + payload.length)
  frame[0] = 0x81
  if (payload.length < 126) {
    frame[1] = payload.length
  } else if (payload.length <= 0xffff) {
    frame[1] = 126
    frame.writeUInt16BE(payload.length, 2)
  } else {
    frame[1] = 127
    frame.writeBigUInt64BE(BigInt(payload.length), 2)
  }
  payload.copy(frame, 2 + lengthBytes)
  return frame
}

function unmaskWirePayload(payload: Uint8Array, mask: Uint8Array): Buffer {
  const output = Buffer.alloc(payload.length)
  for (let index = 0; index < payload.length; index++) output[index] = payload[index] ^ mask[index % 4]
  return output
}

const originalXdgCacheHome = process.env.XDG_CACHE_HOME
const originalInstallationID = process.env[OPENAI_WS_INSTALLATION_ID_ENV]
const originalResponseProcessed = process.env[RESPONSE_PROCESSED_ENV]
const originalOpenAIOrganization = process.env.OPENAI_ORGANIZATION
const originalOpenAIProject = process.env.OPENAI_PROJECT

afterEach(() => {
  vi.useRealTimers()
  oauthTesting.reset()
  resetPoolForTesting()
  resetCatalogCacheForTesting()
  resetWebSocketConstructorForTesting()
  MockWebSocket.instances = []
  if (originalXdgCacheHome === undefined) delete process.env.XDG_CACHE_HOME
  else process.env.XDG_CACHE_HOME = originalXdgCacheHome
  if (originalInstallationID === undefined) delete process.env[OPENAI_WS_INSTALLATION_ID_ENV]
  else process.env[OPENAI_WS_INSTALLATION_ID_ENV] = originalInstallationID
  if (originalResponseProcessed === undefined) delete process.env[RESPONSE_PROCESSED_ENV]
  else process.env[RESPONSE_PROCESSED_ENV] = originalResponseProcessed
  if (originalOpenAIOrganization === undefined) delete process.env.OPENAI_ORGANIZATION
  else process.env.OPENAI_ORGANIZATION = originalOpenAIOrganization
  if (originalOpenAIProject === undefined) delete process.env.OPENAI_PROJECT
  else process.env.OPENAI_PROJECT = originalOpenAIProject
  vi.restoreAllMocks()
})

function writeOpenCodePackageCache(cacheHome: string, cacheName: string, version: string, source = ""): string {
  const entry = path.join(cacheHome, "opencode", "packages", cacheName)
  const packageRoot = path.join(entry, "node_modules", "openai-ws-opencode")
  mkdirSync(path.join(packageRoot, "dist"), { recursive: true })
  writeFileSync(path.join(packageRoot, "package.json"), JSON.stringify({ name: "openai-ws-opencode", version }))
  writeFileSync(path.join(packageRoot, "dist", "constants.js"), source)
  return entry
}

describe("package exports", () => {
  test("root export is exactly one plugin function", async () => {
    const mod = await import("../src/index.js")
    const functions = Object.values(mod).filter((value) => typeof value === "function")
    expect(functions).toHaveLength(1)
    expect(functions[0]).toBe(plugin)
  })
})

describe("setup", () => {
  test("writes plugin and provider idempotently", () => {
    const once = patchConfigText("{}")
    const twice = patchConfigText(once)
    const parsed = JSON.parse(twice)
    expect(parsed.plugin).toEqual(["openai-ws-opencode@latest"])
    const models = Object.values(parsed.provider["openai-ws"].models)
    expect(models.length).toBeGreaterThan(0)
    expect(models.every((model: any) => model.reasoning === true && model.tool_call === true)).toBe(true)
    expect(models.every((model: any) => !("status" in model))).toBe(true)
    expect(twice).toBe(once)
  })

  test("does not duplicate existing versioned plugin specifiers", () => {
    const patched = patchConfigText(JSON.stringify({ plugin: ["openai-ws-opencode@0.1.0"] }))
    const parsed = JSON.parse(patched)
    expect(parsed.plugin).toEqual(["openai-ws-opencode@0.1.0"])
  })

  test("preserves existing file tarball plugin specifiers", () => {
    const plugin = "file:/tmp/openai-ws-opencode-0.1.4.tgz"
    const patched = patchConfigText(JSON.stringify({ plugin: [plugin] }))
    const parsed = JSON.parse(patched)
    expect(parsed.plugin).toEqual([plugin])
  })

  test("uses an explicit plugin specifier to replace existing package entries", () => {
    const plugin = "file:/tmp/openai-ws-opencode-0.1.4.tgz"
    const patched = patchConfigText(JSON.stringify({ plugin: ["openai-ws-opencode@0.1.0"] }), plugin, true)
    const parsed = JSON.parse(patched)
    expect(parsed.plugin).toEqual([plugin])
  })

  test("moves stale OpenCode @latest cache aside during setup", async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "openai-ws-opencode-cache-"))
    try {
      process.env.XDG_CACHE_HOME = dir
      const staleEntry = writeOpenCodePackageCache(
        dir,
        "openai-ws-opencode@latest",
        "0.1.1",
        'export const OAUTH_PORT = 1456\nexport const CODEX_ORIGINATOR = "codex_cli_rs"\n',
      )

      await setupOpenCodeConfig({ configPath: path.join(dir, "opencode.json") })

      expect(existsSync(staleEntry)).toBe(false)
      const staleSiblings = readdirSync(path.dirname(staleEntry)).filter((name) => name.startsWith("openai-ws-opencode@latest.stale-"))
      expect(staleSiblings).toHaveLength(1)
      expect(existsSync(path.join(path.dirname(staleEntry), staleSiblings[0], "node_modules", "openai-ws-opencode", "package.json"))).toBe(true)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test("leaves current or newer OpenCode @latest cache in place", async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "openai-ws-opencode-cache-"))
    try {
      process.env.XDG_CACHE_HOME = dir
      const currentEntry = writeOpenCodePackageCache(dir, "openai-ws-opencode@latest", "99.0.0", 'export const OAUTH_PORT = 1455\nexport const CODEX_ORIGINATOR = "opencode"\n')

      await setupOpenCodeConfig({ configPath: path.join(dir, "opencode.json") })

      expect(existsSync(currentEntry)).toBe(true)
      expect(readdirSync(path.dirname(currentEntry)).some((name) => name.includes(".stale-"))).toBe(false)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test("repairs only the OpenCode @latest package cache", async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "openai-ws-opencode-cache-"))
    try {
      process.env.XDG_CACHE_HOME = dir
      const exactEntry = writeOpenCodePackageCache(dir, "openai-ws-opencode@0.1.1", "0.1.1", "export const OAUTH_PORT = 1456\n")

      await setupOpenCodeConfig({ configPath: path.join(dir, "opencode.json") })

      expect(existsSync(exactEntry)).toBe(true)
      expect(readdirSync(path.dirname(exactEntry))).toEqual(["openai-ws-opencode@0.1.1"])
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test("can skip cache repair for config-only setup", async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "openai-ws-opencode-cache-"))
    try {
      process.env.XDG_CACHE_HOME = dir
      const staleEntry = writeOpenCodePackageCache(dir, "openai-ws-opencode@latest", "0.1.1", "export const OAUTH_PORT = 1456\n")

      await setupOpenCodeConfig({ configPath: path.join(dir, "opencode.json"), cacheRepair: false } as any)

      expect(existsSync(staleEntry)).toBe(true)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test("warns without failing setup when cache repair fails", async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "openai-ws-opencode-cache-"))
    try {
      process.env.XDG_CACHE_HOME = dir
      const cachePath = path.join(dir, "opencode", "packages", "openai-ws-opencode@latest")
      const configPath = path.join(dir, "opencode.json")
      mkdirSync(path.dirname(cachePath), { recursive: true })
      writeFileSync(cachePath, "not a directory")
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {})

      await setupOpenCodeConfig({ configPath })

      expect(existsSync(configPath)).toBe(true)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining(cachePath))
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test("parses --no-cache-repair for config-only setup", async () => {
    const setupModule = (await import("../bin/setup.ts")) as any
    expect(setupModule.parseArgs(["--no-cache-repair"])).toMatchObject({ cacheRepair: false })
  })

  test("detects npm bin symlink execution", () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "openai-ws-opencode-"))
    try {
      const target = path.join(dir, "setup.js")
      const link = path.join(dir, "openai-ws-opencode")
      writeFileSync(target, "")
      symlinkSync(target, link)
      expect(isDirectExecution(pathToFileURL(target).href, link)).toBe(true)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})

describe("oauth authorize URL", () => {
  test("uses a separate openai-ws flow with native OpenCode OAuth params", async () => {
    const auth = await createBrowserAuthorization()
    const callback = auth.callback()
    try {
      const url = new URL(auth.url)
      const redirectUri = new URL(url.searchParams.get("redirect_uri") ?? "")
      expect(url.origin).toBe("https://auth.openai.com")
      expect(url.pathname).toBe("/oauth/authorize")
      expect(url.searchParams.get("client_id")).toBe(CLIENT_ID)
      expect(url.searchParams.get("originator")).toBe("opencode")
      expect(url.searchParams.get("scope")).toBe("openid profile email offline_access")
      expect(url.searchParams.get("state")).toMatch(/^[A-Za-z0-9_-]{32,}$/)
      expect(url.searchParams.get("codex_cli_simplified_flow")).toBe("true")
      expect(url.searchParams.get("id_token_add_organizations")).toBe("true")
      expect(redirectUri.port).toBe("1455")
      expect(redirectUri.pathname).toBe("/auth/callback")
    } finally {
      oauthTesting.reset()
      await expect(callback).resolves.toEqual({ type: "failed" })
    }
  })
})

describe("body and headers", () => {
  test("prepares API and OAuth response bodies", () => {
    process.env[OPENAI_WS_INSTALLATION_ID_ENV] = "install_1"
    const api = prepareBody(
      { stream: true, stream_options: {}, service_tier: "priority", client_metadata: { existing: "yes", ignored: 1 } },
      false,
      { sessionID: "sess_1", agent: "review", stablePrefixHash: "prefix_1" },
    )
    expect(api).toMatchObject({
      stream: true,
      store: false,
      service_tier: "priority",
      prompt_cache_key: "prefix_1",
      client_metadata: {
        existing: "yes",
        "x-codex-installation-id": "install_1",
        "x-codex-window-id": "sess_1",
        "x-openai-subagent": "review",
      },
    })
    expect(api).not.toHaveProperty("instructions")
    expect(api).not.toHaveProperty("stream_options")

    const oauth = prepareBody({ stream: true, max_output_tokens: 10, max_tokens: 10 }, true)
    expect(oauth.store).toBe(false)
    expect(oauth.stream).toBe(true)
    expect(oauth.max_output_tokens).toBe(10)
    expect(oauth.max_tokens).toBe(10)
  })

  test("builds required API key and OAuth websocket headers", () => {
    process.env.OPENAI_ORGANIZATION = "org_1"
    process.env.OPENAI_PROJECT = "proj_1"
    expect(apiKeyWebSocketHeaders("api-key-test")).toEqual({
      Authorization: "Bearer api-key-test",
      originator: CODEX_ORIGINATOR,
      "OpenAI-Beta": "responses_websockets=2026-02-06",
      "OpenAI-Organization": "org_1",
      "OpenAI-Project": "proj_1",
      "User-Agent": USER_AGENT,
    })
    expect(oauthWebSocketHeaders("access-test", "acct_1")).toEqual({
      Authorization: "Bearer access-test",
      "ChatGPT-Account-Id": "acct_1",
      originator: CODEX_ORIGINATOR,
      "OpenAI-Beta": "responses_websockets=2026-02-06",
      "User-Agent": USER_AGENT,
    })
  })
})

describe("models", () => {
  test("curated fallback models match official visible websocket models", () => {
    const resolved = resolveModels()
    expect(Object.keys(resolved).sort()).toEqual(["gpt-5.2", "gpt-5.3-codex", "gpt-5.4", "gpt-5.4-mini", "gpt-5.5"])
    expect(resolved["gpt-5.5"].limit).toMatchObject({ context: 1050000, output: 128000 })
    expect(resolved["gpt-5.4"].limit).toMatchObject({ context: 1050000, output: 128000 })
    expect(resolved["gpt-5.4-mini"].limit).toMatchObject({ context: 400000, output: 128000 })
    expect(resolved["gpt-5.3-codex"].limit).toMatchObject({ context: 400000, output: 128000 })
    expect(resolved["gpt-5.2"].limit).toMatchObject({ context: 400000, output: 128000 })
    expect(resolved["gpt-5.5"].variants).toHaveProperty("none")
    expect(resolved["gpt-5.4"].variants).toHaveProperty("none")
    expect(resolved["gpt-5.4-mini"].variants).toHaveProperty("none")
    expect(resolved["gpt-5.2"].variants).toHaveProperty("none")
    expect(resolved["gpt-5.3-codex"].variants).not.toHaveProperty("none")
    expect(resolved["gpt-5.4-mini"].variants).toHaveProperty("xhigh")
    for (const model of Object.values(resolved)) expect(model.variants).not.toHaveProperty("minimal")
  })

  test("preserves user overrides last", () => {
    const [modelID] = Object.keys(resolveModels())
    const resolved = resolveModels({
      [modelID]: {
        name: "Custom GPT",
        limit: { context: 1, output: 2 },
      },
    })
    expect(resolved[modelID].name).toBe("Custom GPT")
    expect(resolved[modelID].limit).toEqual({ context: 1, output: 2 })
    expect(resolved[modelID].providerID).toBe("openai-ws")
  })

  test("resolves OAuth models from the Codex catalog", () => {
    const resolved = resolveModelsForOAuth([
      {
        slug: "gpt-5.4-codex",
        display_name: "GPT-5.4 Codex",
        context_window: 123456,
        auto_compact_token_limit: 64000,
        prefer_websockets: true,
        supports_reasoning_summaries: true,
        supported_reasoning_levels: [{ effort: "low" }, { effort: "medium" }, { effort: "high" }],
      },
      {
        slug: "non-ws-model",
        display_name: "Hidden",
        context_window: 1000,
        prefer_websockets: false,
      },
    ])
    expect(resolved["gpt-5.4-codex"]).toMatchObject({
      providerID: "openai-ws",
      name: "GPT-5.4 Codex (WebSocket)",
      family: "gpt-codex",
      limit: { context: 123456, output: 64000 },
    })
    expect(resolved["gpt-5.4-codex"].variants.medium).toMatchObject({ reasoningEffort: "medium", reasoningSummary: "auto" })
    expect(resolved["non-ws-model"]).toBeUndefined()
  })

  test("uses Codex catalog limits instead of bundled setup limits for OAuth models", () => {
    const resolved = resolveModelsForOAuth(
      [
        {
          slug: "gpt-5.5",
          display_name: "GPT-5.5",
          context_window: 272000,
          auto_compact_token_limit: null,
          prefer_websockets: true,
          supported_reasoning_levels: [{ effort: "low" }, { effort: "medium" }, { effort: "high" }, { effort: "xhigh" }],
        },
      ],
      {
        "gpt-5.5": {
          limit: { context: 1050000, output: 128000 },
          name: "Bundled setup GPT-5.5",
        },
      },
    )

    expect(resolved["gpt-5.5"].name).toBe("GPT-5.5 (WebSocket)")
    expect(resolved["gpt-5.5"].limit).toEqual({ context: 272000, output: 128000 })
  })

  test("falls back to bundled models when the Codex catalog is unavailable or empty", () => {
    expect(Object.keys(resolveModelsForOAuth(undefined)).sort()).toEqual(Object.keys(resolveModels()).sort())
    expect(Object.keys(resolveModelsForOAuth([])).sort()).toEqual(Object.keys(resolveModels()).sort())
  })

  test("filters API-key models to the OpenAI model id list and synthesizes candidate matches", () => {
    const resolved = resolveModelsForApiKey(new Set(["gpt-5.5", "gpt-5.99-mini", "unrelated-model"]))
    expect(Object.keys(resolved).sort()).toEqual(["gpt-5.5", "gpt-5.99-mini"])
    expect(resolved["gpt-5.5"].limit).toMatchObject({ context: 1050000, output: 128000 })
    expect(resolved["gpt-5.99-mini"].limit).toMatchObject({ context: 272000, output: 128000 })
  })

  test("fetches the Codex catalog with OAuth headers", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ "dist-tags": { alpha: "0.131.0-alpha.4" }, version: "0.130.0" }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ models: [{ slug: "gpt-5.4-codex", context_window: 1 }] }), { status: 200 }))
    const catalog = await fetchCodexCatalog({ accessToken: "access-test", accountId: "acct_1", fetchImpl })
    expect(fetchImpl.mock.calls[0][0]).toBe("https://registry.npmjs.org/@openai/codex")
    const [url, init] = fetchImpl.mock.calls[1] as [URL, RequestInit]
    expect(url.toString()).toBe("https://chatgpt.com/backend-api/codex/models?client_version=0.131.0-alpha.4")
    expect(init.headers).toMatchObject({
      Authorization: "Bearer access-test",
      "ChatGPT-Account-Id": "acct_1",
      originator: CODEX_ORIGINATOR,
      "OpenAI-Beta": "responses_websockets=2026-02-06",
      "User-Agent": USER_AGENT,
    })
    expect(catalog).toEqual([{ slug: "gpt-5.4-codex", context_window: 1 }])
  })

  test("resolves Codex client_version from the npm alpha dist-tag", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(new Response(JSON.stringify({ "dist-tags": { alpha: "0.131.0-alpha.4" }, version: "0.130.0" }), { status: 200 }))
    await expect(resolveCodexClientVersion({ fetchImpl })).resolves.toBe("0.131.0-alpha.4")
    expect(fetchImpl).toHaveBeenCalledWith(
      "https://registry.npmjs.org/@openai/codex",
      expect.objectContaining({ headers: expect.objectContaining({ Accept: "application/json", "User-Agent": USER_AGENT }) }),
    )
  })

  test("falls back to package version when Codex client_version metadata is unavailable", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(new Response("nope", { status: 500 }))
    await expect(resolveCodexClientVersion({ fetchImpl })).resolves.toBe("0.1.9")
    expect(fallbackCodexClientVersion("codex-rs/0.131.0-alpha.4")).toBe("0.131.0-alpha.4")
  })

  test("fetches OpenAI model ids and returns undefined on non-200 responses", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ data: [{ id: "gpt-5.5" }, { id: "other" }] }), { status: 200 }))
      .mockResolvedValueOnce(new Response("nope", { status: 500 }))
    const ids = await fetchOpenAIModelIds({ apiKey: "api-key-test", fetchImpl })
    expect(ids).toEqual(new Set(["gpt-5.5", "other"]))
    expect(fetchImpl.mock.calls[0][0]).toBe("https://api.openai.com/v1/models")
    expect(fetchImpl.mock.calls[0][1].headers).toMatchObject({
      Authorization: "Bearer api-key-test",
      "User-Agent": USER_AGENT,
    })
    const failed = await fetchOpenAIModelIds({ apiKey: "other-key", fetchImpl })
    expect(failed).toBeUndefined()
  })
})

describe("plugin auth loader", () => {
  test("registers provider models and bridges API key responses through websocket", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          data: [{ id: "gpt-5.5" }, { id: "gpt-5.88" }],
        }),
        { status: 200 },
      ),
    )
    const hooks = await plugin({ client: { auth: { set: vi.fn() } } } as any)
    const provider = { models: {} }
    const loaded = await hooks.auth?.loader?.(async () => ({ type: "api", key: "api-key-test" }) as any, provider as any)
    const modelID = Object.keys(provider.models)[0]
    expect((provider.models as any)[modelID].providerID).toBe("openai-ws")
    expect((provider.models as any)["gpt-5.88"]).toBeDefined()
    expect((provider.models as any)["gpt-5.4"]).toBeUndefined()
    expect(loaded?.baseURL).toBe("https://api.openai.com/v1")

    const response = await loaded?.fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      body: JSON.stringify({ model: modelID, input: "hi", stream: true }),
    })
    expect(response?.headers.get("content-type")).toContain("text/event-stream")

    const ws = MockWebSocket.instances[0]
    expect(ws.url).toBe("wss://api.openai.com/v1/responses")
    expect(ws.options.headers).toEqual({
      Authorization: "Bearer api-key-test",
      originator: CODEX_ORIGINATOR,
      "OpenAI-Beta": "responses_websockets=2026-02-06",
      "User-Agent": USER_AGENT,
    })
    ws.open()
    expect(JSON.parse(ws.sent[0])).toMatchObject({ type: "response.create", model: modelID })
  })

  test("refreshes OAuth auth inside fetch instead of freezing loader token", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: any) => {
      const url = String(input)
      if (url.includes("/backend-api/codex/models")) {
        return new Response(JSON.stringify({ models: [] }), { status: 200 })
      }
      if (url.includes("/oauth/token")) {
        return new Response(
          JSON.stringify({
            access_token: "fresh-access",
            refresh_token: "refresh-new",
            expires_in: 3600,
          }),
          { status: 200 },
        )
      }
      return new Response("unexpected", { status: 500 })
    })
    const setAuth = vi.fn()
    const hooks = await plugin({ client: { auth: { set: setAuth } } } as any)
    const loaded = await hooks.auth?.loader?.(
      async () => ({ type: "oauth", refresh: "refresh-old", access: "stale-access", expires: Date.now() - 1 }) as any,
      { models: {} } as any,
    )

    await loaded?.fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      body: JSON.stringify({ model: "gpt-5.5", input: "hi", stream: true }),
    })

    expect(setAuth).toHaveBeenCalled()
    expect(MockWebSocket.instances[0].options.headers.Authorization).toBe("Bearer fresh-access")
  })
})

describe("oauth", () => {
  test("rejects callback state mismatch", async () => {
    const browser = await (await import("../src/auth/oauth.js")).createBrowserAuthorization()
    const response = {
      writeHead: vi.fn(),
      end: vi.fn(),
    }
    await oauthTesting.handleCallback({ url: "/auth/callback?code=abc&state=wrong" } as any, response as any)
    await expect(browser.callback()).resolves.toEqual({ type: "failed" })
    expect(response.writeHead).toHaveBeenCalledWith(400, expect.any(Object))
  })
})

describe("websocket bridge", () => {
  test("sends response.create with ws headers", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      oauthWebSocketHeaders("access-test", "acct_1"),
      { model: "gpt-5.3-codex", input: "hi", stream: true },
      true,
      { sessionID: "sess_1", agent: "primary" },
    )
    expect(response.headers.get("content-type")).toContain("text/event-stream")
    const ws = MockWebSocket.instances[0]
    expect(ws.options.headers).toMatchObject({
      Authorization: "Bearer access-test",
      "ChatGPT-Account-Id": "acct_1",
      originator: CODEX_ORIGINATOR,
      "OpenAI-Beta": "responses_websockets=2026-02-06",
      "User-Agent": USER_AGENT,
      session_id: "sess_1",
      "session-id": "sess_1",
      thread_id: "sess_1",
      "thread-id": "sess_1",
      "x-codex-window-id": "sess_1",
    })
    expect(ws.options.perMessageDeflate).toBe(true)
    ws.open()
    expect(JSON.parse(ws.sent[0])).toMatchObject({
      type: "response.create",
      model: "gpt-5.3-codex",
      input: "hi",
      store: false,
      stream: true,
    })
  })

  test("captures turn-state upgrade metadata and sends it on the next turn", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const first = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
      { sessionID: "sess_1", agent: "review", stablePrefixHash: "prefix_1" },
    )
    const firstWs = MockWebSocket.instances[0]
    firstWs.upgrade({
      "x-codex-turn-state": "turn_1",
      "x-models-etag": "etag_1",
      "x-reasoning-included": "true",
      "openai-model": "gpt-5.5",
    })
    firstWs.open()
    expect(firstWs.options.headers).toMatchObject({
      session_id: "sess_1",
      "x-codex-window-id": "sess_1",
      "x-openai-subagent": "review",
    })
    expect(firstWs.options.headers).not.toHaveProperty("x-codex-turn-state")
    firstWs.serverMessage({ type: "response.completed", response: { id: "resp_1" } })
    const firstReader = first.body!.getReader()
    while (!(await firstReader.read()).done) {}
    expect(firstWs.terminateCount).toBeGreaterThan(0)

    const second = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "again", stream: true },
      false,
      { sessionID: "sess_1", agent: "review", stablePrefixHash: "prefix_1" },
    )
    const secondWs = MockWebSocket.instances[1]
    expect(secondWs.options.headers).toMatchObject({
      "x-codex-turn-state": "turn_1",
      "x-codex-window-id": "sess_1",
      "x-openai-subagent": "review",
    })
    secondWs.open()
    expect(JSON.parse(secondWs.sent[0])).toMatchObject({
      type: "response.create",
      previous_response_id: "resp_1",
      prompt_cache_key: "prefix_1",
      client_metadata: {
        "x-codex-window-id": "sess_1",
        "x-openai-subagent": "review",
      },
    })
    secondWs.serverMessage({ type: "response.completed", response: { id: "resp_2" } })
    const secondReader = second.body!.getReader()
    while (!(await secondReader.read()).done) {}
  })

  test("can acknowledge completed responses with response.processed", async () => {
    process.env[RESPONSE_PROCESSED_ENV] = "1"
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const ws = MockWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: "response.completed", response: { id: "resp_done" } })
    const reader = response.body!.getReader()
    while (!(await reader.read()).done) {}
    expect(ws.sent.map((value) => JSON.parse(value))).toEqual([
      expect.objectContaining({ type: "response.create" }),
      { type: "response.processed", response_id: "resp_done" },
    ])
  })

  test("maps wrapped websocket errors to stream errors", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const reader = response.body!.getReader()
    const ws = MockWebSocket.instances[0]
    ws.open()
    ws.serverMessage({
      type: "error",
      status: 429,
      error: { code: "usage_limit_reached", message: "usage limit reached" },
    })
    await expect(reader.read()).rejects.toThrow(/OpenAI WebSocket error 429 usage_limit_reached: usage limit reached/)
  })

  test("rejects unexpected binary websocket events", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const reader = response.body!.getReader()
    const ws = MockWebSocket.instances[0]
    ws.open()
    ws.serverBinary()
    await expect(reader.read()).rejects.toThrow(/unexpected binary websocket event/)
  })

  test("does not log secret-like values", () => {
    const log = vi.spyOn(console, "log")
    const warn = vi.spyOn(console, "warn")
    const error = vi.spyOn(console, "error")
    apiKeyWebSocketHeaders("api-key-secret")
    oauthWebSocketHeaders("access-secret", "acct-secret")
    expect(log).not.toHaveBeenCalled()
    expect(warn).not.toHaveBeenCalled()
    expect(error).not.toHaveBeenCalled()
  })

  test("does not replay response.create after the websocket closes", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const reader = response.body!.getReader()
    const first = MockWebSocket.instances[0]
    first.open()
    expect(first.sent).toHaveLength(1)
    first.close()

    await expect(reader.read()).rejects.toThrow(/response\.create was already sent/)
    expect(MockWebSocket.instances).toHaveLength(1)
  })

  test("errors after close following streamed data instead of fabricating incomplete", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const ws = MockWebSocket.instances[0]
    ;(ws as any).ping = undefined
    ws.open()
    ws.serverMessage({
      type: "response.created",
      sequence_number: 0,
      response: {
        id: "resp_1",
        model: "gpt-5.5",
        service_tier: null,
        usage: {
          input_tokens: 1,
          output_tokens: 0,
          total_tokens: 1,
          input_tokens_details: { cached_tokens: 0 },
          output_tokens_details: { reasoning_tokens: 0 },
        },
      },
    })
    ws.serverMessage({ type: "response.output_text.delta", sequence_number: 1, delta: "hi" })

    const reader = response.body!.getReader()
    const first = await reader.read()
    const second = await reader.read()
    expect(new TextDecoder().decode(first.value)).toContain("response.created")
    expect(new TextDecoder().decode(second.value)).toContain("response.output_text.delta")

    ws.emit("close", 1006, Buffer.from("abnormal"))

    await expect(reader.read()).rejects.toThrow(/response\.create was already sent.*closeCode=1006.*closeReason="abnormal"/s)
    expect(MockWebSocket.instances).toHaveLength(1)
  })

  test("errors after replay-safe response.created only", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const first = MockWebSocket.instances[0]
    first.open()
    first.serverMessage({ type: "response.created", sequence_number: 0, response: { id: "resp_1" } })

    const reader = response.body!.getReader()
    const created = await reader.read()
    expect(new TextDecoder().decode(created.value)).toContain("response.created")

    first.emit("close", 1006, Buffer.from("abnormal"))
    await expect(reader.read()).rejects.toThrow(/response\.create was already sent/)
    expect(MockWebSocket.instances).toHaveLength(1)
  })

  test("retries only before response.create has been sent", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const reader = response.body!.getReader()

    for (let attempt = 0; attempt < 4; attempt++) {
      const ws = MockWebSocket.instances[attempt]
      expect(ws).toBeDefined()
      ws.emit("close", 1011, Buffer.from("server_error"))
      if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, 450))
    }

    await expect(reader.read()).rejects.toThrow(
      /retry limit reached before response\.create was sent.*reconnectAttempts=3\/3.*closeCode=1011/s,
    )
    expect(MockWebSocket.instances).toHaveLength(4)
  })

  test("evicts stale idle connections before reuse", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const first = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const firstWs = MockWebSocket.instances[0]
    firstWs.open()
    firstWs.serverMessage({ type: "response.completed" })
    const firstReader = first.body!.getReader()
    while (!(await firstReader.read()).done) {}
    expect(connectionPool).toHaveLength(1)
    expect(connectionPool[0].busy).toBe(false)

    connectionPool[0].lastActivityAt = 0

    bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hello", stream: true },
      false,
    )
    expect(MockWebSocket.instances).toHaveLength(2)
    expect(firstWs.readyState).toBe(3)
  })

  test("release schedules heartbeat when websocket supports ping", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const ws = MockWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: "response.completed" })
    const reader = response.body!.getReader()
    while (!(await reader.read()).done) {}

    expect(connectionPool).toHaveLength(1)
    expect(connectionPool[0].heartbeatTimer).not.toBeNull()
    expect(connectionPool[0].pongTimer).toBeNull()
  })

  test("active pending heartbeat pings and pong timeout errors the stream", async () => {
    vi.useFakeTimers()
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const ws = MockWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: "response.output_text.delta", sequence_number: 1, delta: "hi" })
    const reader = response.body!.getReader()
    await reader.read()

    vi.advanceTimersByTime(30_000)
    await Promise.resolve()
    expect(ws.pingCount).toBeGreaterThan(0)
    vi.advanceTimersByTime(10_000)
    await Promise.resolve()

    await expect(reader.read()).rejects.toThrow(/response\.create was already sent.*closeReason="pong timeout"/s)
    expect(ws.terminateCount).toBeGreaterThan(0)
  })

  test("replay-unsafe tool frame idle timeout errors the stream", async () => {
    vi.useFakeTimers()
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const ws = MockWebSocket.instances[0]
    ;(ws as any).ping = undefined
    ws.open()
    ws.serverMessage({
      type: "response.output_item.done",
      sequence_number: 3,
      item: { type: "function_call", name: "morph-mcp_edit_file", call_id: "call_1" },
      response_id: "resp_tool",
    })
    const reader = response.body!.getReader()
    expect(new TextDecoder().decode((await reader.read()).value)).toContain("morph-mcp_edit_file")

    vi.advanceTimersByTime(90_000)
    await Promise.resolve()

    await expect(reader.read()).rejects.toThrow(/response idle timeout after response\.output_item\.done/)
    expect(ws.terminateCount).toBeGreaterThan(0)
  })

  test("idle timeout errors when no frames arrive after response.create", async () => {
    vi.useFakeTimers()
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const ws = MockWebSocket.instances[0]
    ;(ws as any).ping = undefined
    ws.open()
    const reader = response.body!.getReader()

    for (let elapsed = 0; elapsed < 90_000; elapsed += 30_000) {
      vi.advanceTimersByTime(30_000)
      await Promise.resolve()
      ws.pong()
    }

    await expect(reader.read()).rejects.toThrow(/response idle timeout after response\.create/)
    expect(ws.terminateCount).toBeGreaterThan(0)
  })

  test("replay-safe frames refresh the idle timeout but still error without completion", async () => {
    vi.useFakeTimers()
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const ws = MockWebSocket.instances[0]
    ;(ws as any).ping = undefined
    ws.open()
    ws.serverMessage({ type: "response.created", sequence_number: 0, response: { id: "resp_safe" } })
    const reader = response.body!.getReader()
    expect(new TextDecoder().decode((await reader.read()).value)).toContain("response.created")

    vi.advanceTimersByTime(30_000)
    await Promise.resolve()
    ws.pong()
    vi.advanceTimersByTime(30_000)
    await Promise.resolve()
    ws.pong()
    vi.advanceTimersByTime(29_999)
    await Promise.resolve()
    ws.serverMessage({ type: "response.in_progress", sequence_number: 1, response: { id: "resp_safe" } })
    expect(new TextDecoder().decode((await reader.read()).value)).toContain("response.in_progress")
    vi.advanceTimersByTime(30_000)
    await Promise.resolve()
    ws.pong()
    vi.advanceTimersByTime(30_000)
    await Promise.resolve()
    ws.pong()
    vi.advanceTimersByTime(29_999)
    await Promise.resolve()
    let pending = false
    const pendingRead = reader.read().then((result) => {
      pending = false
      return result
    })
    pending = true
    await Promise.resolve()
    expect(pending).toBe(true)

    vi.advanceTimersByTime(1)
    await Promise.resolve()

    await expect(pendingRead).rejects.toThrow(/response idle timeout after response\.in_progress/)
  })

  test("terminal event clears response idle timeout", async () => {
    vi.useFakeTimers()
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const ws = MockWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: "response.output_text.delta", sequence_number: 1, delta: "hi" })
    ws.serverMessage({ type: "response.completed", sequence_number: 2, response: { id: "resp_done" } })
    const reader = response.body!.getReader()
    let sawCompleted = false
    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      if (new TextDecoder().decode(value).includes("response.completed")) sawCompleted = true
    }
    expect(sawCompleted).toBe(true)
    expect(connectionPool[0].pending).toBeNull()
  })

  test("completed stream finalization prevents old abort from canceling reused connections", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const firstController = new AbortController()
    const first = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
      {},
      firstController.signal,
    )
    const ws = MockWebSocket.instances[0]
    ws.open()
    ws.serverMessage({ type: "response.completed", response: { id: "resp_done" } })
    const firstReader = first.body!.getReader()
    while (!(await firstReader.read()).done) {}
    expect(connectionPool).toHaveLength(1)
    expect(connectionPool[0].busy).toBe(false)

    const second = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "again", stream: true },
      false,
    )
    expect(MockWebSocket.instances).toHaveLength(1)
    expect(ws.sent).toHaveLength(2)

    firstController.abort()
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(connectionPool[0].pending).not.toBeNull()
    expect(ws.sent.map((value) => JSON.parse(value).type)).toEqual(["response.create", "response.create"])

    ws.serverMessage({ type: "response.completed", response: { id: "resp_done_2" } })
    const secondReader = second.body!.getReader()
    let sawCompleted = false
    while (true) {
      const { value, done } = await secondReader.read()
      if (done) break
      if (new TextDecoder().decode(value).includes("response.completed")) sawCompleted = true
    }
    expect(sawCompleted).toBe(true)
  })

  test("active abort sends response.cancel and closes the stream", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const controller = new AbortController()
    const response = bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
      {},
      controller.signal,
    )
    const ws = MockWebSocket.instances[0]
    ws.open()
    const reader = response.body!.getReader()

    controller.abort()

    await expect(reader.read()).rejects.toThrow(/aborted/i)
    expect(ws.sent.map((value) => JSON.parse(value).type)).toEqual(["response.create", "response.cancel"])
    expect(ws.terminateCount).toBeGreaterThan(0)
  })

  test("live websocket integration sends upstream-shaped headers and body across the wire", async () => {
    if (typeof (globalThis as { Bun?: { listen?: unknown } }).Bun?.listen !== "function") return
    const received: WireTurn[] = []
    const server = listenWireWebSocket(received)

    try {
      const readAll = async (response: Response) => {
        const reader = response.body!.getReader()
        let text = ""
        while (true) {
          const { value, done } = await reader.read()
          if (done) break
          text += new TextDecoder().decode(value)
        }
        return text
      }

      const first = bridgeWebSocket(
        `ws://127.0.0.1:${server.port}/responses`,
        apiKeyWebSocketHeaders("api-key-test"),
        { model: "gpt-5.5", input: "hi", stream: true },
        false,
        { sessionID: "sess_live", agent: "review", stablePrefixHash: "prefix_live" },
      )
      const firstText = await readAll(first)
      const second = bridgeWebSocket(
        `ws://127.0.0.1:${server.port}/responses`,
        apiKeyWebSocketHeaders("api-key-test"),
        { model: "gpt-5.5", input: "again", stream: true },
        false,
        { sessionID: "sess_live", agent: "review", stablePrefixHash: "prefix_live" },
      )
      const secondText = await readAll(second)

      expect(received).toHaveLength(2)
      expect(received[0].headers).toMatchObject({
        authorization: "Bearer api-key-test",
        originator: CODEX_ORIGINATOR,
        "openai-beta": "responses_websockets=2026-02-06",
        "user-agent": USER_AGENT,
        session_id: "sess_live",
        "x-codex-window-id": "sess_live",
        "x-openai-subagent": "review",
      })
      expect(received[0].headers).not.toHaveProperty("x-codex-turn-state")
      expect(received[1].headers).toMatchObject({ "x-codex-turn-state": "turn_live" })
      expect(received[0].body).toMatchObject({
        type: "response.create",
        model: "gpt-5.5",
        input: "hi",
        store: false,
        stream: true,
        prompt_cache_key: "prefix_live",
        client_metadata: {
          "x-codex-window-id": "sess_live",
          "x-openai-subagent": "review",
        },
      })
      expect(received[1].body).toMatchObject({ type: "response.create", previous_response_id: "resp_live_hi" })
      expect(firstText).toContain("response.completed")
      expect(secondText).toContain("response.completed")
    } finally {
      resetPoolForTesting()
      server.stop(true)
    }
  })

  test("concurrent streams are bounded and queued per scope", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const responses = Array.from({ length: 5 }, (_, index) =>
      bridgeWebSocket(
        "wss://example.test/responses",
        apiKeyWebSocketHeaders("api-key-test"),
        { model: "gpt-5.5", input: `hi ${index}`, stream: true },
        false,
        { sessionID: "sess_1", agent: "agent" },
      ),
    )
    expect(MockWebSocket.instances).toHaveLength(4)
    for (const ws of MockWebSocket.instances) ws.open()
    expect(MockWebSocket.instances.every((ws) => ws.sent.length === 1)).toBe(true)

    MockWebSocket.instances[0].serverMessage({ type: "response.completed", response: { id: "resp_done" } })
    const reader = responses[0].body!.getReader()
    while (!(await reader.read()).done) {}
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(MockWebSocket.instances).toHaveLength(4)
    expect(MockWebSocket.instances.some((ws) => ws.sent.length === 2)).toBe(true)
  })

  test("clears delayed reconnect after abort before any frame is forwarded", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const controller = new AbortController()
    bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
      {},
      controller.signal,
    )
    const first = MockWebSocket.instances[0]
    first.close()
    controller.abort()
    await new Promise((resolve) => setTimeout(resolve, 150))
    expect(MockWebSocket.instances).toHaveLength(1)
  })

  test("canceling a queued stream settles without waiting for acquisition", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    const responses = Array.from({ length: 5 }, (_, index) =>
      bridgeWebSocket(
        "wss://example.test/responses",
        apiKeyWebSocketHeaders("api-key-test"),
        { model: "gpt-5.5", input: `hi ${index}`, stream: true },
        false,
        { sessionID: "sess_1", agent: "agent" },
      ),
    )
    expect(MockWebSocket.instances).toHaveLength(4)
    const reader = responses[4].body!.getReader()

    await reader.cancel()
    await new Promise((resolve) => setTimeout(resolve, 0))

    MockWebSocket.instances[0].open()
    MockWebSocket.instances[0].serverMessage({ type: "response.completed", response: { id: "resp_done" } })
    const firstReader = responses[0].body!.getReader()
    while (!(await firstReader.read()).done) {}
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(MockWebSocket.instances).toHaveLength(4)
    expect(MockWebSocket.instances.every((ws) => ws.sent.length <= 1)).toBe(true)
  })
})
