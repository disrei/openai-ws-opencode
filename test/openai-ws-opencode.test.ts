import { EventEmitter } from "node:events"
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { describe, expect, test, afterEach, vi } from "vitest"
import plugin from "../src/index.js"
import { createBrowserAuthorization } from "../src/auth/oauth.js"
import { CLIENT_ID, CODEX_ORIGINATOR } from "../src/constants.js"
import {
  apiKeyWebSocketHeaders,
  bridgeWebSocket,
  oauthTesting,
  oauthWebSocketHeaders,
  prepareBody,
  resetPoolForTesting,
  resetWebSocketConstructorForTesting,
  resolveModels,
  resolveModelsFromCatalog,
  setWebSocketConstructorForTesting,
} from "../src/testing.js"
import { isDirectExecution, patchConfigText, setupOpenCodeConfig } from "../bin/setup.ts"

class MockWebSocket extends EventEmitter {
  static instances: MockWebSocket[] = []
  readyState = 0
  sent: string[] = []
  url: string
  options: { headers: Record<string, string> }

  constructor(url: string, options: { headers: Record<string, string> }) {
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

  open() {
    this.readyState = 1
    this.emit("open")
  }

  serverMessage(frame: unknown) {
    this.emit("message", JSON.stringify(frame))
  }
}

const originalXdgCacheHome = process.env.XDG_CACHE_HOME

afterEach(() => {
  oauthTesting.reset()
  resetPoolForTesting()
  resetWebSocketConstructorForTesting()
  MockWebSocket.instances = []
  if (originalXdgCacheHome === undefined) delete process.env.XDG_CACHE_HOME
  else process.env.XDG_CACHE_HOME = originalXdgCacheHome
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
    const api = prepareBody({ stream: true, stream_options: {}, service_tier: "priority" }, false)
    expect(api).toMatchObject({ instructions: "You are a helpful assistant.", service_tier: "priority" })
    expect(api).not.toHaveProperty("stream")
    expect(api).not.toHaveProperty("stream_options")

    const oauth = prepareBody({ stream: true, max_output_tokens: 10, max_tokens: 10 }, true)
    expect(oauth.store).toBe(false)
    expect(oauth).not.toHaveProperty("max_output_tokens")
    expect(oauth).not.toHaveProperty("max_tokens")
  })

  test("builds required API key and OAuth websocket headers", () => {
    expect(apiKeyWebSocketHeaders("api-key-test")).toEqual({
      Authorization: "Bearer api-key-test",
      "OpenAI-Beta": "responses_websockets=2026-02-06",
    })
    expect(oauthWebSocketHeaders("access-test", "acct_1")).toEqual({
      Authorization: "Bearer access-test",
      "ChatGPT-Account-Id": "acct_1",
      originator: CODEX_ORIGINATOR,
      "OpenAI-Beta": "responses_websockets=2026-02-06",
    })
  })
})

describe("models", () => {
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

  test("can augment curated models from models.dev catalog without pinning real model names", () => {
    const resolved = resolveModelsFromCatalog({
      openai: {
        models: {
          "gpt-5.99": {
            name: "GPT-5.99",
            family: "gpt",
            reasoning: true,
            temperature: false,
            limit: { context: 123456, input: 100000, output: 64000 },
            release_date: "2026-01-01",
          },
        },
      },
    })
    expect(resolved["gpt-5.99"]).toMatchObject({
      providerID: "openai-ws",
      name: "GPT-5.99 (WebSocket)",
      limit: { context: 123456, input: 100000, output: 64000 },
    })
  })
})

describe("plugin auth loader", () => {
  test("registers provider models and bridges API key responses through websocket", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          openai: {
            models: {
              "gpt-5.88": {
                name: "GPT-5.88",
                reasoning: true,
                temperature: false,
                limit: { context: 1000, output: 2000 },
              },
            },
          },
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
      "OpenAI-Beta": "responses_websockets=2026-02-06",
    })
    ws.open()
    expect(JSON.parse(ws.sent[0])).toMatchObject({ type: "response.create", model: modelID })
  })

  test("refreshes OAuth auth inside fetch instead of freezing loader token", async () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: any) => {
      const url = String(input)
      if (url.includes("models.dev")) {
        return new Response(JSON.stringify({ openai: { models: {} } }), { status: 200 })
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
    })
    ws.open()
    expect(JSON.parse(ws.sent[0])).toMatchObject({
      type: "response.create",
      model: "gpt-5.3-codex",
      input: "hi",
      store: false,
    })
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

  test("does not retry after response.create was sent before close", () => {
    setWebSocketConstructorForTesting(MockWebSocket as any)
    bridgeWebSocket(
      "wss://example.test/responses",
      apiKeyWebSocketHeaders("api-key-test"),
      { model: "gpt-5.5", input: "hi", stream: true },
      false,
    )
    const first = MockWebSocket.instances[0]
    first.open()
    expect(first.sent).toHaveLength(1)
    first.close()
    expect(MockWebSocket.instances).toHaveLength(1)
  })

  test("clears delayed reconnect after abort before response.create", async () => {
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
})
