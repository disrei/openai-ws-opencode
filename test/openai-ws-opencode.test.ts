import { EventEmitter } from "node:events"
import { describe, expect, test, afterEach, vi } from "vitest"
import plugin from "../src/index.js"
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
import { patchConfigText } from "../bin/setup.ts"

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

afterEach(() => {
  oauthTesting.reset()
  resetPoolForTesting()
  resetWebSocketConstructorForTesting()
  MockWebSocket.instances = []
  vi.restoreAllMocks()
})

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
      originator: "codex_cli_rs",
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
      originator: "codex_cli_rs",
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
