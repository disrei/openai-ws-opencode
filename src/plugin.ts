import type { Hooks, Plugin } from "@opencode-ai/plugin"
import {
  CODEX_API_BASE,
  CODEX_API_ENDPOINT,
  INTERNAL_AGENT_HEADER,
  INTERNAL_MODEL_HEADER,
  INTERNAL_RESET_PREVIOUS_RESPONSE_HEADER,
  INTERNAL_SESSION_HEADER,
  OPENAI_API_BASE,
  OPENAI_WS_URL,
  PROVIDER_ID,
  CUSTOM_PROVIDER_ID,
  type CustomProviderConfig,
} from "./constants.js"
import { oauthMethods } from "./auth/oauth.js"
import { extractAccountId, refreshAccessToken, tokenExpiry, type StoredOAuthAuth } from "./auth/tokens.js"
import { wsLog } from "./log.js"
import { fetchCodexCatalog, fetchOpenAIModelIds } from "./models/catalog.js"
import { CODEX_EFFECTIVE_CONTEXT_WINDOW, CODEX_OUTPUT_TOKEN_LIMIT, OPENAI_WS_MODELS } from "./models/defaults.js"
import { resolveModelsForApiKey, resolveModelsForOAuth } from "./models/resolve.js"
import { prepareHttpFallbackBody } from "./transport/body.js"
import { bridgeWebSocket } from "./transport/bridge.js"
import { closeConnections, ensureWarmConnection, invalidateStaleAuthConnections } from "./transport/pool.js"
import { extractTransportContext, httpAuthHeaders, transportIdentity, customProviderTransportIdentity } from "./transport/headers.js"

const providerModelRefreshVersion = new WeakMap<object, number>()

function nextProviderModelRefreshVersion(provider: object): number {
  const version = (providerModelRefreshVersion.get(provider) ?? 0) + 1
  providerModelRefreshVersion.set(provider, version)
  return version
}

function refreshProviderModelsInBackground(
  provider: any,
  load: () => Promise<Record<string, unknown> | undefined>,
): void {
  if (!provider || typeof provider !== "object") return
  const version = nextProviderModelRefreshVersion(provider)
  void (async () => {
    try {
      const models = await load()
      if (models && providerModelRefreshVersion.get(provider) === version) provider.models = models as any
    } catch {}
  })()
}

type ApiAuth = { type: "api"; key?: string }
type OAuthAuth = StoredOAuthAuth
type OpenAIWSAuth = ApiAuth | OAuthAuth | undefined

const DEFAULT_BUNDLED_LIMIT = {
  context: CODEX_EFFECTIVE_CONTEXT_WINDOW,
  input: CODEX_EFFECTIVE_CONTEXT_WINDOW,
  output: CODEX_OUTPUT_TOKEN_LIMIT,
}

function shouldResetPreviousResponseForMessage(message: unknown): boolean {
  if (!message || typeof message !== "object" || Array.isArray(message)) return false
  const parts = (message as { parts?: unknown }).parts
  if (!Array.isArray(parts)) return false
  return parts.some((part) => {
    if (!part || typeof part !== "object" || Array.isArray(part)) return false
    const value = part as { type?: unknown; metadata?: Record<string, unknown> }
    return value.type === "text" && value.metadata?.compaction_continue === true
  })
}

function bundledLimitFor(modelID: unknown): { context: number; input?: number; output: number } {
  if (typeof modelID === "string") {
    const match = OPENAI_WS_MODELS[modelID]
    if (match?.limit?.context) return match.limit
  }
  return DEFAULT_BUNDLED_LIMIT
}

function isPositiveFinite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
}

async function resolveOAuthAuth(auth: OAuthAuth, client: any): Promise<{ accessToken: string; accountId?: string }> {
  const expiryBufferMs = 5 * 60 * 1000
  if (auth.access && auth.expires && auth.expires > Date.now() + expiryBufferMs) {
    return { accessToken: auth.access, accountId: auth.accountId }
  }

  const tokens = await refreshAccessToken(auth.refresh)
  const accountId = extractAccountId(tokens) ?? auth.accountId
  await client.auth.set({
    path: { id: PROVIDER_ID },
    body: {
      type: "oauth",
      refresh: tokens.refresh_token ?? auth.refresh,
      access: tokens.access_token,
      expires: tokenExpiry(tokens.expires_in),
      ...(accountId ? { accountId } : {}),
    },
  })
  return { accessToken: tokens.access_token, accountId }
}

function hasFreshOAuthAccess(auth: OAuthAuth): auth is OAuthAuth & { access: string; expires: number } {
  const expiryBufferMs = 5 * 60 * 1000
  return typeof auth.access === "string" && auth.access.length > 0 && typeof auth.expires === "number" && auth.expires > Date.now() + expiryBufferMs
}

function shouldBridge(url: URL, init?: RequestInit, customWsUrl?: string): boolean {
  if (init?.method?.toUpperCase() !== "POST" || typeof init.body !== "string") return false
  if (customWsUrl) return true
  return url.pathname.includes("/v1/responses") || url.pathname.includes("/backend-api/codex/responses")
}

function extractCustomWsConfig(provider: any): CustomProviderConfig | undefined {
  if (!provider?.options) return undefined
  const ws = provider.options.ws
  if (typeof ws !== "string" || !ws) return undefined
  wsLog(`[openai-ws] Custom WebSocket URL detected: ${ws}`)
  return {
    name: provider.name ?? "Custom WebSocket",
    api: provider.api ?? OPENAI_API_BASE,
    ws,
    npm: provider.npm ?? "@ai-sdk/openai",
    models: provider.models,
    headers: typeof provider.options.wsHeaders === "object" ? provider.options.wsHeaders : undefined,
  }
}

function extractApiKeyFromOptions(provider: any): string | undefined {
  if (!provider?.options) return undefined
  const key = provider.options.apiKey
  if (typeof key === "string" && key) return key
  return undefined
}

const OpenAIWebSocketPlugin: Plugin = async ({ client }) => {
  wsLog("=== Plugin initialized ===")
  const pendingPreviousResponseResetSessions = new Set<string>()
  const hooks: Hooks = {
    "chat.headers": async (input, output) => {
      if (input.model.providerID !== CUSTOM_PROVIDER_ID) return
      const headers = (output.headers ??= {})
      headers[INTERNAL_SESSION_HEADER] = input.sessionID
      headers[INTERNAL_AGENT_HEADER] = input.agent
      const modelID = input.model.id ?? (input.model as any).modelID
      headers[INTERNAL_MODEL_HEADER] = modelID
      if (pendingPreviousResponseResetSessions.delete(input.sessionID) || shouldResetPreviousResponseForMessage(input.message)) {
        headers[INTERNAL_RESET_PREVIOUS_RESPONSE_HEADER] = "1"
      }
    },

    "chat.params": async (input, _output) => {
      if (input.model.providerID !== CUSTOM_PROVIDER_ID) return
      const model = input.model as { limit?: { context?: number; input?: number; output?: number } }
      const bundledLimit = bundledLimitFor(input.model.id ?? (input.model as any).modelID)
      const current = model.limit ?? {}
      const context = isPositiveFinite(current.context) ? current.context : bundledLimit.context
      const inputCandidate = isPositiveFinite(current.input)
        ? current.input
        : isPositiveFinite(bundledLimit.input)
          ? bundledLimit.input
          : bundledLimit.context
      const output = isPositiveFinite(current.output) ? current.output : bundledLimit.output
      model.limit = {
        context,
        input: Math.min(inputCandidate, context),
        output,
      }
    },

    event: async ({ event }) => {
      if (event.type === "session.deleted") {
        const deletedSessionID = (event as any).properties?.info?.id
        if (typeof deletedSessionID === "string") pendingPreviousResponseResetSessions.delete(deletedSessionID)
        closeConnections((conn) => conn.activeSessionID === deletedSessionID || conn.lastSessionID === deletedSessionID)
        return
      }
      const eventType = String(event.type)
      if (eventType === "session.next.compaction.ended" && typeof (event as any).sessionID === "string") {
        pendingPreviousResponseResetSessions.add((event as any).sessionID)
        return
      }
      if (eventType === "server.instance.disposed" || eventType === "global.disposed") {
        pendingPreviousResponseResetSessions.clear()
        closeConnections()
      }
    },

    auth: {
      provider: CUSTOM_PROVIDER_ID,
      async loader(getAuth, provider) {
        wsLog("=== auth.loader called ===")
        const auth = (await getAuth()) as OpenAIWSAuth
        const customWs = extractCustomWsConfig(provider)
        const configApiKey = extractApiKeyFromOptions(provider)

        if (auth?.type === "api" && auth.key) {
          const apiKey = auth.key
          const baseURL = customWs?.api ?? OPENAI_API_BASE

          if (!customWs && provider) {
            refreshProviderModelsInBackground(provider, async () => {
              const allowedIds = await fetchOpenAIModelIds({ apiKey })
              return resolveModelsForApiKey(allowedIds, provider.models as any) as Record<string, unknown>
            })
          }

          const identity = customWs
            ? customProviderTransportIdentity(customWs, apiKey)
            : transportIdentity({ type: "api", apiKey })
          invalidateStaleAuthConnections(identity.wsUrl, identity.wsHeaders)
          ensureWarmConnection(identity.wsUrl, identity.wsHeaders)
          return {
            apiKey,
            baseURL,
            async fetch(input: RequestInfo | URL, init?: RequestInit) {
              wsLog("=== custom auth fetch called ===")
              const url = typeof input === "string" ? new URL(input) : input instanceof URL ? input : new URL(input.url)
              const context = extractTransportContext(init?.headers)
              if (shouldBridge(url, init, customWs?.ws)) {
                try {
                  const body = JSON.parse(init?.body as string) as Record<string, unknown>
                  if (body.stream !== false) {
                    return bridgeWebSocket(identity.wsUrl, identity.wsHeaders, body, false, context, init?.signal ?? undefined, (fallbackSignal) =>
                      globalThis.fetch(input, {
                        ...init,
                        signal: fallbackSignal,
                        body: JSON.stringify(prepareHttpFallbackBody(body, false)),
                        headers: httpAuthHeaders(context.forwardHeaders, { type: "api", apiKey }),
                      }),
                    )
                  }
                } catch {}
              }

              if (init?.method?.toUpperCase() === "POST" && typeof init.body === "string") {
                try {
                  init = { ...init, body: JSON.stringify(prepareHttpFallbackBody(JSON.parse(init.body), false)) }
                } catch {}
              }
              return globalThis.fetch(input, {
                ...init,
                headers: httpAuthHeaders(context.forwardHeaders, { type: "api", apiKey }),
              })
            },
          }
        }

        if (configApiKey) {
          const apiKey = configApiKey
          const baseURL = customWs?.api ?? OPENAI_API_BASE

          if (!customWs && provider) {
            refreshProviderModelsInBackground(provider, async () => {
              const allowedIds = await fetchOpenAIModelIds({ apiKey })
              return resolveModelsForApiKey(allowedIds, provider.models as any) as Record<string, unknown>
            })
          }

          const identity = customWs
            ? customProviderTransportIdentity(customWs, apiKey)
            : transportIdentity({ type: "api", apiKey })
          wsLog(`[openai-ws] Auth from config. WebSocket URL: ${identity.wsUrl}, API Base: ${baseURL}`)
          invalidateStaleAuthConnections(identity.wsUrl, identity.wsHeaders)
          ensureWarmConnection(identity.wsUrl, identity.wsHeaders)
          return {
            apiKey,
            baseURL,
            async fetch(input: RequestInfo | URL, init?: RequestInit) {
              const url = typeof input === "string" ? new URL(input) : input instanceof URL ? input : new URL(input.url)
              const context = extractTransportContext(init?.headers)
              if (shouldBridge(url, init, customWs?.ws)) {
                try {
                  const body = JSON.parse(init?.body as string) as Record<string, unknown>
                  if (body.stream !== false) {
                    wsLog(`[openai-ws] >>> Using WebSocket for streaming: ${identity.wsUrl}`)
                    return bridgeWebSocket(identity.wsUrl, identity.wsHeaders, body, false, context, init?.signal ?? undefined, (fallbackSignal) =>
                      globalThis.fetch(input, {
                        ...init,
                        signal: fallbackSignal,
                        body: JSON.stringify(prepareHttpFallbackBody(body, false)),
                        headers: httpAuthHeaders(context.forwardHeaders, { type: "api", apiKey }),
                      }),
                    )
                  }
                } catch {}
              }

              wsLog(`[openai-ws] >>> Using HTTP fallback for non-streaming request`)
              if (init?.method?.toUpperCase() === "POST" && typeof init.body === "string") {
                try {
                  init = { ...init, body: JSON.stringify(prepareHttpFallbackBody(JSON.parse(init.body), false)) }
                } catch {}
              }
              return globalThis.fetch(input, {
                ...init,
                headers: httpAuthHeaders(context.forwardHeaders, { type: "api", apiKey }),
              })
            },
          }
        }

        if (!customWs && auth?.type === "oauth") {
          if (provider) {
            refreshProviderModelsInBackground(provider, async () => {
              const currentAuth = await resolveOAuthAuth(auth, client)
              const catalog = await fetchCodexCatalog({ accessToken: currentAuth.accessToken, accountId: currentAuth.accountId })
              return resolveModelsForOAuth(catalog, provider.models as any) as Record<string, unknown>
            })
          }
          if (hasFreshOAuthAccess(auth)) {
            const initialIdentity = transportIdentity({ type: "oauth", accessToken: auth.access, accountId: auth.accountId })
            invalidateStaleAuthConnections(initialIdentity.wsUrl, initialIdentity.wsHeaders)
            ensureWarmConnection(initialIdentity.wsUrl, initialIdentity.wsHeaders)
          }
          return {
            apiKey: auth.access ?? "",
            baseURL: CODEX_API_BASE,
            async fetch(input: RequestInfo | URL, init?: RequestInit) {
              const currentAuth = (await getAuth()) as OAuthAuth | undefined
              if (currentAuth?.type !== "oauth") throw new Error("OpenAI WebSocket OAuth auth is missing")
              const { accessToken, accountId } = await resolveOAuthAuth(currentAuth, client)
              const identity = transportIdentity({ type: "oauth", accessToken, accountId })
              invalidateStaleAuthConnections(identity.wsUrl, identity.wsHeaders)
              const url = typeof input === "string" ? new URL(input) : input instanceof URL ? input : new URL(input.url)
              const context = extractTransportContext(init?.headers)
              if (shouldBridge(url, init)) {
                try {
                  const body = JSON.parse(init?.body as string) as Record<string, unknown>
                  if (body.stream !== false) {
                    const rewrittenUrl = url.pathname.includes("/v1/responses") ? new URL(CODEX_API_ENDPOINT) : url
                    return bridgeWebSocket(identity.wsUrl, identity.wsHeaders, body, true, context, init?.signal ?? undefined, (fallbackSignal) =>
                      globalThis.fetch(rewrittenUrl, {
                        ...init,
                        signal: fallbackSignal,
                        body: JSON.stringify(prepareHttpFallbackBody(body, true)),
                        headers: httpAuthHeaders(context.forwardHeaders, { type: "oauth", accessToken, accountId }),
                      }),
                    )
                  }
                } catch {}
              }

              if (init?.method?.toUpperCase() === "POST" && typeof init.body === "string") {
                try {
                  init = { ...init, body: JSON.stringify(prepareHttpFallbackBody(JSON.parse(init.body), true)) }
                } catch {}
              }

              const rewrittenUrl = url.pathname.includes("/v1/responses") ? new URL(CODEX_API_ENDPOINT) : url
              return globalThis.fetch(rewrittenUrl, {
                ...init,
                headers: httpAuthHeaders(context.forwardHeaders, { type: "oauth", accessToken, accountId }),
              })
            },
          }
        }

        const baseURL = customWs?.api ?? OPENAI_API_BASE
        return {
          apiKey: "",
          baseURL,
          async fetch(input: RequestInfo | URL, init?: RequestInit) {
            throw new Error("OpenAI WebSocket auth is missing; run `opencode auth login openai-ws`.")
          },
        }
      },
      methods: oauthMethods,
    },
  }

  return hooks
}

export default OpenAIWebSocketPlugin

