import type { Hooks, Plugin } from "@opencode-ai/plugin"
import crypto from "node:crypto"
import {
  CODEX_API_BASE,
  CODEX_API_ENDPOINT,
  INTERNAL_AGENT_HEADER,
  INTERNAL_MODEL_HEADER,
  INTERNAL_PREFIX_HASH_HEADER,
  INTERNAL_SESSION_HEADER,
  OPENAI_API_BASE,
  PROVIDER_ID,
} from "./constants.js"
import { oauthMethods } from "./auth/oauth.js"
import { extractAccountId, refreshAccessToken, tokenExpiry, type StoredOAuthAuth } from "./auth/tokens.js"
import { fetchCodexCatalog, fetchOpenAIModelIds } from "./models/catalog.js"
import { resolveModelsForApiKey, resolveModelsForOAuth } from "./models/resolve.js"
import { prepareHttpFallbackBody } from "./transport/body.js"
import { bridgeWebSocket } from "./transport/bridge.js"
import { closeConnections, ensureWarmConnection } from "./transport/pool.js"
import { extractTransportContext, httpAuthHeaders, transportIdentity } from "./transport/headers.js"

type ApiAuth = { type: "api"; key?: string }
type OAuthAuth = StoredOAuthAuth
type OpenAIWSAuth = ApiAuth | OAuthAuth | undefined

function stableHash(value: unknown): string {
  return crypto.createHash("sha256").update(JSON.stringify(value ?? "")).digest("hex").slice(0, 32)
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

function shouldBridge(url: URL, init?: RequestInit): boolean {
  return (
    init?.method?.toUpperCase() === "POST" &&
    (url.pathname.includes("/v1/responses") || url.pathname.includes("/backend-api/codex/responses")) &&
    typeof init.body === "string"
  )
}

const OpenAIWebSocketPlugin: Plugin = async ({ client }) => {
  const hooks: Hooks = {
    "chat.headers": async (input, output) => {
      if (input.model.providerID !== PROVIDER_ID) return
      const headers = (output.headers ??= {})
      headers[INTERNAL_SESSION_HEADER] = input.sessionID
      headers[INTERNAL_AGENT_HEADER] = input.agent
      headers[INTERNAL_MODEL_HEADER] = input.model.id ?? (input.model as any).modelID
      headers[INTERNAL_PREFIX_HASH_HEADER] = stableHash(input.message)
    },

    event: async ({ event }) => {
      if (event.type === "session.deleted") {
        const deletedSessionID = (event as any).properties?.info?.id
        closeConnections((conn) => conn.activeSessionID === deletedSessionID || conn.lastSessionID === deletedSessionID)
        return
      }
      const eventType = String(event.type)
      if (eventType === "server.instance.disposed" || eventType === "global.disposed") {
        closeConnections()
      }
    },

    auth: {
      provider: PROVIDER_ID,
      async loader(getAuth, provider) {
        const auth = (await getAuth()) as OpenAIWSAuth

        if (auth?.type === "api" && auth.key) {
          const apiKey = auth.key
          if (provider) {
            const allowedIds = await fetchOpenAIModelIds({ apiKey })
            provider.models = resolveModelsForApiKey(allowedIds, provider.models as any) as any
          }
          const identity = transportIdentity({ type: "api", apiKey })
          ensureWarmConnection(identity.wsUrl, identity.wsHeaders)
          return {
            apiKey,
            baseURL: OPENAI_API_BASE,
            async fetch(input: RequestInfo | URL, init?: RequestInit) {
              const url = typeof input === "string" ? new URL(input) : input instanceof URL ? input : new URL(input.url)
              const context = extractTransportContext(init?.headers)
              if (shouldBridge(url, init)) {
                try {
                  const body = JSON.parse(init?.body as string) as Record<string, unknown>
                  if (body.stream !== false) {
                    return bridgeWebSocket(identity.wsUrl, identity.wsHeaders, body, false, context, init?.signal ?? undefined)
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

        if (auth?.type === "oauth") {
          const initialAuth = await resolveOAuthAuth(auth, client)
          if (provider) {
            const catalog = await fetchCodexCatalog({ accessToken: initialAuth.accessToken, accountId: initialAuth.accountId })
            provider.models = resolveModelsForOAuth(catalog, provider.models as any) as any
          }
          const initialIdentity = transportIdentity({ type: "oauth", accessToken: initialAuth.accessToken, accountId: initialAuth.accountId })
          ensureWarmConnection(initialIdentity.wsUrl, initialIdentity.wsHeaders)
          return {
            apiKey: initialAuth.accessToken,
            baseURL: CODEX_API_BASE,
            async fetch(input: RequestInfo | URL, init?: RequestInit) {
              const currentAuth = (await getAuth()) as OAuthAuth | undefined
              if (currentAuth?.type !== "oauth") throw new Error("OpenAI WebSocket OAuth auth is missing")
              const { accessToken, accountId } = await resolveOAuthAuth(currentAuth, client)
              const identity = transportIdentity({ type: "oauth", accessToken, accountId })
              const url = typeof input === "string" ? new URL(input) : input instanceof URL ? input : new URL(input.url)
              const context = extractTransportContext(init?.headers)
              if (shouldBridge(url, init)) {
                try {
                  const body = JSON.parse(init?.body as string) as Record<string, unknown>
                  if (body.stream !== false) {
                    return bridgeWebSocket(identity.wsUrl, identity.wsHeaders, body, true, context, init?.signal ?? undefined)
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

        throw new Error("OpenAI WebSocket auth is missing; run `opencode auth login openai-ws`.")
      },
      methods: oauthMethods,
    },
  }

  return hooks
}

export default OpenAIWebSocketPlugin
