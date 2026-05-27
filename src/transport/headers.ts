import {
  CODEX_ORIGINATOR,
  CODEX_WS_URL,
  INTERNAL_AGENT_HEADER,
  INTERNAL_MODEL_HEADER,
  INTERNAL_PREFIX_HASH_HEADER,
  INTERNAL_SESSION_HEADER,
  OPENAI_WS_BETA,
  OPENAI_WS_URL,
  USER_AGENT,
  type CustomProviderConfig,
} from "../constants.js"

export type TransportContext = {
  sessionID?: string
  agent?: string
  modelID?: string
  stablePrefixHash?: string
}

function defaultHeaders(): Record<string, string> {
  return {
    originator: CODEX_ORIGINATOR,
    "OpenAI-Beta": OPENAI_WS_BETA,
    "User-Agent": USER_AGENT,
  }
}

function organizationProjectHeaders(): Record<string, string> {
  return {
    ...(process.env.OPENAI_ORGANIZATION ? { "OpenAI-Organization": process.env.OPENAI_ORGANIZATION } : {}),
    ...(process.env.OPENAI_PROJECT ? { "OpenAI-Project": process.env.OPENAI_PROJECT } : {}),
  }
}

export function apiKeyWebSocketHeaders(apiKey: string): Record<string, string> {
  return {
    ...defaultHeaders(),
    ...organizationProjectHeaders(),
    Authorization: `Bearer ${apiKey}`,
  }
}

export function oauthWebSocketHeaders(accessToken: string, accountId?: string): Record<string, string> {
  return {
    ...defaultHeaders(),
    Authorization: `Bearer ${accessToken}`,
    ...(accountId ? { "ChatGPT-Account-Id": accountId } : {}),
  }
}

export function httpAuthHeaders(
  baseHeaders: HeadersInit | undefined,
  auth: { type: "api"; apiKey: string } | { type: "oauth"; accessToken: string; accountId?: string },
): Headers {
  const headers = new Headers(baseHeaders)
  headers.delete("authorization")
  headers.delete("Authorization")
  headers.set("Authorization", `Bearer ${auth.type === "api" ? auth.apiKey : auth.accessToken}`)
  for (const [key, value] of Object.entries(defaultHeaders())) headers.set(key, value)
  if (auth.type === "api") {
    for (const [key, value] of Object.entries(organizationProjectHeaders())) headers.set(key, value)
  }
  if (auth.type === "oauth") {
    if (auth.accountId) headers.set("ChatGPT-Account-Id", auth.accountId)
  }
  return headers
}

export function transportIdentity(auth: { type: "api"; apiKey: string } | { type: "oauth"; accessToken: string; accountId?: string }) {
  if (auth.type === "oauth") {
    return {
      isOAuth: true,
      wsUrl: CODEX_WS_URL,
      wsHeaders: oauthWebSocketHeaders(auth.accessToken, auth.accountId),
    }
  }
  return {
    isOAuth: false,
    wsUrl: OPENAI_WS_URL,
    wsHeaders: apiKeyWebSocketHeaders(auth.apiKey),
  }
}

export function extractTransportContext(input?: HeadersInit): TransportContext & { forwardHeaders: Headers } {
  const forwardHeaders = new Headers(input)
  const sessionID = forwardHeaders.get(INTERNAL_SESSION_HEADER) ?? undefined
  const agent = forwardHeaders.get(INTERNAL_AGENT_HEADER) ?? undefined
  const modelID = forwardHeaders.get(INTERNAL_MODEL_HEADER) ?? undefined
  const stablePrefixHash = forwardHeaders.get(INTERNAL_PREFIX_HASH_HEADER) ?? undefined

  forwardHeaders.delete(INTERNAL_SESSION_HEADER)
  forwardHeaders.delete(INTERNAL_AGENT_HEADER)
  forwardHeaders.delete(INTERNAL_MODEL_HEADER)
  forwardHeaders.delete(INTERNAL_PREFIX_HASH_HEADER)

  return { sessionID, agent, modelID, stablePrefixHash, forwardHeaders }
}

export function customProviderWebSocketHeaders(apiKey: string, customHeaders?: Record<string, string>): Record<string, string> {
  return {
    ...defaultHeaders(),
    ...organizationProjectHeaders(),
    Authorization: `Bearer ${apiKey}`,
    ...customHeaders,
  }
}

export function customProviderTransportIdentity(config: CustomProviderConfig, apiKey: string): {
  isOAuth: boolean
  wsUrl: string
  wsHeaders: Record<string, string>
} {
  return {
    isOAuth: false,
    wsUrl: config.ws,
    wsHeaders: customProviderWebSocketHeaders(apiKey, config.headers),
  }
}
