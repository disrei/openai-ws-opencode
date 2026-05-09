import crypto from "node:crypto"
import { CODEX_CLI_NPM_ENDPOINT, CODEX_MODELS_ENDPOINT, CODEX_ORIGINATOR, OPENAI_MODELS_ENDPOINT, OPENAI_WS_BETA, USER_AGENT } from "../constants.js"

const DEFAULT_TIMEOUT_MS = 800
const CATALOG_TTL_MS = 30 * 60 * 1000
const SKIP_CATALOG_ENV = "OPENAI_WS_OPENCODE_SKIP_CATALOG"

type CacheEntry<T> = {
  expiresAt: number
  value: T | undefined
}

const cache = new Map<string, CacheEntry<unknown>>()

export type CodexReasoningLevel = {
  effort?: string
  description?: string
}

export type CodexModelInfo = {
  slug?: string
  display_name?: string
  description?: string
  context_window?: number
  max_context_window?: number
  effective_context_window_percent?: number
  supported_in_api?: boolean
  priority?: number
  supports_reasoning_summaries?: boolean
  support_verbosity?: boolean
  default_verbosity?: string
  default_reasoning_level?: string
  supported_reasoning_levels?: CodexReasoningLevel[]
  auto_compact_token_limit?: number | null
  prefer_websockets?: boolean
  input_modalities?: string[]
  available_in_plans?: string[]
  base_instructions?: string
  raw?: Record<string, unknown>
}

type FetchOptions = {
  fetchImpl?: typeof fetch
  timeoutMs?: number
}

type CodexCatalogOptions = FetchOptions & {
  accessToken: string
  accountId?: string
}

type OpenAIModelsOptions = FetchOptions & {
  apiKey: string
}

function cacheKey(scope: string, credential: string, accountId = ""): string {
  return `${scope}:${crypto.createHash("sha256").update(`${credential}:${accountId}`).digest("hex")}`
}

function cached<T>(key: string): { hit: true; value: T | undefined } | { hit: false } {
  const entry = cache.get(key) as CacheEntry<T> | undefined
  if (!entry) return { hit: false }
  if (entry.expiresAt <= Date.now()) {
    cache.delete(key)
    return { hit: false }
  }
  return { hit: true, value: entry.value }
}

function setCached<T>(key: string, value: T | undefined): T | undefined {
  cache.set(key, { expiresAt: Date.now() + CATALOG_TTL_MS, value })
  return value
}

function timeoutSignal(timeoutMs: number): AbortSignal | undefined {
  return typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function" ? AbortSignal.timeout(timeoutMs) : undefined
}

export function fallbackCodexClientVersion(userAgent = USER_AGENT): string {
  return userAgent.includes("/") ? userAgent.split("/").at(-1) || userAgent : userAgent
}

export async function resolveCodexClientVersion(options: FetchOptions = {}): Promise<string> {
  if (process.env[SKIP_CATALOG_ENV] === "1") return fallbackCodexClientVersion()
  const key = "codex-cli-alpha-version"
  const existing = cached<string>(key)
  if (existing.hit && existing.value) return existing.value

  const fetchImpl = options.fetchImpl ?? globalThis.fetch
  try {
    const response = await fetchImpl(CODEX_CLI_NPM_ENDPOINT, {
      signal: timeoutSignal(options.timeoutMs ?? DEFAULT_TIMEOUT_MS),
      headers: { Accept: "application/json", "User-Agent": USER_AGENT },
    })
    if (!response.ok) return setCached(key, fallbackCodexClientVersion()) ?? fallbackCodexClientVersion()
    const json = (await response.json()) as { "dist-tags"?: { alpha?: unknown }; version?: unknown }
    const version = typeof json["dist-tags"]?.alpha === "string" ? json["dist-tags"].alpha : typeof json.version === "string" ? json.version : undefined
    return setCached(key, version ?? fallbackCodexClientVersion()) ?? fallbackCodexClientVersion()
  } catch {
    return setCached(key, fallbackCodexClientVersion()) ?? fallbackCodexClientVersion()
  }
}

export async function fetchCodexCatalog(options: CodexCatalogOptions): Promise<CodexModelInfo[] | undefined> {
  if (process.env[SKIP_CATALOG_ENV] === "1") return undefined
  const key = cacheKey("codex", options.accessToken, options.accountId)
  const existing = cached<CodexModelInfo[]>(key)
  if (existing.hit) return existing.value

  const fetchImpl = options.fetchImpl ?? globalThis.fetch
  const url = new URL(CODEX_MODELS_ENDPOINT)
  url.searchParams.set("client_version", await resolveCodexClientVersion({ fetchImpl, timeoutMs: options.timeoutMs }))
  try {
    const response = await fetchImpl(url, {
      signal: timeoutSignal(options.timeoutMs ?? DEFAULT_TIMEOUT_MS),
      headers: {
        Authorization: `Bearer ${options.accessToken}`,
        ...(options.accountId ? { "ChatGPT-Account-Id": options.accountId } : {}),
        originator: CODEX_ORIGINATOR,
        "OpenAI-Beta": OPENAI_WS_BETA,
        "User-Agent": USER_AGENT,
      },
    })
    if (!response.ok) return setCached(key, undefined)
    const json = (await response.json()) as { models?: unknown }
    if (!Array.isArray(json.models)) return setCached(key, undefined)
    return setCached(key, json.models as CodexModelInfo[])
  } catch {
    return setCached(key, undefined)
  }
}

export async function fetchOpenAIModelIds(options: OpenAIModelsOptions): Promise<Set<string> | undefined> {
  if (process.env[SKIP_CATALOG_ENV] === "1") return undefined
  const key = cacheKey("openai", options.apiKey)
  const existing = cached<Set<string>>(key)
  if (existing.hit) return existing.value

  const fetchImpl = options.fetchImpl ?? globalThis.fetch
  try {
    const response = await fetchImpl(OPENAI_MODELS_ENDPOINT, {
      signal: timeoutSignal(options.timeoutMs ?? DEFAULT_TIMEOUT_MS),
      headers: {
        Authorization: `Bearer ${options.apiKey}`,
        "User-Agent": USER_AGENT,
      },
    })
    if (!response.ok) return setCached(key, undefined)
    const json = (await response.json()) as { data?: Array<{ id?: unknown }> }
    if (!Array.isArray(json.data)) return setCached(key, undefined)
    return setCached(
      key,
      new Set(json.data.map((model) => (typeof model.id === "string" ? model.id : undefined)).filter((id): id is string => Boolean(id))),
    )
  } catch {
    return setCached(key, undefined)
  }
}

export function resetCatalogCacheForTesting(): void {
  cache.clear()
}
