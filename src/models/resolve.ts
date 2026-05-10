import { OPENAI_API_BASE, PROVIDER_ID } from "../constants.js"
import type { CodexModelInfo } from "./catalog.js"
import { codexLimit, makeVariants, OPENAI_WS_MODELS, type OpenAIWSModelDef } from "./defaults.js"

export type ProviderModelConfig = {
  id: string
  providerID: string
  api: { id: string; npm: string; url: string }
  name: string
  capabilities: {
    temperature: boolean
    reasoning: boolean
    attachment: boolean
    toolcall: boolean
    input: { text: boolean; audio: boolean; image: boolean; video: boolean; pdf: boolean }
    output: { text: boolean; audio: boolean; image: boolean; video: boolean; pdf: boolean }
    interleaved: boolean
  }
  cost: { input: number; output: number; cache: { read: number; write: number } }
  limit: OpenAIWSModelDef["limit"]
  status: "active"
  options: Record<string, unknown>
  headers: Record<string, string>
  variants: Record<string, Record<string, unknown>>
  family?: string
  release_date?: string
  [key: string]: unknown
}

export type ProviderModelOverrides = Record<string, Partial<ProviderModelConfig>>

export type OpenCodeConfigModel = {
  name: string
  family?: string
  release_date?: string
  attachment: boolean
  reasoning: boolean
  temperature: boolean
  tool_call: boolean
  limit: OpenAIWSModelDef["limit"]
  modalities: {
    input: Array<"text" | "image">
    output: Array<"text">
  }
  cost: { input: number; output: number; cache_read: number; cache_write: number }
  options: Record<string, unknown>
  provider: { npm: string; api: string }
  variants: Record<string, Record<string, unknown>>
}

export function modelToProviderConfig(id: string, model: OpenAIWSModelDef): ProviderModelConfig {
  return {
    id,
    providerID: PROVIDER_ID,
    api: { id, npm: "@ai-sdk/openai", url: OPENAI_API_BASE },
    name: model.name,
    capabilities: {
      temperature: model.temperature,
      reasoning: model.reasoning,
      attachment: true,
      toolcall: true,
      input: { text: true, audio: false, image: true, video: false, pdf: false },
      output: { text: true, audio: false, image: false, video: false, pdf: false },
      interleaved: false,
    },
    cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
    limit: { ...model.limit },
    status: "active",
    options: {},
    headers: {},
    variants: model.variants,
    ...(model.family ? { family: model.family } : {}),
    ...(model.release_date ? { release_date: model.release_date } : {}),
  }
}

function variantsForModel(id: string, model: Pick<OpenAIWSModelDef, "reasoning" | "family">): Record<string, Record<string, unknown>> {
  if (!model.reasoning) return {}
  if (id.endsWith("-pro") || model.family === "gpt-pro") return makeVariants(["high"], false)
  if (id.includes("codex")) return makeVariants(["low", "medium", "high", "xhigh"], false)
  return makeVariants(["low", "medium", "high", "xhigh"], false)
}

function isOpenAIWSCandidate(id: string): boolean {
  return /^gpt-5(?:\.\d+)?(?:-(?:codex(?:-spark)?|pro|mini|nano|chat-latest))?$/.test(id)
}

function fallbackModelFor(id: string): OpenAIWSModelDef {
  const result: OpenAIWSModelDef = {
    name: `${id} (WebSocket)`,
    reasoning: true,
    temperature: false,
    limit: codexLimit(),
    variants: {},
    ...(id.includes("codex") ? { family: "gpt-codex" } : id.endsWith("-pro") ? { family: "gpt-pro" } : {}),
  }
  result.variants = variantsForModel(id, result)
  return result
}

function modelFromCodexCatalog(model: CodexModelInfo): [string, OpenAIWSModelDef] | undefined {
  const id = model.slug
  if (!id) return undefined
  if (model.prefer_websockets !== true && !isOpenAIWSCandidate(id)) return undefined
  const efforts = (model.supported_reasoning_levels ?? [])
    .map((level) => level.effort)
    .filter((effort): effort is string => Boolean(effort))
  const reasoning = efforts.length > 0
  const result: OpenAIWSModelDef = {
    name: `${model.display_name ?? id} (WebSocket)`,
    reasoning,
    temperature: false,
    limit: codexLimit(),
    variants: reasoning ? makeVariants(efforts, Boolean(model.supports_reasoning_summaries)) : {},
    ...(id.includes("codex") ? { family: "gpt-codex" } : id.endsWith("-pro") ? { family: "gpt-pro" } : {}),
  }
  return [id, result]
}

function toProviderModels(
  models: Record<string, OpenAIWSModelDef>,
  overrides: ProviderModelOverrides = {},
  options: { authoritativeModels?: boolean } = {},
): Record<string, ProviderModelConfig> {
  const providerModels: Record<string, ProviderModelConfig> = {}
  for (const [id, model] of Object.entries(models)) {
    const resolved = modelToProviderConfig(id, model)
    providerModels[id] = options.authoritativeModels ? { ...(overrides[id] ?? {}), ...resolved } : { ...resolved, ...(overrides[id] ?? {}) }
  }
  for (const [id, override] of Object.entries(overrides)) {
    if (providerModels[id]) continue
    providerModels[id] = {
      ...modelToProviderConfig(id, {
        name: typeof override.name === "string" ? override.name : `${id} (WebSocket)`,
        reasoning: Boolean(override.capabilities?.reasoning),
        temperature: Boolean(override.capabilities?.temperature),
        limit: override.limit ?? { context: 128000, output: 16384 },
        variants: override.variants ?? {},
      }),
      ...override,
    }
  }
  return providerModels
}

export function resolveModels(overrides: ProviderModelOverrides = {}): Record<string, ProviderModelConfig> {
  return toProviderModels({ ...OPENAI_WS_MODELS }, overrides)
}

export function resolveModelsForOAuth(
  catalog: CodexModelInfo[] | undefined,
  overrides: ProviderModelOverrides = {},
): Record<string, ProviderModelConfig> {
  if (!catalog?.length) return resolveModels(overrides)
  const models: Record<string, OpenAIWSModelDef> = {}
  for (const model of catalog) {
    const resolved = modelFromCodexCatalog(model)
    if (resolved) models[resolved[0]] = resolved[1]
  }
  if (Object.keys(models).length === 0) return resolveModels(overrides)
  return toProviderModels(models, overrides, { authoritativeModels: true })
}

export function resolveModelsForApiKey(
  allowedIds: Set<string> | undefined,
  overrides: ProviderModelOverrides = {},
): Record<string, ProviderModelConfig> {
  if (!allowedIds) return resolveModels(overrides)
  const models: Record<string, OpenAIWSModelDef> = {}
  for (const [id, model] of Object.entries(OPENAI_WS_MODELS)) {
    if (allowedIds.has(id)) models[id] = model
  }
  for (const id of allowedIds) {
    if (!models[id] && isOpenAIWSCandidate(id)) models[id] = fallbackModelFor(id)
  }
  if (Object.keys(models).length === 0) {
    return resolveModels(overrides)
  }
  return toProviderModels(models, overrides)
}

export function modelToOpenCodeConfig(model: OpenAIWSModelDef): OpenCodeConfigModel {
  return {
    name: model.name,
    ...(model.family ? { family: model.family } : {}),
    ...(model.release_date ? { release_date: model.release_date } : {}),
    attachment: true,
    reasoning: model.reasoning,
    temperature: model.temperature,
    tool_call: true,
    limit: { ...model.limit },
    modalities: {
      input: ["text", "image"],
      output: ["text"],
    },
    cost: { input: 0, output: 0, cache_read: 0, cache_write: 0 },
    options: {},
    provider: { npm: "@ai-sdk/openai", api: OPENAI_API_BASE },
    variants: model.variants,
  }
}

const GENERATED_CONFIG_MODEL_KEYS = new Set([
  "name",
  "family",
  "release_date",
  "attachment",
  "reasoning",
  "temperature",
  "tool_call",
  "limit",
  "modalities",
  "cost",
  "provider",
  "variants",
])

const LEGACY_PLUGIN_MODEL_IDS = new Set(["gpt-5.5-pro", "gpt-5.3-codex-spark", "gpt-5.2-codex", "gpt-5.1-codex"])

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function sanitizeBuiltInConfigOverride(override: unknown): Record<string, unknown> {
  if (!isRecord(override)) return {}
  const sanitized = { ...override }
  for (const key of GENERATED_CONFIG_MODEL_KEYS) delete sanitized[key]
  return sanitized
}

export function providerConfigModels(overrides: Record<string, unknown> = {}) {
  const models: Record<string, unknown> = {}
  for (const [id, model] of Object.entries(OPENAI_WS_MODELS)) {
    models[id] = {
      ...modelToOpenCodeConfig(model),
      ...sanitizeBuiltInConfigOverride(overrides[id]),
    }
  }
  for (const [id, override] of Object.entries(overrides)) {
    if (models[id]) continue
    if (LEGACY_PLUGIN_MODEL_IDS.has(id)) continue
    models[id] = override
  }
  return models
}

export function providerConfig(overrides: ProviderModelOverrides = {}) {
  return {
    api: OPENAI_API_BASE,
    name: "OpenAI WebSocket",
    npm: "@ai-sdk/openai",
    models: providerConfigModels(overrides as Record<string, unknown>),
  }
}
