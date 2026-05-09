import { OPENAI_API_BASE, PROVIDER_ID } from "../constants.js"
import { makeVariants, OPENAI_WS_MODELS, type OpenAIWSModelDef } from "./defaults.js"

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

type ModelsDevModel = {
  id?: string
  name?: string
  family?: string
  attachment?: boolean
  reasoning?: boolean
  temperature?: boolean
  tool_call?: boolean
  release_date?: string
  limit?: { context?: number; input?: number; output?: number }
}

type ModelsDevCatalog = {
  openai?: {
    models?: Record<string, ModelsDevModel>
  }
}

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
    limit: model.limit,
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
  if (id.includes("codex-spark")) return makeVariants(["low", "medium", "high"], false)
  if (id.includes("codex")) return makeVariants(["low", "medium", "high"])
  return makeVariants(["none", "minimal", "low", "medium", "high"])
}

function isOpenAIWSCandidate(id: string): boolean {
  return /^gpt-5(?:\.\d+)?(?:-(?:codex(?:-spark)?|pro|mini|nano|chat-latest))?$/.test(id)
}

function modelFromCatalog(id: string, model: ModelsDevModel): OpenAIWSModelDef | undefined {
  if (!isOpenAIWSCandidate(id)) return undefined
  const context = model.limit?.context
  const output = model.limit?.output
  if (!context || !output) return undefined
  const result: OpenAIWSModelDef = {
    name: `${model.name ?? id} (WebSocket)`,
    reasoning: model.reasoning ?? true,
    temperature: model.temperature ?? false,
    limit: {
      context,
      ...(model.limit?.input ? { input: model.limit.input } : {}),
      output,
    },
    variants: {},
    ...(model.family ? { family: model.family } : {}),
    ...(model.release_date ? { release_date: model.release_date } : {}),
  }
  result.variants = variantsForModel(id, result)
  return result
}

export function resolveModelsFromCatalog(
  catalog: ModelsDevCatalog | undefined,
  overrides: ProviderModelOverrides = {},
): Record<string, ProviderModelConfig> {
  const models: Record<string, OpenAIWSModelDef> = { ...OPENAI_WS_MODELS }
  for (const [id, model] of Object.entries(catalog?.openai?.models ?? {})) {
    const resolved = modelFromCatalog(id, model)
    if (resolved) models[id] = resolved
  }

  const providerModels: Record<string, ProviderModelConfig> = {}
  for (const [id, model] of Object.entries(models)) {
    providerModels[id] = {
      ...modelToProviderConfig(id, model),
      ...(overrides[id] ?? {}),
    }
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
  return resolveModelsFromCatalog(undefined, overrides)
}

export async function resolveModelsBestEffort(
  overrides: ProviderModelOverrides = {},
  options: { fetchImpl?: typeof fetch; timeoutMs?: number } = {},
): Promise<Record<string, ProviderModelConfig>> {
  if (process.env.OPENAI_WS_OPENCODE_SKIP_CATALOG === "1") return resolveModels(overrides)
  const fetchImpl = options.fetchImpl ?? globalThis.fetch
  const timeoutMs = options.timeoutMs ?? 800
  try {
    const response = await fetchImpl("https://models.dev/api.json", {
      signal: AbortSignal.timeout(timeoutMs),
      headers: { "User-Agent": "openai-ws-opencode/0.1.3" },
    })
    if (!response.ok) return resolveModels(overrides)
    return resolveModelsFromCatalog((await response.json()) as ModelsDevCatalog, overrides)
  } catch {
    return resolveModels(overrides)
  }
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
    limit: model.limit,
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

export function providerConfigModels(overrides: Record<string, unknown> = {}) {
  const models: Record<string, unknown> = {}
  for (const [id, model] of Object.entries(OPENAI_WS_MODELS)) {
    models[id] = {
      ...modelToOpenCodeConfig(model),
      ...((overrides[id] as Record<string, unknown> | undefined) ?? {}),
    }
  }
  for (const [id, override] of Object.entries(overrides)) {
    if (models[id]) continue
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
