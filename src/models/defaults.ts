export interface OpenAIWSModelDef {
  name: string
  reasoning: boolean
  temperature: boolean
  limit: { context: number; input?: number; output: number }
  variants: Record<string, Record<string, unknown>>
  family?: string
  release_date?: string
}

export function makeVariants(efforts: string[], includeSummary = true): Record<string, Record<string, unknown>> {
  return Object.fromEntries(
    efforts.map((effort) => [
      effort,
      {
        reasoningEffort: effort,
        ...(includeSummary ? { reasoningSummary: "auto" } : {}),
        include: ["reasoning.encrypted_content"],
      },
    ]),
  )
}

export const OPENAI_WS_MODELS: Record<string, OpenAIWSModelDef> = {
  "gpt-5.5": {
    name: "GPT 5.5 (WebSocket)",
    reasoning: true,
    temperature: false,
    limit: { context: 1050000, input: 922000, output: 128000 },
    variants: makeVariants(["none", "minimal", "low", "medium", "high"]),
    release_date: "2026-04-23",
  },
  "gpt-5.5-pro": {
    name: "GPT 5.5 Pro (WebSocket)",
    reasoning: true,
    temperature: false,
    limit: { context: 1050000, input: 922000, output: 128000 },
    variants: makeVariants(["high"], false),
    family: "gpt-pro",
    release_date: "2026-04-23",
  },
  "gpt-5.4": {
    name: "GPT 5.4 (WebSocket)",
    reasoning: true,
    temperature: true,
    limit: { context: 1050000, output: 128000 },
    variants: makeVariants(["none", "minimal", "low", "medium", "high"]),
    release_date: "2026-03-05",
  },
  "gpt-5.3-codex": {
    name: "GPT 5.3 Codex (WebSocket)",
    reasoning: true,
    temperature: false,
    limit: { context: 400000, input: 272000, output: 128000 },
    variants: makeVariants(["low", "medium", "high"]),
    family: "gpt-codex",
    release_date: "2026-02-05",
  },
  "gpt-5.3-codex-spark": {
    name: "GPT 5.3 Codex Spark (WebSocket)",
    reasoning: true,
    temperature: false,
    limit: { context: 128000, output: 128000 },
    variants: makeVariants(["low", "medium", "high"], false),
    family: "gpt-codex",
    release_date: "2026-03-10",
  },
  "gpt-5.2": {
    name: "GPT 5.2 (WebSocket)",
    reasoning: true,
    temperature: true,
    limit: { context: 272000, output: 128000 },
    variants: makeVariants(["none", "low", "medium", "high"]),
    release_date: "2025-12-10",
  },
  "gpt-5.2-codex": {
    name: "GPT 5.2 Codex (WebSocket)",
    reasoning: true,
    temperature: false,
    limit: { context: 272000, output: 128000 },
    variants: makeVariants(["low", "medium", "high"]),
    family: "gpt-codex",
    release_date: "2025-12-15",
  },
  "gpt-5.1-codex": {
    name: "GPT 5.1 Codex (WebSocket)",
    reasoning: true,
    temperature: false,
    limit: { context: 272000, output: 128000 },
    variants: makeVariants(["low", "medium", "high"]),
    family: "gpt-codex",
    release_date: "2025-10-01",
  },
}
