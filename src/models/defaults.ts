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

export const CODEX_EFFECTIVE_CONTEXT_WINDOW = 272000
export const CODEX_OUTPUT_TOKEN_LIMIT = 128000

export function codexLimit(): OpenAIWSModelDef["limit"] {
  return {
    context: CODEX_EFFECTIVE_CONTEXT_WINDOW,
    input: CODEX_EFFECTIVE_CONTEXT_WINDOW,
    output: CODEX_OUTPUT_TOKEN_LIMIT,
  }
}

export const OPENAI_WS_MODELS: Record<string, OpenAIWSModelDef> = {
  "gpt-5.5": {
    name: "GPT 5.5 (WebSocket)",
    reasoning: true,
    temperature: false,
    limit: codexLimit(),
    variants: makeVariants(["low", "medium", "high", "xhigh"], false),
    release_date: "2026-04-23",
  },
  "gpt-5.4": {
    name: "GPT 5.4 (WebSocket)",
    reasoning: true,
    temperature: true,
    limit: codexLimit(),
    variants: makeVariants(["low", "medium", "high", "xhigh"], false),
    release_date: "2026-03-05",
  },
  "gpt-5.4-mini": {
    name: "GPT 5.4 Mini (WebSocket)",
    reasoning: true,
    temperature: true,
    limit: codexLimit(),
    variants: makeVariants(["low", "medium", "high", "xhigh"], false),
    release_date: "2026-03-05",
  },
  "gpt-5.3-codex": {
    name: "GPT 5.3 Codex (WebSocket)",
    reasoning: true,
    temperature: false,
    limit: codexLimit(),
    variants: makeVariants(["low", "medium", "high", "xhigh"], false),
    family: "gpt-codex",
    release_date: "2026-02-05",
  },
  "gpt-5.2": {
    name: "GPT 5.2 (WebSocket)",
    reasoning: true,
    temperature: true,
    limit: codexLimit(),
    variants: makeVariants(["low", "medium", "high", "xhigh"]),
    release_date: "2025-12-10",
  },
}
