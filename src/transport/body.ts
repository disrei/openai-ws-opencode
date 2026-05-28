import {
  BACKGROUND_ORCHESTRATION_ENV,
  OPENAI_WS_INSTALLATION_ID_ENV,
  X_CODEX_INSTALLATION_ID_HEADER,
  X_CODEX_WINDOW_ID_HEADER,
  X_OPENAI_SUBAGENT_HEADER,
} from "../constants.js"
import crypto from "node:crypto"
import { wsLog } from "../log.js"
import type { TransportContext } from "./headers.js"
const DEFAULT_INSTRUCTIONS = "You are a helpful assistant."

function mergeClientMetadata(requestBody: Record<string, unknown>, context: TransportContext): Record<string, string> | undefined {
  const existing = requestBody.client_metadata
  const metadata =
    existing && typeof existing === "object" && !Array.isArray(existing)
      ? Object.fromEntries(
          Object.entries(existing as Record<string, unknown>).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
        )
      : {}
  const installationID = process.env[OPENAI_WS_INSTALLATION_ID_ENV]
  if (installationID) metadata[X_CODEX_INSTALLATION_ID_HEADER] = installationID
  if (context.sessionID) metadata[X_CODEX_WINDOW_ID_HEADER] = context.sessionID
  if (context.agent && context.agent !== "primary") metadata[X_OPENAI_SUBAGENT_HEADER] = context.agent
  return Object.keys(metadata).length ? metadata : undefined
}

function responsePartType(role: unknown): "input_text" | "output_text" {
  return role === "assistant" ? "output_text" : "input_text"
}

function normalizeContentPart(part: unknown, role: unknown): unknown {
  if (!part || typeof part !== "object" || Array.isArray(part)) return part
  const value = part as Record<string, unknown>
  if (value.type === "text" && typeof value.text === "string") return { ...value, type: responsePartType(role) }
  return value
}

function normalizeMessageContent(content: unknown, role: unknown): unknown {
  if (typeof content === "string") return [{ type: responsePartType(role), text: content }]
  if (Array.isArray(content)) return content.map((part) => normalizeContentPart(part, role))
  return content
}

function normalizeInput(input: unknown): unknown {
  if (typeof input === "string") {
    return [{ type: "message", role: "user", content: [{ type: "input_text", text: input }] }]
  }
  if (!Array.isArray(input)) return input
  return input.map((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return item
    const value = item as Record<string, unknown>
    if (!("role" in value) && value.type !== "message") return value
    const role = value.role ?? "user"
    return {
      ...value,
      type: value.type ?? "message",
      role,
      content: normalizeMessageContent(value.content, role),
    }
  })
}

function shouldUseBackgroundResponses(): boolean {
  return process.env[BACKGROUND_ORCHESTRATION_ENV] === "1"
}

function stableJson(value: unknown): string {
  if (value === null || value === undefined) return "null"
  if (typeof value === "string") return JSON.stringify(value)
  if (typeof value === "number" || typeof value === "boolean") return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map((item) => stableJson(item)).join(",")}]`
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b))
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${stableJson(item)}`).join(",")}}`
  }
  return JSON.stringify(String(value))
}

function promptCacheHash(value: unknown): string {
  return crypto.createHash("sha256").update(stableJson(value)).digest("hex").slice(0, 32)
}

function canonicalPromptMessage(message: Record<string, unknown>): Record<string, unknown> {
  const role = message.role
  const content = normalizeMessageContent(message.content, role)
  return {
    role,
    ...(typeof message.name === "string" ? { name: message.name } : {}),
    content,
  }
}

function leadingStablePromptMessages(input: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(input) || input.length === 0) return []
  const messages: Array<Record<string, unknown>> = []
  for (const item of input) {
    if (!item || typeof item !== "object" || Array.isArray(item)) break
    const value = item as Record<string, unknown>
    if (value.type !== "message") break
    if (value.role !== "developer" && value.role !== "system") break
    messages.push(canonicalPromptMessage(value))
  }
  return messages
}

function normalizedInstructions(requestBody: Record<string, unknown>): string | undefined {
  return typeof requestBody.instructions === "string" && requestBody.instructions !== "" ? requestBody.instructions : undefined
}

function stablePromptSeed(requestBody: Record<string, unknown>, instructions: string | undefined): Record<string, unknown> | undefined {
  const promptMessages = leadingStablePromptMessages(requestBody.input)
  const hasPromptMessages = promptMessages.length > 0
  const hasInstructionsOnlyPrefix = instructions !== undefined && instructions !== DEFAULT_INSTRUCTIONS
  if (!hasPromptMessages && !hasInstructionsOnlyPrefix) return undefined
  return {
    model: requestBody.model ?? null,
    instructions: instructions ?? null,
    include: requestBody.include ?? null,
    reasoning: requestBody.reasoning ?? null,
    text: requestBody.text ?? null,
    tool_choice: requestBody.tool_choice ?? null,
    tools: requestBody.tools ?? null,
    prompt_messages: promptMessages,
  }
}

export function prepareBody(
  requestBody: Record<string, unknown>,
  isOAuth: boolean,
  context: TransportContext = {},
): Record<string, unknown> {
  const { stream_options: _streamOptions, ...wsBody } = requestBody

  if (!wsBody.instructions) wsBody.instructions = DEFAULT_INSTRUCTIONS
  if (wsBody.input !== undefined) wsBody.input = normalizeInput(wsBody.input)
  if (wsBody.store === undefined) wsBody.store = false
  if (wsBody.stream === undefined) wsBody.stream = true
  if (wsBody.background === undefined && shouldUseBackgroundResponses()) wsBody.background = true
  delete wsBody.max_output_tokens
  delete wsBody.max_response_output_tokens
  wsLog(`prepareBody keys=${Object.keys(wsBody).sort().join(",")}`)
  if (isOAuth) {
    delete wsBody.max_tokens
  }
  if (wsBody.prompt_cache_key === undefined) {
    if (context.stablePrefixHash) {
      wsBody.prompt_cache_key = context.stablePrefixHash
    } else {
      const seed = stablePromptSeed(wsBody, normalizedInstructions(wsBody))
      if (seed) wsBody.prompt_cache_key = promptCacheHash(seed)
    }
  }
  const clientMetadata = mergeClientMetadata(wsBody, context)
  if (clientMetadata) wsBody.client_metadata = clientMetadata

  return wsBody
}

export function prepareHttpFallbackBody(requestBody: Record<string, unknown>, _isOAuth: boolean): Record<string, unknown> {
  const next = { ...requestBody }
  if (next.store === undefined) next.store = false
  return next
}
