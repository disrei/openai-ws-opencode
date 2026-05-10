import {
  OPENAI_WS_INSTALLATION_ID_ENV,
  X_CODEX_INSTALLATION_ID_HEADER,
  X_CODEX_WINDOW_ID_HEADER,
  X_OPENAI_SUBAGENT_HEADER,
} from "../constants.js"
import type { TransportContext } from "./headers.js"

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

export function prepareBody(
  requestBody: Record<string, unknown>,
  isOAuth: boolean,
  context: TransportContext = {},
): Record<string, unknown> {
  const { stream_options: _streamOptions, ...wsBody } = requestBody

  if (!wsBody.instructions) wsBody.instructions = "You are a helpful assistant."
  if (wsBody.input !== undefined) wsBody.input = normalizeInput(wsBody.input)
  if (wsBody.store === undefined) wsBody.store = false
  if (wsBody.stream === undefined) wsBody.stream = true
  if (isOAuth) {
    delete wsBody.max_output_tokens
    delete wsBody.max_tokens
  }
  if (wsBody.prompt_cache_key === undefined && context.stablePrefixHash) wsBody.prompt_cache_key = context.stablePrefixHash
  const clientMetadata = mergeClientMetadata(wsBody, context)
  if (clientMetadata) wsBody.client_metadata = clientMetadata

  return wsBody
}

export function prepareHttpFallbackBody(requestBody: Record<string, unknown>, _isOAuth: boolean): Record<string, unknown> {
  const next = { ...requestBody }
  if (next.store === undefined) next.store = false
  return next
}
