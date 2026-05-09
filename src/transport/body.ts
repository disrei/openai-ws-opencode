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

export function prepareBody(
  requestBody: Record<string, unknown>,
  _isOAuth: boolean,
  context: TransportContext = {},
): Record<string, unknown> {
  const { stream_options: _streamOptions, ...wsBody } = requestBody

  if (wsBody.store === undefined) wsBody.store = false
  if (wsBody.stream === undefined) wsBody.stream = true
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
