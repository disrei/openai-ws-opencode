import { DEFAULT_INSTRUCTIONS } from "../constants.js"

export function prepareBody(requestBody: Record<string, unknown>, isOAuth: boolean): Record<string, unknown> {
  const { stream: _stream, stream_options: _streamOptions, ...wsBody } = requestBody

  if (!wsBody.instructions) wsBody.instructions = DEFAULT_INSTRUCTIONS

  if (isOAuth) {
    if (wsBody.store === undefined) wsBody.store = false
    delete wsBody.max_output_tokens
    delete wsBody.max_tokens
  }

  return wsBody
}

export function prepareHttpFallbackBody(requestBody: Record<string, unknown>, isOAuth: boolean): Record<string, unknown> {
  const next = { ...requestBody }
  if (!next.instructions) next.instructions = DEFAULT_INSTRUCTIONS
  if (isOAuth && next.store === undefined) next.store = false
  return next
}
