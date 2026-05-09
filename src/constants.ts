export const PROVIDER_ID = "openai-ws"

export const CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
export const ISSUER = "https://auth.openai.com"
export const OPENAI_API_BASE = "https://api.openai.com/v1"
export const CODEX_API_BASE = "https://chatgpt.com/backend-api/codex"
export const CODEX_API_ENDPOINT = `${CODEX_API_BASE}/responses`
export const OPENAI_WS_URL = "wss://api.openai.com/v1/responses"
export const CODEX_WS_URL = "wss://chatgpt.com/backend-api/codex/responses"
export const OPENAI_WS_BETA = "responses_websockets=2026-02-06"
export const CODEX_ORIGINATOR = "opencode"
export const CODEX_OAUTH_SCOPE = "openid profile email offline_access"
export const OAUTH_PORT = 1455
export const DEFAULT_INSTRUCTIONS = "You are a helpful assistant."

export const INTERNAL_SESSION_HEADER = "x-openai-ws-opencode-session-id"
export const INTERNAL_AGENT_HEADER = "x-openai-ws-opencode-agent"
export const INTERNAL_MODEL_HEADER = "x-openai-ws-opencode-model-id"
export const INTERNAL_PREFIX_HASH_HEADER = "x-openai-ws-opencode-prefix-hash"
