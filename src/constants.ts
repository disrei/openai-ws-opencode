export const PROVIDER_ID = "openai-ws"

export const CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
export const ISSUER = "https://auth.openai.com"
export const OPENAI_API_BASE = "https://api.openai.com/v1"
export const CODEX_API_BASE = "https://chatgpt.com/backend-api/codex"
export const CODEX_API_ENDPOINT = `${CODEX_API_BASE}/responses`
export const CODEX_MODELS_ENDPOINT = `${CODEX_API_BASE}/models`
export const CODEX_CLI_NPM_ENDPOINT = "https://registry.npmjs.org/@openai/codex"
export const OPENAI_MODELS_ENDPOINT = `${OPENAI_API_BASE}/models`
export const OPENAI_WS_URL = "wss://api.openai.com/v1/responses"
export const CODEX_WS_URL = "wss://chatgpt.com/backend-api/codex/responses"
export const OPENAI_WS_BETA = "responses_websockets=2026-02-06"
export const CODEX_ORIGINATOR = "opencode"
export const USER_AGENT = "openai-ws-opencode/0.1.20"
export const CODEX_OAUTH_SCOPE = "openid profile email offline_access"
export const OAUTH_PORT = 1455

export const X_CODEX_TURN_STATE_HEADER = "x-codex-turn-state"
export const X_CODEX_WINDOW_ID_HEADER = "x-codex-window-id"
export const X_CODEX_INSTALLATION_ID_HEADER = "x-codex-installation-id"
export const X_OPENAI_SUBAGENT_HEADER = "x-openai-subagent"
export const X_MODELS_ETAG_HEADER = "x-models-etag"
export const X_REASONING_INCLUDED_HEADER = "x-reasoning-included"
export const OPENAI_MODEL_HEADER = "openai-model"
export const OPENAI_WS_INSTALLATION_ID_ENV = "OPENAI_WS_OPENCODE_INSTALLATION_ID"
export const BACKGROUND_ORCHESTRATION_ENV = "OPENAI_WS_OPENCODE_BACKGROUND_ORCHESTRATION"
export const RESPONSE_PROCESSED_ENV = "OPENAI_WS_OPENCODE_RESPONSE_PROCESSED"
export const RESPONSE_PROCESSED_DISABLE_ENV = "OPENAI_WS_OPENCODE_RESPONSE_PROCESSED_DISABLE"

export const INTERNAL_SESSION_HEADER = "x-openai-ws-opencode-session-id"
export const INTERNAL_AGENT_HEADER = "x-openai-ws-opencode-agent"
export const INTERNAL_MODEL_HEADER = "x-openai-ws-opencode-model-id"
export const INTERNAL_PREFIX_HASH_HEADER = "x-openai-ws-opencode-prefix-hash"
