# openai-ws-opencode

Standalone OpenCode provider plugin for routing OpenAI Responses API streaming requests over WebSocket transport.

## What it provides

- Provider id: `openai-ws`
- API-key transport: `wss://api.openai.com/v1/responses`
- ChatGPT/Codex OAuth transport: `wss://chatgpt.com/backend-api/codex/responses`
- Curated GPT/Codex model table with OpenCode variants
- Auth-aware model discovery at auth-load time: Codex backend models for ChatGPT OAuth, OpenAI `/v1/models` IDs for API keys, bundled table fallback for both
- WebSocket-to-SSE bridge so OpenCode can keep using its normal streaming path

## Install

```sh
npm install -g openai-ws-opencode
openai-ws-opencode setup --global
opencode auth login openai-ws
```

For project-local setup:

```sh
openai-ws-opencode setup --project
opencode auth login openai-ws
```

The setup command is idempotent. It adds the npm plugin entry and a fallback `provider.openai-ws` model registry to `opencode.json`, preserving existing user model overrides.

At runtime the plugin also attempts short best-effort model discovery for the selected auth mode. ChatGPT/Codex OAuth uses `https://chatgpt.com/backend-api/codex/models`; API-key auth uses `https://api.openai.com/v1/models` as an availability filter around the bundled WebSocket model table. Both paths fall back to the bundled table if discovery is unavailable.

Set `OPENAI_WS_OPENCODE_SKIP_CATALOG=1` to skip runtime catalog lookups and use only the bundled fallback table plus your config overrides.

## Manual config

If you do not want to use the setup command, add this shape to your OpenCode config and copy model entries from `openai-ws-opencode setup --path <temp-file>` output:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": ["openai-ws-opencode@latest"],
  "provider": {
    "openai-ws": {
      "name": "OpenAI WebSocket",
      "api": "https://api.openai.com/v1",
      "npm": "@ai-sdk/openai",
      "models": {
        "gpt-5.3-codex": {
          "name": "GPT 5.3 Codex (WebSocket)",
          "reasoning": true,
          "temperature": false,
          "attachment": true,
          "tool_call": true,
          "limit": { "context": 400000, "input": 272000, "output": 128000 },
          "modalities": { "input": ["text", "image"], "output": ["text"] },
          "options": {},
          "provider": { "npm": "@ai-sdk/openai", "api": "https://api.openai.com/v1" }
        }
      }
    }
  }
}
```

## Auth modes

### API key

Choose **OpenAI (WebSocket) - API Key** during `opencode auth login openai-ws`.

This path uses the public OpenAI Responses WebSocket endpoint and sends:

- `Authorization: Bearer <OPENAI_API_KEY>`
- `OpenAI-Beta: responses_websockets=2026-02-06`

### ChatGPT/Codex OAuth

Choose the browser or headless ChatGPT Pro/Plus OAuth option during `opencode auth login openai-ws`.

This path uses the Codex/ChatGPT backend and sends:

- `Authorization: Bearer <access-token>`
- `ChatGPT-Account-Id` when available
- `originator: opencode`
- `OpenAI-Beta: responses_websockets=2026-02-06`

Important: the OAuth/Codex WebSocket path is unofficial, reuses Codex OAuth behavior, targets a private ChatGPT backend, can break without notice, and may carry account or terms-of-service risk. This project is not affiliated with OpenAI or OpenCode.

## Local development install

From this repo:

```sh
bun install
bun run build
npm pack
```

Then point OpenCode at the packed tarball through the plugin array:

```jsonc
{
  "plugin": ["openai-ws-opencode@file:/absolute/path/openai-ws-opencode-0.1.5.tgz"]
}
```

Direct `file:///absolute/path/dist/index.js` imports are useful for debugging, but npm/tarball installs better match how OpenCode resolves dependencies.

## Development

```sh
bun install
bun run lint
bun run typecheck
bun test
bun run build
npm pack --dry-run
```

The root package export intentionally exposes only the OpenCode plugin function because OpenCode calls every function exported by the root module. Test helpers are available from `openai-ws-opencode/testing`.

## Troubleshooting

- **Provider is not visible:** run `openai-ws-opencode setup --global` or add `provider.openai-ws` manually.
- **Missing auth:** run `opencode auth login openai-ws`.
- **OAuth refresh failed:** re-run `opencode auth login openai-ws` and choose a ChatGPT OAuth method.
- **WebSocket 401/403:** verify the selected auth mode and account entitlement. OAuth/Codex behavior may change upstream.
- **Local `file://` install cannot find `ws`:** use the packed tarball install path or add dependencies to your OpenCode config directory.
