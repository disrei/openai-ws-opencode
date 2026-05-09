# openai-ws-opencode — agent notes

## Project Overview

This repo is a TypeScript OpenCode plugin that adds an `openai-ws` provider. It routes OpenAI Responses API streaming requests over WebSocket transport while presenting OpenCode with its normal provider/auth/fetch interface.

The plugin supports two auth paths: OpenAI API keys for the public `wss://api.openai.com/v1/responses` endpoint, and ChatGPT/Codex OAuth for the private `wss://chatgpt.com/backend-api/codex/responses` path. Keep claims about OAuth/Codex transport scoped to the current implementation; that backend is unofficial and may change without notice.

## Tech Stack

- Runtime/build: Bun + TypeScript ESM.
- Plugin API: `@opencode-ai/plugin`.
- WebSocket client: `ws`.
- Config editing: `jsonc-parser` for idempotent `opencode.json` updates.
- Test runner: Vitest.
- Package manager/lockfile: Bun (`bun.lock`).

## Architecture

```text
bin/setup.ts              Idempotent OpenCode config patcher, npm bin entrypoint, stale cache repair
src/index.ts              Root package export; keep this to one plugin function
src/plugin.ts             OpenCode hooks, auth loader, provider fetch bridge, session cleanup
src/constants.ts          Provider IDs, endpoint URLs, beta headers, env var names
src/auth/oauth.ts         ChatGPT/Codex browser and headless OAuth flows
src/auth/tokens.ts        OAuth token exchange, refresh, JWT claim parsing
src/models/defaults.ts    Bundled OpenAI WebSocket model table and variants
src/models/catalog.ts     Auth-aware model catalog fetchers and cache
src/models/resolve.ts     Provider model config generation and catalog mapping
src/transport/body.ts     Request body adaptation for API-key and OAuth transports
src/transport/headers.ts  Auth and internal transport header handling
src/transport/bridge.ts   WebSocket-to-SSE response bridge
src/transport/pool.ts     Warm connection pool, request queueing, retries, turn-state tracking
src/testing.ts            Stable test helper exports for `openai-ws-opencode/testing`
test/openai-ws-opencode.test.ts Smoke/unit coverage for setup, auth, models, and transport
package.json              Build/test/package metadata
```

OpenCode calls the plugin through the auth hook. `auth.loader` may refresh provider models from the active auth identity's catalog endpoint, warms a WebSocket connection for that identity, and returns a fetch implementation that bridges streaming Responses requests to SSE. Non-streaming or fallback requests are rewritten and forwarded over HTTP.

The root package export intentionally exposes only the OpenCode plugin function because OpenCode calls every function exported by the root module. Test helpers belong behind the `openai-ws-opencode/testing` export.

## Coding Conventions

- Use TypeScript ESM with semicolons omitted, matching existing files.
- Keep provider ID behavior centralized around `openai-ws` and `PROVIDER_ID`.
- Keep setup idempotent: running config patching twice must produce identical config text.
- Preserve user model overrides when patching `provider.openai-ws.models`.
- Keep public API-key behavior separate from ChatGPT/Codex OAuth behavior; shared helpers are fine only when the headers, endpoint, and body semantics stay explicit.
- Keep the root export limited to the default plugin function from `src/index.ts`.

## Testing and Quality

Run validators after edits:

```sh
bun run lint
bun run typecheck
bun run test
bun run build
```

Before packaging work, also run:

```sh
npm pack --dry-run --json
```

Done means typechecking passes, tests pass, the build passes, setup remains idempotent, and any transport behavior change has focused smoke coverage.

## File Placement Rules

- Put setup/config-patching and OpenCode cache repair changes in `bin/setup.ts`.
- Put plugin hook, auth loader, provider fetch, and lifecycle cleanup changes in `src/plugin.ts`.
- Put ChatGPT/Codex OAuth flow changes in `src/auth/oauth.ts` and token exchange/refresh parsing in `src/auth/tokens.ts`.
- Put model table updates in `src/models/defaults.ts` and model resolution/catalog behavior in `src/models/resolve.ts`.
- Put request body rewriting in `src/transport/body.ts` and auth/internal header behavior in `src/transport/headers.ts`.
- Put WebSocket stream bridging in `src/transport/bridge.ts`; put connection reuse, queueing, retries, stream idle timeouts, and turn-state behavior in `src/transport/pool.ts`.
- Add externally visible behavior coverage in `test/openai-ws-opencode.test.ts`.

## Safe-Change Rules

- Do not remove the setup CLI unless another install path keeps `openai-ws` visible in OpenCode provider lists.
- Provider model resolution must not depend on a required live call to the Codex `/models` or OpenAI `/v1/models` endpoints; the bundled model table must remain a fallback. Do not reintroduce `models.dev` as a runtime dependency.
- Do not change OAuth ports, originator headers, beta headers, or Codex endpoint constants without tests that cover the exact header/URL behavior.
- Do not store ChatGPT OAuth artifacts, account IDs, or captured upstream payloads in the repo.

## Commands

- Install: `bun install`
- Lint: `bun run lint`
- Typecheck: `bun run typecheck`
- Test: `bun run test`
- Build: `bun run build`
- Package dry-run: `npm pack --dry-run --json`

## After Release
OpenCode installs cached npm plugin packages under `~/.cache/opencode/packages/`; clear only the `openai-ws-opencode@latest` cache entry when testing a freshly published package locally. Keep auth data intact.
