import http, { type IncomingMessage, type ServerResponse } from "node:http"
import crypto from "node:crypto"
import { CLIENT_ID, CODEX_OAUTH_SCOPE, CODEX_ORIGINATOR, ISSUER, OAUTH_PORT } from "../constants.js"
import { exchangeCodeForTokens, extractAccountId, tokenExpiry, type TokenResponse } from "./tokens.js"

type PendingOAuth = {
  pkce: PkceCodes
  state: string
  redirectUri: string
  resolve: (tokens: TokenResponse) => void
  reject: (error: Error) => void
  timeout: ReturnType<typeof setTimeout>
}

interface PkceCodes {
  verifier: string
  challenge: string
}

let oauthServer: http.Server | undefined
let oauthRedirectUri: string | undefined
let pendingOAuth: PendingOAuth | undefined

const HTML_OK = "<!doctype html><html><body><h1>Authorization successful</h1><p>Return to OpenCode.</p></body></html>"
const HTML_ERROR = "<!doctype html><html><body><h1>Authorization failed</h1></body></html>"

function base64Url(buffer: ArrayBuffer | Buffer): string {
  return Buffer.isBuffer(buffer) ? buffer.toString("base64url") : Buffer.from(new Uint8Array(buffer)).toString("base64url")
}

function randomString(length: number): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
  const bytes = crypto.randomBytes(length)
  return Array.from(bytes, (byte) => chars[byte % chars.length]).join("")
}

async function generatePKCE(): Promise<PkceCodes> {
  const verifier = randomString(43)
  const challenge = base64Url(await crypto.webcrypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier)))
  return { verifier, challenge }
}

function generateState(): string {
  return base64Url(crypto.randomBytes(32))
}

function buildAuthorizeUrl(redirectUri: string, pkce: PkceCodes, state: string): string {
  const params = new URLSearchParams({
    response_type: "code",
    client_id: CLIENT_ID,
    redirect_uri: redirectUri,
    scope: CODEX_OAUTH_SCOPE,
    code_challenge: pkce.challenge,
    code_challenge_method: "S256",
    id_token_add_organizations: "true",
    codex_cli_simplified_flow: "true",
    state,
    originator: CODEX_ORIGINATOR,
  })
  return `${ISSUER}/oauth/authorize?${params.toString()}`
}

function reply(res: ServerResponse, status: number, body: string) {
  res.writeHead(status, { "Content-Type": "text/html" })
  res.end(body)
}

async function handleCallback(req: IncomingMessage, res: ServerResponse) {
  const url = new URL(req.url ?? "/", `http://localhost:${OAUTH_PORT}`)
  if (url.pathname !== "/auth/callback") {
    reply(res, 404, "Not found")
    return
  }

  const current = pendingOAuth
  if (!current) {
    reply(res, 400, HTML_ERROR)
    return
  }

  const state = url.searchParams.get("state")
  if (!state || state !== current.state) {
    pendingOAuth = undefined
    clearTimeout(current.timeout)
    current.reject(new Error("OAuth state mismatch"))
    reply(res, 400, HTML_ERROR)
    return
  }

  const error = url.searchParams.get("error")
  const code = url.searchParams.get("code")
  pendingOAuth = undefined
  clearTimeout(current.timeout)

  if (error || !code) {
    current.reject(new Error(error || "Missing OAuth code"))
    reply(res, 400, HTML_ERROR)
    return
  }

  try {
    current.resolve(await exchangeCodeForTokens(code, current.redirectUri, current.pkce.verifier))
    reply(res, 200, HTML_OK)
  } catch (cause) {
    current.reject(cause instanceof Error ? cause : new Error("OAuth token exchange failed"))
    reply(res, 400, HTML_ERROR)
  }
}

async function startOAuthServer(): Promise<string> {
  if (oauthServer && oauthRedirectUri) return oauthRedirectUri
  const server = http.createServer((req, res) => {
    void handleCallback(req, res)
  })
  oauthServer = server

  try {
    await listen(server, OAUTH_PORT)
  } catch (error) {
    oauthServer = undefined
    oauthRedirectUri = undefined
    server.close()
    if (isAddressInUse(error)) throw new Error(`OpenAI WebSocket OAuth requires localhost:${OAUTH_PORT}; stop the process using that port and retry.`)
    throw error
  }

  oauthRedirectUri = `http://localhost:${OAUTH_PORT}/auth/callback`
  return oauthRedirectUri
}

function listen(server: http.Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error) => {
      server.off("listening", onListening)
      reject(error)
    }
    const onListening = () => {
      server.off("error", onError)
      resolve()
    }
    server.once("error", onError)
    server.once("listening", onListening)
    server.listen(port, "127.0.0.1")
  })
}

function isAddressInUse(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "EADDRINUSE"
}

export function stopOAuthServer() {
  oauthServer?.close()
  oauthServer = undefined
  oauthRedirectUri = undefined
  if (pendingOAuth) {
    clearTimeout(pendingOAuth.timeout)
    pendingOAuth.reject(new Error("OAuth server stopped"))
    pendingOAuth = undefined
  }
}

export async function createBrowserAuthorization() {
  const redirectUri = await startOAuthServer()
  const pkce = await generatePKCE()
  const state = generateState()
  const callbackPromise = new Promise<TokenResponse>((resolve, reject) => {
    const timeout = setTimeout(() => {
      pendingOAuth = undefined
      reject(new Error("OAuth callback timeout"))
    }, 5 * 60 * 1000)
    if (typeof timeout === "object" && "unref" in timeout) timeout.unref()
    pendingOAuth = { pkce, state, redirectUri, resolve, reject, timeout }
  })

  return {
    url: buildAuthorizeUrl(redirectUri, pkce, state),
    instructions: "Complete authorization in your browser. This window will close automatically.",
    method: "auto" as const,
    async callback() {
      try {
        const tokens = await callbackPromise
        if (!tokens.refresh_token) throw new Error("OAuth refresh token missing")
        stopOAuthServer()
        return {
          type: "success" as const,
          refresh: tokens.refresh_token,
          access: tokens.access_token,
          expires: tokenExpiry(tokens.expires_in),
          accountId: extractAccountId(tokens),
        }
      } catch {
        stopOAuthServer()
        return { type: "failed" as const }
      }
    },
  }
}

export async function createDeviceAuthorization() {
  const deviceResponse = await fetch(`${ISSUER}/api/accounts/deviceauth/usercode`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "User-Agent": "openai-ws-opencode/0.1.4" },
    body: JSON.stringify({ client_id: CLIENT_ID }),
  })
  if (!deviceResponse.ok) throw new Error("Failed to initiate device authorization")
  const deviceData = (await deviceResponse.json()) as {
    device_auth_id: string
    user_code: string
    interval?: string
  }
  const intervalMs = Math.max(Number.parseInt(deviceData.interval ?? "5", 10) || 5, 1) * 1000

  return {
    url: `${ISSUER}/codex/device`,
    instructions: `Enter code: ${deviceData.user_code}`,
    method: "auto" as const,
    async callback() {
      for (;;) {
        const response = await fetch(`${ISSUER}/api/accounts/deviceauth/token`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "User-Agent": "openai-ws-opencode/0.1.4" },
          body: JSON.stringify({
            device_auth_id: deviceData.device_auth_id,
            user_code: deviceData.user_code,
          }),
        })

        if (response.ok) {
          const data = (await response.json()) as { authorization_code: string; code_verifier: string }
          const tokens = await exchangeCodeForTokens(data.authorization_code, `${ISSUER}/deviceauth/callback`, data.code_verifier)
          if (!tokens.refresh_token) return { type: "failed" as const }
          return {
            type: "success" as const,
            refresh: tokens.refresh_token,
            access: tokens.access_token,
            expires: tokenExpiry(tokens.expires_in),
            accountId: extractAccountId(tokens),
          }
        }

        if (response.status !== 403 && response.status !== 404) return { type: "failed" as const }
        await new Promise((resolve) => setTimeout(resolve, intervalMs + 3000))
      }
    },
  }
}

export const oauthMethods = [
  {
    label: "OpenAI (WebSocket) - ChatGPT Pro/Plus (browser)",
    type: "oauth" as const,
    authorize: createBrowserAuthorization,
  },
  {
    label: "OpenAI (WebSocket) - ChatGPT Pro/Plus (headless)",
    type: "oauth" as const,
    authorize: createDeviceAuthorization,
  },
  {
    label: "OpenAI (WebSocket) - API Key",
    type: "api" as const,
  },
]

export const oauthTesting = {
  handleCallback,
  getPendingState: () => pendingOAuth?.state,
  reset: stopOAuthServer,
}
