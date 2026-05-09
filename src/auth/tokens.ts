import { CLIENT_ID, ISSUER } from "../constants.js"

export interface TokenResponse {
  id_token?: string
  access_token: string
  refresh_token?: string
  expires_in?: number
}

export interface StoredOAuthAuth {
  type: "oauth"
  refresh: string
  access?: string
  expires?: number
  accountId?: string
}

export function parseJwtClaims(token: string): Record<string, any> | undefined {
  const parts = token.split(".")
  if (parts.length !== 3) return undefined
  try {
    return JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"))
  } catch {
    return undefined
  }
}

export function extractAccountId(tokens: Pick<TokenResponse, "id_token" | "access_token">): string | undefined {
  for (const token of [tokens.id_token, tokens.access_token]) {
    if (!token) continue
    const claims = parseJwtClaims(token)
    const id =
      claims?.chatgpt_account_id ||
      claims?.["https://api.openai.com/auth"]?.chatgpt_account_id ||
      claims?.organizations?.[0]?.id
    if (typeof id === "string" && id) return id
  }
  return undefined
}

export async function exchangeCodeForTokens(code: string, redirectUri: string, verifier: string): Promise<TokenResponse> {
  const response = await fetch(`${ISSUER}/oauth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      code,
      redirect_uri: redirectUri,
      client_id: CLIENT_ID,
      code_verifier: verifier,
    }).toString(),
  })
  if (!response.ok) throw new Error(`Token exchange failed: ${response.status}`)
  return response.json() as Promise<TokenResponse>
}

export async function refreshAccessToken(refreshToken: string): Promise<TokenResponse> {
  const response = await fetch(`${ISSUER}/oauth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: refreshToken,
      client_id: CLIENT_ID,
    }).toString(),
  })
  if (!response.ok) throw new Error(`Token refresh failed: ${response.status}`)
  return response.json() as Promise<TokenResponse>
}

export function tokenExpiry(expiresInSeconds = 3600): number {
  return Date.now() + expiresInSeconds * 1000
}
