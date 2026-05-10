export const transportConfig = {
  connectTimeoutMs: 15_000,
  maxReconnectAttempts: 5,
  reconnectBaseDelayMs: 100,
  reconnectMaxDelayMs: 2_000,
  reconnectJitterRatio: 0.5,
  retryAfterMaxDelayMs: 10_000,
  idleEvictMs: 120_000,
  staleReuseMs: 60_000,
  streamIdleTimeoutMs: 600_000,
  preFirstBytePing: ":\n\n",
  maxConnectionsPerScope: 4,
  connectionMaxAgeMs: 55 * 60 * 1000,
}
