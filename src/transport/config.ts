export const transportConfig = {
  connectTimeoutMs: 10_000,
  maxReconnectAttempts: 3,
  reconnectBaseDelayMs: 100,
  reconnectMaxDelayMs: 2_000,
  idleEvictMs: 120_000,
  staleReuseMs: 60_000,
  streamIdleTimeoutMs: 300_000,
  maxConnectionsPerScope: 4,
  connectionMaxAgeMs: 55 * 60 * 1000,
}
