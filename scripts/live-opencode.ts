import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process"
import { existsSync } from "node:fs"
import { copyFile, mkdir, mkdtemp, readdir, rm, symlink, writeFile } from "node:fs/promises"
import net from "node:net"
import os from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const defaultOpencodeBin = path.join(os.homedir(), ".opencode", "bin", "opencode")
const opencodeBin = process.env.OPENCODE_BIN ?? (existsSync(defaultOpencodeBin) ? defaultOpencodeBin : "opencode")
const model = process.env.OPENAI_WS_LIVE_MODEL ?? "openai-ws/gpt-5.4-mini"
const agent = process.env.OPENAI_WS_LIVE_AGENT
const streamStartTimeoutMs = numberEnv("OPENAI_WS_LIVE_STREAM_START_MS", 3_000)
const turnTimeoutMs = numberEnv("OPENAI_WS_LIVE_TURN_TIMEOUT_MS", 60_000)
const abortCloseTimeoutMs = numberEnv("OPENAI_WS_LIVE_ABORT_CLOSE_MS", 10_000)
const largeContextChars = numberEnv("OPENAI_WS_LIVE_LARGE_CHARS", 32_000)
const serverStartTimeoutMs = numberEnv("OPENAI_WS_LIVE_SERVER_START_MS", 15_000)
const compactTimeoutMs = numberEnv("OPENAI_WS_LIVE_COMPACT_TIMEOUT_MS", 90_000)
const requireAuth = process.env.OPENAI_WS_LIVE === "1" || process.env.OPENAI_WS_LIVE_REQUIRED === "1"
const keepArtifacts = process.env.OPENAI_WS_LIVE_KEEP_ARTIFACTS === "1"
let opencodeEnv: NodeJS.ProcessEnv = {}

type CommandResult = {
  stdout: string
  stderr: string
  code: number | null
  signal: NodeJS.Signals | null
}

type LiveRunResult = CommandResult & {
  events: unknown[]
  assistantText: string
  sawTool: boolean
  streamStarted: boolean
}

function numberEnv(name: string, fallback: number): number {
  const value = process.env[name]
  if (!value) return fallback
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function nonce(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`
}

function redact(text: string): string {
  return text
    .replace(/(Authorization["':=\s]+Bearer\s+)[^\s"',}]+/gi, "$1<redacted>")
    .replace(/\bBearer\s+[A-Za-z0-9._-]+/g, "Bearer <redacted>")
    .replace(/\bsk-[A-Za-z0-9_-]+\b/g, "sk-<redacted>")
    .replace(/(ChatGPT-Account-Id["':=\s]+)[^\s"',}]+/gi, "$1<redacted>")
    .replace(/(OPENAI_API_KEY=)[^\s]+/gi, "$1<redacted>")
}

function tail(text: string, max = 6_000): string {
  return text.length > max ? text.slice(text.length - max) : text
}

function spawnCommand(command: string, args: string[], options: { cwd: string; timeoutMs: number; env?: NodeJS.ProcessEnv }): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: { ...process.env, ...options.env },
      stdio: ["ignore", "pipe", "pipe"],
    })
    let stdout = ""
    let stderr = ""
    let settled = false
    const timeout = setTimeout(() => {
      if (settled) return
      settled = true
      terminate(child)
      reject(new Error(`${command} ${args.join(" ")} timed out after ${options.timeoutMs}ms\n${redact(tail(stderr || stdout))}`))
    }, options.timeoutMs)
    timeout.unref?.()

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8")
    })
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8")
    })
    child.on("error", (error) => {
      if (settled) return
      settled = true
      clearTimeout(timeout)
      reject(error)
    })
    child.on("close", (code, signal) => {
      if (settled) return
      settled = true
      clearTimeout(timeout)
      resolve({ stdout, stderr, code, signal })
    })
  })
}

function uniqueExisting(paths: Array<string | undefined>): string[] {
  return [...new Set(paths.filter((value): value is string => Boolean(value)))].filter((value) => existsSync(value))
}

async function linkOrCopy(source: string, target: string) {
  if (!existsSync(source) || existsSync(target)) return
  try {
    await symlink(source, target)
  } catch {
    await copyFile(source, target)
  }
}

async function mirrorOpenCodeAuth(dataHome: string) {
  const destDir = path.join(dataHome, "opencode")
  await mkdir(destDir, { recursive: true })
  const home = os.homedir()
  const sourceDirs = uniqueExisting([
    process.env.OPENAI_WS_LIVE_AUTH_DIR,
    process.env.XDG_DATA_HOME ? path.join(process.env.XDG_DATA_HOME, "opencode") : undefined,
    path.join(home, ".local", "share", "opencode"),
    path.join(home, ".config", "opencode"),
    path.join(home, ".opencode"),
  ])
  const authFiles = uniqueExisting([
    process.env.OPENAI_WS_LIVE_AUTH_FILE,
    ...sourceDirs.map((dir) => path.join(dir, "auth.json")),
  ])
  for (const authFile of authFiles) await linkOrCopy(authFile, path.join(destDir, "auth.json"))

  for (const sourceDir of sourceDirs) {
    let entries: string[]
    try {
      entries = await readdir(sourceDir)
    } catch {
      continue
    }
    for (const entry of entries) {
      if (!/auth|credential|token|account/i.test(entry)) continue
      await linkOrCopy(path.join(sourceDir, entry), path.join(destDir, entry))
    }
  }
}

async function prepareOpenCodeSandbox(tmpDir: string): Promise<NodeJS.ProcessEnv> {
  const tempHome = path.join(tmpDir, "home")
  const configHome = path.join(tmpDir, "xdg-config")
  const dataHome = path.join(tmpDir, "xdg-data")
  const cacheHome = path.join(tmpDir, "xdg-cache")
  const stateHome = path.join(tmpDir, "xdg-state")
  const configDir = path.join(tmpDir, "opencode-config-dir")
  await Promise.all([
    mkdir(tempHome, { recursive: true }),
    mkdir(configHome, { recursive: true }),
    mkdir(path.join(dataHome, "opencode"), { recursive: true }),
    mkdir(cacheHome, { recursive: true }),
    mkdir(stateHome, { recursive: true }),
    mkdir(configDir, { recursive: true }),
  ])

  await mirrorOpenCodeAuth(dataHome)

  return {
    HOME: tempHome,
    XDG_CONFIG_HOME: configHome,
    XDG_DATA_HOME: dataHome,
    XDG_CACHE_HOME: cacheHome,
    XDG_STATE_HOME: stateHome,
    OPENCODE_CONFIG_DIR: configDir,
  }
}

function terminate(child: ChildProcessWithoutNullStreams) {
  if (child.exitCode !== null || child.signalCode !== null) return
  child.kill("SIGTERM")
  const killTimer = setTimeout(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL")
  }, 2_000)
  killTimer.unref?.()
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.unref()
    server.on("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      server.close((error) => {
        if (error) reject(error)
        else resolve(typeof address === "object" && address ? address.port : 0)
      })
    })
  })
}

async function runChecked(command: string, args: string[], options: { cwd: string; timeoutMs: number; env?: NodeJS.ProcessEnv }) {
  const result = await spawnCommand(command, args, options)
  if (result.code !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed with code ${result.code ?? result.signal}\n${redact(tail(result.stderr || result.stdout))}`,
    )
  }
  return result
}

function tryParseJson(line: string): unknown | undefined {
  const trimmed = line.trim()
  if (!trimmed || (!trimmed.startsWith("{") && !trimmed.startsWith("["))) return undefined
  try {
    return JSON.parse(trimmed)
  } catch {
    return undefined
  }
}

function stringValues(value: unknown, keyFilter?: (key: string) => boolean): string[] {
  const found: string[] = []
  const visit = (current: unknown, key = "") => {
    if (typeof current === "string") {
      if (!keyFilter || keyFilter(key)) found.push(current)
      return
    }
    if (Array.isArray(current)) {
      for (const item of current) visit(item, key)
      return
    }
    if (!current || typeof current !== "object") return
    for (const [nextKey, nextValue] of Object.entries(current as Record<string, unknown>)) visit(nextValue, nextKey)
  }
  visit(value)
  return found
}

function eventType(value: unknown): string {
  if (!value || typeof value !== "object") return ""
  const type = (value as { type?: unknown }).type
  return typeof type === "string" ? type : ""
}

function eventString(value: unknown, path: string): string | undefined {
  if (!value || typeof value !== "object") return undefined
  let current: unknown = value
  for (const key of path.split(".")) {
    if (!current || typeof current !== "object") return undefined
    current = (current as Record<string, unknown>)[key]
  }
  return typeof current === "string" ? current : undefined
}

function isStreamingSignal(value: unknown, raw = ""): boolean {
  const type = eventType(value)
  if (/response\..*\.delta|message\.part|assistant|tool/i.test(type)) return true
  if (/response\.(output_text\.delta|output_item\.done|function_call)|message\.part|assistant/i.test(raw)) return true
  if (!value || typeof value !== "object") return false
  const role = (value as { role?: unknown }).role
  if (role === "user") return false
  return stringValues(value, (key) => ["text", "delta", "content"].includes(key)).some((text) => text.trim().length > 0)
}

function hasToolSignal(value: unknown): boolean {
  if (!value || typeof value !== "object") return false
  const record = value as Record<string, unknown>
  if (record.role === "user") return false
  const type = typeof record.type === "string" ? record.type : ""
  if (/\btool\b|tool_call|function_call/i.test(type)) return true
  for (const child of Object.values(record)) {
    if (Array.isArray(child)) {
      if (child.some((item) => hasToolSignal(item))) return true
    } else if (hasToolSignal(child)) {
      return true
    }
  }
  return false
}

function collectAssistantText(events: unknown[]): string {
  const chunks: string[] = []
  const visit = (value: unknown, assistantScope = false) => {
    if (!value || typeof value !== "object") return
    const record = value as Record<string, unknown>
    const type = typeof record.type === "string" ? record.type : ""
    const role = record.role
    const nextScope = assistantScope || role === "assistant" || /assistant|message\.part|response\.output|delta/i.test(type)
    for (const [key, child] of Object.entries(record)) {
      if (nextScope && typeof child === "string" && ["text", "content", "delta"].includes(key)) chunks.push(child)
      else if (Array.isArray(child)) child.forEach((item) => visit(item, nextScope))
      else visit(child, nextScope)
    }
  }
  events.forEach((event) => visit(event))
  return chunks.join("")
}

function sessionIDFromEvents(events: unknown[]): string | undefined {
  for (const event of events) {
    const candidate =
      eventString(event, "sessionID") ??
      eventString(event, "session.id") ??
      eventString(event, "part.sessionID") ??
      eventString(event, "properties.sessionID")
    if (candidate?.startsWith("ses_")) return candidate
  }
  return undefined
}

function eventIndex(events: unknown[], pattern: RegExp): number {
  return events.findIndex((event) => pattern.test(eventType(event)))
}

function assertRuntimeTurn(result: LiveRunResult, label: string) {
  if (result.code !== 0) throw new Error(`${label} exited with code ${result.code ?? result.signal}`)
  if (/NpmInstallFailedError|failed to install plugin/i.test(result.stderr)) {
    throw new Error(`${label} did not load the local plugin package cleanly\n${redact(tail(result.stderr))}`)
  }
  if (!/service=plugin\b.*path=openai-ws-opencode@file:/i.test(result.stderr)) {
    throw new Error(`${label} did not load the packed local openai-ws-opencode plugin\n${redact(tail(result.stderr))}`)
  }
  if (!result.streamStarted) {
    throw new Error(`${label} exited successfully but no OpenAI stream event was observed\n${redact(tail(result.stderr || result.stdout))}`)
  }

  const startIndex = eventIndex(result.events, /step[_-]start|text|message\.part|response\..*\.delta|response\.output_item\.done/i)
  const finishIndex = eventIndex(result.events, /step[_-]finish|response\.completed/i)
  const sawIdle = /service=bus type=session\.idle publishing|service=session\.prompt .*exiting loop/i.test(result.stderr)
  if (finishIndex < 0 && !sawIdle) {
    throw new Error(`${label} did not emit a terminal step/response event\n${redact(tail(result.stdout || result.stderr))}`)
  }
  if (finishIndex >= 0 && startIndex >= 0 && startIndex > finishIndex) {
    throw new Error(`${label} terminal event arrived before any streaming event\n${redact(tail(result.stdout || result.stderr))}`)
  }
}

async function writeArtifacts(dir: string, name: string, result: Pick<CommandResult, "stdout" | "stderr">) {
  await writeFile(path.join(dir, `${name}.stdout.log`), redact(result.stdout), "utf8")
  await writeFile(path.join(dir, `${name}.stderr.log`), redact(result.stderr), "utf8")
}

async function startOpenCodeServer(
  name: string,
  projectDir: string,
  artifactsDir: string,
): Promise<{ url: string; stop: () => Promise<CommandResult> }> {
  const port = await freePort()
  if (!port) throw new Error("could not allocate a local OpenCode server port")
  const url = `http://127.0.0.1:${port}`
  const child = spawn(opencodeBin, ["serve", "--hostname", "127.0.0.1", "--port", String(port), "--print-logs", "--log-level", "DEBUG"], {
    cwd: projectDir,
    env: { ...process.env, ...opencodeEnv },
    stdio: ["ignore", "pipe", "pipe"],
  })
  let stdout = ""
  let stderr = ""
  let stopped = false
  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString("utf8")
  })
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString("utf8")
  })

  const startedAt = Date.now()
  while (Date.now() - startedAt < serverStartTimeoutMs) {
    if (child.exitCode !== null || child.signalCode !== null) {
      await writeArtifacts(artifactsDir, `${name}-server`, { stdout, stderr })
      throw new Error(`${name} server exited before it was ready\n${redact(tail(stderr || stdout))}`)
    }
    try {
      const response = await fetch(`${url}/config`)
      if (response.ok) break
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 100))
  }

  try {
    const response = await fetch(`${url}/config`)
    if (!response.ok) throw new Error(`GET /config returned ${response.status}`)
  } catch (error) {
    terminate(child)
    await writeArtifacts(artifactsDir, `${name}-server`, { stdout, stderr })
    throw new Error(`${name} server did not become ready: ${error instanceof Error ? error.message : String(error)}`)
  }

  return {
    url,
    stop: () =>
      new Promise((resolve) => {
        if (stopped) {
          resolve({ stdout, stderr, code: child.exitCode, signal: child.signalCode })
          return
        }
        stopped = true
        child.once("close", (code, signal) => {
          void writeArtifacts(artifactsDir, `${name}-server`, { stdout, stderr }).then(() => resolve({ stdout, stderr, code, signal }))
        })
        terminate(child)
      }),
  }
}

async function runLiveTurn(
  name: string,
  prompt: string,
  projectDir: string,
  artifactsDir: string,
  extraArgs: string[] = [],
): Promise<LiveRunResult> {
  const args = [
    "run",
    "--format",
    "json",
    "--print-logs",
    "--log-level",
    "DEBUG",
    "--model",
    model,
    "--variant",
    "low",
    "--dangerously-skip-permissions",
    "--dir",
    projectDir,
    ...(agent ? ["--agent", agent] : []),
    ...extraArgs,
    prompt,
  ]

  return new Promise((resolve, reject) => {
    const child = spawn(opencodeBin, args, {
      cwd: projectDir,
      env: { ...process.env, ...opencodeEnv },
      stdio: ["ignore", "pipe", "pipe"],
    })
    let stdout = ""
    let stderr = ""
    let lineBuffer = ""
    let streamStarted = false
    let llmStreamBoundarySeen = false
    let settled = false
    const events: unknown[] = []
    let sawTool = false
    let streamTimer: ReturnType<typeof setTimeout> | undefined

    const finishReject = async (error: Error) => {
      if (settled) return
      settled = true
      if (streamTimer) clearTimeout(streamTimer)
      clearTimeout(overallTimer)
      terminate(child)
      await writeArtifacts(artifactsDir, name, { stdout, stderr })
      reject(error)
    }

    const startStreamTimer = () => {
      llmStreamBoundarySeen = true
      markStreaming()
      if (streamTimer || streamStarted) return
      streamTimer = setTimeout(() => {
        void finishReject(
          new Error(
            `${name} did not start streaming from ${model} within ${streamStartTimeoutMs}ms\n${redact(tail(stderr || stdout))}`,
          ),
        )
      }, streamStartTimeoutMs)
      streamTimer.unref?.()
    }

    const markStreaming = () => {
      if (streamStarted) return
      streamStarted = true
      if (streamTimer) clearTimeout(streamTimer)
    }

    const processLine = (line: string) => {
      const parsed = tryParseJson(line)
      if (parsed !== undefined) {
        events.push(parsed)
        if (hasToolSignal(parsed)) sawTool = true
        if (isStreamingSignal(parsed, line)) markStreaming()
      } else if (isStreamingSignal(undefined, line)) {
        markStreaming()
      }
    }

    const overallTimer = setTimeout(() => {
      void finishReject(new Error(`${name} timed out after ${turnTimeoutMs}ms\n${redact(tail(stderr || stdout))}`))
    }, turnTimeoutMs)
    overallTimer.unref?.()

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString("utf8")
      stdout += text
      lineBuffer += text
      const lines = lineBuffer.split(/\r?\n/)
      lineBuffer = lines.pop() ?? ""
      for (const line of lines) processLine(line)
    })
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString("utf8")
      stderr += text
      if (/service=llm\b.*providerID=openai-ws\b.*small=false\b.*\bstream\b/i.test(text)) startStreamTimer()
      if (llmStreamBoundarySeen && /response\.(created|completed|output_text\.delta|output_item|function_call)|message\.part\.(updated|delta)/i.test(text)) {
        markStreaming()
      }
      if (/\b(tool|tool_call|function_call)\b/i.test(text)) sawTool = true
    })
    child.on("error", (error) => {
      void finishReject(error)
    })
    child.on("close", (code, signal) => {
      if (settled) return
      settled = true
      if (streamTimer) clearTimeout(streamTimer)
      clearTimeout(overallTimer)
      if (lineBuffer.trim()) processLine(lineBuffer)
      const result = {
        stdout,
        stderr,
        code,
        signal,
        events,
        sawTool,
        streamStarted,
        assistantText: collectAssistantText(events),
      }
      void writeArtifacts(artifactsDir, name, result).then(() => {
        if (code !== 0) {
          reject(new Error(`${name} failed with code ${code ?? signal}\n${redact(tail(stderr || stdout))}`))
          return
        }
        resolve(result)
      }, reject)
    })
  })
}

async function runAbortTurn(name: string, prompt: string, projectDir: string, artifactsDir: string): Promise<LiveRunResult> {
  const args = [
    "run",
    "--format",
    "json",
    "--print-logs",
    "--log-level",
    "DEBUG",
    "--model",
    model,
    "--dangerously-skip-permissions",
    "--dir",
    projectDir,
    ...(agent ? ["--agent", agent] : []),
    prompt,
  ]

  return new Promise((resolve, reject) => {
    const child = spawn(opencodeBin, args, {
      cwd: projectDir,
      env: { ...process.env, ...opencodeEnv },
      stdio: ["ignore", "pipe", "pipe"],
    })
    let stdout = ""
    let stderr = ""
    let lineBuffer = ""
    const events: unknown[] = []
    let sawTool = false
    let streamTimer: ReturnType<typeof setTimeout> | undefined
    let closeTimer: ReturnType<typeof setTimeout> | undefined
    let settled = false
    let streamStarted = false
    let llmStreamBoundarySeen = false

    const cleanup = () => {
      if (streamTimer) clearTimeout(streamTimer)
      if (closeTimer) clearTimeout(closeTimer)
      clearTimeout(overallTimer)
    }

    const fail = async (error: Error) => {
      if (settled) return
      settled = true
      cleanup()
      terminate(child)
      await writeArtifacts(artifactsDir, name, { stdout, stderr })
      reject(error)
    }

    const markStreaming = () => {
      if (streamStarted) return
      streamStarted = true
      if (streamTimer) clearTimeout(streamTimer)
      terminate(child)
      closeTimer = setTimeout(() => {
        void fail(new Error(`${name} streamed but did not close within ${abortCloseTimeoutMs}ms after abort`))
      }, abortCloseTimeoutMs)
      closeTimer.unref?.()
    }

    const startStreamTimer = () => {
      llmStreamBoundarySeen = true
      if (streamTimer || streamStarted) return
      streamTimer = setTimeout(() => {
        void fail(new Error(`${name} did not start streaming from ${model} within ${streamStartTimeoutMs}ms\n${redact(tail(stderr || stdout))}`))
      }, streamStartTimeoutMs)
      streamTimer.unref?.()
    }

    const processLine = (line: string) => {
      const parsed = tryParseJson(line)
      if (parsed !== undefined) {
        events.push(parsed)
        if (hasToolSignal(parsed)) sawTool = true
        if (isStreamingSignal(parsed, line)) markStreaming()
      } else if (isStreamingSignal(undefined, line)) {
        markStreaming()
      }
    }

    const overallTimer = setTimeout(() => {
      void fail(new Error(`${name} did not reach the LLM stream boundary within ${turnTimeoutMs}ms\n${redact(tail(stderr || stdout))}`))
    }, turnTimeoutMs)
    overallTimer.unref?.()

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString("utf8")
      stdout += text
      lineBuffer += text
      const lines = lineBuffer.split(/\r?\n/)
      lineBuffer = lines.pop() ?? ""
      for (const line of lines) processLine(line)
    })
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString("utf8")
      stderr += text
      if (/service=llm\b.*providerID=openai-ws\b.*small=false\b.*\bstream\b/i.test(text)) startStreamTimer()
      if (llmStreamBoundarySeen && /response\.(output_text\.delta|function_call)|message\.part\.delta/i.test(text)) {
        markStreaming()
      }
    })
    child.on("error", (error) => {
      void fail(error)
    })
    child.on("close", (code, signal) => {
      if (settled) return
      settled = true
      cleanup()
      if (lineBuffer.trim()) processLine(lineBuffer)
      const result = {
        stdout,
        stderr,
        code,
        signal,
        events,
        sawTool,
        streamStarted,
        assistantText: collectAssistantText(events),
      }
      void writeArtifacts(artifactsDir, name, result).then(() => {
        if (!streamStarted) {
          reject(new Error(`${name} exited before stream start with code ${code ?? signal}\n${redact(tail(stderr || stdout))}`))
          return
        }
        resolve(result)
      }, reject)
    })
  })
}

function safeArtifactName(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")
}

async function assertIncludes(result: LiveRunResult, expected: string, label: string, projectDir: string, artifactsDir: string) {
  const haystack = `${result.assistantText}\n${result.stdout}`
  if (haystack.includes(expected)) return

  const sessionID = sessionIDFromEvents(result.events)
  if (sessionID) {
    const exported = await spawnCommand(opencodeBin, ["export", sessionID], {
      cwd: projectDir,
      timeoutMs: 30_000,
      env: opencodeEnv,
    })
    await writeArtifacts(artifactsDir, `${safeArtifactName(label)}-export`, exported)
    if (`${exported.stdout}\n${exported.stderr}`.includes(expected)) return
  }

  throw new Error(`${label} did not include ${expected}\n${redact(tail(result.stdout || result.stderr))}`)
}

function largeContextPrompt(expected: string): string {
  const filler = "context-block ".repeat(Math.ceil(largeContextChars / "context-block ".length)).slice(0, largeContextChars)
  return [
    "Read the full prompt and preserve the final nonce exactly.",
    filler,
    `Final nonce: ${expected}`,
    `Reply exactly ${expected} and nothing else.`,
  ].join("\n")
}

async function compactSession(serverUrl: string, sessionID: string, projectDir: string, artifactsDir: string) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), compactTimeoutMs)
  try {
    const response = await fetch(`${serverUrl}/session/${sessionID}/summarize?directory=${encodeURIComponent(projectDir)}`, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify({ providerID: "openai-ws", modelID: model.replace(/^openai-ws\//, "") }),
      signal: controller.signal,
    })
    const text = await response.text()
    await writeArtifacts(artifactsDir, "compact-session", {
      stdout: text,
      stderr: `POST /session/${sessionID}/summarize -> ${response.status}`,
    })
    if (/text\/html/i.test(response.headers.get("content-type") ?? "")) {
      throw new Error(`compact hit the web UI fallback instead of the session API: ${response.status}`)
    }
    if (!response.ok) throw new Error(`compact returned ${response.status}: ${redact(tail(text))}`)
  } finally {
    clearTimeout(timeout)
  }
}

async function packLocalPlugin(tmpDir: string): Promise<string> {
  await runChecked("bun", ["run", "build"], { cwd: repoRoot, timeoutMs: 120_000 })
  const packed = await runChecked("npm", ["pack", "--pack-destination", tmpDir, "--ignore-scripts", "--json"], {
    cwd: repoRoot,
    timeoutMs: 120_000,
  })
  try {
    const parsed = JSON.parse(packed.stdout) as Array<{ filename?: string }>
    const filename = parsed[0]?.filename
    if (filename) return path.join(tmpDir, filename)
  } catch {}
  const candidates = (await readdir(tmpDir)).filter((entry) => entry.endsWith(".tgz"))
  if (candidates.length === 1) return path.join(tmpDir, candidates[0])
  throw new Error(`Could not determine packed tarball path from npm pack output:\n${packed.stdout}`)
}

async function main() {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "openai-ws-opencode-live-"))
  const projectDir = path.join(tmpDir, "project")
  const artifactsDir = path.join(tmpDir, "artifacts")
  let success = false
  try {
    await mkdir(projectDir, { recursive: true })
    await mkdir(artifactsDir, { recursive: true })
    opencodeEnv = await prepareOpenCodeSandbox(tmpDir)
    const tarball = await packLocalPlugin(tmpDir)
    const configPath = path.join(projectDir, "opencode.json")
    await runChecked("node", ["bin/setup.js", "--path", configPath, "--plugin", `openai-ws-opencode@file:${tarball}`, "--no-cache-repair"], {
      cwd: repoRoot,
      timeoutMs: 30_000,
    })
    opencodeEnv.OPENCODE_CONFIG = configPath

    const auth = await spawnCommand(opencodeBin, ["auth", "list"], { cwd: projectDir, timeoutMs: 30_000, env: opencodeEnv })
    await writeArtifacts(artifactsDir, "auth-list", auth)
    if (auth.code !== 0 || !/openai-ws/i.test(`${auth.stdout}\n${auth.stderr}`)) {
      const message = `Skipping live OpenCode harness: no local openai-ws auth found. Run \`opencode auth login openai-ws\` or set OPENAI_WS_LIVE=1 to require this check.`
      if (requireAuth) throw new Error(`${message}\n${redact(tail(auth.stderr || auth.stdout))}`)
      console.warn(message)
      success = true
      return
    }

    const basicNonce = nonce("BASIC_OK")
    const basic = await runLiveTurn("basic-response", `Reply exactly ${basicNonce} and nothing else.`, projectDir, artifactsDir)
    assertRuntimeTurn(basic, "basic response")
    await assertIncludes(basic, basicNonce, "basic response", projectDir, artifactsDir)

    const toolNonce = nonce("TOOL_OK")
    const toolFile = path.join(projectDir, "live-tool-nonce.txt")
    await writeFile(toolFile, `${toolNonce}\n`, "utf8")
    const tool = await runLiveTurn(
      "tool-use",
      `Use a file-reading tool to read ${toolFile}. Then reply exactly with the file contents and nothing else.`,
      projectDir,
      artifactsDir,
    )
    assertRuntimeTurn(tool, "tool response")
    await assertIncludes(tool, toolNonce, "tool response", projectDir, artifactsDir)
    if (!tool.sawTool) throw new Error(`tool-use completed but no tool activity was observed\n${redact(tail(tool.stdout || tool.stderr))}`)

    const continueNonce = nonce("CONT")
    const first = await runLiveTurn(
      "continue-first",
      `Remember this nonce for the next turn: ${continueNonce}. Reply exactly FIRST_OK and nothing else.`,
      projectDir,
      artifactsDir,
    )
    assertRuntimeTurn(first, "first continuation response")
    await assertIncludes(first, "FIRST_OK", "first continuation response", projectDir, artifactsDir)

    const second = await runLiveTurn(
      "continue-second",
      "Using only the previous turn, reply exactly with the nonce I asked you to remember and nothing else.",
      projectDir,
      artifactsDir,
      ["--continue"],
    )
    assertRuntimeTurn(second, "continued response")
    await assertIncludes(second, continueNonce, "continued response", projectDir, artifactsDir)

    const resumeNonce = nonce("RESUME")
    const resumeFirst = await runLiveTurn(
      "resume-first",
      `Remember this resume nonce for the next message: ${resumeNonce}. Reply exactly RESUME_READY and nothing else.`,
      projectDir,
      artifactsDir,
    )
    assertRuntimeTurn(resumeFirst, "resume first response")
    await assertIncludes(resumeFirst, "RESUME_READY", "resume first response", projectDir, artifactsDir)
    const sessionID = sessionIDFromEvents(resumeFirst.events)
    if (!sessionID) throw new Error(`resume-first completed but no session id was emitted\n${redact(tail(resumeFirst.stdout || resumeFirst.stderr))}`)

    const resumeSecond = await runLiveTurn(
      "resume-second",
      "Using only the existing session memory, reply exactly with the resume nonce and nothing else.",
      projectDir,
      artifactsDir,
      ["--session", sessionID],
    )
    assertRuntimeTurn(resumeSecond, "resumed response")
    await assertIncludes(resumeSecond, resumeNonce, "resumed response", projectDir, artifactsDir)

    const compactNonce = nonce("COMPACT")
    const compactFirst = await runLiveTurn(
      "compact-first",
      `Remember this compact nonce for after compaction: ${compactNonce}. Reply exactly COMPACT_READY and nothing else.`,
      projectDir,
      artifactsDir,
    )
    assertRuntimeTurn(compactFirst, "compact first response")
    await assertIncludes(compactFirst, "COMPACT_READY", "compact first response", projectDir, artifactsDir)
    const compactSessionID = sessionIDFromEvents(compactFirst.events)
    if (!compactSessionID) {
      throw new Error(`compact-first completed but no session id was emitted\n${redact(tail(compactFirst.stdout || compactFirst.stderr))}`)
    }

    const compactServer = await startOpenCodeServer("compact", projectDir, artifactsDir)
    try {
      await compactSession(compactServer.url, compactSessionID, projectDir, artifactsDir)
      const compactSecond = await runLiveTurn(
        "compact-second",
        "Using only the compacted session, reply exactly with the compact nonce and nothing else.",
        projectDir,
        artifactsDir,
        ["--session", compactSessionID],
      )
      assertRuntimeTurn(compactSecond, "compacted session response")
      await assertIncludes(compactSecond, compactNonce, "compacted session response", projectDir, artifactsDir)
    } finally {
      await compactServer.stop()
    }

    const abort = await runAbortTurn(
      "abort-after-stream",
      "Start your answer immediately with ABORT_STREAM_STARTED, then repeat ABORT_STREAM_STARTED one hundred times separated by spaces. Do not use tools.",
      projectDir,
      artifactsDir,
    )
    if (!abort.streamStarted) throw new Error("abort-after-stream closed before any stream evidence was observed")

    const recoveryNonce = nonce("RECOVERY")
    const recovery = await runLiveTurn(
      "post-abort-recovery",
      `Reply exactly ${recoveryNonce} and nothing else.`,
      projectDir,
      artifactsDir,
    )
    assertRuntimeTurn(recovery, "post-abort recovery response")
    await assertIncludes(recovery, recoveryNonce, "post-abort recovery response", projectDir, artifactsDir)

    const largeNonce = nonce("LARGE")
    const large = await runLiveTurn("large-context", largeContextPrompt(largeNonce), projectDir, artifactsDir)
    assertRuntimeTurn(large, "large context response")
    await assertIncludes(large, largeNonce, "large context response", projectDir, artifactsDir)

    success = true
    console.log(`Live OpenCode harness passed with ${model}.`)
  } catch (error) {
    console.error(`Live OpenCode harness failed. Artifacts: ${artifactsDir}`)
    throw error
  } finally {
    if (success && !keepArtifacts) await rm(tmpDir, { recursive: true, force: true })
    else console.error(`Keeping live harness artifacts at ${tmpDir}`)
  }
}

main().catch((error) => {
  console.error(redact(error instanceof Error ? error.stack ?? error.message : String(error)))
  process.exit(1)
})
