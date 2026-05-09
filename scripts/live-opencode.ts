import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process"
import { existsSync } from "node:fs"
import { copyFile, mkdir, mkdtemp, readdir, rm, symlink, writeFile } from "node:fs/promises"
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

async function writeArtifacts(dir: string, name: string, result: Pick<CommandResult, "stdout" | "stderr">) {
  await writeFile(path.join(dir, `${name}.stdout.log`), redact(result.stdout), "utf8")
  await writeFile(path.join(dir, `${name}.stderr.log`), redact(result.stderr), "utf8")
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
      if (/service=llm\b.*providerID=openai-ws\b.*\bstream\b/i.test(text)) startStreamTimer()
      if (/response\.(created|completed|output_text\.delta|output_item|function_call)/i.test(text)) markStreaming()
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

function assertIncludes(result: LiveRunResult, expected: string, label: string) {
  const haystack = `${result.assistantText}\n${result.stdout}`
  if (!haystack.includes(expected)) {
    throw new Error(`${label} did not include ${expected}\n${redact(tail(result.stdout || result.stderr))}`)
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
    await runChecked(
      "node",
      ["bin/setup.js", "--path", configPath, "--plugin", `file:${tarball}`, "--no-cache-repair"],
      { cwd: repoRoot, timeoutMs: 30_000 },
    )
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
    assertIncludes(basic, basicNonce, "basic response")

    const toolNonce = nonce("TOOL_OK")
    const toolFile = path.join(projectDir, "live-tool-nonce.txt")
    await writeFile(toolFile, `${toolNonce}\n`, "utf8")
    const tool = await runLiveTurn(
      "tool-use",
      `Use a file-reading tool to read ${toolFile}. Then reply exactly with the file contents and nothing else.`,
      projectDir,
      artifactsDir,
    )
    assertIncludes(tool, toolNonce, "tool response")
    if (!tool.sawTool) throw new Error(`tool-use completed but no tool activity was observed\n${redact(tail(tool.stdout || tool.stderr))}`)

    const continueNonce = nonce("CONT")
    const first = await runLiveTurn(
      "continue-first",
      `Remember this nonce for the next turn: ${continueNonce}. Reply exactly FIRST_OK and nothing else.`,
      projectDir,
      artifactsDir,
    )
    assertIncludes(first, "FIRST_OK", "first continuation response")

    const second = await runLiveTurn(
      "continue-second",
      "Using only the previous turn, reply exactly with the nonce I asked you to remember and nothing else.",
      projectDir,
      artifactsDir,
      ["--continue"],
    )
    assertIncludes(second, continueNonce, "continued response")

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
