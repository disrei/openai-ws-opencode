#!/usr/bin/env node
import { realpathSync } from "node:fs"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { applyEdits, format, modify, parse } from "jsonc-parser"
import { fetchCodexCatalog, type CodexModelInfo } from "../src/models/catalog.js"
import { providerConfig } from "../src/models/resolve.js"

const DEFAULT_PLUGIN_SPEC = "openai-ws-opencode@latest"
const PACKAGE_NAME = "openai-ws-opencode"
const SCHEMA = "https://opencode.ai/config.json"

export type SetupMode = "global" | "project"

export interface SetupOptions {
  mode?: SetupMode
  cwd?: string
  configPath?: string
  pluginSpec?: string
  cacheRepair?: boolean
  catalog?: CodexModelInfo[]
  catalogFetch?: () => Promise<CodexModelInfo[] | undefined>
}

interface CacheRepairOptions {
  cacheHome?: string
  packageVersion?: string
  now?: () => Date
}

interface CacheRepairResult {
  cachePath: string
  movedTo?: string
  repaired: boolean
}

function globalConfigPath(): string {
  const configHome = process.env.XDG_CONFIG_HOME ?? path.join(os.homedir(), ".config")
  return path.join(configHome, "opencode", "opencode.json")
}

function projectConfigPath(cwd: string): string {
  return path.join(cwd, "opencode.json")
}

function targetPath(options: SetupOptions): string {
  if (options.configPath) return path.resolve(options.configPath)
  if (options.mode === "project") return projectConfigPath(options.cwd ?? process.cwd())
  return globalConfigPath()
}

function applyJsonPatch(text: string, jsonPath: Array<string | number>, value: unknown): string {
  return applyEdits(
    text,
    modify(text, jsonPath, value, {
      formattingOptions: { insertSpaces: true, tabSize: 2, eol: "\n" },
    }),
  )
}

function pluginPackageName(specifier: string): string {
  if (specifier.startsWith("file:")) {
    const filePath = specifier.startsWith("file://") ? new URL(specifier).pathname : specifier.slice("file:".length)
    const base = path.basename(filePath)
    if (base === PACKAGE_NAME || base.startsWith(`${PACKAGE_NAME}-`) || base.startsWith(`${PACKAGE_NAME}@`)) return PACKAGE_NAME
    return base.replace(/(?:\.tgz|\.[cm]?[jt]s)$/, "")
  }
  const lastAt = specifier.lastIndexOf("@")
  return lastAt > 0 ? specifier.slice(0, lastAt) : specifier
}

export function patchConfigText(input: string, pluginSpec = DEFAULT_PLUGIN_SPEC, replacePluginSpec = false, catalog?: CodexModelInfo[]): string {
  let text = input.trim() ? input : "{}"
  const config = (parse(text) ?? {}) as Record<string, any>

  if (!config.$schema) text = applyJsonPatch(text, ["$schema"], SCHEMA)

  const current = ((parse(text) ?? {}) as Record<string, any>).plugin
  const plugins = Array.isArray(current) ? [...current] : []
  const existingPluginIndex = plugins.findIndex((plugin) => typeof plugin === "string" && pluginPackageName(plugin) === PACKAGE_NAME)
  if (existingPluginIndex === -1) {
    plugins.push(pluginSpec)
  } else if (replacePluginSpec) {
    plugins[existingPluginIndex] = pluginSpec
  }
  text = applyJsonPatch(text, ["plugin"], plugins)

  const next = (parse(text) ?? {}) as Record<string, any>
  const existingModels = next.provider?.["openai-ws"]?.models ?? {}
  text = applyJsonPatch(text, ["provider", "openai-ws"], providerConfig(existingModels, catalog))

  return applyEdits(
    text,
    format(text, undefined, {
      insertSpaces: true,
      tabSize: 2,
      eol: "\n",
    }),
  )
}

function isNotFound(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
}

function cacheHomePath(cacheHome = process.env.XDG_CACHE_HOME ?? path.join(os.homedir(), ".cache")): string {
  return cacheHome
}

const CATALOG_FETCH_TIMEOUT_MS = 1500

interface CodexAuthFile {
  tokens?: {
    access_token?: unknown
    account_id?: unknown
  }
}

async function readCodexAuthSnapshot(): Promise<{ accessToken: string; accountId?: string } | undefined> {
  const file = path.join(os.homedir(), ".codex", "auth.json")
  let raw: string
  try {
    raw = await fs.readFile(file, "utf8")
  } catch {
    return undefined
  }
  let parsed: CodexAuthFile
  try {
    parsed = JSON.parse(raw) as CodexAuthFile
  } catch {
    return undefined
  }
  const accessToken = typeof parsed?.tokens?.access_token === "string" ? parsed.tokens.access_token : undefined
  if (!accessToken) return undefined
  const accountId = typeof parsed?.tokens?.account_id === "string" ? parsed.tokens.account_id : undefined
  return { accessToken, accountId }
}

async function resolveLiveCatalog(
  customFetch?: () => Promise<CodexModelInfo[] | undefined>,
): Promise<CodexModelInfo[] | undefined> {
  try {
    if (customFetch) return await customFetch()
    if (process.env.OPENAI_WS_OPENCODE_SKIP_CATALOG === "1") return undefined
    const snapshot = await readCodexAuthSnapshot()
    if (!snapshot) return undefined
    return await fetchCodexCatalog({
      accessToken: snapshot.accessToken,
      accountId: snapshot.accountId,
      timeoutMs: CATALOG_FETCH_TIMEOUT_MS,
    })
  } catch {
    return undefined
  }
}

function openCodeLatestCachePath(cacheHome?: string): string {
  return path.join(cacheHomePath(cacheHome), "opencode", "packages", `${PACKAGE_NAME}@latest`)
}

async function pathExists(file: string): Promise<boolean> {
  try {
    await fs.access(file)
    return true
  } catch (error) {
    if (isNotFound(error)) return false
    throw error
  }
}

async function readPackageVersion(packageJsonPath: string): Promise<string | undefined> {
  try {
    const text = await fs.readFile(packageJsonPath, "utf8")
    const parsed = JSON.parse(text) as { version?: unknown }
    return typeof parsed.version === "string" ? parsed.version : undefined
  } catch (error) {
    if (isNotFound(error)) return undefined
    throw error
  }
}

async function setupPackageVersion(): Promise<string> {
  const packageJsonPath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "package.json")
  const version = await readPackageVersion(packageJsonPath)
  if (!version) throw new Error(`Unable to read setup package version from ${packageJsonPath}`)
  return version
}

function semverParts(version: string): [number, number, number] | undefined {
  const match = /^(\d+)\.(\d+)\.(\d+)/.exec(version)
  if (!match) return undefined
  return [Number(match[1]), Number(match[2]), Number(match[3])]
}

function compareSemver(left: string, right: string): number {
  const leftParts = semverParts(left)
  const rightParts = semverParts(right)
  if (!leftParts || !rightParts) return 0
  for (let index = 0; index < leftParts.length; index++) {
    if (leftParts[index] !== rightParts[index]) return leftParts[index] < rightParts[index] ? -1 : 1
  }
  return 0
}

const SOURCE_EXTENSIONS = new Set([".cjs", ".js", ".mjs", ".ts"])
const BAD_OAUTH_PORT = /\bOAUTH_PORT\b\s*[:=]\s*1456\b/
const BAD_CODEX_ORIGINATOR = /\bCODEX_ORIGINATOR\b\s*[:=]\s*["']codex_cli_rs["']/

async function hasKnownBadConstants(packageRoot: string): Promise<boolean> {
  const directories = [packageRoot]
  let checkedFiles = 0

  while (directories.length > 0 && checkedFiles < 500) {
    const current = directories.pop()!
    let entries: Array<import("node:fs").Dirent>
    try {
      entries = await fs.readdir(current, { withFileTypes: true })
    } catch (error) {
      if (isNotFound(error)) continue
      throw error
    }

    for (const entry of entries) {
      const entryPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        if (entry.name !== "node_modules" && entry.name !== ".git") directories.push(entryPath)
        continue
      }
      if (!entry.isFile() || !SOURCE_EXTENSIONS.has(path.extname(entry.name))) continue

      checkedFiles += 1
      const source = await fs.readFile(entryPath, "utf8")
      if (BAD_OAUTH_PORT.test(source) || BAD_CODEX_ORIGINATOR.test(source)) return true
      if (checkedFiles >= 500) break
    }
  }

  return false
}

function timestampForPath(date: Date): string {
  return date.toISOString().replace(/[:.]/g, "-")
}

async function staleDestination(cachePath: string, now: Date): Promise<string> {
  const base = `${cachePath}.stale-${timestampForPath(now)}`
  let candidate = base
  let suffix = 1
  while (await pathExists(candidate)) {
    candidate = `${base}-${suffix}`
    suffix += 1
  }
  return candidate
}

export async function repairStaleOpenCodeLatestCache(options: CacheRepairOptions = {}): Promise<CacheRepairResult> {
  const cachePath = openCodeLatestCachePath(options.cacheHome)
  if (!(await pathExists(cachePath))) return { cachePath, repaired: false }

  const packageRoot = path.join(cachePath, "node_modules", PACKAGE_NAME)
  const cachedVersion = await readPackageVersion(path.join(packageRoot, "package.json"))
  const currentVersion = options.packageVersion ?? (await setupPackageVersion())
  const staleVersion = cachedVersion ? compareSemver(cachedVersion, currentVersion) < 0 : false
  const staleConstants = await hasKnownBadConstants(packageRoot)

  if (!staleVersion && !staleConstants) return { cachePath, repaired: false }

  const movedTo = await staleDestination(cachePath, options.now?.() ?? new Date())
  await fs.rename(cachePath, movedTo)
  return { cachePath, movedTo, repaired: true }
}

export async function setupOpenCodeConfig(options: SetupOptions = {}): Promise<string> {
  const file = targetPath(options)
  let existing = ""
  try {
    existing = await fs.readFile(file, "utf8")
  } catch (error: any) {
    if (error?.code !== "ENOENT") throw error
  }

  const catalog = options.catalog ?? (await resolveLiveCatalog(options.catalogFetch))
  const updated = patchConfigText(existing, options.pluginSpec ?? DEFAULT_PLUGIN_SPEC, Boolean(options.pluginSpec), catalog)
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, updated.endsWith("\n") ? updated : `${updated}\n`, "utf8")
  if (options.cacheRepair !== false) {
    try {
      await repairStaleOpenCodeLatestCache()
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      console.warn(`Warning: could not repair OpenCode cache at ${openCodeLatestCachePath()}. Remove it manually if OpenCode keeps loading stale plugin code. ${message}`)
    }
  }
  return file
}

export function parseArgs(argv: string[]): SetupOptions {
  const options: SetupOptions = { mode: "global" }
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index]
    if (arg === "--project") options.mode = "project"
    else if (arg === "--global") options.mode = "global"
    else if (arg === "--path") options.configPath = argv[++index]
    else if (arg === "--plugin") options.pluginSpec = argv[++index]
    else if (arg === "--no-cache-repair") options.cacheRepair = false
    else if (arg === "--help" || arg === "-h") {
      console.log("Usage: openai-ws-opencode setup [--global|--project] [--path <opencode.json>] [--plugin <specifier>] [--no-cache-repair]")
      process.exit(0)
    }
  }
  return options
}

export function isDirectExecution(moduleUrl = import.meta.url, argvPath = process.argv[1]): boolean {
  if (!argvPath) return false
  const modulePath = fileURLToPath(moduleUrl)
  try {
    return realpathSync(modulePath) === realpathSync(argvPath)
  } catch {
    return path.resolve(modulePath) === path.resolve(argvPath)
  }
}

if (isDirectExecution()) {
  setupOpenCodeConfig(parseArgs(process.argv.slice(2)))
    .then((file) => {
      console.log(`Updated OpenCode config: ${file}`)
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error))
      process.exit(1)
    })
}
