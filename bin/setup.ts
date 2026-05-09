#!/usr/bin/env node
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { applyEdits, format, modify, parse } from "jsonc-parser"
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
  if (specifier.startsWith("file://")) return path.basename(new URL(specifier).pathname).replace(/\.[cm]?[jt]s$/, "")
  const lastAt = specifier.lastIndexOf("@")
  return lastAt > 0 ? specifier.slice(0, lastAt) : specifier
}

export function patchConfigText(input: string, pluginSpec = DEFAULT_PLUGIN_SPEC): string {
  let text = input.trim() ? input : "{}"
  const config = (parse(text) ?? {}) as Record<string, any>

  if (!config.$schema) text = applyJsonPatch(text, ["$schema"], SCHEMA)

  const current = ((parse(text) ?? {}) as Record<string, any>).plugin
  const plugins = Array.isArray(current) ? [...current] : []
  if (!plugins.some((plugin) => typeof plugin === "string" && pluginPackageName(plugin) === PACKAGE_NAME)) {
    plugins.push(pluginSpec)
  }
  text = applyJsonPatch(text, ["plugin"], plugins)

  const next = (parse(text) ?? {}) as Record<string, any>
  const existingModels = next.provider?.["openai-ws"]?.models ?? {}
  text = applyJsonPatch(text, ["provider", "openai-ws"], providerConfig(existingModels))

  return applyEdits(
    text,
    format(text, undefined, {
      insertSpaces: true,
      tabSize: 2,
      eol: "\n",
    }),
  )
}

export async function setupOpenCodeConfig(options: SetupOptions = {}): Promise<string> {
  const file = targetPath(options)
  let existing = ""
  try {
    existing = await fs.readFile(file, "utf8")
  } catch (error: any) {
    if (error?.code !== "ENOENT") throw error
  }

  const updated = patchConfigText(existing, options.pluginSpec ?? DEFAULT_PLUGIN_SPEC)
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, updated.endsWith("\n") ? updated : `${updated}\n`, "utf8")
  return file
}

function parseArgs(argv: string[]): SetupOptions {
  const options: SetupOptions = { mode: "global" }
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index]
    if (arg === "--project") options.mode = "project"
    else if (arg === "--global") options.mode = "global"
    else if (arg === "--path") options.configPath = argv[++index]
    else if (arg === "--plugin") options.pluginSpec = argv[++index]
    else if (arg === "--help" || arg === "-h") {
      console.log("Usage: openai-ws-opencode setup [--global|--project] [--path <opencode.json>] [--plugin <specifier>]")
      process.exit(0)
    }
  }
  return options
}

if (import.meta.url === `file://${process.argv[1]}`) {
  setupOpenCodeConfig(parseArgs(process.argv.slice(2)))
    .then((file) => {
      console.log(`Updated OpenCode config: ${file}`)
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error))
      process.exit(1)
    })
}
