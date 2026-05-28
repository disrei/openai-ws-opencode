import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { VERBOSE_LOG_ENV, VERBOSE_LOG_MAX_ROUNDS_ENV } from "./constants.js"

const LOG_FILE = path.join(os.tmpdir(), "openai-ws-opencode.log")
const ROUND_MARKER = "[kv-debug] round-start "
const DEFAULT_MAX_ROUNDS = 10

export function verboseLogEnabled() {
  return process.env[VERBOSE_LOG_ENV] !== "0"
}

function verboseLogMaxRounds() {
  const raw = Number.parseInt(process.env[VERBOSE_LOG_MAX_ROUNDS_ENV] ?? "", 10)
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_MAX_ROUNDS
}

function trimVerboseLog(raw: string): string {
  const lines = raw.split(/\r?\n/)
  if (lines.at(-1) === "") lines.pop()
  const markers: number[] = []
  for (const [index, line] of lines.entries()) {
    if (line.includes(ROUND_MARKER)) markers.push(index)
  }
  const maxRounds = verboseLogMaxRounds()
  if (markers.length <= maxRounds) return lines.length === 0 ? "" : `${lines.join("\n")}\n`
  return `${lines.slice(markers[markers.length - maxRounds]).join("\n")}\n`
}

function trimVerboseLogFile() {
  try {
    const raw = fs.readFileSync(LOG_FILE, "utf8")
    const trimmed = trimVerboseLog(raw)
    if (trimmed !== raw) fs.writeFileSync(LOG_FILE, trimmed, "utf8")
  } catch {}
}

export function wsLog(msg: string) {
  if (!verboseLogEnabled()) return
  try {
    fs.appendFileSync(LOG_FILE, `[${new Date().toISOString()}] ${msg}\n`)
    if (msg.includes(ROUND_MARKER)) trimVerboseLogFile()
  } catch {}
}

export function clearVerboseLogForTesting() {
  try {
    fs.rmSync(LOG_FILE, { force: true })
  } catch {}
}

export function readVerboseLogForTesting() {
  try {
    return fs.readFileSync(LOG_FILE, "utf8")
  } catch {
    return ""
  }
}

export function appendVerboseLogForTesting(msg: string) {
  wsLog(msg)
}
