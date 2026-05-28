import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const setupFile = path.join(repoRoot, "bin", "setup.js")
const SHEBANG = "#!/usr/bin/env node"

export function normalizeSetupShebang(source: string): string {
  const trimmed = source.replace(/^(?:#!\/usr\/bin\/env node\r?\n)+/, "")
  return `${SHEBANG}\n${trimmed.replace(/^\r?\n+/, "")}`
}

export function normalizeSetupShebangFile(file = setupFile): void {
  const source = fs.readFileSync(file, "utf8")
  const normalized = normalizeSetupShebang(source)
  if (normalized !== source) fs.writeFileSync(file, normalized)
}

if (import.meta.main) normalizeSetupShebangFile()
