import { spawnSync } from "node:child_process"
import { existsSync } from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const hooksPath = ".githooks"

function git(args: string[]) {
  return spawnSync("git", args, {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  })
}

if (process.env.OPENAI_WS_SKIP_HOOKS_INSTALL === "1") process.exit(0)
if (!existsSync(path.join(repoRoot, ".git")) || !existsSync(path.join(repoRoot, hooksPath, "pre-push"))) process.exit(0)

const current = git(["config", "--get", "core.hooksPath"])
if (current.status === 0 && current.stdout.trim() === hooksPath) process.exit(0)

const updated = git(["config", "core.hooksPath", hooksPath])
if (updated.status !== 0) {
  process.stderr.write(updated.stderr || "Unable to configure git hooks\n")
  process.exit(updated.status ?? 1)
}

process.stdout.write(`Configured git hooks path: ${hooksPath}\n`)
