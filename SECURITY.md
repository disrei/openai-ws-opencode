# Security Policy

Report security issues privately to the repository owner instead of opening a public issue.

This plugin does not intentionally log API keys, OAuth access tokens, refresh tokens, authorization headers, or account IDs. Do not attach secrets, auth JSON files, screenshots containing tokens, or raw request headers to bug reports.

The ChatGPT/Codex OAuth transport uses unofficial backend behavior and may change without notice. Review your OpenAI/OpenCode account policies before using it. If the Codex OAuth client or backend behavior is revoked or materially changes, the mitigation path is to disable the OAuth/Codex auth method in a patch release while keeping API-key transport available.

The plugin performs a short best-effort request to `https://models.dev/api.json` to refresh public model metadata unless `OPENAI_WS_OPENCODE_SKIP_CATALOG=1` is set. No credentials are sent to that catalog endpoint.

Before release, maintainers should run validators, inspect `git diff`, run a secrets scan, and inspect `npm pack --dry-run` output for accidental credential or local-config inclusion.
