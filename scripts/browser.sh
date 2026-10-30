#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/runtime.sh"

# Browser QA is optional; normal launch and API checks do not need Playwright.
requested_node="${NODE_BIN:-}"
NODE_BIN="$(find_node)"
if [[ -z "${PLAYWRIGHT_MODULE:-}" ]] && [[ -d "$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright" ]]; then
  export PLAYWRIGHT_MODULE="$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright"
  if [[ -z "$requested_node" ]] && [[ -x "$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node" ]]; then
    NODE_BIN="$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node"
  fi
fi
if ! "$NODE_BIN" -e 'if (Number(process.versions.node.split(".")[0]) < 20) process.exit(1)' >/dev/null 2>&1; then
  echo "Optional Playwright checks require Node.js 20+. Set NODE_BIN=/absolute/path/to/newer/node." >&2
  exit 1
fi
exec "$NODE_BIN" tests/browser_smoke.cjs

