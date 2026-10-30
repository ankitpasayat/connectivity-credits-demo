#!/usr/bin/env bash
# Shared runtime discovery for macOS/Linux. Source from other scripts.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

find_python() {
  local candidate resolved
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    candidate="$PYTHON_BIN"
    if ! "$candidate" -c 'import sys; assert sys.version_info >= (3, 11)' >/dev/null 2>&1; then
      echo "PYTHON_BIN must point to Python 3.11 or newer." >&2
      return 1
    fi
    printf '%s\n' "$candidate"
    return
  fi
  for candidate in python3.13 python3.12 python3.11 python3 \
    "$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3"; do
    resolved="$(command -v "$candidate" 2>/dev/null || true)"
    if [[ -n "$resolved" ]] && "$resolved" -c 'import sys; assert sys.version_info >= (3, 11)' >/dev/null 2>&1; then
      printf '%s\n' "$resolved"
      return
    fi
  done
  echo "Python 3.11+ is required. Install it, or set PYTHON_BIN=/absolute/path/to/python3." >&2
  return 1
}

find_node() {
  local candidate resolved
  if [[ -n "${NODE_BIN:-}" ]]; then
    candidate="$NODE_BIN"
    if ! "$candidate" -e 'const [a,b]=process.versions.node.split(".").map(Number);if(a<22||(a===22&&b<16))process.exit(1)' >/dev/null 2>&1; then
      echo "NODE_BIN must point to Node.js 22.16 or newer." >&2
      return 1
    fi
    printf '%s\n' "$candidate"
    return
  fi
  for candidate in node "$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node"; do
    resolved="$(command -v "$candidate" 2>/dev/null || true)"
    if [[ -n "$resolved" ]] && "$resolved" -e 'const [a,b]=process.versions.node.split(".").map(Number);if(a<22||(a===22&&b<16))process.exit(1)' >/dev/null 2>&1; then
      printf '%s\n' "$resolved"
      return
    fi
  done
  echo "Node.js 22.16+ and npm are required. Set NODE_BIN=/absolute/path/to/node if needed." >&2
  return 1
}

prepare_test_python() {
  local selected_python
  selected_python="$(find_python)"
  if [[ ! -x .venv/bin/python ]]; then
    "$selected_python" -m venv .venv
  fi
  if ! .venv/bin/python -c 'import sys; assert sys.version_info >= (3, 11)' >/dev/null 2>&1; then
    echo ".venv uses an older Python. Remove that test virtualenv and rerun this script." >&2
    return 1
  fi
  .venv/bin/python -m pip install --disable-pip-version-check -r requirements-dev.lock
}

prepare_node() {
  NODE_BIN="$(find_node)"
  export PATH="$(dirname "$NODE_BIN"):$PATH"
  NPM_BIN="${NPM_BIN:-$(command -v npm || true)}"
  if [[ -z "$NPM_BIN" ]]; then
    echo "npm is required. Install Node.js with npm, or set NPM_BIN=/absolute/path/to/npm." >&2
    return 1
  fi
}

cleanup_owned_broker() {
  if [[ -n "${CONNECTIVITY_LAUNCHER_BROKER_PID:-}" ]]; then
    local current_pid
    current_pid="$(./scripts/kafka-local.sh status | "$NODE_BIN" -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>process.stdout.write(String(JSON.parse(s).pid||"")))')"
    if [[ "$current_pid" == "$CONNECTIVITY_LAUNCHER_BROKER_PID" ]]; then ./scripts/kafka-local.sh stop; fi
    unset CONNECTIVITY_LAUNCHER_BROKER_PID
  fi
}

prepare_postgres() {
  if [[ -z "${DATABASE_URL:-}" ]] && [[ -z "${CONNECTIVITY_DATABASE_URL:-}" ]]; then
    local before_pid
    before_pid="$(./scripts/postgres-local.sh status | "$NODE_BIN" -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>process.stdout.write(String(JSON.parse(s).pid||"")))')"
    ./scripts/postgres-local.sh start
    source "$PROJECT_ROOT/.data/postgres/app.env"
    if [[ -z "$before_pid" ]]; then
      export CONNECTIVITY_LAUNCHER_POSTGRES_PID="$(./scripts/postgres-local.sh status | "$NODE_BIN" -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>process.stdout.write(String(JSON.parse(s).pid||"")))')"
    fi
  fi
}

cleanup_owned_services() {
  if [[ -n "${CONNECTIVITY_LAUNCHER_POSTGRES_PID:-}" ]]; then
    local current_pid
    current_pid="$(./scripts/postgres-local.sh status | "$NODE_BIN" -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>process.stdout.write(String(JSON.parse(s).pid||"")))')"
    if [[ "$current_pid" == "$CONNECTIVITY_LAUNCHER_POSTGRES_PID" ]]; then ./scripts/postgres-local.sh stop; fi
    unset CONNECTIVITY_LAUNCHER_POSTGRES_PID
  fi
  cleanup_owned_broker
}

