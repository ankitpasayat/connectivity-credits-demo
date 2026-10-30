#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/runtime.sh"

prepare_test_python
prepare_node
trap cleanup_owned_services EXIT
prepare_postgres
(cd server && "$NPM_BIN" ci --no-audit --no-fund && "$NPM_BIN" run build && "$NPM_BIN" test)
(cd frontend && "$NPM_BIN" ci --no-audit --no-fund && "$NPM_BIN" run build)
NODE_BIN="$NODE_BIN" .venv/bin/python -m pytest tests "$@"

