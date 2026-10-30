#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/runtime.sh"

prepare_node
trap cleanup_owned_services EXIT
prepare_postgres
(cd server && "$NPM_BIN" ci --no-audit --no-fund && "$NPM_BIN" run build)
(cd frontend && "$NPM_BIN" ci --no-audit --no-fund && "$NPM_BIN" run build)
"$NODE_BIN" scripts/check-database.mjs
exec "$NODE_BIN" scripts/dev.mjs

