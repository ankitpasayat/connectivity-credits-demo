#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/runtime.sh"
selected_python="$(find_python)"
if [[ $# -eq 0 ]]; then set -- status; fi
exec "$selected_python" scripts/kafka_runtime.py "$@"

