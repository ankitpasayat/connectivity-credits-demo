#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/runtime.sh"
prepare_node
before_pid="$(./scripts/kafka-local.sh status | "$NODE_BIN" -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>process.stdout.write(String(JSON.parse(s).pid||"")))')"
./scripts/kafka-local.sh start
if [[ -z "$before_pid" ]]; then
  export CONNECTIVITY_LAUNCHER_BROKER_PID="$(./scripts/kafka-local.sh status | "$NODE_BIN" -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>process.stdout.write(String(JSON.parse(s).pid||"")))')"
fi
trap cleanup_owned_broker EXIT
export CONNECTIVITY_TRANSPORT=kafka
export CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS="${CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:${KAFKA_LOCAL_PORT:-19092}}"
exec "$PROJECT_ROOT/scripts/start.sh"

