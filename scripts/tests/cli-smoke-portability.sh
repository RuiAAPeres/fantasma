#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_PATH="$ROOT_DIR/scripts/cli-smoke.sh"
COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.yaml"

assert_present() {
  local pattern="$1"
  local path="${2:-$SCRIPT_PATH}"
  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "expected pattern '$pattern' missing in $path" >&2
    exit 1
  fi
}

assert_absent() {
  local pattern="$1"
  local path="${2:-$SCRIPT_PATH}"
  if rg -n "$pattern" "$path" >/dev/null; then
    echo "unexpected pattern '$pattern' present in $path" >&2
    exit 1
  fi
}

assert_present 'COMPOSE_POSTGRES_PORTS="\$\{FANTASMA_CLI_SMOKE_POSTGRES_PORTS:-127\.0\.0\.1::5432\}"'
assert_present 'COMPOSE_INGEST_PORTS="\$\{FANTASMA_CLI_SMOKE_INGEST_PORTS:-127\.0\.0\.1::8081\}"'
assert_present 'COMPOSE_API_PORTS="\$\{FANTASMA_CLI_SMOKE_API_PORTS:-127\.0\.0\.1::8082\}"'
assert_present 'FANTASMA_POSTGRES_PORTS="\$COMPOSE_POSTGRES_PORTS"'
assert_present 'FANTASMA_INGEST_PORTS="\$COMPOSE_INGEST_PORTS"'
assert_present 'FANTASMA_API_PORTS="\$COMPOSE_API_PORTS"'
assert_present 'compose port "\$service" "\$container_port"'
assert_present 'resolve_service_url'
assert_absent 'API_URL="\$\{FANTASMA_CLI_SMOKE_API_URL:-http://localhost:8082\}"'
assert_absent 'INGEST_URL="\$\{FANTASMA_CLI_SMOKE_INGEST_URL:-http://localhost:8081\}"'
assert_present '\$\{FANTASMA_POSTGRES_PORTS:-5432:5432\}' "$COMPOSE_FILE"
assert_present '\$\{FANTASMA_INGEST_PORTS:-8081:8081\}' "$COMPOSE_FILE"
assert_present '\$\{FANTASMA_API_PORTS:-8082:8082\}' "$COMPOSE_FILE"

echo "cli smoke portability audit passed"
