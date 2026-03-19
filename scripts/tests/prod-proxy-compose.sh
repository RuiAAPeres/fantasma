#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.yaml"
PROD_COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.prod.yaml"

assert_present() {
  local pattern="$1"
  local path="$2"

  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "expected pattern '$pattern' missing in $path" >&2
    exit 1
  fi
}

assert_present '^services:$' "$PROD_COMPOSE_FILE"
assert_present '^  fantasma-api:$' "$PROD_COMPOSE_FILE"
assert_present '^      proxy:$' "$PROD_COMPOSE_FILE"
assert_present '\$\{FANTASMA_PROXY_NETWORK_ALIAS:-fantasma-api\}' "$PROD_COMPOSE_FILE"
assert_present '^networks:$' "$PROD_COMPOSE_FILE"
assert_present '^  proxy:$' "$PROD_COMPOSE_FILE"
assert_present '^    external: true$' "$PROD_COMPOSE_FILE"
assert_present '\$\{FANTASMA_PROXY_NETWORK_NAME:-proxy\}' "$PROD_COMPOSE_FILE"

rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT

FANTASMA_ADMIN_TOKEN=test-token docker compose \
  -f "$COMPOSE_FILE" \
  -f "$PROD_COMPOSE_FILE" \
  config >"$rendered"

assert_present '^      proxy:$' "$rendered"
assert_present '^        aliases:$' "$rendered"
assert_present '^          - fantasma-api$' "$rendered"
assert_present '^  proxy:$' "$rendered"
assert_present '^    external: true$' "$rendered"
assert_present '^    name: proxy$' "$rendered"

echo "prod proxy compose audit passed"
