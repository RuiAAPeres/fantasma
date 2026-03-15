#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.yaml"
BENCH_COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.bench.yaml"
TEST_COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.test.yaml"
CLI_SMOKE="$ROOT_DIR/scripts/cli-smoke.sh"
COMPOSE_SMOKE="$ROOT_DIR/scripts/compose-smoke.sh"
DOCKER_TEST="$ROOT_DIR/scripts/docker-test.sh"
RECLAIM_SCRIPT="$ROOT_DIR/scripts/docker-reclaim.sh"

assert_present() {
  local pattern="$1"
  local path="$2"

  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "expected pattern '$pattern' missing in $path" >&2
    exit 1
  fi
}

assert_absent() {
  local pattern="$1"
  local path="$2"

  if rg -n "$pattern" "$path" >/dev/null; then
    echo "unexpected pattern '$pattern' still present in $path" >&2
    exit 1
  fi
}

assert_present '^name:\s+fantasma-local$' "$COMPOSE_FILE"
assert_present '^name:\s+fantasma-bench$' "$BENCH_COMPOSE_FILE"
assert_present '^name:\s+fantasma-tests$' "$TEST_COMPOSE_FILE"

assert_absent 'fantasma-cli-smoke-\$\$' "$CLI_SMOKE"
assert_absent 'fantasma-smoke-\$\$' "$COMPOSE_SMOKE"
assert_present 'compose down --volumes --remove-orphans --rmi local' "$CLI_SMOKE"
assert_present 'compose down --volumes --remove-orphans --rmi local' "$COMPOSE_SMOKE"
assert_present 'compose down --volumes --remove-orphans --rmi local' "$DOCKER_TEST"
assert_present 'FANTASMA_DOCKER_TEST_KEEP_CACHE' "$DOCKER_TEST"

assert_present 'docker image rm -f' "$RECLAIM_SCRIPT"
assert_present 'docker volume rm -f' "$RECLAIM_SCRIPT"

echo "docker disk footprint audit passed"
