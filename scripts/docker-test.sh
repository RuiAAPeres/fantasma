#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.test.yaml"
PROJECT_NAME="fantasma-tests"
KEEP_CONTAINERS="${FANTASMA_DOCKER_TEST_KEEP_CONTAINERS:-0}"
KEEP_CACHE="${FANTASMA_DOCKER_TEST_KEEP_CACHE:-0}"
HEALTH_TIMEOUT_SECONDS="${FANTASMA_DOCKER_TEST_HEALTH_TIMEOUT_SECONDS:-60}"

compose() {
  docker compose \
    -p "$PROJECT_NAME" \
    -f "$COMPOSE_FILE" \
    "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

print_logs() {
  compose logs --tail=200 || true
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM

  if [[ "$KEEP_CONTAINERS" == "1" ]]; then
    echo "keeping docker test containers because FANTASMA_DOCKER_TEST_KEEP_CONTAINERS=1" >&2
    exit "$exit_code"
  fi

  if [[ "$KEEP_CACHE" == "1" ]]; then
    echo "keeping docker test cache volumes because FANTASMA_DOCKER_TEST_KEEP_CACHE=1" >&2
    compose down --remove-orphans >/dev/null 2>&1 || true
    exit "$exit_code"
  fi

  compose down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
  exit "$exit_code"
}

wait_for_postgres() {
  local deadline container_id health
  deadline=$((SECONDS + HEALTH_TIMEOUT_SECONDS))
  container_id="$(compose ps -q postgres)"

  if [[ -z "$container_id" ]]; then
    echo "postgres test container was not created" >&2
    return 1
  fi

  while true; do
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id")"

    case "$health" in
      healthy)
        return 0
        ;;
      unhealthy|exited|dead)
        echo "postgres test container became $health" >&2
        return 1
        ;;
    esac

    if (( SECONDS >= deadline )); then
      echo "timed out waiting for postgres test container health" >&2
      return 1
    fi

    sleep 2
  done
}

main() {
  local status

  require_cmd docker
  docker info >/dev/null 2>&1 || {
    echo "docker daemon is unavailable" >&2
    exit 1
  }

  export LOCAL_UID LOCAL_GID
  LOCAL_UID="$(id -u)"
  LOCAL_GID="$(id -g)"

  trap cleanup EXIT INT TERM

  compose up -d postgres

  if ! wait_for_postgres; then
    print_logs >&2
    exit 1
  fi

  set +e
  compose run --build --rm workspace-tests cargo test --workspace "$@"
  status=$?
  set -e

  if (( status != 0 )); then
    print_logs >&2
  fi

  exit "$status"
}

main "$@"
