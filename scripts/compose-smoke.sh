#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.yaml"
PROJECT_NAME="${FANTASMA_SMOKE_PROJECT_NAME:-fantasma-smoke-$$}"
MIN_FREE_KB=5000000
TIMEOUT_SECONDS=60
SLEEP_SECONDS=2

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

check_disk() {
  local free_kb
  free_kb="$(df -Pk "$ROOT_DIR" | awk 'NR==2 {print $4}')"
  if [[ -z "$free_kb" || "$free_kb" -lt "$MIN_FREE_KB" ]]; then
    echo "insufficient free disk space for compose smoke test" >&2
    df -h "$ROOT_DIR" >&2
    exit 1
  fi
}

check_docker() {
  docker info >/dev/null 2>&1 || {
    echo "docker daemon is unavailable" >&2
    exit 1
  }
}

print_logs() {
  compose logs --tail=200 || true
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$exit_code"
}

wait_for_http() {
  local deadline now
  deadline=$((SECONDS + TIMEOUT_SECONDS))

  while true; do
    if curl -fsS "http://localhost:8081/health" >/dev/null 2>&1 &&
      curl -fsS "http://localhost:8082/health" >/dev/null 2>&1; then
      return 0
    fi

    now=$SECONDS
    if (( now >= deadline )); then
      echo "timed out waiting for ingest and api health endpoints" >&2
      print_logs >&2
      exit 1
    fi

    sleep "$SLEEP_SECONDS"
  done
}

poll_metrics() {
  local deadline now count_response count_compact duration_response duration_compact
  local event_aggregate_response event_aggregate_compact event_daily_response event_daily_compact
  deadline=$((SECONDS + TIMEOUT_SECONDS))

  while true; do
    count_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/sessions/count/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=2026-01-01&end_date=2026-01-02" \
      -H "Authorization: Bearer fg_pat_dev" || true)"
    duration_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/sessions/duration/total/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=2026-01-01&end_date=2026-01-02" \
      -H "Authorization: Bearer fg_pat_dev" || true)"
    event_aggregate_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/events/aggregate?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-01-01&end_date=2026-01-02&platform=ios&group_by=provider" \
      -H "Authorization: Bearer fg_pat_dev" || true)"
    event_daily_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/events/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-01-01&end_date=2026-01-02&group_by=provider" \
      -H "Authorization: Bearer fg_pat_dev" || true)"
    count_compact="$(printf '%s' "$count_response" | tr -d '[:space:]')"
    duration_compact="$(printf '%s' "$duration_response" | tr -d '[:space:]')"
    event_aggregate_compact="$(printf '%s' "$event_aggregate_response" | tr -d '[:space:]')"
    event_daily_compact="$(printf '%s' "$event_daily_response" | tr -d '[:space:]')"

    if [[ "$count_compact" == *'"metric":"sessions_count_daily"'* &&
          "$count_compact" == *'"date":"2026-01-01","value":1'* &&
          "$count_compact" == *'"date":"2026-01-02","value":0'* &&
          "$duration_compact" == *'"metric":"session_duration_total_daily"'* &&
          "$duration_compact" == *'"date":"2026-01-01","value":600'* &&
          "$duration_compact" == *'"date":"2026-01-02","value":0'* &&
          "$event_aggregate_compact" == *'"metric":"event_count"'* &&
          "$event_aggregate_compact" == *'"provider":"strava"'* &&
          "$event_aggregate_compact" == *'"value":2'* &&
          "$event_daily_compact" == *'"metric":"event_count_daily"'* &&
          "$event_daily_compact" == *'"provider":"strava"'* &&
          "$event_daily_compact" == *'"date":"2026-01-01","value":2'* &&
          "$event_daily_compact" == *'"date":"2026-01-02","value":0'* ]]; then
      printf '%s\n' "$count_response"
      printf '%s\n' "$duration_response"
      printf '%s\n' "$event_aggregate_response"
      printf '%s\n' "$event_daily_response"
      return 0
    fi

    now=$SECONDS
    if (( now >= deadline )); then
      echo "timed out waiting for derived metrics to appear" >&2
      print_logs >&2
      exit 1
    fi

    sleep "$SLEEP_SECONDS"
  done
}

main() {
  require_cmd docker
  require_cmd curl
  check_disk
  check_docker

  trap cleanup EXIT INT TERM

  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  compose up -d --build
  wait_for_http

  curl -fsS -X POST "http://localhost:8081/v1/events" \
    -H "Content-Type: application/json" \
    -H "X-Fantasma-Key: fg_ing_test" \
    -d '{
      "events": [
        {
          "event": "app_open",
          "timestamp": "2026-01-01T00:00:00Z",
          "install_id": "compose-install-1",
          "platform": "ios",
          "app_version": "1.0.0",
          "os_version": "18.3",
          "properties": {
            "provider": "strava"
          }
        },
        {
          "event": "app_open",
          "timestamp": "2026-01-01T00:10:00Z",
          "install_id": "compose-install-1",
          "platform": "ios",
          "app_version": "1.0.0",
          "os_version": "18.3",
          "properties": {
            "provider": "strava"
          }
        }
      ]
    }' >/dev/null

  poll_metrics
}

main "$@"
