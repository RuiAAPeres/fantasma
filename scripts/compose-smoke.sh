#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.yaml"
PROJECT_NAME="${FANTASMA_SMOKE_PROJECT_NAME:-fantasma-smoke}"
ADMIN_TOKEN=""
MIN_FREE_KB=5000000
TIMEOUT_SECONDS=60
SLEEP_SECONDS=2
INGEST_KEY=""
READ_KEY=""
KEEP_STACK="${FANTASMA_SMOKE_KEEP_STACK:-0}"

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

generate_admin_token() {
  python3 - <<'PY'
import secrets

print(f"fg_pat_{secrets.token_urlsafe(24)}")
PY
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

  if [[ "$KEEP_STACK" == "1" ]]; then
    echo "keeping compose smoke stack because FANTASMA_SMOKE_KEEP_STACK=1" >&2
    exit "$exit_code"
  fi

  compose down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
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
  local installs_response installs_compact active_response active_compact event_response event_compact
  local hourly_count_response hourly_count_compact
  deadline=$((SECONDS + TIMEOUT_SECONDS))

  while true; do
    count_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-02" \
      -H "X-Fantasma-Key: ${READ_KEY}" || true)"
    duration_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/sessions?metric=duration_total&granularity=day&start=2026-01-01&end=2026-01-02" \
      -H "X-Fantasma-Key: ${READ_KEY}" || true)"
    installs_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/sessions?metric=new_installs&granularity=day&start=2026-01-01&end=2026-01-02" \
      -H "X-Fantasma-Key: ${READ_KEY}" || true)"
    active_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-01-02&interval=day" \
      -H "X-Fantasma-Key: ${READ_KEY}" || true)"
    hourly_count_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/sessions?metric=count&granularity=hour&start=2026-01-01T00:00:00Z&end=2026-01-01T01:00:00Z" \
      -H "X-Fantasma-Key: ${READ_KEY}" || true)"
    event_response="$(curl -fsS \
      "http://localhost:8082/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-02&platform=ios&group_by=provider" \
      -H "X-Fantasma-Key: ${READ_KEY}" || true)"
    count_compact="$(printf '%s' "$count_response" | tr -d '[:space:]')"
    duration_compact="$(printf '%s' "$duration_response" | tr -d '[:space:]')"
    installs_compact="$(printf '%s' "$installs_response" | tr -d '[:space:]')"
    active_compact="$(printf '%s' "$active_response" | tr -d '[:space:]')"
    hourly_count_compact="$(printf '%s' "$hourly_count_response" | tr -d '[:space:]')"
    event_compact="$(printf '%s' "$event_response" | tr -d '[:space:]')"

    if [[ "$count_compact" == *'"metric":"count"'* &&
          "$count_compact" == *'"granularity":"day"'* &&
          "$count_compact" == *'"bucket":"2026-01-01","value":1'* &&
          "$count_compact" == *'"bucket":"2026-01-02","value":0'* &&
          "$duration_compact" == *'"metric":"duration_total"'* &&
          "$duration_compact" == *'"bucket":"2026-01-01","value":600'* &&
          "$duration_compact" == *'"bucket":"2026-01-02","value":0'* &&
          "$installs_compact" == *'"metric":"new_installs"'* &&
          "$installs_compact" == *'"bucket":"2026-01-01","value":1'* &&
          "$installs_compact" == *'"bucket":"2026-01-02","value":0'* &&
          "$active_compact" == *'"metric":"active_installs"'* &&
          "$active_compact" == *'"interval":"day"'* &&
          "$active_compact" == *'"start":"2026-01-01","end":"2026-01-02"'* &&
          "$active_compact" == *'"start":"2026-01-01","end":"2026-01-01","value":1'* &&
          "$active_compact" == *'"start":"2026-01-02","end":"2026-01-02","value":0'* &&
          "$hourly_count_compact" == *'"metric":"count"'* &&
          "$hourly_count_compact" == *'"granularity":"hour"'* &&
          "$hourly_count_compact" == *'"bucket":"2026-01-01T00:00:00Z","value":1'* &&
          "$hourly_count_compact" == *'"bucket":"2026-01-01T01:00:00Z","value":0'* &&
          "$event_compact" == *'"metric":"count"'* &&
          "$event_compact" == *'"provider":"strava"'* &&
          "$event_compact" == *'"bucket":"2026-01-01","value":2'* &&
          "$event_compact" == *'"bucket":"2026-01-02","value":0'* ]]; then
      printf '%s\n' "$count_response"
      printf '%s\n' "$duration_response"
      printf '%s\n' "$installs_response"
      printf '%s\n' "$active_response"
      printf '%s\n' "$hourly_count_response"
      printf '%s\n' "$event_response"
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
  require_cmd python3
  ADMIN_TOKEN="${FANTASMA_ADMIN_TOKEN:-$(generate_admin_token)}"
  export FANTASMA_ADMIN_TOKEN="$ADMIN_TOKEN"
  check_disk
  check_docker

  trap cleanup EXIT INT TERM

  compose down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
  compose up -d --build
  wait_for_http

  local provisioned
  provisioned="$(
    "$ROOT_DIR/scripts/provision-project.sh" \
      --api-url "http://localhost:8082" \
      --admin-token "$ADMIN_TOKEN" \
      --project-name "Compose Smoke" \
      --ingest-key-name "smoke-ingest" \
      --read-key-name "smoke-read"
  )"
  INGEST_KEY="$(
    python3 - "$provisioned" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(payload["ingest_key"]["secret"])
PY
  )"
  READ_KEY="$(
    python3 - "$provisioned" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(payload["read_key"]["secret"])
PY
  )"

  curl -fsS -X POST "http://localhost:8081/v1/events" \
    -H "Content-Type: application/json" \
    -H "X-Fantasma-Key: ${INGEST_KEY}" \
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
