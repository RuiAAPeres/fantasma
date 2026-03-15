#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.yaml"
PROJECT_NAME="${FANTASMA_CLI_SMOKE_PROJECT_NAME:-fantasma-cli-smoke-$$}"
ADMIN_TOKEN=""
INSTANCE_NAME="${FANTASMA_CLI_SMOKE_INSTANCE:-local}"
API_URL="${FANTASMA_CLI_SMOKE_API_URL:-http://localhost:8082}"
INGEST_URL="${FANTASMA_CLI_SMOKE_INGEST_URL:-http://localhost:8081}"
PROJECT_LABEL="${FANTASMA_CLI_SMOKE_PROJECT_NAME_LABEL:-CLI Smoke}"
TIMEOUT_SECONDS=60
SLEEP_SECONDS=2
XDG_CONFIG_HOME="$(mktemp -d)"

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

cli() {
  XDG_CONFIG_HOME="$XDG_CONFIG_HOME" cargo run -q -p fantasma-cli -- "$@"
}

print_logs() {
  compose logs --tail=200 || true
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  rm -rf "$XDG_CONFIG_HOME"
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$exit_code"
}

wait_for_http() {
  local deadline
  deadline=$((SECONDS + TIMEOUT_SECONDS))

  while ((SECONDS < deadline)); do
    if curl -fsS "${INGEST_URL}/health" >/dev/null 2>&1 &&
      curl -fsS "${API_URL}/health" >/dev/null 2>&1; then
      return 0
    fi

    sleep "$SLEEP_SECONDS"
  done

  echo "timed out waiting for Fantasma health endpoints" >&2
  print_logs >&2
  exit 1
}

extract_line_value() {
  local label="$1"
  local text="$2"

  printf '%s\n' "$text" | awk -F': ' -v label="$label" '$1 == label {print $2}'
}

poll_metrics() {
  local deadline metrics_json value
  deadline=$((SECONDS + TIMEOUT_SECONDS))

  while ((SECONDS < deadline)); do
    metrics_json="$(cli metrics sessions --metric count --granularity day --start 2026-01-01 --end 2026-01-01 --json)"
    value="$(
      python3 - "$metrics_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
series = payload.get("series", [])
points = series[0]["points"] if series else []
print(points[0]["value"] if points else "")
PY
    )"

    if [[ "$value" == "1" ]]; then
      printf '%s\n' "$metrics_json"
      return 0
    fi

    sleep "$SLEEP_SECONDS"
  done

  echo "timed out waiting for CLI metrics query to observe ingested data" >&2
  print_logs >&2
  exit 1
}

main() {
  require_cmd cargo
  require_cmd curl
  require_cmd docker
  require_cmd mktemp
  require_cmd python3
  ADMIN_TOKEN="${FANTASMA_ADMIN_TOKEN:-$(generate_admin_token)}"
  export FANTASMA_ADMIN_TOKEN="$ADMIN_TOKEN"

  trap cleanup EXIT INT TERM

  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  compose up -d --build
  wait_for_http

  cli instances add "$INSTANCE_NAME" --url "$API_URL" >/dev/null
  cli auth login --instance "$INSTANCE_NAME" --token "$ADMIN_TOKEN" >/dev/null

  local project_output project_id ingest_key
  project_output="$(cli projects create --name "$PROJECT_LABEL" --ingest-key-name cli-smoke-ingest)"
  project_id="$(extract_line_value "project id" "$project_output")"
  ingest_key="$(extract_line_value "ingest key" "$project_output")"

  if [[ -z "$project_id" || -z "$ingest_key" ]]; then
    echo "failed to parse project creation output" >&2
    printf '%s\n' "$project_output" >&2
    exit 1
  fi

  cli projects use "$project_id" >/dev/null
  cli keys create --kind read --name cli-smoke-read >/dev/null

  curl -fsS -X POST "${INGEST_URL}/v1/events" \
    -H "Content-Type: application/json" \
    -H "X-Fantasma-Key: ${ingest_key}" \
    -d '{
      "events": [
        {
          "event": "app_open",
          "timestamp": "2026-01-01T00:00:00Z",
          "install_id": "cli-smoke-install-1",
          "platform": "ios",
          "app_version": "1.0.0",
          "os_version": "18.3"
        }
      ]
    }' >/dev/null

  poll_metrics
}

main "$@"
