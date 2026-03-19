#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker/compose.yaml"
PROJECT_NAME="${FANTASMA_CLI_SMOKE_PROJECT_NAME:-fantasma-cli-smoke}"
ADMIN_TOKEN=""
INSTANCE_NAME="${FANTASMA_CLI_SMOKE_INSTANCE:-local}"
API_URL="${FANTASMA_CLI_SMOKE_API_URL:-}"
INGEST_URL="${FANTASMA_CLI_SMOKE_INGEST_URL:-}"
PROJECT_LABEL="${FANTASMA_CLI_SMOKE_PROJECT_NAME_LABEL:-CLI Smoke}"
TIMEOUT_SECONDS=60
SLEEP_SECONDS=2
KEEP_STACK="${FANTASMA_CLI_SMOKE_KEEP_STACK:-0}"
XDG_CONFIG_HOME="$(mktemp -d)"
COMPOSE_POSTGRES_PORTS="${FANTASMA_CLI_SMOKE_POSTGRES_PORTS:-127.0.0.1::5432}"
COMPOSE_INGEST_PORTS="${FANTASMA_CLI_SMOKE_INGEST_PORTS:-127.0.0.1::8081}"
COMPOSE_API_PORTS="${FANTASMA_CLI_SMOKE_API_PORTS:-127.0.0.1::8082}"

compose() {
  FANTASMA_POSTGRES_PORTS="$COMPOSE_POSTGRES_PORTS" \
    FANTASMA_INGEST_PORTS="$COMPOSE_INGEST_PORTS" \
    FANTASMA_API_PORTS="$COMPOSE_API_PORTS" \
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

resolve_service_url() {
  local service="$1"
  local container_port="$2"
  local binding

  binding="$(compose port "$service" "$container_port" | tail -n 1)"
  if [[ -z "$binding" ]]; then
    echo "failed to resolve published port for ${service}:${container_port}" >&2
    print_logs >&2
    exit 1
  fi

  python3 - "$binding" <<'PY'
import sys

binding = sys.argv[1].strip()
if binding.startswith("["):
    host, _, remainder = binding[1:].partition("]")
    port = remainder.lstrip(":")
else:
    host, port = binding.rsplit(":", 1)

print(f"http://{host}:{port}")
PY
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  rm -rf "$XDG_CONFIG_HOME"

  if [[ "$KEEP_STACK" == "1" ]]; then
    echo "keeping CLI smoke stack because FANTASMA_CLI_SMOKE_KEEP_STACK=1" >&2
    exit "$exit_code"
  fi

  compose down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
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

assert_usage_events() {
  local expected_project_id="$1"
  local usage_start_day="$2"
  local usage_end_day="$3"
  local usage_json total_events project_count observed_project_id observed_events
  usage_json="$(cli usage events --start "$usage_start_day" --end "$usage_end_day" --json)"
  IFS=$'\t' read -r total_events project_count observed_project_id observed_events <<EOF
$(python3 - "$usage_json" "$expected_project_id" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
projects = payload.get("projects", [])
project = projects[0] if projects else {}
print(
    f"{payload.get('total_events_processed', '')}\t{len(projects)}\t"
    f"{project.get('project', {}).get('id', '')}\t{project.get('events_processed', '')}"
)
PY
)
EOF
  if [[ "$total_events" == "1" && "$project_count" == "1" && "$observed_project_id" == "$expected_project_id" && "$observed_events" == "1" ]]; then
    printf '%s\n' "$usage_json"
    return 0
  fi

  echo "usage events did not report the expected raw accepted-event total" >&2
  printf '%s\n' "$usage_json" >&2
  exit 1
}

poll_metrics() {
  local deadline metrics_json value active_json active_value live_json live_value
  deadline=$((SECONDS + TIMEOUT_SECONDS))

  while ((SECONDS < deadline)); do
    metrics_json="$(cli metrics sessions --metric count --granularity day --start 2026-01-01 --end 2026-01-01 --json)"
    active_json="$(cli metrics sessions --metric active_installs --start 2026-01-01 --end 2026-01-01 --interval day --json)"
    live_json="$(cli metrics live-installs --json)"
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
    active_value="$(
      python3 - "$active_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
series = payload.get("series", [])
points = series[0]["points"] if series else []
print(points[0]["value"] if points else "")
PY
    )"
    live_value="$(
      python3 - "$live_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(payload.get("value", ""))
PY
    )"

    if [[ "$value" == "1" && "$active_value" == "1" && "$live_value" == "1" ]]; then
      printf '%s\n' "$metrics_json"
      printf '%s\n' "$active_json"
      printf '%s\n' "$live_json"
      return 0
    fi

    sleep "$SLEEP_SECONDS"
  done

  echo "timed out waiting for CLI metrics query to observe ingested data" >&2
  print_logs >&2
  exit 1
}

assert_zero_match_range_delete() {
  local project_id="$1"
  local deadline deletion_json deletion_id get_json status list_json listed_id

  deletion_json="$(
    cli projects deletions range \
      --project "$project_id" \
      --start-at 2027-01-01T00:00:00Z \
      --end-before 2027-01-02T00:00:00Z \
      --json
  )"
  deletion_id="$(
    python3 - "$deletion_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(payload.get("deletion", {}).get("id", ""))
PY
  )"

  if [[ -z "$deletion_id" ]]; then
    echo "failed to parse deletion id from CLI range-delete output" >&2
    printf '%s\n' "$deletion_json" >&2
    exit 1
  fi

  deadline=$((SECONDS + TIMEOUT_SECONDS))
  while ((SECONDS < deadline)); do
    get_json="$(cli projects deletions get "$deletion_id" --project "$project_id" --json)"
    status="$(
      python3 - "$get_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print(payload.get("deletion", {}).get("status", ""))
PY
    )"
    if [[ "$status" == "succeeded" ]]; then
      list_json="$(cli projects deletions list --project "$project_id" --json)"
      listed_id="$(
        python3 - "$list_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
deletions = payload.get("deletions", [])
print(deletions[0]["id"] if deletions else "")
PY
      )"
      if [[ "$listed_id" == "$deletion_id" ]]; then
        return 0
      fi
    fi

    sleep "$SLEEP_SECONDS"
  done

  echo "timed out waiting for zero-match range deletion to succeed through the CLI" >&2
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
  if [[ -z "$INGEST_URL" ]]; then
    INGEST_URL="$(resolve_service_url fantasma-ingest 8081)"
  fi
  if [[ -z "$API_URL" ]]; then
    API_URL="$(resolve_service_url fantasma-api 8082)"
  fi
  wait_for_http

  cli instances add "$INSTANCE_NAME" --url "$API_URL" >/dev/null
  cli auth login --instance "$INSTANCE_NAME" --token "$ADMIN_TOKEN" >/dev/null

  local project_output project_id ingest_key usage_start_day usage_end_day
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

  usage_start_day="$(date -u +%F)"
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

  usage_end_day="$(date -u +%F)"

  assert_usage_events "$project_id" "$usage_start_day" "$usage_end_day"
  poll_metrics
  assert_zero_match_range_delete "$project_id"
}

main "$@"
