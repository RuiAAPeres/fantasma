#!/usr/bin/env bash
set -euo pipefail

API_URL="${FANTASMA_API_URL:-http://localhost:8082}"
ADMIN_TOKEN="${FANTASMA_ADMIN_TOKEN:-}"
PROJECT_NAME="Fantasma Local Project"
INGEST_KEY_NAME="default-ingest"
READ_KEY_NAME=""

usage() {
  cat <<'EOF'
Usage: scripts/provision-project.sh [options]

Options:
  --api-url URL           Fantasma API base URL (default: http://localhost:8082)
  --admin-token TOKEN     Operator bearer token (default: FANTASMA_ADMIN_TOKEN)
  --project-name NAME     Project name to create
  --ingest-key-name NAME  Name for the initial ingest key
  --read-key-name NAME    Optional name for an additional read key
  --help                  Show this message
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

json_project_payload() {
  python3 - "$PROJECT_NAME" "$INGEST_KEY_NAME" <<'PY'
import json
import sys

print(json.dumps({
    "name": sys.argv[1],
    "ingest_key_name": sys.argv[2],
}))
PY
}

json_read_key_payload() {
  python3 - "$READ_KEY_NAME" <<'PY'
import json
import sys

print(json.dumps({
    "name": sys.argv[1],
    "kind": "read",
}))
PY
}

project_id_from_response() {
  python3 - "$1" <<'PY'
import json
import sys

print(json.loads(sys.argv[1])["project"]["id"])
PY
}

merge_provisioning_responses() {
  python3 - "$1" "$2" <<'PY'
import json
import sys

created = json.loads(sys.argv[1])
read_key = json.loads(sys.argv[2])
created["read_key"] = read_key["key"]
print(json.dumps(created, indent=2))
PY
}

while (($# > 0)); do
  case "$1" in
    --api-url)
      API_URL="$2"
      shift 2
      ;;
    --admin-token)
      ADMIN_TOKEN="$2"
      shift 2
      ;;
    --project-name)
      PROJECT_NAME="$2"
      shift 2
      ;;
    --ingest-key-name)
      INGEST_KEY_NAME="$2"
      shift 2
      ;;
    --read-key-name)
      READ_KEY_NAME="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_cmd curl
require_cmd python3

API_URL="${API_URL%/}"

if [[ -z "$ADMIN_TOKEN" ]]; then
  echo "operator bearer token required: pass --admin-token or set FANTASMA_ADMIN_TOKEN" >&2
  exit 1
fi

create_project_response="$(
  curl -fsS \
    -X POST \
    "${API_URL}/v1/projects" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$(json_project_payload)"
)"

if [[ -z "$READ_KEY_NAME" ]]; then
  printf '%s\n' "$create_project_response"
  exit 0
fi

project_id="$(project_id_from_response "$create_project_response")"
create_read_key_response="$(
  curl -fsS \
    -X POST \
    "${API_URL}/v1/projects/${project_id}/keys" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$(json_read_key_payload)"
)"

merge_provisioning_responses "$create_project_response" "$create_read_key_response"
