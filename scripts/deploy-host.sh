#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST=""
TARGET_DIR="/home/rui/fantasma"
REF=""
DEPLOY_PROD=1
DEPLOY_DEMO=1

usage() {
  cat <<'EOF'
Usage: scripts/deploy-host.sh --host USER@HOST [options]

Options:
  --host USER@HOST   SSH target for the deployment host
  --target-dir DIR   Remote checkout path (default: /home/rui/fantasma)
  --ref REF          Commit SHA, tag, or ref to deploy (default: local HEAD)
  --skip-prod        Do not deploy the fantasma-prod stack
  --skip-demo        Do not deploy the fantasma-demo stack
  --help             Show this message
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

while (($# > 0)); do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --target-dir)
      TARGET_DIR="$2"
      shift 2
      ;;
    --ref)
      REF="$2"
      shift 2
      ;;
    --skip-prod)
      DEPLOY_PROD=0
      shift
      ;;
    --skip-demo)
      DEPLOY_DEMO=0
      shift
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

require_cmd git
require_cmd ssh

if [[ -z "$HOST" ]]; then
  echo "--host is required" >&2
  usage >&2
  exit 1
fi

if [[ "$DEPLOY_PROD" -eq 0 && "$DEPLOY_DEMO" -eq 0 ]]; then
  echo "nothing to deploy: both --skip-prod and --skip-demo were set" >&2
  exit 1
fi

if [[ -z "$REF" ]]; then
  REF="$(git -C "$ROOT_DIR" rev-parse HEAD)"
fi

ssh "$HOST" "bash -s -- '$TARGET_DIR' '$REF' '$DEPLOY_PROD' '$DEPLOY_DEMO'" <<'REMOTE'
set -euo pipefail

target_dir="$1"
ref="$2"
deploy_prod="$3"
deploy_demo="$4"

if [[ ! -d "$target_dir/.git" ]]; then
  echo "remote checkout is not git-backed: $target_dir" >&2
  exit 1
fi

cd "$target_dir"

git fetch --prune origin
git checkout --detach "$ref"
resolved_ref="$(git rev-parse HEAD)"

if [[ "$deploy_prod" == "1" ]]; then
  if [[ ! -f .env.production ]]; then
    echo "missing .env.production in $target_dir" >&2
    exit 1
  fi

  docker compose \
    --env-file .env.production \
    -f infra/docker/compose.yaml \
    -f infra/docker/compose.prod.yaml \
    -p fantasma-prod \
    up -d --build
fi

if [[ "$deploy_demo" == "1" ]]; then
  if [[ ! -f .env.demo ]]; then
    echo "missing .env.demo in $target_dir" >&2
    exit 1
  fi

  docker compose \
    --env-file .env.demo \
    -f infra/docker/compose.yaml \
    -f infra/docker/compose.prod.yaml \
    -p fantasma-demo \
    up -d --build
fi

echo "deployed commit $resolved_ref"
REMOTE
