#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST=""
TARGET_DIR="/home/rui/fantasma"
REPO_URL="https://github.com/RuiAAPeres/fantasma.git"
REF=""

usage() {
  cat <<'EOF'
Usage: scripts/bootstrap-host-checkout.sh --host USER@HOST [options]

Options:
  --host USER@HOST   SSH target for the deployment host
  --target-dir DIR   Remote checkout path (default: /home/rui/fantasma)
  --repo-url URL     Git repository URL (default: public GitHub clone URL)
  --ref REF          Commit SHA, tag, or ref to check out (default: local HEAD)
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
    --repo-url)
      REPO_URL="$2"
      shift 2
      ;;
    --ref)
      REF="$2"
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

require_cmd git
require_cmd ssh

if [[ -z "$HOST" ]]; then
  echo "--host is required" >&2
  usage >&2
  exit 1
fi

if [[ -z "$REF" ]]; then
  REF="$(git -C "$ROOT_DIR" rev-parse HEAD)"
fi

ssh "$HOST" "bash -s -- '$TARGET_DIR' '$REPO_URL' '$REF'" <<'REMOTE'
set -euo pipefail

target_dir="$1"
repo_url="$2"
ref="$3"
parent_dir="$(dirname "$target_dir")"
base_name="$(basename "$target_dir")"
bootstrap_dir="$(mktemp -d "${parent_dir}/${base_name}.bootstrap.XXXXXX")"
backup_dir=""

cleanup() {
  if [[ -d "$bootstrap_dir" ]]; then
    rm -rf "$bootstrap_dir"
  fi
}

trap cleanup EXIT

mkdir -p "$parent_dir"

if [[ -d "$target_dir/.git" ]]; then
  echo "remote checkout already git-backed at $target_dir"
  exit 0
fi

if [[ -e "$target_dir" ]]; then
  backup_dir="${target_dir}.pre-git.$(date +%Y%m%d%H%M%S)"
  mv "$target_dir" "$backup_dir"
fi

git clone "$repo_url" "$bootstrap_dir"
git -C "$bootstrap_dir" fetch --prune origin
git -C "$bootstrap_dir" checkout --detach "$ref"

if [[ -n "$backup_dir" ]]; then
  find "$backup_dir" -maxdepth 1 -type f -name '.env*' ! -name '.env.example' \
    -exec cp {} "$bootstrap_dir"/ \;
fi

mv "$bootstrap_dir" "$target_dir"

trap - EXIT

echo "bootstrapped git-backed checkout at $target_dir"
if [[ -n "$backup_dir" ]]; then
  echo "previous directory preserved at $backup_dir"
fi
echo "checked out $(git -C "$target_dir" rev-parse HEAD)"
REMOTE
