#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_PATH="$ROOT_DIR/scripts/verify-changed-surface.sh"

if [[ ! -f "$SCRIPT_PATH" ]]; then
  echo "missing verification script: $SCRIPT_PATH" >&2
  exit 1
fi

assert_present() {
  local pattern="$1"
  if ! rg -n "$pattern" "$SCRIPT_PATH" >/dev/null; then
    echo "expected pattern '$pattern' missing in $SCRIPT_PATH" >&2
    exit 1
  fi
}

assert_present 'list\)'
assert_present 'print\)'
assert_present 'run\)'
assert_present 'api-contract'
assert_present 'worker-contract'
assert_present 'cli-operator'
assert_present 'rust-ci'
assert_present 'script-only'
assert_present 'cargo fmt --all --check'
assert_present 'cargo clippy --workspace --all-targets -- -D warnings'
assert_present '\./scripts/docker-test\.sh -p fantasma-worker --test pipeline --quiet'
assert_present '\./scripts/cli-smoke\.sh'

echo "verification surface audit passed"
