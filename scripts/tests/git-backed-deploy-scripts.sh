#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BOOTSTRAP_SCRIPT="$ROOT_DIR/scripts/bootstrap-host-checkout.sh"
DEPLOY_SCRIPT="$ROOT_DIR/scripts/deploy-host.sh"
DEPLOYMENT_DOC="$ROOT_DIR/docs/deployment.md"

assert_present() {
  local pattern="$1"
  local path="$2"

  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "expected pattern '$pattern' missing in $path" >&2
    exit 1
  fi
}

bash -n "$BOOTSTRAP_SCRIPT"
bash -n "$DEPLOY_SCRIPT"

assert_present 'git-backed' "$DEPLOYMENT_DOC"
assert_present 'scripts/bootstrap-host-checkout.sh' "$DEPLOYMENT_DOC"
assert_present 'scripts/deploy-host.sh' "$DEPLOYMENT_DOC"
assert_present 'git checkout --detach' "$DEPLOYMENT_DOC"

echo "git-backed deploy script audit passed"
