#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_PATH="$ROOT_DIR/scripts/ci/should-run-cli-smoke.sh"

assert_gate() {
  local expected="$1"
  shift

  local actual
  actual="$(printf '%s\n' "$@" | bash "$SCRIPT_PATH")"

  if [[ "$actual" != "$expected" ]]; then
    echo "expected gate result '$expected' but got '$actual' for paths:" >&2
    printf '  %s\n' "$@" >&2
    exit 1
  fi
}

assert_gate true "crates/fantasma-cli/src/app.rs"
assert_gate true "Cargo.toml"
assert_gate true "scripts/provision-project.sh"
assert_gate true "scripts/ci/should-run-cli-smoke.sh"
assert_gate true "scripts/tests/cli-dogfood-gate.sh"
assert_gate true ".github/workflows/ci.yml"
assert_gate true "docs/deployment.md"
assert_gate true "crates/fantasma-api/src/http.rs"
assert_gate true "crates/fantasma-auth/src/lib.rs"
assert_gate true "crates/fantasma-core/src/events.rs"
assert_gate true "crates/fantasma-worker/src/worker.rs"
assert_gate true "crates/fantasma-store/src/lib.rs"
assert_gate true "crates/fantasma-ingest/src/http.rs"
assert_gate true "infra/docker/Dockerfile.api"
assert_gate true "schemas/openapi/fantasma.yaml"
assert_gate true "infra/docker/compose.yaml"
assert_gate false "README.md"
assert_gate false "docs/architecture.md"
assert_gate false "sdks/ios/FantasmaSDK/README.md"

assert_gate true \
  "docs/architecture.md" \
  "crates/fantasma-cli/src/cli.rs"

echo "cli dogfood gate cases passed"
