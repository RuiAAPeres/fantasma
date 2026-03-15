#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STORE_LIB="$ROOT_DIR/crates/fantasma-store/src/lib.rs"
DEPLOYMENT_DOC="$ROOT_DIR/docs/deployment.md"

assert_absent() {
  local pattern="$1"
  local path="$2"

  if rg -n "$pattern" "$path" >/dev/null; then
    echo "unexpected dead-flow pattern '$pattern' still present in $path" >&2
    exit 1
  fi
}

assert_present() {
  local pattern="$1"
  local path="$2"

  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "expected pattern '$pattern' missing in $path" >&2
    exit 1
  fi
}

assert_absent '^pub async fn fetch_event_metrics_cube_rows\(' "$STORE_LIB"
assert_absent '^pub async fn fetch_event_metrics_aggregate_cube_rows\(' "$STORE_LIB"
assert_absent '^pub async fn fetch_session_metrics_cube_rows\(' "$STORE_LIB"
assert_absent '^pub async fn fetch_events_after\(' "$STORE_LIB"
assert_absent '^pub async fn load_install_first_seen_in_tx\(' "$STORE_LIB"
assert_absent '^pub async fn load_install_session_state_in_tx\(' "$STORE_LIB"
assert_absent '^pub async fn increment_session_daily_for_new_session_in_tx\(' "$STORE_LIB"
assert_absent '^pub async fn add_session_daily_duration_delta_in_tx\(' "$STORE_LIB"
assert_absent '^pub async fn fetch_session_daily_range\(' "$STORE_LIB"
assert_absent '^pub async fn rebuild_session_daily_days_with_telemetry_in_tx\(' "$STORE_LIB"

assert_absent 'Run the CLI-driven smoke path against the same local stack:' "$DEPLOYMENT_DOC"
assert_present 'Run the CLI-driven smoke path in its own disposable stack:' "$DEPLOYMENT_DOC"

echo "dead flow audit passed"
