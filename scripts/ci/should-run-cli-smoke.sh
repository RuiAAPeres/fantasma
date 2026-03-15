#!/usr/bin/env bash
set -euo pipefail

matches_cli_dogfood_scope() {
  local path="$1"

  case "$path" in
    Cargo.toml|\
    Cargo.lock|\
    .github/workflows/ci.yml|\
    docs/deployment.md|\
    infra/docker/Dockerfile.api|\
    infra/docker/Dockerfile.ingest|\
    infra/docker/Dockerfile.worker|\
    infra/docker/compose.yaml|\
    schemas/openapi/fantasma.yaml|\
    scripts/ci/should-run-cli-smoke.sh|\
    scripts/cli-smoke.sh|\
    scripts/provision-project.sh|\
    scripts/tests/cli-dogfood-gate.sh|\
    crates/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

main() {
  local paths=()
  local path

  if (($# > 0)); then
    paths=("$@")
  else
    while IFS= read -r path; do
      paths+=("$path")
    done
  fi

  for path in "${paths[@]}"; do
    [[ -n "$path" ]] || continue
    if matches_cli_dogfood_scope "$path"; then
      printf 'true\n'
      return 0
    fi
  done

  printf 'false\n'
}

main "$@"
