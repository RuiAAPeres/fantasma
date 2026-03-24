#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage:
  ./scripts/verify-changed-surface.sh list
  ./scripts/verify-changed-surface.sh print <profile>
  ./scripts/verify-changed-surface.sh run <profile>

profiles:
  api-contract
  worker-contract
  cli-operator
  react-native-sdk
  rust-ci
  script-only
EOF
}

profile_commands() {
  case "${1:-}" in
    api-contract)
      cat <<'EOF'
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p fantasma-core --quiet
cargo test -p fantasma-api --lib --quiet
cargo test -p fantasma-cli --test http_flows --quiet
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet
./scripts/cli-smoke.sh
EOF
      ;;
    worker-contract)
      cat <<'EOF'
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p fantasma-worker --lib --quiet
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet
EOF
      ;;
    cli-operator)
      cat <<'EOF'
bash -n scripts/cli-smoke.sh scripts/provision-project.sh scripts/ci/should-run-cli-smoke.sh scripts/tests/cli-dogfood-gate.sh
bash scripts/tests/cli-dogfood-gate.sh
cargo test -p fantasma-cli --test http_flows --quiet
./scripts/cli-smoke.sh
EOF
      ;;
    react-native-sdk)
      cat <<'EOF'
pnpm install
pnpm --dir sdks/react-native/fantasma-react-native typecheck
pnpm --dir sdks/react-native/fantasma-react-native test
pnpm --dir sdks/react-native/fantasma-react-native lint
pnpm --dir sdks/react-native/fantasma-react-native build
pnpm --dir sdks/react-native/fantasma-react-native bridge:ios:check
ANDROID_HOME="${ANDROID_HOME:?set ANDROID_HOME}" ANDROID_SDK_ROOT="${ANDROID_SDK_ROOT:-$ANDROID_HOME}" pnpm --dir sdks/react-native/fantasma-react-native bridge:android:check
swift test --package-path . --filter FantasmaReactNativeBridgeTests
swift test --package-path . --filter FantasmaSDKTests
swift test --package-path . -Xswiftc -strict-concurrency=complete
cd sdks/android && ANDROID_HOME="${ANDROID_HOME:?set ANDROID_HOME}" ANDROID_SDK_ROOT="${ANDROID_SDK_ROOT:-$ANDROID_HOME}" ./gradlew :fantasma-sdk:testDebugUnitTest --tests 'com.fantasma.sdk.reactnative.FantasmaReactNativeBridgeTest'
cd sdks/android && ANDROID_HOME="${ANDROID_HOME:?set ANDROID_HOME}" ANDROID_SDK_ROOT="${ANDROID_SDK_ROOT:-$ANDROID_HOME}" ./gradlew :fantasma-sdk:testDebugUnitTest
cd sdks/android && ANDROID_HOME="${ANDROID_HOME:?set ANDROID_HOME}" ANDROID_SDK_ROOT="${ANDROID_SDK_ROOT:-$ANDROID_HOME}" ./gradlew :fantasma-sdk:lintDebug :fantasma-sdk:assembleDebugAndroidTest :demo:assembleDebug ktlintCheck detekt
EOF
      ;;
    rust-ci)
      cat <<'EOF'
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
./scripts/docker-test.sh --quiet
EOF
      ;;
    script-only)
      cat <<'EOF'
echo "Run bash -n against the changed scripts and the matching scripts/tests/* audit helpers for this slice."
EOF
      ;;
    *)
      echo "unknown profile: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
}

list_profiles() {
  cat <<'EOF'
api-contract
worker-contract
cli-operator
react-native-sdk
rust-ci
script-only
EOF
}

run_profile() {
  local profile="$1"
  while IFS= read -r command; do
    [[ -z "$command" ]] && continue
    echo "+ $command"
    bash -lc "$command"
  done < <(profile_commands "$profile")
}

main() {
  local mode="${1:-}"

  case "$mode" in
    list)
      list_profiles
      ;;
    print)
      [[ $# -eq 2 ]] || {
        usage >&2
        exit 1
      }
      profile_commands "$2"
      ;;
    run)
      [[ $# -eq 2 ]] || {
        usage >&2
        exit 1
      }
      run_profile "$2"
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
