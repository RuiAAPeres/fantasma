# Device Dimension And MacOS Platform Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the built-in `device` dimension and `platform=macos` across Fantasma's backend contracts, worker-owned aggregates, and first-party SDK emissions without increasing the existing public dimension budgets.

**Architecture:** Land the change from canonical source-of-truth outward: schemas and parser tests first, then store migrations and worker rebuild logic, then SDK payload emitters and docs. Keep historical data truthful by backfilling canonical source rows to `device='unknown'` and rebuilding worker-owned aggregates from those canonical tables.

**Tech Stack:** Rust (`axum`, `sqlx`, worker/store tests), Swift Package Manager, Android/Kotlin, Flutter/Dart, React Native bridge tests, OpenAPI + JSON Schema docs.

---

## Chunk 1: Backend Contract

### Task 1: Lock the public contract with failing tests

**Files:**
- Modify: `crates/fantasma-core/src/events.rs`
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-api/tests/auth.rs`
- Test: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] **Step 1: Write failing tests for `device` and `platform=macos`**
- [ ] **Step 2: Run the targeted Rust tests and verify they fail for the missing contract**
- [ ] **Step 3: Implement the minimal core/API contract changes**
- [ ] **Step 4: Re-run the targeted Rust tests and verify they pass**

## Chunk 2: Store + Worker

### Task 2: Add canonical storage and migration coverage

**Files:**
- Create: `crates/fantasma-store/migrations/0022_device_dimension_and_macos_platform.sql`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Write failing migration/store tests for `device` columns, `macos`, and rebuild prerequisites**
- [ ] **Step 2: Run the targeted store tests and verify they fail**
- [ ] **Step 3: Add the migration and minimal store support**
- [ ] **Step 4: Re-run the targeted store tests and verify they pass**

### Task 3: Extend worker rebuild and aggregation paths

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/src/sessionization.rs`
- Test: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] **Step 1: Write failing worker/pipeline tests for `device`, `macos`, and historical `unknown` reads**
- [ ] **Step 2: Run the targeted worker tests and verify they fail**
- [ ] **Step 3: Implement the minimal worker/rebuild changes**
- [ ] **Step 4: Re-run the targeted worker tests and verify they pass**

## Chunk 3: SDK Emission

### Task 4: Update Apple and Android-native emitters

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/FantasmaCore.swift`
- Modify: `sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests/FantasmaSDKTests.swift`
- Modify: `sdks/android/fantasma-sdk/src/main/java/com/fantasma/sdk/internal/EventEnvelope.kt`
- Modify: `sdks/android/fantasma-sdk/src/main/java/com/fantasma/sdk/internal/FantasmaRuntime.kt`
- Modify: Android SDK test files under `sdks/android/fantasma-sdk/src/test/...`

- [ ] **Step 1: Write failing iOS and Android payload tests**
- [ ] **Step 2: Run the targeted SDK tests and verify they fail**
- [ ] **Step 3: Implement the minimal emitter changes**
- [ ] **Step 4: Re-run the targeted SDK tests and verify they pass**

### Task 5: Update Flutter and React Native alignment

**Files:**
- Modify: `sdks/flutter/fantasma_flutter/lib/src/metadata.dart`
- Modify: `sdks/flutter/fantasma_flutter/lib/src/models.dart`
- Modify: `sdks/flutter/fantasma_flutter/test/fantasma_client_test.dart`
- Modify: `sdks/react-native/fantasma-react-native/ios/vendor/FantasmaSDK/FantasmaCore.swift`
- Modify: `sdks/react-native/fantasma-react-native/android/src/main/java/com/fantasma/sdk/internal/EventEnvelope.kt`
- Modify: `sdks/react-native/fantasma-react-native/android/src/main/java/com/fantasma/sdk/internal/FantasmaRuntime.kt`
- Modify: `sdks/react-native/fantasma-react-native/tests/client.test.ts`

- [ ] **Step 1: Write failing Flutter and React Native payload tests**
- [ ] **Step 2: Run the targeted package tests and verify they fail**
- [ ] **Step 3: Implement the minimal emitter/alignment changes**
- [ ] **Step 4: Re-run the targeted package tests and verify they pass**

## Chunk 4: Docs + Verification

### Task 6: Update public docs and run the affected verification matrix

**Files:**
- Modify: `schemas/events/mobile-event.schema.json`
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/architecture.md`
- Modify: `docs/STATUS.md`
- Modify: `sdks/ios/FantasmaSDK/README.md`
- Modify: `sdks/android/README.md`
- Modify: `sdks/flutter/fantasma_flutter/README.md`
- Modify: `sdks/react-native/fantasma-react-native/README.md`
- Modify: `mintlify/sdk-behavior.mdx`

- [ ] **Step 1: Update the contract and SDK docs**
- [ ] **Step 2: Run `cargo fmt --all --check`**
- [ ] **Step 3: Run `cargo clippy --workspace --all-targets -- -D warnings`**
- [ ] **Step 4: Run `./scripts/verify-changed-surface.sh run api-contract`**
- [ ] **Step 5: Run `./scripts/verify-changed-surface.sh run worker-contract`**
- [ ] **Step 6: Run `./scripts/verify-changed-surface.sh run react-native-sdk`**
- [ ] **Step 7: Run Flutter package verification**
- [ ] **Step 8: Record completion in `docs/STATUS.md` with exact verification commands and outcomes**
