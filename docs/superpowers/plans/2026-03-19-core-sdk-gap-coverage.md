# Core + SDK Gap Coverage Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the audit-identified regression-test gaps in Rust core/auth and the iOS SDK without widening product scope or changing production behavior unless a failing test proves a bug.

**Architecture:** Add narrow tests at the contract boundaries where behavior currently exists but is not explicitly locked: Rust event/auth validation helpers and SDK upload/destination normalization edge cases. Keep the changes local to existing test modules so the public contract remains easy to trace.

**Tech Stack:** Rust unit tests with `cargo test`, Swift Testing via `swift test`, existing Fantasma SDK test harnesses.

---

## Chunk 1: Rust Contract Gaps

### Task 1: Lock event validation boundaries in `fantasma-core`

**Files:**
- Modify: `crates/fantasma-core/src/events.rs`

- [ ] **Step 1: Write the failing tests**

Add unit tests for:
- property keys at the 63/64 character boundary
- invalid leading digit / empty property key
- event payloads above the 8 KiB cap

- [ ] **Step 2: Run the targeted test module to verify the new tests fail for the expected reason**

Run: `cargo test -p fantasma-core events::tests -- --nocapture`

- [ ] **Step 3: Adjust production code only if a test exposes a real mismatch**

Expected: no production change unless the tests reveal a contract bug.

- [ ] **Step 4: Re-run the targeted module**

Run: `cargo test -p fantasma-core events::tests -- --nocapture`
Expected: PASS

### Task 2: Lock admin auth error paths in `fantasma-auth`

**Files:**
- Modify: `crates/fantasma-auth/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

Add unit tests for:
- missing authorization header -> `MissingCredentials`
- wrong bearer token -> `InvalidCredentials`
- malformed auth scheme / non-UTF8 header -> `InvalidCredentials`
- short ingest key prefix fallback in `derive_key_prefix`

- [ ] **Step 2: Run the targeted auth tests to verify new coverage**

Run: `cargo test -p fantasma-auth -- --nocapture`

- [ ] **Step 3: Adjust production code only if a test exposes a real mismatch**

Expected: no production change unless the tests reveal a bug.

- [ ] **Step 4: Re-run the targeted auth tests**

Run: `cargo test -p fantasma-auth -- --nocapture`
Expected: PASS

## Chunk 2: SDK Contract Gaps

### Task 3: Cover terminal and retryable ingest failures in the SDK

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests/FantasmaSDKTests.swift`

- [ ] **Step 1: Write the failing tests**

Add SDK behavior tests for:
- `401 unauthorized` flush behavior
- `422 invalid_request` / validation failure flush behavior
- `500 internal_server_error` remaining retryable
- `409` with malformed or unrelated error envelope remaining retryable

- [ ] **Step 2: Run the targeted Swift tests to verify failures or current behavior**

Run: `swift test --package-path /Users/ruiperes/Code/fantasma --filter FantasmaSDKBehaviorTests`

- [ ] **Step 3: Adjust production code only if the new tests prove a real contract mismatch**

Expected: if the desired behavior differs from current code, implement the smallest change in `EventUploader.swift` or `FantasmaCore.swift`.

- [ ] **Step 4: Re-run the targeted Swift behavior tests**

Run: `swift test --package-path /Users/ruiperes/Code/fantasma --filter FantasmaSDKBehaviorTests`
Expected: PASS

### Task 4: Cover destination normalization edges in the SDK

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests/FantasmaSDKTests.swift`

- [ ] **Step 1: Write the failing tests**

Add tests proving that configuration normalization treats equivalent destinations as the same when:
- host/scheme casing differs
- trailing slashes differ
- write-key whitespace is trimmed but path differences still change the destination

- [ ] **Step 2: Run the targeted Swift tests**

Run: `swift test --package-path /Users/ruiperes/Code/fantasma --filter FantasmaSDKBehaviorTests`

- [ ] **Step 3: Adjust production code only if a test proves the normalizer is wrong**

Expected: no production change unless a failing test identifies a real normalization bug.

- [ ] **Step 4: Re-run the targeted Swift behavior tests**

Run: `swift test --package-path /Users/ruiperes/Code/fantasma --filter FantasmaSDKBehaviorTests`
Expected: PASS

## Chunk 3: Verification

### Task 5: Run the affected suites end to end

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the directly affected Rust suites**

Run: `cargo test -p fantasma-core -p fantasma-auth`

- [ ] **Step 2: Run the directly affected API query/parser suite**

Run: `cargo test -p fantasma-api --lib`

- [ ] **Step 3: Run the SDK package tests**

Run: `swift test --package-path /Users/ruiperes/Code/fantasma`

- [ ] **Step 4: Record completion in project memory**

Update `docs/STATUS.md` with the gap-coverage slice and the verification commands that passed.
