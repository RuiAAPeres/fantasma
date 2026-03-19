# Ingest Cap and SDK Batching Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise the server ingest batch cap to 200 and move the iOS SDK defaults to 100-event uploads every 30 seconds.

**Architecture:** Keep the ingest path shape unchanged and only widen the validated batch envelope on the server. Keep the iOS SDK conservative by increasing its local upload batch size from 50 to 100 and relaxing the periodic timer from 10s to 30s so backlog drain improves without pushing the client all the way to the server cap.

**Tech Stack:** Rust (`fantasma-core`, `fantasma-ingest`), Swift (`FantasmaSDK`), OpenAPI YAML, project status docs.

---

## Chunk 1: Server-side ingest cap

### Task 1: Update ingest tests first

**Files:**
- Modify: `crates/fantasma-ingest/tests/http.rs`

- [ ] **Step 1: Write/adjust failing tests for the new accepted/rejected batch sizes**
- [ ] **Step 2: Run the targeted ingest HTTP tests to verify the old limit still fails the new expectation**
- [ ] **Step 3: Commit the failing test-only change if working in commit-sized slices**

### Task 2: Raise the validated batch cap

**Files:**
- Modify: `crates/fantasma-core/src/events.rs`

- [ ] **Step 1: Change the shared max batch constant from 100 to 200**
- [ ] **Step 2: Keep validation error text aligned with the constant**
- [ ] **Step 3: Re-run the targeted ingest HTTP tests and confirm green**

## Chunk 2: iOS SDK batching defaults

### Task 3: Update SDK tests first

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests/FantasmaSDKTests.swift`

- [ ] **Step 1: Adjust tests that assert the old 50-event batching/default timer behavior**
- [ ] **Step 2: Run the focused Swift tests to verify they fail under the current defaults**

### Task 4: Raise SDK batch size and relax periodic flush interval

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/FantasmaCore.swift`

- [ ] **Step 1: Change live defaults from 50 -> 100 events and 10s -> 30s**
- [ ] **Step 2: Re-run the focused Swift tests and confirm green**

## Chunk 3: Contract/docs alignment

### Task 5: Update public contract text and project memory

**Files:**
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Update the OpenAPI description/examples to reflect the widened ingest batch contract if they mention limits/default expectations**
- [ ] **Step 2: Record the start/finish of the change in `docs/STATUS.md`**

## Chunk 4: Verification

### Task 6: Run impacted checks

**Files:**
- Modify: none

- [ ] **Step 1: Run targeted Rust ingest tests**
- [ ] **Step 2: Run targeted Swift SDK tests**
- [ ] **Step 3: Run `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` because the change touches workspace Rust code**
- [ ] **Step 4: Summarize exact evidence and any remaining caveats before handoff**
