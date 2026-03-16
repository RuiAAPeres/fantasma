# Swift SDK CI Flake Fix Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Swift SDK CI green again by removing the flaky batch-assembly destination-drift race from the Swift test suite without changing intended SDK behavior.

**Architecture:** Treat the failure as a test-synchronization bug first because the exact CI failure is a nondeterministic race in `destinationDriftDuringBatchAssemblyDiscardsPreparedBatch`, while the production `swift test -Xswiftc -strict-concurrency=complete` command still passes locally. Add a focused regression that waits on a deterministic signal from the reconfiguration path, then apply the smallest harness or SDK test hook needed so the test blocks until pending reconfiguration is actually observable by the actor before the upload gate is released.

**Tech Stack:** Swift Package Manager, Swift Testing, Swift concurrency, GitHub Actions

---

### Task 1: Reproduce and Pin the Flake

**Files:**
- Create: `docs/superpowers/plans/2026-03-16-swift-sdk-ci-flake-fix.md`
- Modify: `docs/STATUS.md`
- Test: `sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests/FantasmaSDKTests.swift`

- [ ] **Step 1: Record the active CI investigation**

Update `docs/STATUS.md` with a brief active note that the March 16, 2026 Swift SDK CI failure is being debugged as a flaky destination-drift test.

- [ ] **Step 2: Add a failing regression for the missing synchronization**

Extend the Swift SDK tests so the batch-assembly reconfiguration path can wait on a deterministic signal that the pending destination change has actually been registered before the upload boundary is released.

- [ ] **Step 3: Run the focused Swift test to verify the regression fails for the pre-fix race**

Run: `swift test --filter destinationDriftDuringBatchAssemblyDiscardsPreparedBatch`
Expected: FAIL before the synchronization fix when the old batch is allowed to upload ahead of the pending reconfiguration.

### Task 2: Apply the Minimal Fix

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/Fantasma.swift`
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/FantasmaCore.swift`
- Modify: `sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests/FantasmaSDKTests.swift`

- [ ] **Step 1: Add the smallest observable hook or harness change**

Expose only the minimal testing seam needed to know when `configure` has actually reached the actor and registered pending reconfiguration, keeping the public API unchanged.

- [ ] **Step 2: Make the flaky test wait on the deterministic condition**

Update `destinationDriftDuringBatchAssemblyDiscardsPreparedBatch` to wait for the new signal before releasing the upload boundary, and keep the assertions focused on preserving the discard-before-upload behavior.

- [ ] **Step 3: Run the focused Swift test to verify it passes**

Run: `swift test --filter destinationDriftDuringBatchAssemblyDiscardsPreparedBatch`
Expected: PASS

### Task 3: Verify and Close

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the full affected Swift verification**

Run: `swift test -Xswiftc -strict-concurrency=complete`
Expected: PASS

- [ ] **Step 2: Stress the focused test for flake resistance**

Run: `for i in {1..20}; do swift test --filter destinationDriftDuringBatchAssemblyDiscardsPreparedBatch >/tmp/fantasma-swift-flake.log 2>&1 || { cat /tmp/fantasma-swift-flake.log; exit 1; }; done`
Expected: PASS all iterations

- [ ] **Step 3: Record completion in project memory**

Update `docs/STATUS.md` with the root cause, the fix, and the fresh verification commands.
