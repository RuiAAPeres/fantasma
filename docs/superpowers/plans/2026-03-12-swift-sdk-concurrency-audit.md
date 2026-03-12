# Swift SDK Concurrency Audit Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the iOS SDK so its public API is async-first, explicitly throwing, and internally aligned with Swift 6 structured concurrency while preserving Fantasma's durable ingest behavior.

**Architecture:** Keep `Fantasma` as the static entrypoint, but remove the synchronous blocking façade and push all stateful behavior behind actor isolation. The runtime should create and own one shared core lazily, storage and identity should stop relying on unchecked cross-actor sharing, and background/timer driven flushes should use cancellation-friendly async primitives.

**Tech Stack:** Swift 6, Swift Testing, Foundation, SQLite C API, UIKit notifications.

---

### Task 1: Record The Audit Work

**Files:**
- Modify: `docs/STATUS.md`
- Create: `docs/superpowers/plans/2026-03-12-swift-sdk-concurrency-audit.md`

- [ ] **Step 1: Mark the Swift SDK audit as active in project memory**
- [ ] **Step 2: Save this implementation plan in the repository**

### Task 2: Add Red Coverage For The New SDK Contract

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests/FantasmaSDKTests.swift`

- [ ] **Step 1: Add failing tests for the async/throwing static API shape**
- [ ] **Step 2: Add failing tests for misuse and failure propagation (`track` before configure, invalid config, flush/upload failures, durable queue retention)**
- [ ] **Step 3: Run focused Swift tests to confirm the new assertions fail for the expected reasons**

### Task 3: Refactor The Runtime To Swift 6 Async Boundaries

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/Fantasma.swift`
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/FantasmaCore.swift`
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/SQLiteQueue.swift`
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/IdentityStore.swift`
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/EventUploader.swift`
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/Transport.swift`

- [ ] **Step 1: Replace the blocking façade with async static methods and a public typed error surface**
- [ ] **Step 2: Make shared core creation lazy and throwing instead of `try!` based**
- [ ] **Step 3: Move queue and identity access behind actor-isolated boundaries and remove repo-owned unchecked sendability where practical**
- [ ] **Step 4: Rework periodic/background flush scheduling to structured concurrency primitives**
- [ ] **Step 5: Keep behavior minimal and rerun focused tests until the new contract passes**

### Task 4: Align Docs, Demo, And CI

**Files:**
- Modify: `sdks/ios/README.md`
- Modify: `sdks/ios/FantasmaSDK/README.md`
- Modify: `apps/demo-ios/FantasmaDemo/FantasmaDemoApp.swift`
- Modify: `apps/demo-ios/FantasmaDemo/ContentView.swift`
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Update public SDK docs and demo usage to `try await` and typed `URL` configuration**
- [ ] **Step 2: Add Swift SDK strict-concurrency verification to CI**
- [ ] **Step 3: Move the audit entry in `docs/STATUS.md` from active to completed with the shipped changes**

### Task 5: Verify Before Completion

**Files:**
- Verify only

- [ ] **Step 1: Run the focused SDK test suite**
- [ ] **Step 2: Run `swift test -Xswiftc -strict-concurrency=complete` for the package**
- [ ] **Step 3: Review the final diff for accidental API or behavior drift before handoff**
