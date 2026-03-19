# Doc and Remote Benchmark Follow-up Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Document the new ingest and iOS flush defaults, then benchmark the remote host against the updated behavior.

**Architecture:** Keep the documentation changes tightly scoped to the existing ingest and SDK behavior sections so the public and operator-facing docs reflect the new `200 / 100 / 30s` defaults without broad rewriting. Then rerun the remote-host benchmark using the real host and report the measured write and readiness behavior of the updated stack.

**Tech Stack:** Markdown docs, OpenAPI-aligned product docs, remote benchmark over SSH against Docker-hosted Fantasma services.

---

## Chunk 1: Documentation alignment

### Task 1: Update ingest and SDK behavior docs

**Files:**
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`
- Modify: `sdks/ios/FantasmaSDK/README.md`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Read only the existing ingest and SDK behavior sections that mention batching or flush cadence**
- [ ] **Step 2: Update the docs to state the new ingest cap, SDK upload batch size, and periodic flush interval**
- [ ] **Step 3: Record the completed doc follow-up in `docs/STATUS.md`**

## Chunk 2: Remote benchmark rerun

### Task 2: Benchmark the updated host

**Files:**
- Modify: none

- [ ] **Step 1: Deploy the updated code path to the remote host if the benchmark should reflect the live stack**
- [ ] **Step 2: Run a write-throughput benchmark on the remote demo stack with the new ingest cap in place**
- [ ] **Step 3: Run a bounded readiness benchmark so the result distinguishes accepted writes from worker catch-up**
- [ ] **Step 4: Summarize exact commands, measured throughput, and any timeout/catch-up caveats**
