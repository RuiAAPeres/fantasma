# CLI Event Discovery Surface Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the remaining public event read routes through `fantasma-cli` without breaking the existing metrics command shape.

**Architecture:** Keep the current `metrics events` series command unchanged and add adjacent CLI entrypoints for total event volume, top events, and event catalog reads. Reuse the existing stored read-key resolution, filter parsing, and HTTP status guidance so all read-key-backed metrics commands behave uniformly.

**Tech Stack:** Rust, Clap, Reqwest, Tokio tests, Axum test server

---

### Task 1: Lock Down The Missing CLI Surface

**Files:**
- Modify: `crates/fantasma-cli/tests/http_flows.rs`

- [x] **Step 1: Write failing tests for the missing commands**
- [x] **Step 2: Run the targeted CLI HTTP-flow test command and confirm the new cases fail for the expected reason**

### Task 2: Add The CLI Commands And Request Handling

**Files:**
- Modify: `crates/fantasma-cli/src/cli.rs`
- Modify: `crates/fantasma-cli/src/app.rs`

- [x] **Step 1: Add Clap args/subcommands for total, top, and catalog event reads**
- [x] **Step 2: Implement minimal HTTP request and text/json rendering support**
- [x] **Step 3: Run the targeted CLI HTTP-flow test command and confirm the new cases pass**

### Task 3: Document And Verify The Slice

**Files:**
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [x] **Step 1: Update operator CLI examples to include the new read surfaces**
- [x] **Step 2: Run repo-scope verification for the touched CLI/docs slice**
- [x] **Step 3: Record the completed slice in `docs/STATUS.md`**
