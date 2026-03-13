# CI Clippy Regression Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the `CI` workflow by fixing the current `cargo clippy --workspace --all-targets -- -D warnings` failure on the Rust job.

**Architecture:** Keep the repair narrow. Fix the two newly surfaced `fantasma-bench` lint violations directly, and treat the long-standing store query helper signatures as an intentional API shape by documenting a targeted `clippy::too_many_arguments` allowance instead of forcing an unrelated refactor into this CI repair.

**Tech Stack:** Rust, Clippy, Cargo workspace checks, Markdown project memory.

---

### Task 1: Reproduce The Failing Rust Check

**Files:**
- Modify: `docs/STATUS.md`
- Test: `.github/workflows/ci.yml`

- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] Confirm the exact failing lints and capture the root cause in project memory before changing code.

### Task 2: Apply The Smallest Lint-Safe Fix

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] Replace the `fantasma-bench` patterns that now trip `manual_is_multiple_of` and `redundant_closure`.
- [ ] Add a targeted `clippy::too_many_arguments` allowance in `fantasma-store` for the intentionally wide query helper surface, keeping the API unchanged.

### Task 3: Re-Verify The Rust CI Slice

**Files:**
- Modify: `docs/STATUS.md`

- [ ] Run `cargo fmt --all --check`
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] Run `DATABASE_URL=postgres://fantasma:fantasma@127.0.0.1:5432/fantasma cargo test --workspace --quiet` against the CI-style Postgres service
- [ ] Record the completed repair and fresh verification commands in `docs/STATUS.md`
