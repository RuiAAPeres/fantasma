# CI Stability Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stabilize Fantasma's GitHub Actions by fixing the deterministic `CI` failure in `fantasma-bench` and recalibrating benchmark budgets so the `Performance` workflow stops failing on normal runner variance.

**Architecture:** Keep the fix narrow. Preserve the benchmark CLI contract while removing the Clippy-triggering enum names, and lock the budget recalibration to a regression test that evaluates committed CI budgets against retained runner samples gathered during the investigation.

**Tech Stack:** Rust, Clippy, serde/JSON budget fixtures, GitHub Actions workflow contracts, Markdown project memory.

---

### Task 1: Lock The Budget Instability In A Regression Test

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Test: `crates/fantasma-bench/src/main.rs`

- [ ] Add a focused unit test that loads the committed CI budget file and evaluates it against retained `main` workflow samples that previously failed or nearly failed on GitHub runners.
- [ ] Run `cargo test -p fantasma-bench retained_ci_runner_samples_fit_committed_budgets -- --exact`
- [ ] Confirm the new test fails with the current budget thresholds before updating the budget file.

### Task 2: Remove The Deterministic Clippy Failure

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Test: `crates/fantasma-bench/src/main.rs`

- [ ] Rename the internal `Scenario` enum variants so they no longer trip `clippy::enum_variant_names`.
- [ ] Keep the external CLI and serialized `kebab-case` scenario names unchanged.
- [ ] Re-run the existing parser/unit coverage for `fantasma-bench`.
- [ ] Run `cargo clippy -p fantasma-bench --all-targets -- -D warnings` and confirm the crate is lint-clean.

### Task 3: Recalibrate Performance Budgets To Retained Runner History

**Files:**
- Modify: `crates/fantasma-bench/budgets/ci.json`
- Modify: `docs/STATUS.md`

- [ ] Update the committed CI budget thresholds to cover the retained `main` run samples gathered during investigation, while keeping the guardrails meaningful.
- [ ] Record the recalibration in `docs/STATUS.md` once the new thresholds are settled.
- [ ] Re-run `cargo test -p fantasma-bench retained_ci_runner_samples_fit_committed_budgets -- --exact` and confirm it passes with the updated budgets.

### Task 4: Verify The Workflow Contracts End To End

**Files:**
- Modify: `docs/STATUS.md`

- [ ] Run `cargo test -p fantasma-bench --quiet`
- [ ] Run `cargo clippy -p fantasma-bench --all-targets -- -D warnings`
- [ ] Run `./scripts/docker-test.sh --quiet` so DB-backed workspace verification stays on the repository's Docker path instead of relying on a host `DATABASE_URL`
- [ ] If verification is clean, update `docs/STATUS.md` to move this slice from active work to completed work with the specific commands/results captured in notes.
