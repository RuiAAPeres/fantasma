# Fantasma CLI Test Hardening Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lock down the remaining uncovered Fantasma CLI command branches, JSON output paths, config normalization, and save-path durability with targeted tests.

**Architecture:** Keep the existing split between fast config-focused unit tests in `cli_config.rs`/`config.rs` and higher-level command behavior tests in `http_flows.rs` backed by the fake Axum server. Add production changes only when a new failing test proves a real CLI bug or missing branch.

**Tech Stack:** Rust, Tokio, Axum test server, Clap parser tests, tempfile-based config fixtures

---

## Chunk 1: Project Memory And Scope

### Task 1: Record the hardening pass in project memory

**Files:**
- Modify: `docs/STATUS.md`
- Create: `docs/superpowers/plans/2026-03-13-cli-test-hardening.md`

- [ ] Add an `Active` note for the CLI test-hardening pass.
- [ ] Save this plan file under `docs/superpowers/plans/`.

## Chunk 2: Integration Coverage

### Task 2: Expand `http_flows.rs` coverage

**Files:**
- Modify: `crates/fantasma-cli/tests/http_flows.rs`
- Modify if tests expose bugs: `crates/fantasma-cli/src/app.rs`

- [ ] Write failing tests for `auth logout` with explicit and implicit instance selection.
- [ ] Write failing tests for `projects use` persistence and later command reuse of the active project.
- [ ] Write failing tests for `keys create --kind ingest` to prove print-only behavior and no local state mutation.
- [ ] Write failing tests for `status --json` and `keys list --json`.
- [ ] Write failing tests for the 401/404/422 corrective-error matrix across login, project/key management, revoke, and metrics.
- [ ] Write a failing multi-profile isolation test that switches the active instance and proves requests use the right credentials/project state.
- [ ] Write failing tests for active/non-active instance removal edge cases.
- [ ] Write failing tests for invalid filter handling and exact query serialization, including repeated `--group-by`.
- [ ] Run `cargo test -p fantasma-cli --test http_flows` to verify the new cases fail for the expected reason before fixing anything.
- [ ] Apply the minimum production changes needed to make the new tests pass, then rerun the same test target.

## Chunk 3: Config Coverage

### Task 3: Expand config-focused coverage

**Files:**
- Modify: `crates/fantasma-cli/tests/cli_config.rs`
- Modify if tests expose bugs: `crates/fantasma-cli/src/config.rs`

- [ ] Write failing normalization tests for host-root, trailing-slash, and path-prefixed URLs.
- [ ] Write a failing load-path normalization test proving saved config is normalized on read.
- [ ] Write failing atomic-save cleanup tests for failpoint cleanup and success-path temp-file cleanup.
- [ ] If practical without platform breakage, document or test the non-Unix fail-closed boundary.
- [ ] Run `cargo test -p fantasma-cli --test cli_config` (and `--lib` if config.rs unit tests change) to verify the new cases fail before any implementation change.
- [ ] Apply the minimum production changes needed, then rerun the focused config tests.

## Chunk 4: Verification And Project Memory

### Task 4: Close out the hardening pass

**Files:**
- Modify: `docs/STATUS.md`
- Modify if command coverage changes operator semantics: `docs/deployment.md`

- [ ] Run `cargo fmt -p fantasma-cli -- --check`.
- [ ] Run `cargo test -p fantasma-cli`.
- [ ] Run `cargo run -p fantasma-cli -- --help`.
- [ ] Run `XDG_CONFIG_HOME="$(mktemp -d)" cargo run -p fantasma-cli -- status`.
- [ ] Run `git diff --check -- Cargo.toml README.md docs/STATUS.md docs/architecture.md docs/deployment.md crates/fantasma-cli`.
- [ ] Replace the `Active` note in `docs/STATUS.md` with a `Completed` note summarizing the new coverage and any implementation fixes.
