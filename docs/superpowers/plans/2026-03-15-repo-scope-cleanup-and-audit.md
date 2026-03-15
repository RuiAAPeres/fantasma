# Repo Scope Cleanup And Audit Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove out-of-repo client positioning from the repository, delete obsolete placeholder artifacts, and leave the repo in a tidier, verified state with an explicit audit summary.

**Architecture:** This slice is documentation and repository-shape cleanup, not a product feature. Treat Fantasma as an API/CLI and SDK repository, remove any shipped placeholder assets and Compose wiring that imply extra bundled clients, then rename remaining example fixtures so operator/read-key flows stay generic and CLI-first.

**Tech Stack:** Markdown docs, Docker Compose, Bash, Rust tests, Cargo workspace tooling.

---

### Task 1: Record Cleanup Start And Inventory The Extra Client Surface

**Files:**
- Modify: `docs/STATUS.md`
- Inspect: `README.md`
- Inspect: `docs/deployment.md`
- Inspect: `docs/architecture.md`
- Inspect: `infra/docker/compose.yaml`
- Inspect: `apps/`

- [ ] **Step 1: Record the active cleanup/audit in project memory**

Update `docs/STATUS.md` so the repo history clearly shows that repo-scope client cleanup and a deep tidiness audit started on March 15, 2026.

- [ ] **Step 2: Verify the current reference inventory**

Run a repo-wide string search over active docs, apps, infra, scripts, and crates for stale out-of-repo client positioning.

Expected: references across docs, Compose, the obsolete placeholder assets, and a few test/example fixture names.

### Task 2: Remove Obsolete Placeholder Client Artifacts

**Files:**
- Delete: obsolete placeholder client files under `apps/`
- Modify: `README.md`
- Modify: `docs/deployment.md`
- Modify: `docs/architecture.md`
- Modify: `infra/docker/compose.yaml`

- [ ] **Step 1: Remove the obsolete shipped placeholder client**

Delete the placeholder client files and remove the extra Compose service/profile wiring so the deployment surface matches the repo scope.

- [ ] **Step 2: Rewrite product-facing docs to be API/CLI only**

Update `README.md`, `docs/deployment.md`, and `docs/architecture.md` so they no longer describe extra bundled clients or hosted surfaces as repository concerns.

- [ ] **Step 3: Verify references are gone from shipped surfaces**

Run a repo-wide string search over shipped surfaces (`README.md`, `docs/`, `infra/`, and `apps/`) to confirm the obsolete client wording is gone.

Expected: no matches outside historical plan/spec artifacts that intentionally preserve past context.

### Task 3: Rename Leftover Fixture Language

**Files:**
- Modify: `scripts/provision-project.sh`
- Modify: `crates/fantasma-api/tests/auth.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Replace stale fixture names with generic operator/read names**

Rename stale read-key examples to neutral names such as `"local-read"` or `"cli-read"` without changing behavior.

- [ ] **Step 2: Run targeted checks on the edited surfaces**

Run: `cargo test -p fantasma-api --test auth`

Expected: PASS

Run: `cargo test -p fantasma-worker --test pipeline`

Expected: PASS

Run: `cargo test -p fantasma-store api_key_lifecycle_round_trips_kinds_and_revocation -- --exact`

Expected: PASS

### Task 4: Deep Tidy Audit And Final Verification

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run workspace hygiene checks**

Run: `cargo fmt --all --check`

Expected: PASS

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS

- [ ] **Step 2: Re-scan for stale out-of-repo client positioning**

Run a final repo-wide string search over active docs, apps, infra, scripts, and crates to confirm the old client positioning is gone.

Expected: only acceptable historical references in `docs/STATUS.md` or archived planning/spec documents, with no active product/deployment/docs surfaces left.

- [ ] **Step 3: Record finish state and audit outcomes**

Update `docs/STATUS.md` with the completed cleanup, the removal of obsolete placeholder client surfaces, and the most important audit findings or remaining follow-ups.
