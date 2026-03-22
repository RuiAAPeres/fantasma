# Verification Process Hardening Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Fantasma's local verification expectations explicit enough that agents stop pushing backend and contract changes with narrower-than-CI proof.

**Architecture:** Tighten repository policy in `AGENTS.md`, document a small verification matrix in a dedicated developer-facing doc, and add one script that maps named change surfaces to the exact commands that should pass before push. Keep the script simple and deterministic: it prints and executes checked-in command lists rather than guessing from git diff.

**Tech Stack:** Bash, Markdown, existing Docker verification helpers

---

## Chunk 1: Policy And Verification Matrix

### Task 1: Record the change surface and the new rules

**Files:**
- Modify: `AGENTS.md`
- Modify: `docs/STATUS.md`
- Create: `docs/verification.md`

- [ ] **Step 1: Tighten the agent workflow rules**

Add explicit rules that:
- backend/API/worker contract changes must run the full affected Docker-backed suite before push
- red CI must be reproduced locally from the exact failing job before repush
- targeted subsets are not sufficient when CI covers a broader layer

- [ ] **Step 2: Write the verification matrix**

Document named local verification surfaces such as:
- `api-contract`
- `worker-contract`
- `cli-operator`
- `rust-ci`
- `script-only`

For each profile, list exact commands and when to use it.

## Chunk 2: Script And Audit

### Task 2: Add a checked-in verification runner

**Files:**
- Create: `scripts/verify-changed-surface.sh`
- Create: `scripts/tests/verify-changed-surface-audit.sh`

- [ ] **Step 1: Write the failing audit first**

Add a shell audit that expects:
- the script to exist
- a `list` mode
- the required profiles
- the required commands for `worker-contract` and `api-contract`

- [ ] **Step 2: Run the audit and watch it fail**

Run:
```bash
bash scripts/tests/verify-changed-surface-audit.sh
```

Expected:
- fail because `scripts/verify-changed-surface.sh` does not exist yet

- [ ] **Step 3: Implement the script**

Add a Bash script that:
- supports `list`
- supports `print <profile>`
- supports `run <profile>`
- exits non-zero for unknown profiles
- executes commands sequentially with clear echoing

- [ ] **Step 4: Run the audit and syntax checks**

Run:
```bash
bash scripts/tests/verify-changed-surface-audit.sh
bash -n scripts/verify-changed-surface.sh scripts/tests/verify-changed-surface-audit.sh
```

Expected:
- all pass

## Chunk 3: Final Proof And Project Memory

### Task 3: Verify the new process tooling and record completion

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Exercise the script profiles**

Run:
```bash
./scripts/verify-changed-surface.sh list
./scripts/verify-changed-surface.sh print api-contract
./scripts/verify-changed-surface.sh print worker-contract
```

Expected:
- profile list and command matrices print correctly

- [ ] **Step 2: Record completion**

Update `docs/STATUS.md` with the final rules/tooling and the exact verification commands that passed.
