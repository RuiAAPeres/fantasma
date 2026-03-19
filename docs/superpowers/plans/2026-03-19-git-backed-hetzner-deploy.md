# Git-Backed Hetzner Deploy Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Hetzner Fantasma deployment git-backed and deploy by pinned commit SHA instead of copying files into a mutable host directory.

**Architecture:** Keep the current single-host Docker Compose model and on-host builds, but require the host checkout to be a real git clone. Add one bootstrap helper that converts a host path into a git-backed checkout while preserving host-local `.env*` files, and one deploy helper that fetches and checks out a pinned commit before running the prod and demo Compose updates.

**Tech Stack:** Bash, Git, SSH, Docker Compose, Markdown docs

---

## Chunk 1: Host Checkout Helpers

### Task 1: Add local-run SSH helpers for bootstrap and deploy

**Files:**
- Create: `scripts/bootstrap-host-checkout.sh`
- Create: `scripts/deploy-host.sh`
- Modify: `.gitignore`

- [ ] **Step 1: Add a bootstrap helper**

Create a script that:
- runs from the local checkout
- connects to a remote host over SSH
- converts a target directory into a git-backed clone if it is currently just copied files
- preserves host-local `.env*` files during the conversion
- checks out a pinned commit in detached-HEAD mode

- [ ] **Step 2: Add a deploy helper**

Create a script that:
- runs from the local checkout
- defaults to deploying the local `HEAD` commit SHA unless `--ref` overrides it
- connects to the host over SSH
- fetches the target commit
- checks out that commit in detached-HEAD mode
- runs the prod and demo `docker compose ... up -d --build` commands from the pinned revision

- [ ] **Step 3: Ignore host-local env files**

Update `.gitignore` so `.env.production`, `.env.demo`, and `.env.local` stay untracked in normal development and on the deployed host checkout.

## Chunk 2: Docs And Project Memory

### Task 2: Document the git-backed deployment path

**Files:**
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Update deployment docs**

Document:
- the host checkout must be a real git clone
- deploys should pin a commit SHA instead of following a mutable branch tip
- the first-time bootstrap flow
- the normal deploy flow using the new scripts
- the fact that host-local `.env.production` and `.env.demo` stay outside git history

- [ ] **Step 2: Update status**

Record:
- the git-backed deployment decision
- the new bootstrap and deploy helpers
- the verification used for the scripts

## Chunk 3: Verification And Host Migration

### Task 3: Verify and migrate the live Hetzner checkout

**Files:**
- Modify: `scripts/bootstrap-host-checkout.sh`
- Modify: `scripts/deploy-host.sh`
- Modify: `scripts/tests/git-backed-deploy-scripts.sh`

- [ ] **Step 1: Add a cheap shell regression**

Create a small shell test that:
- syntax-checks the new deploy scripts with `bash -n`
- asserts the deployment docs mention the new git-backed flow

- [ ] **Step 2: Run local verification**

Run:

```bash
bash -n scripts/bootstrap-host-checkout.sh
bash -n scripts/deploy-host.sh
bash scripts/tests/git-backed-deploy-scripts.sh
```

Expected:
- both scripts are shell-valid
- the regression script passes

- [ ] **Step 3: Bootstrap the Hetzner checkout**

Run the bootstrap helper against the live host so `/home/rui/fantasma` becomes a real clone while preserving `.env.production` and `.env.demo`.

- [ ] **Step 4: Confirm the host is now git-backed**

Check on the host:
- `/home/rui/fantasma/.git` exists
- `git rev-parse HEAD` succeeds
- `git status --short` stays clean apart from ignored host-local env files

