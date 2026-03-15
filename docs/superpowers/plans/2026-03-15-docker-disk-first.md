# Docker Disk-First Workflow Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Fantasma's local Docker workflows default to aggressive disk cleanup so repeated smoke and test runs do not accumulate large image and volume footprints.

**Architecture:** Keep the runtime service images themselves simple, but stop generating many one-off Compose project/image names and persistent cache volumes by default. Use stable Compose project names, disk-first cleanup flags in the smoke/test scripts, and one explicit cleanup helper for existing Fantasma-owned Docker artifacts.

**Tech Stack:** Docker Compose, Bash, Rust workspace verification, repo docs

---

### Task 1: Lock Down Disk-First Expectations

**Files:**
- Create: `scripts/tests/docker-disk-footprint.sh`

- [x] **Step 1: Write a failing audit script for stable Compose names and aggressive cleanup flags**
- [x] **Step 2: Run the audit script and confirm it fails against the current Docker setup**

### Task 2: Switch Compose And Scripts To Disk-First Defaults

**Files:**
- Modify: `infra/docker/compose.yaml`
- Modify: `infra/docker/compose.bench.yaml`
- Modify: `infra/docker/compose.test.yaml`
- Modify: `scripts/cli-smoke.sh`
- Modify: `scripts/compose-smoke.sh`
- Modify: `scripts/docker-test.sh`

- [x] **Step 1: Add stable Compose project names for local, bench, and test workflows**
- [x] **Step 2: Make smoke and test cleanup paths remove local images and cache volumes by default**
- [x] **Step 3: Keep opt-in escape hatches for debugging and cache retention**

### Task 3: Add Existing-Cruft Cleanup And Docs

**Files:**
- Create: `scripts/docker-reclaim.sh`
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [x] **Step 1: Add a helper that removes Fantasma-owned Docker images and volumes already on disk**
- [x] **Step 2: Document the new disk-first defaults and the explicit reclaim path**
- [x] **Step 3: Record the completed slice in project memory**

### Task 4: Verify End To End

**Files:**
- Modify: `docs/superpowers/plans/2026-03-15-docker-disk-first.md`

- [x] **Step 1: Run the new audit script red-green**
- [x] **Step 2: Run compose config and shell syntax verification**
- [x] **Step 3: Run the Docker-backed workspace tests and smoke path checks that this change can affect**

Verification note: `./scripts/docker-test.sh` passed. A live `./scripts/cli-smoke.sh` run reached stack startup and then failed because host port `5432` was already allocated by another local container, so the smoke-path behavior change itself was not the blocker in this session.
