# Production Proxy Network Wiring Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Fantasma's production API proxy attachment part of checked-in deploy config so `api.usefantasma.com` survives future redeploys without a manual `docker network connect`.

**Architecture:** Keep `infra/docker/compose.yaml` as the local default stack and add a production-only Compose override that joins `fantasma-api` to the shared external `proxy` network with a stable alias. Document the production command path and add a small config-level regression so the proxy wiring stays intentional.

**Tech Stack:** Docker Compose, Bash, Markdown docs

---

## Chunk 1: Production Compose Override

### Task 1: Add the production-only proxy override

**Files:**
- Create: `infra/docker/compose.prod.yaml`

- [ ] **Step 1: Create the production override**

Add a Compose override that:
- keeps `fantasma-api` on the default service network
- also attaches `fantasma-api` to external network `proxy`
- assigns alias `fantasma-api`
- allows the network name and alias to stay env-configurable

- [ ] **Step 2: Keep the local default untouched**

Do not change `infra/docker/compose.yaml` so local `docker compose -f infra/docker/compose.yaml up --build` still works without a shared proxy network.

- [ ] **Step 3: Validate the rendered production config**

Run:

```bash
FANTASMA_ADMIN_TOKEN=test-token docker compose -f infra/docker/compose.yaml -f infra/docker/compose.prod.yaml config
```

Expected:
- render succeeds
- `fantasma-api` includes the external `proxy` network
- alias `fantasma-api` is present on that network

## Chunk 2: Regression Guard

### Task 2: Add a cheap audit for the production proxy shape

**Files:**
- Create: `scripts/tests/prod-proxy-compose.sh`

- [ ] **Step 1: Write a shell audit**

Add a small script that asserts:
- `infra/docker/compose.prod.yaml` exists
- it declares an external proxy network
- `fantasma-api` is attached to that network
- the stable alias `fantasma-api` is present

- [ ] **Step 2: Run the audit**

Run:

```bash
bash scripts/tests/prod-proxy-compose.sh
```

Expected:
- PASS with a short success message

## Chunk 3: Deployment Docs And Project Memory

### Task 3: Document the production path

**Files:**
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Update deployment docs**

Document:
- the production override file
- the Hetzner-style shared `proxy` network usage
- the production bring-up command using both Compose files
- the expectation that Caddy routes `api.usefantasma.com` to alias `fantasma-api`

- [ ] **Step 2: Update project memory**

Record in `docs/STATUS.md`:
- work started on durable production proxy wiring
- completion note and verification once the change lands

## Chunk 4: Verification And Commit

### Task 4: Verify and checkpoint

**Files:**
- Modify: `infra/docker/compose.prod.yaml`
- Modify: `scripts/tests/prod-proxy-compose.sh`
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run verification**

Run:

```bash
FANTASMA_ADMIN_TOKEN=test-token docker compose -f infra/docker/compose.yaml -f infra/docker/compose.prod.yaml config >/tmp/fantasma-prod-compose.yaml
bash scripts/tests/prod-proxy-compose.sh
```

Expected:
- rendered config succeeds
- audit script passes

- [ ] **Step 2: Commit**

```bash
git add infra/docker/compose.prod.yaml scripts/tests/prod-proxy-compose.sh docs/deployment.md docs/STATUS.md docs/superpowers/plans/2026-03-19-prod-proxy-network-wiring.md
git commit -m "ops: make prod proxy wiring durable"
```
