# Swift SDK v0.2.0 Release Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish a new Swift SDK release tag that matches the new D2 ingest contract and update package-facing version references.

**Architecture:** Keep the code unchanged except for release metadata and user-facing install/version references. Use the existing root `Package.swift` distribution shape, update the documented tagged version from `0.1.0` to `0.2.0`, record the release in project memory, verify the Swift package still passes, then cut and push an annotated git tag from the current `main` commit.

**Tech Stack:** Swift Package Manager, git tags, repository docs/OpenAPI

---

### Task 1: Update release-facing version references

**Files:**
- Modify: `Cargo.toml`
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `sdks/ios/README.md`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Write the failing release reference check**

Run: `rg -n "0\\.1\\.0" Cargo.toml schemas/openapi/fantasma.yaml sdks/ios/README.md`
Expected: matches still point at `0.1.0`.

- [ ] **Step 2: Update the release references to `0.2.0`**

Change the workspace version, OpenAPI info version, and Swift package install example to `0.2.0`. Add a `docs/STATUS.md` entry recording that the D2 branch was published as the new Swift SDK release tag.

- [ ] **Step 3: Re-run the release reference check**

Run: `rg -n "0\\.1\\.0|0\\.2\\.0" Cargo.toml schemas/openapi/fantasma.yaml sdks/ios/README.md docs/STATUS.md`
Expected: only intentional historical references to `0.1.0` remain; live install/version references use `0.2.0`.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml schemas/openapi/fantasma.yaml sdks/ios/README.md docs/STATUS.md
git commit -m "Prepare Swift SDK v0.2.0 release"
```

### Task 2: Verify the Swift package and cut the tag

**Files:**
- No file changes required

- [ ] **Step 1: Run the Swift package tests**

Run: `swift test --filter FantasmaSDKTests`
Expected: PASS

- [ ] **Step 2: Confirm the release commit and current tags**

Run: `git log --oneline -n 3 && git tag --sort=-creatordate | head -n 5`
Expected: new release-prep commit on `main`; latest tag is still `v0.1.0`.

- [ ] **Step 3: Create the annotated release tag**

Run: `git tag -a v0.2.0 -m "FantasmaSDK v0.2.0"`
Expected: local annotated `v0.2.0` exists.

- [ ] **Step 4: Push branch and tag**

Run: `git push origin main && git push origin v0.2.0`
Expected: remote branch and tag updated successfully.

