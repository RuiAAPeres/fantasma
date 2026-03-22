# Flutter SDK Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a first-party Flutter SDK with durable local queueing, explicit install-scoped behavior, and contract parity with the existing mobile SDKs.

**Architecture:** Build a Flutter package with a pure Dart core, SQLite-backed storage, typed config/errors, and small environment adapters for platform metadata and lifecycle flush scheduling. Keep each client isolated through an explicit `storageNamespace`, snapshot locale at enqueue time, and persist blocked-destination state in client-local storage.

**Tech Stack:** Flutter, Dart, sqflite, package_info_plus, device_info_plus, http, flutter_test

---

## Summary

- Add `sdks/flutter/fantasma_flutter` as the first-party Flutter package.
- Expose `FantasmaConfig`, `FantasmaClient`, typed exceptions, and `LocaleProvider`.
- Persist queue, install identity, and blocked-destination state in SQLite before upload.
- Auto-populate `platform`, `app_version`, `os_version`, and `locale`, with locale snapshotting at `track()` time.
- Document the SDK in the repo docs and add tests for queue behavior, classification, isolation, and lifecycle-triggered flush scheduling.
