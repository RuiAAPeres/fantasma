# Runtime Security Implementation Plan

**Goal:** Patch reachable TLS and trust-store advisories and make production Docker builds use reviewed dependencies.

**Architecture:** Keep the existing API, ingest, worker and database contracts. Update compatible Rust TLS packages and the supported compiler; retain the Debian bookworm runtime family.

- [x] Update Cargo.lock for rustls >=0.23.46, rustls-webpki >=0.103.16 and webpki-roots >=1.0.10.
- [x] Set infra/docker/Dockerfile.{api,ingest,worker,test} builders to Rust 1.99.1; copy Cargo.lock and use cargo build --locked in each production image.
- [x] Run cargo fmt --all --check, cargo clippy --workspace --all-targets -- -D warnings and the full scripts/docker-test.sh workspace suite; build all three production Docker images.
- [x] Document audit residuals, verification and --pull rebuild requirements in docs/deployment.md and docs/STATUS.md; coordinator owns independent review, commit, push and production rollout.

## Scope and verification updates

Cargo.lock was ignored rather than tracked, so remove its .gitignore entry and commit the reviewed lockfile. Include same-series anyhow 1.0.103, event-listener 5.4.2 and rand 0.8.6/0.9.3 soundness fixes. Native Rust 1.92 fmt/clippy, all 481 Docker workspace tests on Rust 1.99.1, all three release images and the CLI smoke passed. Additional Linux Rust 1.99 Clippy identified five existing http.rs lints; the coordinating agent approved two equivalent idiom edits and narrowly documented result_large_err allowances on three existing HTTP response boundaries, preserving their concrete Response error contract. Final verification passed on both toolchains; the final Docker suite passed all 481 tests and the CLI smoke passed. One further sort idiom was updated only inside a cfg(test) worker helper.
