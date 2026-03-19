# Realistic burst benchmark codification

## Goal
Add first-class `fantasma-bench` scenarios for small, realistic session-readiness bursts while keeping the existing stress scenarios unchanged.

## Scope
- add a dedicated SLO suite for realistic burst readiness
- codify the measured 300-event burst shapes:
  - 300 installs x 1 event
  - 150 installs x 2 events
  - 100 installs x 3 events
- keep the current representative, mixed-repair, stress, and reads-visibility suites intact
- update docs so operators know when to run burst readiness versus stress

## Design
1. Extend the SLO taxonomy with a new `burst-readiness` suite and three scenario kinds.
2. Reuse the existing live-scenario execution path by generating append-only upload blobs for each burst shape.
3. Make SLO expected totals data-driven for the existing `plan=pro` grouped hard gates so small burst scenarios remain exact.
4. Add tests that lock:
   - suite taxonomy and default selection
   - burst schedule shapes and event counts
   - filtered hard-gate expected totals for a burst scenario
5. Update `docs/performance.md` and `docs/STATUS.md` to explain that burst readiness is the realistic customer-freshness benchmark, while stress remains the ceiling/regression benchmark.

## Verification
- targeted `fantasma-bench` unit tests for taxonomy, schedule shape, and filtered expectations
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
