# Demo stack benchmark workflow

## Goal
Replace the ad hoc SSH Python benchmark against a remote demo stack with a checked-in operator workflow.

## Scope
- add a checked-in Python helper that measures the three realistic `300`-event burst scenarios against an existing ingest/API stack
- add a checked-in shell wrapper that runs the helper on a remote host against the live demo stack using `.env.demo`
- document the workflow in deployment/performance docs
- verify by running the wrapper once against the current remote demo stack

## Design
1. Keep the benchmark direct against the existing demo stack instead of trying to reuse the isolated `fantasma-bench` Compose path, because the host already has live services bound on the benchmark ports.
2. Put the benchmark logic in a standalone Python script with explicit args for `api_base_url`, `ingest_base_url`, and `admin_token`.
3. Add a shell wrapper that:
   - copies the helper to the remote host
   - sources `.env.demo` on the remote host
   - derives the loopback demo ports from `FANTASMA_API_PORTS` and `FANTASMA_INGEST_PORTS`
   - runs the helper against the demo stack
   - removes the remote temp helper afterward
4. Keep the workflow boring and host-local: no dashboard changes, no extra infra.

## Verification
- `python3 -m py_compile scripts/demo-stack-burst-bench.py`
- `bash -n scripts/run-demo-stack-burst-bench.sh`
- run the wrapper once against the remote demo stack and capture the JSON result
