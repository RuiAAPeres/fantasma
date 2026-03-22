# Verification Matrix

Fantasma's local verification must match the CI surface your change can break. Do not stop at targeted checks if CI runs a broader layer.

Use the checked-in helper:

```bash
./scripts/verify-changed-surface.sh list
./scripts/verify-changed-surface.sh print <profile>
./scripts/verify-changed-surface.sh run <profile>
```

## Profiles

### `api-contract`

Use for:
- public API changes
- shared response-shape changes
- OpenAPI/schema changes
- CLI request/response contract changes

Commands:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p fantasma-core --quiet`
- `cargo test -p fantasma-api --lib --quiet`
- `cargo test -p fantasma-cli --test http_flows --quiet`
- `FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet`
- `./scripts/cli-smoke.sh`

### `worker-contract`

Use for:
- worker logic changes
- metric/session aggregation changes
- response-contract changes that worker pipeline assertions cover

Commands:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p fantasma-worker --lib --quiet`
- `FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet`

### `cli-operator`

Use for:
- `fantasma-cli` operator workflow changes
- CLI smoke-path changes
- provisioning or operator scripts touched by the CLI smoke flow

Commands:
- `bash -n scripts/cli-smoke.sh scripts/provision-project.sh scripts/ci/should-run-cli-smoke.sh scripts/tests/cli-dogfood-gate.sh`
- `bash scripts/tests/cli-dogfood-gate.sh`
- `cargo test -p fantasma-cli --test http_flows --quiet`
- `./scripts/cli-smoke.sh`

### `rust-ci`

Use for:
- broad Rust workspace changes
- CI repair work
- any case where the safest answer is "match the Rust CI slice"

Commands:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `./scripts/docker-test.sh --quiet`

### `script-only`

Use for:
- new shell scripts
- shell-only maintenance work

Commands:
- `bash -n <changed scripts>`
- matching `scripts/tests/*` audit scripts for the touched helpers

## Red CI Rule

After a red CI run:

1. Pull the failed job logs.
2. Identify the exact failing test or step.
3. Reproduce that CI slice locally.
4. Fix the issue.
5. Rerun the full affected suite locally.
6. Only then push again.

Do not repush based on partial confidence.
