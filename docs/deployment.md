# Deployment

Fantasma is documented for a single-host Docker Compose deployment. The
repository's primary deployment definition lives in
`infra/docker/compose.yaml`.

## Supported Topology

The default stack includes:

- Postgres
- `fantasma-ingest`
- `fantasma-api`
- `fantasma-worker`

Repository files:

- Compose stack: `infra/docker/compose.yaml`
- service images: `infra/docker/Dockerfile.ingest`, `infra/docker/Dockerfile.api`, `infra/docker/Dockerfile.worker`
- benchmark stack: [`infra/docker/compose.bench.yaml`](../infra/docker/compose.bench.yaml)
- operator CLI: `cargo run -p fantasma-cli -- ...`
- CLI smoke helper: [`scripts/cli-smoke.sh`](../scripts/cli-smoke.sh)
- automation helper: [`scripts/provision-project.sh`](../scripts/provision-project.sh)

## Configuration

Shared runtime variables:

- `FANTASMA_DATABASE_URL`: required by ingest, API, and worker
- `FANTASMA_LOG_LEVEL`: optional tracing filter

HTTP service variables:

- `FANTASMA_BIND_ADDRESS`: bind address for `fantasma-ingest` and `fantasma-api`
- `FANTASMA_ADMIN_TOKEN`: required operator bearer token consumed only by `fantasma-api`

Worker variables:

- `FANTASMA_WORKER_IDLE_POLL_INTERVAL_MS`: idle-only sleep interval in milliseconds; defaults to `250`
- `FANTASMA_WORKER_SESSION_BATCH_SIZE`: raw-event batch size for the `session_apply` lane; defaults to `1000`
- `FANTASMA_WORKER_EVENT_BATCH_SIZE`: raw-event batch size for the event-metrics lane; defaults to `5000`
- `FANTASMA_WORKER_SESSION_INCREMENTAL_CONCURRENCY`: bounded parallelism for append-like per-install `session_apply` work; defaults to `8`
- `FANTASMA_WORKER_SESSION_REPAIR_CONCURRENCY`: bounded parallelism for queue-driven `session_repair` work; defaults to `2`

Current Compose defaults:

- all three services get `postgres://fantasma:fantasma@postgres:5432/fantasma`
- `fantasma-ingest` gets `0.0.0.0:8081`
- `fantasma-api` gets `0.0.0.0:8082` and requires `FANTASMA_ADMIN_TOKEN`
- `fantasma-worker` gets the lane-based defaults above

## Data Protection

Fantasma does not currently implement application-level encryption at rest for
Postgres data or the iOS SDK queue.

Current protections and operator expectations:

- project API keys are not stored in plaintext; the backend stores only a hash
  plus redacted prefix
- operators should run Fantasma on encrypted disks or encrypted cloud volumes
- Postgres backups and snapshots should also be encrypted
- non-local deployments should be published behind TLS
- Fantasma's privacy posture is primarily product-shape based: install-scoped
  analytics, no person-level identity, no hidden stitching, and intentionally
  narrow event context

Fantasma should not claim protection against a full server compromise through
at-rest encryption alone. If the repository later adds application-level
encryption, the docs should state exactly which data is encrypted, where keys
live, and what operational tradeoffs apply.

## Startup Behavior

On startup, `fantasma-ingest`, `fantasma-api`, and `fantasma-worker` all:

- connect to Postgres using a pooled connection
- run SQL migrations from `crates/fantasma-store/migrations/`

They do not seed a project or create API keys at startup. Provisioning happens
through the operator management API after the stack is healthy.

There is no separate migration job in the default stack. The worker keeps one
process but runs independent `session_apply`,
`session_repair`, and `event_metrics` lanes internally. `session_apply` is the
only owner of the raw `"sessions"` offset, while `session_repair` drains the
durable install-scoped repair frontier independently of that raw offset. Each
lane repolls immediately after useful work and only sleeps after an idle tick.

## Operator Workflow

The primary manual operator workflow is the Fantasma CLI against the API
service URL. The CLI manages remote instance profiles, validates the operator
token, provisions projects and keys, and runs metrics queries without dropping
operators into raw curl by default.

The first operator bearer token still comes from deployment configuration such
as `FANTASMA_ADMIN_TOKEN`. Fantasma does not expose a separate bootstrap or
account-creation flow in this slice.

## Preconditions

Before bringing the stack up, confirm:

- enough free disk for Docker layers, Cargo build output, and Postgres data
- a healthy Docker daemon
- `docker`, `curl`, and `python3` are installed for the smoke/provisioning path

## Bring Up

Start the default stack:

```bash
export FANTASMA_ADMIN_TOKEN="$(python3 - <<'PY'
import secrets
print(f"fg_pat_{secrets.token_urlsafe(24)}")
PY
)"
docker compose -f infra/docker/compose.yaml up --build
```

Default host ports:

- `5432`: Postgres
- `8081`: ingest
- `8082`: API

Stop the stack and remove volumes:

```bash
docker compose -f infra/docker/compose.yaml down --volumes --remove-orphans
```

## CLI Access

Run the CLI directly from the workspace:

```bash
cargo run -p fantasma-cli -- instances add local --url http://localhost:8082
```

The saved API base URL may include a path prefix when Fantasma is published
behind a reverse proxy, for example `https://ops.example.com/fantasma/`.

Save the install-time operator token for that instance:

```bash
cargo run -p fantasma-cli -- auth login --instance local --token "${FANTASMA_ADMIN_TOKEN}"
```

The CLI stores local state under an XDG-style config path:

- `${XDG_CONFIG_HOME}/fantasma/config.toml` when `XDG_CONFIG_HOME` is set
- `~/.config/fantasma/config.toml` otherwise

Each instance profile stores the API base URL, operator token, active project,
and at most one locally saved `read` key per project. `read` keys are local
operator conveniences created through the CLI; on a new machine, create a new
project `read` key instead of importing an old one.

The current persisted-config path and permission model targets Unix-like
operator machines. Config writes fail closed on non-Unix platforms until the
CLI grows an equally strict credential-storage path there.

## Provision a Project

The CLI is the primary manual provisioning path once the stack is healthy:

```bash
cargo run -p fantasma-cli -- projects create \
  --name "Local Development" \
  --ingest-key-name "ios-sdk"
```

The command prints the project id and the initial ingest key secret once. That
ingest key is not persisted locally by the CLI.

Select the project you want to operate on:

```bash
cargo run -p fantasma-cli -- projects use <project-id>
```

Create and locally save a dedicated `read` key for CLI metrics access:

```bash
cargo run -p fantasma-cli -- keys create --kind read --name "local-cli"
```

The CLI stores at most one local `read` key per project. Creating another one
replaces the previous local entry for that project on that machine.

## Automation Helper

`scripts/provision-project.sh` remains useful for smoke checks and automation
when you want one shell command that creates a project plus an optional `read`
key:

```bash
PROVISIONED="$(./scripts/provision-project.sh \
  --project-name "Local Development" \
  --ingest-key-name "ios-sdk" \
  --read-key-name "local-read")"
printf '%s\n' "$PROVISIONED"
```

Extract the returned secrets if you want shell variables for later commands:

```bash
INGEST_KEY="$(printf '%s' "$PROVISIONED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["ingest_key"]["secret"])')"
READ_KEY="$(printf '%s' "$PROVISIONED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["read_key"]["secret"])')"
```

Use the ingest key only with `POST /v1/events`. Use the read key only with
`/v1/metrics/*`. Keep the script for automation; prefer the CLI for manual
operator work.

## Smoke Verification

Run the preflighted smoke script:

```bash
./scripts/compose-smoke.sh
```

The smoke path:

- checks free disk and Docker availability
- runs the stack in a disposable Compose project
- waits for `http://localhost:8081/health` and `http://localhost:8082/health`
- provisions a project plus scoped ingest/read keys on the blank database
- ingests sample events
- polls worker-derived metrics until the expected data appears
- tears the stack down with volumes on exit

Set `FANTASMA_SMOKE_PROJECT_NAME` if you need a stable Compose project name for
debugging.

Worker-built metrics are asynchronous. If you verify manually, expect a short
delay between ingesting events and seeing derived metric reads update.

Run the CLI-driven smoke path against the same local stack:

```bash
./scripts/cli-smoke.sh
```

That helper uses a fresh `XDG_CONFIG_HOME`, provisions an instance/profile
through the CLI, creates a project plus a local `read` key, ingests one sample
event with the printed ingest key, and polls one CLI metrics query until the
derived session count appears.

## Manual Checks

Use these commands when you need to inspect the running stack manually. Prefer
the CLI for management and metrics, and use raw curl when you specifically need
to inspect the HTTP contract.

Health endpoints:

```bash
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8082/health
```

Show the current CLI/operator status:

```bash
cargo run -p fantasma-cli -- status
```

List projects:

```bash
cargo run -p fantasma-cli -- projects list
```

Create a new `read` key for the active project if this machine does not have
one yet:

```bash
cargo run -p fantasma-cli -- keys create --kind read --name "manual-read"
```

Query derived metrics through the CLI:

```bash
cargo run -p fantasma-cli -- metrics sessions \
  --metric count \
  --granularity day \
  --start 2026-01-01 \
  --end 2026-01-02
```

```bash
cargo run -p fantasma-cli -- metrics events \
  --event app_open \
  --metric count \
  --granularity day \
  --start 2026-01-01 \
  --end 2026-01-02 \
  --filter platform=ios \
  --group-by provider
```

If you need raw shell variables for curl-based checks, use the automation
helper:

```bash
PROVISIONED="$(./scripts/provision-project.sh \
  --project-name "Manual Checks" \
  --ingest-key-name "manual-ingest" \
  --read-key-name "manual-read")"
```

```bash
INGEST_KEY="$(printf '%s' "$PROVISIONED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["ingest_key"]["secret"])')"
READ_KEY="$(printf '%s' "$PROVISIONED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["read_key"]["secret"])')"
```

Ingest sample events:

```bash
curl -fsS -X POST http://localhost:8081/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Fantasma-Key: ${INGEST_KEY}" \
  -d '{
    "events": [
      {
        "event": "app_open",
        "timestamp": "2026-01-01T00:00:00Z",
        "install_id": "compose-install-1",
        "platform": "ios",
        "app_version": "1.0.0",
        "os_version": "18.3",
        "properties": {
          "provider": "strava"
        }
      },
      {
        "event": "app_open",
        "timestamp": "2026-01-01T00:10:00Z",
        "install_id": "compose-install-1",
        "platform": "ios",
        "app_version": "1.0.0",
        "os_version": "18.3",
        "properties": {
          "provider": "strava"
        }
      }
    ]
  }'
```

Query derived metrics:

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-02" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions?metric=duration_total&granularity=day&start=2026-01-01&end=2026-01-02" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions?metric=new_installs&granularity=day&start=2026-01-01&end=2026-01-02" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-02&platform=ios&group_by=provider" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/events?event=app_open&metric=count&granularity=hour&start=2026-01-01T00:00:00Z&end=2026-01-01T01:00:00Z&group_by=provider" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

## Related Docs

- architecture and data flow: [`docs/architecture.md`](architecture.md)
- benchmark-specific commands: [`docs/performance.md`](performance.md)
- iOS demo walkthrough: [`apps/demo-ios/README.md`](../apps/demo-ios/README.md)
