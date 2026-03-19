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
- production proxy override: `infra/docker/compose.prod.yaml`
- service images: `infra/docker/Dockerfile.ingest`, `infra/docker/Dockerfile.api`, `infra/docker/Dockerfile.worker`
- benchmark stack: [`infra/docker/compose.bench.yaml`](../infra/docker/compose.bench.yaml)
- operator CLI: `cargo run -p fantasma-cli -- ...`
- CLI smoke helper: [`scripts/cli-smoke.sh`](../scripts/cli-smoke.sh)
- automation helper: [`scripts/provision-project.sh`](../scripts/provision-project.sh)
- Docker reclaim helper: [`scripts/docker-reclaim.sh`](../scripts/docker-reclaim.sh)

Stable default Compose project names used in local workflows:

- default stack: `fantasma-local`
- benchmark stack: `fantasma-bench`
- Docker-backed workspace tests: `fantasma-tests`
- HTTP smoke helper: `fantasma-smoke`
- CLI smoke helper: `fantasma-cli-smoke`

## Configuration

Shared runtime variables:

- `FANTASMA_DATABASE_URL`: required by ingest, API, and worker
- `FANTASMA_LOG_LEVEL`: optional tracing filter

HTTP service variables:

- `FANTASMA_BIND_ADDRESS`: bind address for `fantasma-ingest` and `fantasma-api`
- `FANTASMA_ADMIN_TOKEN`: required operator bearer token consumed only by `fantasma-api`

Worker variables:

- `FANTASMA_WORKER_IDLE_POLL_INTERVAL_MS`: idle-only sleep interval in milliseconds; defaults to `250`
- `FANTASMA_WORKER_SESSION_BATCH_SIZE`: raw-event batch size for the `session_apply` lane; defaults to `2000`
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

When changing operator-facing behavior, use the CLI as the dogfooding path
during development. Keep direct HTTP checks for route-level correctness and use
the CLI smoke flow as the manual and CI proof that the operator workflow still
works end to end.

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

Those defaults can be overridden at Compose render time with:

- `FANTASMA_POSTGRES_PORTS`
- `FANTASMA_INGEST_PORTS`
- `FANTASMA_API_PORTS`

Each variable maps directly to one Compose `ports` entry such as
`127.0.0.1::8082` or `8082:8082`.

Stop the stack and remove volumes:

```bash
docker compose -f infra/docker/compose.yaml down --volumes --remove-orphans
```

`infra/docker/compose.yaml` declares `name: fantasma-local`, so the default
local stack now keeps one stable Compose project name unless you override it
explicitly with `docker compose -p ...`.

## Production Behind A Shared Proxy

When Fantasma runs on a shared host behind an existing reverse proxy such as
Caddy, keep the local default stack untouched and layer the production override
on top instead of editing `infra/docker/compose.yaml` directly.

`infra/docker/compose.prod.yaml` attaches `fantasma-api` to an external Docker
network so the reverse proxy can reach it by a stable alias, and it also
applies a write-oriented Postgres runtime profile for the single-host Docker
deployment:

- external network name defaults to `proxy`
- API alias defaults to `fantasma-api`
- both can be overridden with `FANTASMA_PROXY_NETWORK_NAME` and
  `FANTASMA_PROXY_NETWORK_ALIAS`
- Postgres tuning defaults now ship through the same override:
  `shared_buffers=1GB`, `effective_cache_size=5GB`,
  `maintenance_work_mem=256MB`, `wal_buffers=16MB`,
  `max_wal_size=4GB`, `min_wal_size=1GB`,
  `checkpoint_timeout=10min`, `checkpoint_completion_target=0.95`,
  `commit_delay=100`, `commit_siblings=8`

Example production bring-up:

```bash
docker compose \
  --env-file .env.production \
  -f infra/docker/compose.yaml \
  -f infra/docker/compose.prod.yaml \
  -p fantasma-prod \
  up -d --build
```

Recommended host-port bindings for single-host production keep Postgres and the
two HTTP services on loopback only, for example:

- `FANTASMA_POSTGRES_PORTS=127.0.0.1:15432:5432`
- `FANTASMA_INGEST_PORTS=127.0.0.1:18081:8081`
- `FANTASMA_API_PORTS=127.0.0.1:18082:8082`

If the production host needs different Postgres runtime values, override the
production-only defaults with:

- `FANTASMA_POSTGRES_SHARED_BUFFERS`
- `FANTASMA_POSTGRES_EFFECTIVE_CACHE_SIZE`
- `FANTASMA_POSTGRES_MAINTENANCE_WORK_MEM`
- `FANTASMA_POSTGRES_WAL_BUFFERS`
- `FANTASMA_POSTGRES_MAX_WAL_SIZE`
- `FANTASMA_POSTGRES_MIN_WAL_SIZE`
- `FANTASMA_POSTGRES_CHECKPOINT_TIMEOUT`
- `FANTASMA_POSTGRES_CHECKPOINT_COMPLETION_TARGET`
- `FANTASMA_POSTGRES_COMMIT_DELAY_US`
- `FANTASMA_POSTGRES_COMMIT_SIBLINGS`

These remain intentionally conservative on durability: the production override
does not disable `fsync`, `full_page_writes`, or `synchronous_commit`.

With the override attached, the shared reverse proxy can route
`api.usefantasma.com` to `fantasma-api:8082` over the shared `proxy` network
without a manual `docker network connect`.

If you have older Fantasma-owned Docker images, containers, or volumes left
behind from earlier local runs, reclaim them explicitly with:

```bash
./scripts/docker-reclaim.sh
```

## Git-Backed Host Deploys

For the live single-host deployment, keep the host checkout as a real git clone
and deploy pinned commits, not copied files and not a mutable branch tip.

Recommended rule:

- bootstrap the host once into a git-backed checkout
- keep `.env.production` and `.env.demo` on the host as untracked local files
- deploy a specific commit SHA in detached-HEAD mode
- run the prod and demo Compose updates from that pinned revision

First-time host bootstrap from a local checkout:

```bash
./scripts/bootstrap-host-checkout.sh --host rui@your-host
```

The bootstrap helper defaults to:

- target dir `/home/rui/fantasma`
- repo URL `https://github.com/RuiAAPeres/fantasma.git`
- ref `HEAD` from the local checkout that launched the script

If the target directory already exists but is not a git repo, the helper moves
it aside to a timestamped backup, clones a fresh checkout, restores host-local
`.env*` files, and checks out the pinned commit.

Normal deploy from a local checkout:

```bash
./scripts/deploy-host.sh --host rui@your-host
```

By default the deploy helper resolves the local checkout `HEAD` commit SHA and
deploys exactly that revision on the host. Override it explicitly when needed:

```bash
./scripts/deploy-host.sh --host rui@your-host --ref <commit-sha>
```

Under the hood the deploy helper:

- runs `git fetch --prune origin`
- runs `git checkout --detach <commit-sha>`
- rebuilds and recreates `fantasma-prod` by default
- rebuilds and recreates `fantasma-demo` only when explicitly requested

That keeps early-stage iteration safe even with frequent changes: the deployed
host always points at one concrete revision, and rollback is just another
deploy with an older commit SHA.

Demo deploy control:

```bash
./scripts/deploy-host.sh --host rui@your-host --with-demo
./scripts/deploy-host.sh --host rui@your-host --demo-only
```

Use the default prod-only path unless the change actually affects the demo
stack.

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
`/v1/metrics/*`, including `GET /v1/metrics/events`,
`GET /v1/metrics/events/catalog`, and `GET /v1/metrics/events/top`. Keep the
script for automation; prefer the CLI for manual operator work. `POST /v1/events`
accepts at most 200 events per request and still enforces the ingest payload
size limit separately.

## Smoke Verification

Run the preflighted smoke script:

```bash
./scripts/compose-smoke.sh
```

The smoke path:

- checks free disk and Docker availability
- starts from a clean local Docker state for the stable `fantasma-smoke`
  Compose project
- waits for `http://localhost:8081/health` and `http://localhost:8082/health`
- provisions a project plus scoped ingest/read keys on the blank database
- ingests sample events
- polls worker-derived metrics until the expected data appears
- tears the stack down with `--volumes --remove-orphans --rmi local` on exit

`./scripts/compose-smoke.sh` is disk-first by default. It clears any prior
`fantasma-smoke` stack before bringing the services up, and it removes the
stack's local images and volumes again during cleanup so repeated local runs do
not keep accumulating Fantasma-owned Docker artifacts.

Set `FANTASMA_SMOKE_PROJECT_NAME` if you need a stable Compose project name for
debugging. Set `FANTASMA_SMOKE_KEEP_STACK=1` if you need the HTTP smoke stack
left running after the script exits.

Worker-built metrics are asynchronous. If you verify manually, expect a short
delay between ingesting events and seeing derived metric reads update.

Run the CLI-driven smoke path in its own disposable stack:

```bash
./scripts/cli-smoke.sh
```

That helper uses the stable `fantasma-cli-smoke` Compose project name, starts
from a fresh `XDG_CONFIG_HOME`, provisions an instance/profile through the CLI,
creates a project plus a local `read` key, ingests one sample event with the
printed ingest key, polls one CLI metrics query until the derived session count
appears, and then removes its local images and volumes with
`--volumes --remove-orphans --rmi local`.

Unlike the main local stack, the CLI smoke helper overrides the Compose port
bindings to ephemeral loopback host ports and discovers the published ingest
and API URLs with `docker compose port`. That keeps the disposable smoke stack
from failing just because local `5432`, `8081`, or `8082` are already in use.

Set `FANTASMA_CLI_SMOKE_PROJECT_NAME` if you need a different stable Compose
project name for debugging. Set `FANTASMA_CLI_SMOKE_KEEP_STACK=1` if you need
the CLI smoke stack left running after the script exits. If you need fixed or
custom smoke bindings, override `FANTASMA_CLI_SMOKE_POSTGRES_PORTS`,
`FANTASMA_CLI_SMOKE_INGEST_PORTS`, or `FANTASMA_CLI_SMOKE_API_PORTS`.

## Docker-Backed Tests

Run DB-backed Rust tests through the Docker helper:

```bash
./scripts/docker-test.sh
```

`./scripts/docker-test.sh` uses the stable `fantasma-tests` Compose project
name and is now disk-first by default. Successful and failed runs both clean up
the test stack with `--volumes --remove-orphans --rmi local`, which drops the
workspace test containers, local images, and the Cargo cache volumes created by
`infra/docker/compose.test.yaml`.

If you need to keep those cache volumes between runs, set
`FANTASMA_DOCKER_TEST_KEEP_CACHE=1`. If you need the containers left behind for
deeper debugging, set `FANTASMA_DOCKER_TEST_KEEP_CONTAINERS=1`.

For operator-facing changes, treat [`scripts/cli-smoke.sh`](../scripts/cli-smoke.sh)
as the required dogfooding check. Keep [`scripts/compose-smoke.sh`](../scripts/compose-smoke.sh)
as an optional lower-level manual smoke when you need direct HTTP visibility.

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

```bash
cargo run -p fantasma-cli -- metrics events-top \
  --start 2026-01-01 \
  --end 2026-01-02 \
  --limit 10
```

```bash
cargo run -p fantasma-cli -- metrics events-catalog \
  --start 2026-01-01 \
  --end 2026-01-02
```

```bash
cargo run -p fantasma-cli -- metrics live-installs
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

`active_installs` stays on `GET /v1/metrics/sessions`, but it uses exact inclusive UTC `start` / `end` plus optional `interval=day|week|month|year` instead of `granularity`. `week|month|year` windows are calendar-shaped and clipped to the request edges.

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-01-02" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-01-02&plan=pro&group_by=provider" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-01-31&interval=week" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions?metric=active_installs&start=2026-01-10&end=2026-03-20&interval=month&plan=pro" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/live_installs" \
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
