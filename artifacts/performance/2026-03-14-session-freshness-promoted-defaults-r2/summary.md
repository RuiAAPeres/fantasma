# Fantasma Derived Metrics SLO Suite

- Environment: local

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

## append-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 31958ms (28161.71 events/s)
- event_metrics_ready_ms: 245ms
- session_metrics_ready_ms: 11385ms
- derived_metrics_ready_ms: 11385ms
- Budget: PASS

## backfill-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 28759ms (31294.34 events/s)
- event_metrics_ready_ms: 234ms
- session_metrics_ready_ms: 16280ms
- derived_metrics_ready_ms: 16280ms
- Budget: PASS

## repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 28249ms (31859.13 events/s)
- repair_ingest: 3000 events in 110ms (27229.81 events/s)
- event_metrics_ready_ms: 863ms
- session_metrics_ready_ms: 2344ms
- derived_metrics_ready_ms: 2344ms
- Budget: PASS

## reads-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 28443ms (31641.43 events/s)
- event_metrics_ready_ms: 238ms
- session_metrics_ready_ms: 11279ms
- derived_metrics_ready_ms: 11279ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 4 | 3 | 5 |
| events_count_hour_grouped | 30 | 31 | 29 | 32 |
| events_count_day_dim4_grouped | 8 | 10 | 8 | 11 |
| events_count_hour_dim4_grouped | 34 | 35 | 33 | 41 |
| sessions_count_day_grouped | 2 | 2 | 2 | 3 |
| sessions_count_hour_grouped | 14 | 15 | 13 | 15 |
| sessions_duration_total_day_grouped | 2 | 2 | 2 | 2 |
| sessions_duration_total_hour_grouped | 14 | 15 | 13 | 15 |
| sessions_new_installs_day_grouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_hour_grouped | 14 | 15 | 13 | 15 |
| events_count_day_ungrouped | 0 | 0 | 0 | 0 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_new_installs_hour_ungrouped | 2 | 3 | 2 | 3 |

## append-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 85971ms (31405.60 events/s)
- event_metrics_ready_ms: 385ms
- session_metrics_ready_ms: 31957ms
- derived_metrics_ready_ms: 31957ms
- Budget: PASS

## backfill-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 100465ms (26874.84 events/s)
- event_metrics_ready_ms: 256ms
- session_metrics_ready_ms: 67788ms
- derived_metrics_ready_ms: 67788ms
- Budget: PASS

## repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 2700000 events in 131247ms (20571.89 events/s)
- repair_ingest: 9000 events in 444ms (20265.40 events/s)
- event_metrics_ready_ms: 1988ms
- session_metrics_ready_ms: 9678ms
- derived_metrics_ready_ms: 9678ms
- Budget: PASS

## reads-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- Failure: scenario execution failed: command failed with status Some(1)
stdout:
#1 [internal] load local bake definitions
#1 reading from stdin 1.55kB done
#1 DONE 0.0s

#2 [fantasma-worker internal] load build definition from Dockerfile.worker
#2 transferring dockerfile: 454B done
#2 DONE 0.0s

#3 [fantasma-api internal] load build definition from Dockerfile.api
#3 transferring dockerfile: 451B done
#3 DONE 0.0s

#4 [fantasma-ingest internal] load build definition from Dockerfile.ingest
#4 transferring dockerfile: 466B done
#4 DONE 0.0s

#5 [fantasma-api internal] load metadata for docker.io/library/rust:1.92-bookworm
#5 ...

#6 [fantasma-api internal] load metadata for docker.io/library/debian:bookworm-slim
#6 ...

#5 [fantasma-api internal] load metadata for docker.io/library/rust:1.92-bookworm
#5 ERROR: failed to do request: Head "https://registry-1.docker.io/v2/library/rust/manifests/1.92-bookworm": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host

#6 [fantasma-ingest internal] load metadata for docker.io/library/debian:bookworm-slim
#6 ERROR: failed to do request: Head "https://registry-1.docker.io/v2/library/debian/manifests/bookworm-slim": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host
------
 > [fantasma-ingest internal] load metadata for docker.io/library/debian:bookworm-slim:
------
------
 > [fantasma-ingest internal] load metadata for docker.io/library/rust:1.92-bookworm:
------

stderr:
 Image fantasma-bench-fantasma-ingest Building 
 Image fantasma-bench-fantasma-api Building 
 Image fantasma-bench-fantasma-worker Building 
Dockerfile.ingest:1

--------------------

   1 | >>> FROM rust:1.92-bookworm AS builder

   2 |     WORKDIR /app

   3 |     

--------------------

target fantasma-ingest: failed to solve: rust:1.92-bookworm: failed to resolve source metadata for docker.io/library/rust:1.92-bookworm: failed to do request: Head "https://registry-1.docker.io/v2/library/rust/manifests/1.92-bookworm": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host


- Budget: FAIL
  - scenario execution failed: command failed with status Some(1)
stdout:
#1 [internal] load local bake definitions
#1 reading from stdin 1.55kB done
#1 DONE 0.0s

#2 [fantasma-worker internal] load build definition from Dockerfile.worker
#2 transferring dockerfile: 454B done
#2 DONE 0.0s

#3 [fantasma-api internal] load build definition from Dockerfile.api
#3 transferring dockerfile: 451B done
#3 DONE 0.0s

#4 [fantasma-ingest internal] load build definition from Dockerfile.ingest
#4 transferring dockerfile: 466B done
#4 DONE 0.0s

#5 [fantasma-api internal] load metadata for docker.io/library/rust:1.92-bookworm
#5 ...

#6 [fantasma-api internal] load metadata for docker.io/library/debian:bookworm-slim
#6 ...

#5 [fantasma-api internal] load metadata for docker.io/library/rust:1.92-bookworm
#5 ERROR: failed to do request: Head "https://registry-1.docker.io/v2/library/rust/manifests/1.92-bookworm": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host

#6 [fantasma-ingest internal] load metadata for docker.io/library/debian:bookworm-slim
#6 ERROR: failed to do request: Head "https://registry-1.docker.io/v2/library/debian/manifests/bookworm-slim": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host
------
 > [fantasma-ingest internal] load metadata for docker.io/library/debian:bookworm-slim:
------
------
 > [fantasma-ingest internal] load metadata for docker.io/library/rust:1.92-bookworm:
------

stderr:
 Image fantasma-bench-fantasma-ingest Building 
 Image fantasma-bench-fantasma-api Building 
 Image fantasma-bench-fantasma-worker Building 
Dockerfile.ingest:1

--------------------

   1 | >>> FROM rust:1.92-bookworm AS builder

   2 |     WORKDIR /app

   3 |     

--------------------

target fantasma-ingest: failed to solve: rust:1.92-bookworm: failed to resolve source metadata for docker.io/library/rust:1.92-bookworm: failed to do request: Head "https://registry-1.docker.io/v2/library/rust/manifests/1.92-bookworm": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host