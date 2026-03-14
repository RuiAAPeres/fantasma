# Fantasma Derived Metrics SLO Suite

- Host: Apple M3 Pro / 36 GiB / Darwin 25.1.0 / aarch64
- Benchmarked at: 2026-03-14T17:22:06.680670+00:00

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

- ingest: 900000 events in 31374ms (28685.74 events/s)
- event_metrics_ready_ms: 242ms
- session_metrics_ready_ms: 9150ms
- derived_metrics_ready_ms: 9150ms
- Budget: PASS

## backfill-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 32046ms (28084.25 events/s)
- event_metrics_ready_ms: 234ms
- session_metrics_ready_ms: 113217ms
- derived_metrics_ready_ms: 113217ms
- Budget: FAIL
  - session_metrics_ready_ms readiness 113217ms exceeded budget 60000ms
  - derived_metrics_ready_ms readiness 113217ms exceeded budget 60000ms

## repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 30637ms (29375.92 events/s)
- repair_ingest: 3000 events in 122ms (24500.00 events/s)
- event_metrics_ready_ms: 841ms
- session_metrics_ready_ms: 101151ms
- derived_metrics_ready_ms: 101151ms
- Budget: FAIL
  - session_metrics_ready_ms readiness 101151ms exceeded budget 60000ms
  - derived_metrics_ready_ms readiness 101151ms exceeded budget 60000ms

## reads-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 32101ms (28036.33 events/s)
- event_metrics_ready_ms: 240ms
- session_metrics_ready_ms: 8756ms
- derived_metrics_ready_ms: 8756ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 5 | 3 | 5 |
| events_count_hour_grouped | 30 | 31 | 29 | 36 |
| events_count_day_dim4_grouped | 9 | 10 | 8 | 14 |
| events_count_hour_dim4_grouped | 33 | 34 | 32 | 35 |
| sessions_count_day_grouped | 2 | 2 | 2 | 3 |
| sessions_count_hour_grouped | 13 | 14 | 13 | 15 |
| sessions_duration_total_day_grouped | 2 | 2 | 2 | 3 |
| sessions_duration_total_hour_grouped | 13 | 14 | 13 | 16 |
| sessions_new_installs_day_grouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_hour_grouped | 14 | 15 | 13 | 17 |
| events_count_day_ungrouped | 0 | 0 | 0 | 1 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 3 |
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

- ingest: 2700000 events in 98147ms (27509.58 events/s)
- event_metrics_ready_ms: 472ms
- session_metrics_ready_ms: 23716ms
- derived_metrics_ready_ms: 23716ms
- Budget: PASS

## backfill-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 109849ms (24578.97 events/s)
- event_metrics_ready_ms: 377ms
- session_metrics_ready_ms: 327066ms
- derived_metrics_ready_ms: 327066ms
- Budget: PASS

## repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 2700000 events in 110800ms (24368.18 events/s)
- repair_ingest: 9000 events in 380ms (23677.17 events/s)
- event_metrics_ready_ms: 2343ms
- session_metrics_ready_ms: 568298ms
- derived_metrics_ready_ms: 568298ms
- Budget: PASS

## reads-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 2700000 events in 92015ms (29343.04 events/s)
- event_metrics_ready_ms: 372ms
- session_metrics_ready_ms: 31471ms
- derived_metrics_ready_ms: 31471ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 8 | 9 | 8 | 9 |
| events_count_hour_grouped | 87 | 88 | 85 | 90 |
| events_count_day_dim4_grouped | 7 | 8 | 6 | 10 |
| events_count_hour_dim4_grouped | 74 | 81 | 71 | 89 |
| sessions_count_day_grouped | 4 | 6 | 4 | 8 |
| sessions_count_hour_grouped | 42 | 50 | 38 | 54 |
| sessions_duration_total_day_grouped | 4 | 6 | 4 | 7 |
| sessions_duration_total_hour_grouped | 41 | 46 | 36 | 49 |
| sessions_new_installs_day_grouped | 4 | 6 | 4 | 8 |
| sessions_new_installs_hour_grouped | 40 | 47 | 36 | 51 |
| events_count_day_ungrouped | 1 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 6 | 7 | 5 | 8 |
| sessions_count_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_count_hour_ungrouped | 6 | 7 | 5 | 10 |
| sessions_duration_total_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 6 | 7 | 5 | 8 |
| sessions_new_installs_day_ungrouped | 0 | 1 | 0 | 1 |
| sessions_new_installs_hour_ungrouped | 6 | 7 | 5 | 9 |

## append-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 5400000 events in 190528ms (28342.16 events/s)
- event_metrics_ready_ms: 734ms
- session_metrics_ready_ms: 51136ms
- derived_metrics_ready_ms: 51136ms
- Budget: PASS

## backfill-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 5400000 events in 227259ms (23761.43 events/s)
- event_metrics_ready_ms: 450ms
- session_metrics_ready_ms: 657273ms
- derived_metrics_ready_ms: 657273ms
- Budget: PASS

## repair-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 5400000 events in 287250ms (18798.94 events/s)
- repair_ingest: 18000 events in 859ms (20936.57 events/s)
- event_metrics_ready_ms: 6384ms
- session_metrics_ready_ms: 1331810ms
- derived_metrics_ready_ms: 1331810ms
- Budget: PASS

## reads-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 5400000 events in 245157ms (22026.64 events/s)
- event_metrics_ready_ms: 730ms
- session_metrics_ready_ms: 45562ms
- derived_metrics_ready_ms: 45562ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 16 | 17 | 15 | 19 |
| events_count_hour_grouped | 173 | 183 | 169 | 196 |
| events_count_day_dim4_grouped | 14 | 18 | 12 | 21 |
| events_count_hour_dim4_grouped | 142 | 149 | 138 | 155 |
| sessions_count_day_grouped | 7 | 8 | 6 | 9 |
| sessions_count_hour_grouped | 74 | 83 | 73 | 84 |
| sessions_duration_total_day_grouped | 7 | 7 | 6 | 7 |
| sessions_duration_total_hour_grouped | 78 | 90 | 73 | 112 |
| sessions_new_installs_day_grouped | 8 | 14 | 6 | 17 |
| sessions_new_installs_hour_grouped | 79 | 104 | 74 | 129 |
| events_count_day_ungrouped | 1 | 1 | 1 | 3 |
| events_count_hour_ungrouped | 23 | 32 | 13 | 97 |
| sessions_count_day_ungrouped | 3 | 6 | 1 | 9 |
| sessions_count_hour_ungrouped | 23 | 32 | 14 | 37 |
| sessions_duration_total_day_ungrouped | 3 | 4 | 1 | 5 |
| sessions_duration_total_hour_ungrouped | 21 | 28 | 13 | 31 |
| sessions_new_installs_day_ungrouped | 2 | 3 | 1 | 4 |
| sessions_new_installs_hour_ungrouped | 17 | 29 | 12 | 41 |