# Fantasma Derived Metrics SLO Suite

- Environment: local

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

## append-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 900000 events in 30234ms (29767.63 events/s)
- event_metrics_ready_ms: 349ms
- session_metrics_ready_ms: 10646ms
- derived_metrics_ready_ms: 10646ms
- Budget: PASS

## backfill-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 900000 events in 31894ms (28218.08 events/s)
- event_metrics_ready_ms: 261ms
- session_metrics_ready_ms: 10138ms
- derived_metrics_ready_ms: 10138ms
- Budget: PASS

## repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 33687ms (26716.51 events/s)
- repair_ingest: 3000 events in 112ms (26599.65 events/s)
- event_metrics_ready_ms: 1027ms
- session_metrics_ready_ms: 1926ms
- derived_metrics_ready_ms: 1926ms
- Budget: PASS

## reads-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 38732ms (23236.33 events/s)
- event_metrics_ready_ms: 243ms
- session_metrics_ready_ms: 2862ms
- derived_metrics_ready_ms: 2862ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 5 | 3 | 8 |
| events_count_hour_grouped | 30 | 32 | 29 | 35 |
| events_count_day_dim4_grouped | 9 | 11 | 8 | 13 |
| events_count_hour_dim4_grouped | 35 | 39 | 34 | 41 |
| sessions_count_day_grouped | 2 | 3 | 2 | 3 |
| sessions_count_hour_grouped | 16 | 18 | 13 | 20 |
| sessions_duration_total_day_grouped | 3 | 4 | 2 | 4 |
| sessions_duration_total_hour_grouped | 16 | 17 | 15 | 21 |
| sessions_new_installs_day_grouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_hour_grouped | 14 | 16 | 13 | 18 |
| events_count_day_ungrouped | 0 | 0 | 0 | 0 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_new_installs_hour_ungrouped | 2 | 3 | 2 | 3 |

## append-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 2700000 events in 90937ms (29690.68 events/s)
- event_metrics_ready_ms: 382ms
- session_metrics_ready_ms: 34335ms
- derived_metrics_ready_ms: 34335ms
- Budget: PASS

## backfill-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 2700000 events in 109093ms (24749.46 events/s)
- event_metrics_ready_ms: 388ms
- session_metrics_ready_ms: 48651ms
- derived_metrics_ready_ms: 48651ms
- Budget: PASS

## repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 2700000 events in 85534ms (31566.13 events/s)
- repair_ingest: 9000 events in 405ms (22189.46 events/s)
- event_metrics_ready_ms: 2486ms
- session_metrics_ready_ms: 7112ms
- derived_metrics_ready_ms: 7112ms
- Budget: PASS

## reads-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 2700000 events in 91004ms (29668.91 events/s)
- event_metrics_ready_ms: 369ms
- session_metrics_ready_ms: 28253ms
- derived_metrics_ready_ms: 28253ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 9 | 12 | 8 | 14 |
| events_count_hour_grouped | 92 | 102 | 85 | 108 |
| events_count_day_dim4_grouped | 7 | 10 | 6 | 12 |
| events_count_hour_dim4_grouped | 75 | 82 | 71 | 87 |
| sessions_count_day_grouped | 5 | 6 | 4 | 11 |
| sessions_count_hour_grouped | 43 | 49 | 38 | 54 |
| sessions_duration_total_day_grouped | 4 | 6 | 4 | 8 |
| sessions_duration_total_hour_grouped | 43 | 50 | 38 | 55 |
| sessions_new_installs_day_grouped | 5 | 6 | 4 | 8 |
| sessions_new_installs_hour_grouped | 41 | 49 | 36 | 54 |
| events_count_day_ungrouped | 0 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 6 | 7 | 5 | 9 |
| sessions_count_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_count_hour_ungrouped | 6 | 7 | 5 | 9 |
| sessions_duration_total_day_ungrouped | 0 | 1 | 0 | 2 |
| sessions_duration_total_hour_ungrouped | 6 | 8 | 5 | 10 |
| sessions_new_installs_day_ungrouped | 0 | 1 | 0 | 1 |
| sessions_new_installs_hour_ungrouped | 6 | 7 | 5 | 9 |

## append-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 5400000 events in 182538ms (29582.74 events/s)
- event_metrics_ready_ms: 621ms
- session_metrics_ready_ms: 64297ms
- derived_metrics_ready_ms: 64297ms
- Budget: PASS

## backfill-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 5400000 events in 185368ms (29131.13 events/s)
- event_metrics_ready_ms: 621ms
- session_metrics_ready_ms: 142979ms
- derived_metrics_ready_ms: 142979ms
- Budget: PASS

## repair-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 5400000 events in 275589ms (19594.39 events/s)
- repair_ingest: 18000 events in 1626ms (11065.31 events/s)
- event_metrics_ready_ms: 4822ms
- session_metrics_ready_ms: 15603ms
- derived_metrics_ready_ms: 15603ms
- Budget: PASS

## reads-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 5400000 events in 268665ms (20099.35 events/s)
- event_metrics_ready_ms: 586ms
- session_metrics_ready_ms: 678ms
- derived_metrics_ready_ms: 678ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 18 | 20 | 17 | 21 |
| events_count_hour_grouped | 182 | 186 | 177 | 194 |
| events_count_day_dim4_grouped | 14 | 15 | 13 | 16 |
| events_count_hour_dim4_grouped | 148 | 152 | 142 | 155 |
| sessions_count_day_grouped | 9 | 10 | 7 | 10 |
| sessions_count_hour_grouped | 80 | 81 | 77 | 82 |
| sessions_duration_total_day_grouped | 10 | 10 | 7 | 11 |
| sessions_duration_total_hour_grouped | 81 | 82 | 76 | 83 |
| sessions_new_installs_day_grouped | 10 | 10 | 7 | 10 |
| sessions_new_installs_hour_grouped | 81 | 94 | 77 | 103 |
| events_count_day_ungrouped | 1 | 1 | 1 | 2 |
| events_count_hour_ungrouped | 12 | 14 | 11 | 15 |
| sessions_count_day_ungrouped | 1 | 1 | 1 | 2 |
| sessions_count_hour_ungrouped | 12 | 14 | 11 | 18 |
| sessions_duration_total_day_ungrouped | 1 | 2 | 1 | 3 |
| sessions_duration_total_hour_ungrouped | 12 | 15 | 11 | 17 |
| sessions_new_installs_day_ungrouped | 1 | 2 | 1 | 3 |
| sessions_new_installs_hour_ungrouped | 12 | 14 | 11 | 16 |