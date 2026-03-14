# Fantasma Derived Metrics SLO: reads-90d

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