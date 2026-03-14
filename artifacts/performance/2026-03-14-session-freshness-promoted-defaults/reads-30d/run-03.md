# Fantasma Derived Metrics SLO: reads-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 32039ms (28090.41 events/s)
- event_metrics_ready_ms: 243ms
- session_metrics_ready_ms: 9303ms
- derived_metrics_ready_ms: 9303ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 5 | 3 | 6 |
| events_count_hour_grouped | 31 | 35 | 29 | 44 |
| events_count_day_dim4_grouped | 9 | 11 | 8 | 13 |
| events_count_hour_dim4_grouped | 35 | 39 | 32 | 41 |
| sessions_count_day_grouped | 2 | 2 | 2 | 3 |
| sessions_count_hour_grouped | 16 | 18 | 13 | 36 |
| sessions_duration_total_day_grouped | 3 | 4 | 2 | 4 |
| sessions_duration_total_hour_grouped | 16 | 19 | 15 | 23 |
| sessions_new_installs_day_grouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_hour_grouped | 14 | 16 | 13 | 18 |
| events_count_day_ungrouped | 0 | 0 | 0 | 0 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_new_installs_hour_ungrouped | 2 | 2 | 2 | 3 |