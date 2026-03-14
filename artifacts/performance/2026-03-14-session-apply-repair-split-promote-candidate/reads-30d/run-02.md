# Fantasma Derived Metrics SLO: reads-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 30591ms (29420.32 events/s)
- event_metrics_ready_ms: 252ms
- session_metrics_ready_ms: 9836ms
- derived_metrics_ready_ms: 9836ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 5 | 3 | 6 |
| events_count_hour_grouped | 30 | 34 | 28 | 38 |
| events_count_day_dim4_grouped | 9 | 10 | 8 | 14 |
| events_count_hour_dim4_grouped | 33 | 34 | 32 | 35 |
| sessions_count_day_grouped | 2 | 2 | 2 | 3 |
| sessions_count_hour_grouped | 13 | 14 | 13 | 15 |
| sessions_duration_total_day_grouped | 2 | 2 | 2 | 3 |
| sessions_duration_total_hour_grouped | 13 | 14 | 13 | 16 |
| sessions_new_installs_day_grouped | 2 | 2 | 2 | 3 |
| sessions_new_installs_hour_grouped | 14 | 15 | 13 | 19 |
| events_count_day_ungrouped | 0 | 0 | 0 | 1 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_new_installs_hour_ungrouped | 2 | 2 | 2 | 2 |