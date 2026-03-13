# Fantasma Derived Metrics SLO: reads-30d

- seed_ingest: 900000 events in 35120ms (25625.99 events/s)
- event_metrics_ready_ms: 237ms
- session_metrics_ready_ms: 26224ms
- derived_metrics_ready_ms: 26224ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 6 | 3 | 9 |
| events_count_hour_grouped | 31 | 37 | 29 | 42 |
| events_count_day_dim4_grouped | 9 | 12 | 7 | 14 |
| events_count_hour_dim4_grouped | 34 | 39 | 28 | 44 |
| sessions_count_day_grouped | 2 | 3 | 2 | 4 |
| sessions_count_hour_grouped | 13 | 16 | 12 | 19 |
| sessions_duration_total_day_grouped | 2 | 2 | 2 | 2 |
| sessions_duration_total_hour_grouped | 14 | 17 | 12 | 22 |
| sessions_new_installs_day_grouped | 2 | 2 | 2 | 3 |
| sessions_new_installs_hour_grouped | 14 | 16 | 12 | 21 |
| events_count_day_ungrouped | 0 | 0 | 0 | 1 |
| events_count_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 3 | 2 | 4 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 2 | 3 | 2 | 5 |
| sessions_new_installs_day_ungrouped | 0 | 1 | 0 | 2 |
| sessions_new_installs_hour_ungrouped | 2 | 4 | 2 | 5 |