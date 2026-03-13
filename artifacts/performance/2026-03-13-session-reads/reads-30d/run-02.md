# Fantasma Derived Metrics SLO: reads-30d

- seed_ingest: 900000 events in 39493ms (22788.58 events/s)
- event_metrics_ready_ms: 66ms
- session_metrics_ready_ms: 14526ms
- derived_metrics_ready_ms: 14526ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 6 | 4 | 6 |
| events_count_hour_grouped | 34 | 37 | 31 | 45 |
| sessions_count_day_grouped | 3 | 3 | 2 | 4 |
| sessions_count_hour_grouped | 15 | 16 | 14 | 18 |
| sessions_duration_total_day_grouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_hour_grouped | 15 | 17 | 14 | 24 |
| sessions_new_installs_day_grouped | 3 | 3 | 2 | 3 |
| sessions_new_installs_hour_grouped | 14 | 17 | 14 | 22 |
| events_count_day_ungrouped | 0 | 1 | 0 | 2 |
| events_count_hour_ungrouped | 2 | 3 | 2 | 4 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_count_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_new_installs_hour_ungrouped | 2 | 3 | 2 | 3 |