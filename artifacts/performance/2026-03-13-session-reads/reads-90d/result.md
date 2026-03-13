# Fantasma Derived Metrics SLO: reads-90d

- seed_ingest: 2700000 events in 99493ms (27137.52 events/s)
- event_metrics_ready_ms: 143ms
- session_metrics_ready_ms: 32820ms
- derived_metrics_ready_ms: 32820ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 10 | 11 | 9 | 11 |
| events_count_hour_grouped | 96 | 98 | 94 | 101 |
| sessions_count_day_grouped | 4 | 5 | 4 | 9 |
| sessions_count_hour_grouped | 38 | 39 | 37 | 41 |
| sessions_duration_total_day_grouped | 4 | 5 | 4 | 5 |
| sessions_duration_total_hour_grouped | 38 | 39 | 36 | 42 |
| sessions_new_installs_day_grouped | 4 | 4 | 3 | 5 |
| sessions_new_installs_hour_grouped | 37 | 38 | 36 | 40 |
| events_count_day_ungrouped | 0 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 6 | 6 | 5 | 6 |
| sessions_count_day_ungrouped | 0 | 1 | 0 | 1 |
| sessions_count_hour_ungrouped | 6 | 6 | 5 | 6 |
| sessions_duration_total_day_ungrouped | 0 | 1 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 6 | 6 | 5 | 6 |
| sessions_new_installs_day_ungrouped | 0 | 1 | 0 | 1 |
| sessions_new_installs_hour_ungrouped | 5 | 6 | 5 | 6 |