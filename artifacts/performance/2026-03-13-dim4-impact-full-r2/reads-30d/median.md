# Fantasma Derived Metrics SLO: reads-30d

- seed_ingest: 900000 events in 30501ms (29507.21 events/s)
- event_metrics_ready_ms: 237ms
- session_metrics_ready_ms: 25996ms
- derived_metrics_ready_ms: 25996ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 5 | 3 | 5 |
| events_count_hour_grouped | 31 | 37 | 30 | 42 |
| events_count_day_dim4_grouped | 9 | 12 | 7 | 14 |
| events_count_hour_dim4_grouped | 33 | 35 | 31 | 36 |
| sessions_count_day_grouped | 2 | 3 | 1 | 3 |
| sessions_count_hour_grouped | 14 | 15 | 13 | 16 |
| sessions_duration_total_day_grouped | 2 | 2 | 2 | 2 |
| sessions_duration_total_hour_grouped | 14 | 15 | 13 | 15 |
| sessions_new_installs_day_grouped | 2 | 2 | 1 | 3 |
| sessions_new_installs_hour_grouped | 13 | 14 | 12 | 15 |
| events_count_day_ungrouped | 0 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 2 | 3 | 2 | 5 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_new_installs_hour_ungrouped | 2 | 2 | 2 | 3 |