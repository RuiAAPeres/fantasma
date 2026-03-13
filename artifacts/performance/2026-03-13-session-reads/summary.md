# Fantasma Derived Metrics SLO Suite

- Host: Apple M3 Pro / 36 GiB / Darwin 25.1.0 / aarch64
- Benchmarked at: 2026-03-13T16:32:31.266302+00:00

## reads-30d

- seed_ingest: 900000 events in 35143ms (25609.15 events/s)
- event_metrics_ready_ms: 62ms
- session_metrics_ready_ms: 14526ms
- derived_metrics_ready_ms: 14526ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 6 | 3 | 6 |
| events_count_hour_grouped | 32 | 37 | 30 | 45 |
| sessions_count_day_grouped | 2 | 3 | 2 | 3 |
| sessions_count_hour_grouped | 14 | 16 | 13 | 19 |
| sessions_duration_total_day_grouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_hour_grouped | 14 | 15 | 13 | 18 |
| sessions_new_installs_day_grouped | 2 | 4 | 2 | 5 |
| sessions_new_installs_hour_grouped | 14 | 15 | 13 | 17 |
| events_count_day_ungrouped | 0 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 2 | 3 | 2 | 4 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_count_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 2 | 4 | 2 | 5 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_new_installs_hour_ungrouped | 2 | 3 | 2 | 3 |

## reads-90d

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

## reads-180d

- seed_ingest: 5400000 events in 205286ms (26304.72 events/s)
- event_metrics_ready_ms: 272ms
- session_metrics_ready_ms: 64953ms
- derived_metrics_ready_ms: 64953ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 18 | 19 | 17 | 23 |
| events_count_hour_grouped | 189 | 191 | 184 | 194 |
| sessions_count_day_grouped | 7 | 7 | 6 | 8 |
| sessions_count_hour_grouped | 73 | 75 | 71 | 82 |
| sessions_duration_total_day_grouped | 7 | 8 | 6 | 9 |
| sessions_duration_total_hour_grouped | 74 | 76 | 71 | 78 |
| sessions_new_installs_day_grouped | 6 | 7 | 6 | 7 |
| sessions_new_installs_hour_grouped | 73 | 75 | 71 | 77 |
| events_count_day_ungrouped | 1 | 1 | 1 | 1 |
| events_count_hour_ungrouped | 11 | 11 | 10 | 11 |
| sessions_count_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_count_hour_ungrouped | 11 | 11 | 10 | 12 |
| sessions_duration_total_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_duration_total_hour_ungrouped | 11 | 11 | 10 | 11 |
| sessions_new_installs_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_new_installs_hour_ungrouped | 10 | 11 | 10 | 15 |