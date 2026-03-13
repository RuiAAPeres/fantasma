# Fantasma Derived Metrics SLO Suite

- Host: Apple M3 Pro / 36 GiB / Darwin 25.1.0 / aarch64
- Benchmarked at: 2026-03-13T21:25:13.496731+00:00

## append-30d

- ingest: 900000 events in 35141ms (25610.79 events/s)
- event_metrics_ready_ms: 236ms
- session_metrics_ready_ms: 29336ms
- derived_metrics_ready_ms: 29336ms
- Budget: PASS

## backfill-30d

- ingest: 900000 events in 33841ms (26594.55 events/s)
- event_metrics_ready_ms: 238ms
- session_metrics_ready_ms: 53903ms
- derived_metrics_ready_ms: 53903ms
- Budget: PASS

## repair-30d

- seed_ingest: 900000 events in 36045ms (24968.24 events/s)
- repair_ingest: 3000 events in 133ms (22552.62 events/s)
- event_metrics_ready_ms: 744ms
- session_metrics_ready_ms: 1706ms
- derived_metrics_ready_ms: 1706ms
- Budget: PASS

## reads-30d

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

## append-90d

- ingest: 2700000 events in 96949ms (27849.50 events/s)
- event_metrics_ready_ms: 377ms
- session_metrics_ready_ms: 76530ms
- derived_metrics_ready_ms: 76530ms
- Budget: PASS

## backfill-90d

- ingest: 2700000 events in 89472ms (30176.83 events/s)
- event_metrics_ready_ms: 349ms
- session_metrics_ready_ms: 284560ms
- derived_metrics_ready_ms: 284560ms
- Budget: PASS

## repair-90d

- seed_ingest: 2700000 events in 88132ms (30635.59 events/s)
- repair_ingest: 9000 events in 381ms (23593.03 events/s)
- event_metrics_ready_ms: 2264ms
- session_metrics_ready_ms: 5192ms
- derived_metrics_ready_ms: 5192ms
- Budget: PASS

## reads-90d

- seed_ingest: 2700000 events in 111410ms (24234.68 events/s)
- event_metrics_ready_ms: 432ms
- session_metrics_ready_ms: 81960ms
- derived_metrics_ready_ms: 81960ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 10 | 11 | 9 | 11 |
| events_count_hour_grouped | 97 | 98 | 96 | 100 |
| events_count_day_dim4_grouped | 6 | 7 | 6 | 7 |
| events_count_hour_dim4_grouped | 73 | 75 | 71 | 79 |
| sessions_count_day_grouped | 4 | 5 | 3 | 5 |
| sessions_count_hour_grouped | 38 | 39 | 37 | 40 |
| sessions_duration_total_day_grouped | 4 | 5 | 3 | 5 |
| sessions_duration_total_hour_grouped | 38 | 40 | 37 | 47 |
| sessions_new_installs_day_grouped | 4 | 5 | 3 | 5 |
| sessions_new_installs_hour_grouped | 38 | 40 | 37 | 44 |
| events_count_day_ungrouped | 1 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 6 | 6 | 5 | 7 |
| sessions_count_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_count_hour_ungrouped | 6 | 6 | 6 | 7 |
| sessions_duration_total_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 6 | 6 | 6 | 7 |
| sessions_new_installs_day_ungrouped | 0 | 1 | 0 | 1 |
| sessions_new_installs_hour_ungrouped | 6 | 6 | 5 | 6 |

## append-180d

- ingest: 5400000 events in 197467ms (27346.22 events/s)
- event_metrics_ready_ms: 652ms
- session_metrics_ready_ms: 176019ms
- derived_metrics_ready_ms: 176019ms
- Budget: PASS

## backfill-180d

- ingest: 5400000 events in 184502ms (29267.83 events/s)
- event_metrics_ready_ms: 719ms
- session_metrics_ready_ms: 750595ms
- derived_metrics_ready_ms: 750595ms
- Budget: PASS

## repair-180d

- seed_ingest: 5400000 events in 200099ms (26986.59 events/s)
- repair_ingest: 18000 events in 759ms (23707.39 events/s)
- event_metrics_ready_ms: 4876ms
- session_metrics_ready_ms: 11993ms
- derived_metrics_ready_ms: 11993ms
- Budget: PASS

## reads-180d

- seed_ingest: 5400000 events in 175775ms (30721.07 events/s)
- event_metrics_ready_ms: 635ms
- session_metrics_ready_ms: 169084ms
- derived_metrics_ready_ms: 169084ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 20 | 20 | 19 | 22 |
| events_count_hour_grouped | 195 | 199 | 193 | 272 |
| events_count_day_dim4_grouped | 12 | 13 | 12 | 14 |
| events_count_hour_dim4_grouped | 142 | 145 | 139 | 149 |
| sessions_count_day_grouped | 7 | 8 | 6 | 8 |
| sessions_count_hour_grouped | 75 | 77 | 73 | 78 |
| sessions_duration_total_day_grouped | 7 | 8 | 6 | 8 |
| sessions_duration_total_hour_grouped | 75 | 77 | 73 | 80 |
| sessions_new_installs_day_grouped | 7 | 8 | 6 | 8 |
| sessions_new_installs_hour_grouped | 75 | 77 | 73 | 79 |
| events_count_day_ungrouped | 1 | 1 | 1 | 1 |
| events_count_hour_ungrouped | 11 | 11 | 10 | 12 |
| sessions_count_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_count_hour_ungrouped | 11 | 12 | 11 | 13 |
| sessions_duration_total_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_duration_total_hour_ungrouped | 11 | 12 | 11 | 12 |
| sessions_new_installs_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_new_installs_hour_ungrouped | 11 | 12 | 10 | 13 |