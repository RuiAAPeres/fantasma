# Fantasma Heavy Benchmark Series

- Profile: heavy
- Repetitions per scenario: 5
- Host: Apple M3 Pro / 36 GiB / Darwin 25.1.0 / aarch64
- Benchmarked at: 2026-03-13T12:28:14.835239+00:00

## hot-path

- ingest: 9000 events in 305ms (29490.69 events/s)
- derived_metrics_ready: 4252ms

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_day_dim2 | 1 | 1 | 0 | 1 |
| events_hour_dim2 | 2 | 2 | 1 | 2 |
| sessions_count_day | 0 | 0 | 0 | 0 |
| sessions_duration_total_day | 0 | 0 | 0 | 0 |

## repair-path

- seed_ingest: 3000 events in 105ms (28516.09 events/s)
- repair_ingest: 1200 events in 44ms (26810.97 events/s)
- seed_ready: 1690ms
- repair_ready: 1146ms

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_day_dim2 | 1 | 2 | 1 | 2 |
| events_hour_dim2 | 4 | 5 | 3 | 6 |
| sessions_count_day | 0 | 0 | 0 | 0 |
| sessions_duration_total_day | 0 | 0 | 0 | 1 |

## scale-path

- ingest: 180000 events in 5350ms (33641.39 events/s)
- derived_metrics_ready: 96352ms

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_day_dim2 | 9 | 10 | 8 | 10 |
| events_hour_dim2 | 51 | 52 | 48 | 55 |
| sessions_count_day | 0 | 0 | 0 | 0 |
| sessions_duration_total_day | 0 | 0 | 0 | 0 |