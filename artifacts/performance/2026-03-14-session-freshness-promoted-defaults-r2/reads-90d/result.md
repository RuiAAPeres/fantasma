# Fantasma Derived Metrics SLO: reads-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- Failure: scenario execution failed: command failed with status Some(1)
stdout:
#1 [internal] load local bake definitions
#1 reading from stdin 1.55kB done
#1 DONE 0.0s

#2 [fantasma-worker internal] load build definition from Dockerfile.worker
#2 transferring dockerfile: 454B done
#2 DONE 0.0s

#3 [fantasma-api internal] load build definition from Dockerfile.api
#3 transferring dockerfile: 451B done
#3 DONE 0.0s

#4 [fantasma-ingest internal] load build definition from Dockerfile.ingest
#4 transferring dockerfile: 466B done
#4 DONE 0.0s

#5 [fantasma-api internal] load metadata for docker.io/library/rust:1.92-bookworm
#5 ...

#6 [fantasma-api internal] load metadata for docker.io/library/debian:bookworm-slim
#6 ...

#5 [fantasma-api internal] load metadata for docker.io/library/rust:1.92-bookworm
#5 ERROR: failed to do request: Head "https://registry-1.docker.io/v2/library/rust/manifests/1.92-bookworm": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host

#6 [fantasma-ingest internal] load metadata for docker.io/library/debian:bookworm-slim
#6 ERROR: failed to do request: Head "https://registry-1.docker.io/v2/library/debian/manifests/bookworm-slim": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host
------
 > [fantasma-ingest internal] load metadata for docker.io/library/debian:bookworm-slim:
------
------
 > [fantasma-ingest internal] load metadata for docker.io/library/rust:1.92-bookworm:
------

stderr:
 Image fantasma-bench-fantasma-ingest Building 
 Image fantasma-bench-fantasma-api Building 
 Image fantasma-bench-fantasma-worker Building 
Dockerfile.ingest:1

--------------------

   1 | >>> FROM rust:1.92-bookworm AS builder

   2 |     WORKDIR /app

   3 |     

--------------------

target fantasma-ingest: failed to solve: rust:1.92-bookworm: failed to resolve source metadata for docker.io/library/rust:1.92-bookworm: failed to do request: Head "https://registry-1.docker.io/v2/library/rust/manifests/1.92-bookworm": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host


- Budget: FAIL
  - scenario execution failed: command failed with status Some(1)
stdout:
#1 [internal] load local bake definitions
#1 reading from stdin 1.55kB done
#1 DONE 0.0s

#2 [fantasma-worker internal] load build definition from Dockerfile.worker
#2 transferring dockerfile: 454B done
#2 DONE 0.0s

#3 [fantasma-api internal] load build definition from Dockerfile.api
#3 transferring dockerfile: 451B done
#3 DONE 0.0s

#4 [fantasma-ingest internal] load build definition from Dockerfile.ingest
#4 transferring dockerfile: 466B done
#4 DONE 0.0s

#5 [fantasma-api internal] load metadata for docker.io/library/rust:1.92-bookworm
#5 ...

#6 [fantasma-api internal] load metadata for docker.io/library/debian:bookworm-slim
#6 ...

#5 [fantasma-api internal] load metadata for docker.io/library/rust:1.92-bookworm
#5 ERROR: failed to do request: Head "https://registry-1.docker.io/v2/library/rust/manifests/1.92-bookworm": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host

#6 [fantasma-ingest internal] load metadata for docker.io/library/debian:bookworm-slim
#6 ERROR: failed to do request: Head "https://registry-1.docker.io/v2/library/debian/manifests/bookworm-slim": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host
------
 > [fantasma-ingest internal] load metadata for docker.io/library/debian:bookworm-slim:
------
------
 > [fantasma-ingest internal] load metadata for docker.io/library/rust:1.92-bookworm:
------

stderr:
 Image fantasma-bench-fantasma-ingest Building 
 Image fantasma-bench-fantasma-api Building 
 Image fantasma-bench-fantasma-worker Building 
Dockerfile.ingest:1

--------------------

   1 | >>> FROM rust:1.92-bookworm AS builder

   2 |     WORKDIR /app

   3 |     

--------------------

target fantasma-ingest: failed to solve: rust:1.92-bookworm: failed to resolve source metadata for docker.io/library/rust:1.92-bookworm: failed to do request: Head "https://registry-1.docker.io/v2/library/rust/manifests/1.92-bookworm": dialing registry-1.docker.io:443 container via direct connection because Docker Desktop has no HTTPS proxy: connecting to registry-1.docker.io:443: dial tcp: lookup registry-1.docker.io: no such host

