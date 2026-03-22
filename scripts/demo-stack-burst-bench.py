#!/usr/bin/env python3

import argparse
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib import error, request

PLANS = ["free", "pro", "team"]
PROVIDERS = ["strava", "garmin", "polar", "oura"]
APP_VERSIONS = ["1.0.0", "1.1.0", "1.2.0"]
OS_VERSIONS = ["18.3", "18.4"]
LOCALES = ["en-US", "pt-PT", "fr-FR"]
DEFAULT_DAY = "2026-01-01"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_POLL_INTERVAL_SECONDS = 0.05

SCENARIOS = {
    "burst-readiness-300-installs-x1": (300, 1),
    "burst-readiness-150-installs-x2": (150, 2),
    "burst-readiness-100-installs-x3": (100, 3),
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark realistic 300-event bursts against an existing Fantasma stack."
    )
    parser.add_argument("--api-base-url", required=True)
    parser.add_argument("--ingest-base-url", required=True)
    parser.add_argument("--admin-token", required=True)
    parser.add_argument("--day", default=DEFAULT_DAY)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=sorted(SCENARIOS.keys()),
        help="Repeat to run a subset. Defaults to all realistic burst scenarios.",
    )
    return parser.parse_args()


def http_json(method, url, body=None, headers=None):
    data = None
    req_headers = {"content-type": "application/json"}
    if headers:
        req_headers.update(headers)
    if body is not None:
        data = json.dumps(body).encode()
    req = request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with request.urlopen(req, timeout=30) as response:
            payload = response.read().decode()
            return response.status, json.loads(payload) if payload else None
    except error.HTTPError as exc:
        payload = exc.read().decode()
        raise RuntimeError(f"{method} {url} -> {exc.code}: {payload}") from exc


def create_project(api_base_url, admin_token, tag):
    status, payload = http_json(
        "POST",
        f"{api_base_url}/v1/projects",
        {"name": f"bench-{tag}", "ingest_key_name": "bench-ingest"},
        {"authorization": f"Bearer {admin_token}"},
    )
    if status != 201:
        raise RuntimeError(f"unexpected create project status {status}")

    project_id = payload["project"]["id"]
    ingest_key = payload["ingest_key"]["secret"]
    status, payload = http_json(
        "POST",
        f"{api_base_url}/v1/projects/{project_id}/keys",
        {"name": "bench-read", "kind": "read"},
        {"authorization": f"Bearer {admin_token}"},
    )
    if status != 201:
        raise RuntimeError(f"unexpected create key status {status}")

    return ingest_key, payload["key"]["secret"]


def build_events(day, install_count, events_per_install, prefix):
    events = []
    for install_index in range(install_count):
        install_id = f"{prefix}-install-{install_index:04d}"
        for event_offset in range(events_per_install):
            events.append(
                {
                    "event": "app_open",
                    "timestamp": f"{day}T00:{event_offset:02d}:00Z",
                    "install_id": install_id,
                    "platform": "ios" if install_index % 2 == 0 else "android",
                    "app_version": APP_VERSIONS[install_index % len(APP_VERSIONS)],
                    "os_version": OS_VERSIONS[install_index % len(OS_VERSIONS)],
                    "locale": LOCALES[install_index % len(LOCALES)],
                }
            )
    return events


def post_batch(ingest_base_url, ingest_key, batch):
    status, payload = http_json(
        "POST",
        f"{ingest_base_url}/v1/events",
        {"events": batch},
        {"x-fantasma-key": ingest_key},
    )
    if status != 202:
        raise RuntimeError(f"unexpected ingest status {status}")
    return payload


def metric_total(read_key, url):
    status, payload = http_json("GET", url, headers={"x-fantasma-key": read_key})
    if status != 200:
        raise RuntimeError(f"unexpected metric status {status}")

    total = 0
    for series in payload.get("series", []):
        for point in series.get("points", []):
            total += int(point.get("value", 0))
    return total


def wait_until(read_key, url, expected, timeout_seconds, poll_interval_seconds):
    started = time.perf_counter()
    last = None
    while True:
        last = metric_total(read_key, url)
        if last == expected:
            return time.perf_counter() - started, last
        if time.perf_counter() - started >= timeout_seconds:
            return None, last
        time.sleep(poll_interval_seconds)


def run_case(args, name, install_count, events_per_install):
    prefix = f"{name}-{uuid.uuid4().hex[:8]}"
    ingest_key, read_key = create_project(args.api_base_url, args.admin_token, prefix)
    events = build_events(args.day, install_count, events_per_install, prefix)
    batches = [events[i : i + 100] for i in range(0, len(events), 100)]

    ingest_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(batches)) as pool:
        accepted = [
            future.result()
            for future in [
                pool.submit(post_batch, args.ingest_base_url, ingest_key, batch)
                for batch in batches
            ]
        ]
    ingest_elapsed = time.perf_counter() - ingest_started

    events_url = (
        f"{args.api_base_url}/v1/metrics/events?event=app_open&metric=count"
        f"&granularity=day&start={args.day}&end={args.day}"
    )
    sessions_url = (
        f"{args.api_base_url}/v1/metrics/sessions?metric=count"
        f"&granularity=day&start={args.day}&end={args.day}"
    )

    event_wait, event_last = wait_until(
        read_key,
        events_url,
        len(events),
        args.timeout_seconds,
        args.poll_interval_seconds,
    )
    session_wait, session_last = wait_until(
        read_key,
        sessions_url,
        install_count,
        args.timeout_seconds,
        args.poll_interval_seconds,
    )

    return {
        "scenario": name,
        "install_count": install_count,
        "events_per_install": events_per_install,
        "total_events": len(events),
        "request_count": len(batches),
        "accepted": accepted,
        "ingest_accepted_s": round(ingest_elapsed, 3),
        "event_ready_from_ingest_done_s": None if event_wait is None else round(event_wait, 3),
        "session_ready_from_ingest_done_s": None
        if session_wait is None
        else round(session_wait, 3),
        "event_ready_from_burst_start_s": None
        if event_wait is None
        else round(ingest_elapsed + event_wait, 3),
        "session_ready_from_burst_start_s": None
        if session_wait is None
        else round(ingest_elapsed + session_wait, 3),
        "event_last_value": event_last,
        "session_last_value": session_last,
    }


def main():
    args = parse_args()
    selected = args.scenario or list(SCENARIOS.keys())
    results = [
        run_case(args, name, *SCENARIOS[name])
        for name in selected
    ]
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
