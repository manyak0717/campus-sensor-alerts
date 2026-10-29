#!/usr/bin/env python3
"""Load test: fire 10,000 readings at the backend within a 1-second window
and measure what actually happens, end to end. Prints a report; run with
--out to also write it to a file.

Two scenarios, because "a burst of 10,000 readings in one second" is
ambiguous about shape:

  A. single-batch:   one POST /readings/batch with all 10,000 readings.
                      Best case for raw accept throughput (one HTTP
                      round trip, one validation pass, one queue-fill).
  B. concurrent-fanout: 200 concurrent POSTs of 50 readings each, fired
                      together with asyncio.gather. This is the more
                      realistic shape for "10,000 campus sensors all
                      reporting around the same moment" -- many
                      independent producers, not one coordinator that
                      already batched everything for you.

For both, we measure:
  - accept latency: wall-clock time until every HTTP response is back
    (i.e. the time for the system to say "got it" to all 10,000 readings)
  - drain latency: additional time, after accept, until /health reports
    persisted count has caught up (i.e. fully written to SQLite) -- this
    is the queueing/back-pressure part of the pipeline, decoupled from
    the HTTP accept path by design (see ARCHITECTURE.md)
  - error count (non-202 responses)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

N_READINGS = 10_000
SENSOR_TYPES = ["temperature", "air_quality", "occupancy"]


def make_reading(i: int, base_time: datetime) -> dict:
    return {
        "sensorId": f"load-test-sensor-{i % 500}",
        "type": SENSOR_TYPES[i % len(SENSOR_TYPES)],
        "value": round(random.uniform(0, 100), 2),
        "timestamp": (base_time + timedelta(microseconds=i)).isoformat(),
    }


async def get_health(client: httpx.AsyncClient) -> dict:
    r = await client.get("/health")
    r.raise_for_status()
    return r.json()


async def wait_for_drain(client: httpx.AsyncClient, target_persisted: int, timeout_s: float = 30.0) -> float:
    """Poll /health until persisted count reaches target. Returns elapsed seconds."""
    start = time.perf_counter()
    while True:
        h = await get_health(client)
        if h["persisted"] >= target_persisted:
            return time.perf_counter() - start
        if time.perf_counter() - start > timeout_s:
            raise TimeoutError(f"drain did not complete in {timeout_s}s; last health={h}")
        await asyncio.sleep(0.05)


async def scenario_single_batch(client: httpx.AsyncClient) -> dict:
    before = await get_health(client)
    base_time = datetime.now(timezone.utc)
    payload = {"readings": [make_reading(i, base_time) for i in range(N_READINGS)]}

    t0 = time.perf_counter()
    r = await client.post("/readings/batch", json=payload, timeout=30.0)
    accept_elapsed = time.perf_counter() - t0
    r.raise_for_status()
    ack = r.json()

    drain_elapsed = await wait_for_drain(client, before["persisted"] + N_READINGS)
    after = await get_health(client)

    return {
        "scenario": "single-batch (1 request x 10,000 readings)",
        "requests": 1,
        "accepted": ack.get("accepted", 0),
        "errors": 0,
        "accept_latency_s": round(accept_elapsed, 4),
        "accept_throughput_per_s": round(N_READINGS / accept_elapsed, 1),
        "drain_latency_s": round(drain_elapsed, 4),
        "total_latency_s": round(accept_elapsed + drain_elapsed, 4),
        "health_before": before,
        "health_after": after,
    }


async def scenario_concurrent_fanout(client: httpx.AsyncClient, n_requests: int = 200) -> dict:
    per_request = N_READINGS // n_requests
    before = await get_health(client)
    base_time = datetime.now(timezone.utc)

    batches = []
    idx = 0
    for _ in range(n_requests):
        batches.append([make_reading(idx + j, base_time) for j in range(per_request)])
        idx += per_request

    async def send_one(batch: list[dict]):
        try:
            r = await client.post("/readings/batch", json={"readings": batch}, timeout=30.0)
            return r.status_code, (r.json() if r.status_code == 202 else r.text)
        except httpx.HTTPError as e:
            return None, str(e)

    t0 = time.perf_counter()
    results = await asyncio.gather(*(send_one(b) for b in batches))
    accept_elapsed = time.perf_counter() - t0

    errors = [res for res in results if res[0] != 202]
    accepted = sum(res[1]["accepted"] for res in results if res[0] == 202)

    drain_elapsed = await wait_for_drain(client, before["persisted"] + accepted)
    after = await get_health(client)

    return {
        "scenario": f"concurrent-fanout ({n_requests} requests x {per_request} readings, fired together)",
        "requests": n_requests,
        "accepted": accepted,
        "errors": len(errors),
        "error_samples": errors[:3],
        "accept_latency_s": round(accept_elapsed, 4),
        "accept_throughput_per_s": round(accepted / accept_elapsed, 1) if accept_elapsed > 0 else None,
        "drain_latency_s": round(drain_elapsed, 4),
        "total_latency_s": round(accept_elapsed + drain_elapsed, 4),
        "health_before": before,
        "health_after": after,
    }


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--out", default=None, help="write JSON report to this path too")
    args = ap.parse_args()

    results = {}
    async with httpx.AsyncClient(base_url=args.base_url) as client:
        try:
            await get_health(client)
        except httpx.HTTPError as e:
            print(f"cannot reach {args.base_url}: {e}", file=sys.stderr)
            sys.exit(1)

        print("=== Scenario A: single batch request (10,000 readings, 1 POST) ===")
        results["single_batch"] = await scenario_single_batch(client)
        print(json.dumps(results["single_batch"], indent=2, default=str))

        print("\n=== Scenario B: concurrent fanout (200 concurrent POSTs, 50 readings each) ===")
        results["concurrent_fanout"] = await scenario_concurrent_fanout(client)
        print(json.dumps(results["concurrent_fanout"], indent=2, default=str))

    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    asyncio.run(main())
