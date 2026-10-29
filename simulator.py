#!/usr/bin/env python3
"""Sensor simulator: generates realistic readings for a set of fake campus
sensors and streams them to a running instance of the backend. Used for
manual demos (watch alerts appear as sensors drift out of range) and as
the building block the load test reuses for its burst scenario.

Usage:
    python simulator.py --base-url http://127.0.0.1:8000 --sensors 20 --rate 5 --duration 60
    python simulator.py --spike library-floor-2   # force one sensor hot, to demo an alert firing
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time
from datetime import datetime, timezone

import httpx

SENSOR_PROFILES = {
    "temperature": {"baseline": (18, 24), "noise": 0.4, "spike": (32, 42)},
    "air_quality": {"baseline": (20, 60), "noise": 3.0, "spike": (150, 300)},
    "occupancy": {"baseline": (0, 15), "noise": 1.5, "spike": (40, 80)},
}


class Sensor:
    def __init__(self, sensor_id: str, metric: str):
        self.sensor_id = sensor_id
        self.metric = metric
        profile = SENSOR_PROFILES[metric]
        self.value = random.uniform(*profile["baseline"])
        self.noise = profile["noise"]
        self.spike_range = profile["spike"]
        self.spiking_until = 0.0

    def trigger_spike(self, duration_s: float = 30.0) -> None:
        self.spiking_until = time.monotonic() + duration_s

    def next_value(self) -> float:
        profile = SENSOR_PROFILES[self.metric]
        if time.monotonic() < self.spiking_until:
            target = random.uniform(*self.spike_range)
            self.value += (target - self.value) * 0.3
        else:
            lo, hi = profile["baseline"]
            self.value += random.uniform(-self.noise, self.noise)
            self.value = max(lo - self.noise, min(hi + self.noise, self.value))
        return round(self.value, 2)

    def reading(self) -> dict:
        return {
            "sensorId": self.sensor_id,
            "type": self.metric,
            "value": self.next_value(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def build_sensors(n: int) -> list[Sensor]:
    metrics = list(SENSOR_PROFILES)
    locations = ["library-floor-1", "library-floor-2", "dining-hall", "gym",
                 "lecture-hall-a", "lecture-hall-b", "dorm-a", "dorm-b"]
    sensors = []
    for i in range(n):
        loc = locations[i % len(locations)]
        metric = metrics[i % len(metrics)]
        sensors.append(Sensor(f"{loc}-{metric}-{i}", metric))
    return sensors


async def stream(base_url: str, sensors: list[Sensor], rate_per_sec: float, duration_s: float) -> None:
    interval = 1.0 / rate_per_sec
    end_at = time.monotonic() + duration_s
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        i = 0
        while time.monotonic() < end_at:
            sensor = sensors[i % len(sensors)]
            i += 1
            try:
                r = await client.post("/readings", json=sensor.reading())
                if r.status_code != 202:
                    print(f"unexpected status {r.status_code}: {r.text}")
            except httpx.HTTPError as e:
                print(f"request failed: {e}")
            await asyncio.sleep(interval)
    print(f"streamed {i} readings")


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--sensors", type=int, default=12, help="number of simulated sensors")
    ap.add_argument("--rate", type=float, default=5.0, help="readings per second, combined across sensors")
    ap.add_argument("--duration", type=float, default=30.0, help="seconds to run")
    ap.add_argument("--spike", action="append", default=[],
                     help="sensor index or location substring to force into a spike (repeatable)")
    args = ap.parse_args()

    sensors = build_sensors(args.sensors)
    for s in sensors:
        if any(tag in s.sensor_id for tag in args.spike):
            s.trigger_spike(duration_s=args.duration)
            print(f"forcing spike on {s.sensor_id}")

    print(f"streaming {args.rate} readings/sec from {len(sensors)} sensors for {args.duration}s "
          f"against {args.base_url}")
    await stream(args.base_url, sensors, args.rate, args.duration)


if __name__ == "__main__":
    asyncio.run(main())
