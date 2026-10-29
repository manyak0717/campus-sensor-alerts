"""The hot path: accept readings fast, process them off the request path.

Flow:
  HTTP handler -> validate shape -> push onto asyncio.Queue -> return 202
  background consumer -> pull off queue -> evaluate rules (fast, in-memory)
                       -> accumulate -> bulk-insert to DB every N items or
                          every FLUSH_INTERVAL seconds, whichever first

Rule evaluation happens per-reading as soon as it's dequeued (cheap, O(1)
amortized per rule), so alerts fire with low latency. The DB write is what
gets batched, since that is the expensive part (a disk round trip).
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger("campus_sensor_alerts.ingestion")

from app import store
from app.config import (
    INGEST_QUEUE_MAXSIZE,
    MAX_WINDOW_SECONDS,
    WRITE_BATCH_SIZE,
    WRITE_FLUSH_INTERVAL_SECONDS,
)
from app.rules_engine import RuleEngine

QueueItem = tuple[str, str, float, datetime]  # sensor_id, metric, value, timestamp

queue: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=INGEST_QUEUE_MAXSIZE)
engine = RuleEngine()

_stats = {"accepted": 0, "persisted": 0, "alerts_created": 0, "rejected_full": 0}


def queue_has_room(n: int) -> bool:
    return queue.qsize() + n <= INGEST_QUEUE_MAXSIZE


def enqueue(items: list[QueueItem]) -> None:
    """Caller must have already checked queue_has_room(len(items))."""
    for item in items:
        queue.put_nowait(item)
    _stats["accepted"] += len(items)


def stats() -> dict:
    return {**_stats, "queueDepth": queue.qsize()}


async def load_rules_into_engine() -> None:
    for rule in await store.list_rules():
        engine.load_rule(rule)


async def rehydrate_windows() -> None:
    """After (re)start, in-memory rolling windows start empty. Replay the
    last MAX_WINDOW_SECONDS of readings from the database so window rules
    don't silently under-count right after a restart. See ARCHITECTURE.md.
    """
    history = await store.recent_readings_for_rehydration(MAX_WINDOW_SECONDS)
    for sensor_id, metric, value, ts in history:
        for rule in engine.matching_rules(sensor_id, metric):
            if rule["kind"] == "window":
                engine.seed_window(rule["id"], sensor_id, ts, value)


async def consume_forever() -> None:
    """The background task. Runs for the lifetime of the process.

    Everything inside the loop body is wrapped in try/except on purpose: an
    asyncio.Task that raises simply stops forever, silently, unless
    something awaits its result — there is no automatic retry and no
    automatic log message while the process is still running. Without this
    guard, one bad reading or one transient DB error permanently wedges
    ingestion: the queue keeps accepting items but nothing ever drains it
    again. See ARCHITECTURE.md "what went wrong while building this" for
    how this was actually found (the queue filling up with nothing ever
    being drained, and a second, related bug below).
    """
    batch: list[QueueItem] = []
    last_flush = time.monotonic()

    while True:
        try:
            timeout = max(0.0, WRITE_FLUSH_INTERVAL_SECONDS - (time.monotonic() - last_flush))
            try:
                item = await asyncio.wait_for(queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                item = None

            if item is not None:
                sensor_id, metric, value, ts = item
                fired = engine.evaluate(sensor_id, metric, value, ts)
                for rule, observed in fired:
                    alert_id = await store.create_alert_if_not_cooling_down(
                        rule["id"], sensor_id, metric, observed, ts
                    )
                    if alert_id is not None:
                        _stats["alerts_created"] += 1
                batch.append(item)

            now = time.monotonic()
            if batch and (len(batch) >= WRITE_BATCH_SIZE or now - last_flush >= WRITE_FLUSH_INTERVAL_SECONDS):
                await store.insert_readings_batch(batch)
                _stats["persisted"] += len(batch)
                batch.clear()
                last_flush = now
            elif not batch:
                # Nothing to flush and the wait above fully timed out (a
                # genuinely idle queue). Restart the flush-interval clock
                # here. Without this line, `last_flush` only ever advances
                # when a non-empty batch is written — on an idle queue it
                # never advances, `timeout` above computes closer and
                # closer to 0 forever, and the loop degenerates into a
                # tight, CPU-pinned busy-spin with asyncio.wait_for(...,
                # timeout=0), which in practice does not reliably give
                # queue.get() a fair chance to see an item that arrives a
                # moment later. This was the actual bug behind "readings
                # get accepted but never processed": the server looked
                # idle, the clock went stale within half a second, and
                # every reading after that point raced against a
                # busy-loop it almost always lost.
                last_flush = now

        except asyncio.CancelledError:
            raise  # let shutdown cancellation propagate, don't swallow it
        except Exception:
            logger.exception(
                "consumer loop error; dropping this iteration's batch (%d items) and continuing",
                len(batch),
            )
            _stats["consumer_errors"] = _stats.get("consumer_errors", 0) + 1
            batch.clear()
            last_flush = time.monotonic()
