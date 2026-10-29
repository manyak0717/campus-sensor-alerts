"""The hot path: accept readings fast, process them off the request path.

HTTP handler -> validate -> asyncio.Queue -> 202. A background consumer
evaluates rules in memory per reading and writes readings + alerts to SQLite
in one transaction per batch. On shutdown it drains the queue first.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
import time

from app import store
from app.config import (
    ALERT_COOLDOWN_SECONDS,
    INGEST_QUEUE_MAXSIZE,
    MAX_WINDOW_SECONDS,
    PRUNE_INTERVAL_SECONDS,
    READINGS_RETENTION_SECONDS,
    WRITE_BATCH_SIZE,
    WRITE_FLUSH_INTERVAL_SECONDS,
)
from app.rules_engine import RuleEngine

logger = logging.getLogger("campus_sensor_alerts.ingestion")

QueueItem = tuple[str, str, float, datetime]          # sensor_id, metric, value, timestamp
AlertItem = tuple[int, str, str, float, datetime]     # rule_id, sensor_id, metric, trigger_value, created_at

queue: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=INGEST_QUEUE_MAXSIZE)
engine = RuleEngine()
accepting: bool = True

_stats = {"accepted": 0, "persisted": 0, "alerts_created": 0, "rejected_full": 0}


def queue_has_room(n: int) -> bool:
    return accepting and (queue.qsize() + n <= INGEST_QUEUE_MAXSIZE)


def record_rejection(n: int = 1) -> None:
    _stats["rejected_full"] += n


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
    """Replay the last MAX_WINDOW_SECONDS of readings and active alert states
    from the database on startup.
    """
    history = await store.recent_readings_for_rehydration(MAX_WINDOW_SECONDS)
    for sensor_id, metric, value, ts in history:
        for rule in engine.matching_rules(sensor_id, metric):
            if rule["kind"] == "window":
                engine.seed_window(rule["id"], sensor_id, ts, value)

    recent_alerts = await store.active_or_recent_alerts(ALERT_COOLDOWN_SECONDS)
    for rule_id, sensor_id, status, created_at in recent_alerts:
        engine.seed_alert_state(rule_id, sensor_id, status, created_at)


async def rehydrate_rule_windows(rule: dict) -> None:
    """Populate a newly created, edited, or re-enabled window rule from recent DB readings."""
    if not rule.get("enabled") or rule.get("kind") != "window":
        return
    history = await store.recent_readings_for_rule(rule)
    for sensor_id, _metric, value, ts in history:
        engine.seed_window(rule["id"], sensor_id, ts, value)


def _process_one(
    item: QueueItem,
    batch: list[QueueItem],
    pending_alerts: list[AlertItem],
    wall_now: datetime,
) -> None:
    sensor_id, metric, value, ts = item
    # Clamp future-skewed sensor clocks to wall_now for alert creation/cooldown
    alert_ts = ts if ts <= wall_now else wall_now
    fired = engine.evaluate(sensor_id, metric, value, ts)
    for rule, observed in fired:
        if engine.should_create_alert(rule["id"], sensor_id, alert_ts):
            pending_alerts.append((rule["id"], sensor_id, metric, observed, alert_ts))
    batch.append(item)


async def _flush_with_retry(
    batch: list[QueueItem],
    pending_alerts: list[AlertItem],
    max_attempts: int = 3,
) -> None:
    if not batch and not pending_alerts:
        return
    for attempt in range(1, max_attempts + 1):
        try:
            persisted_n, alerts_n = await store.flush_ingestion_batch(batch, pending_alerts)
            _stats["persisted"] += persisted_n
            _stats["alerts_created"] += alerts_n
            batch.clear()
            pending_alerts.clear()
            return
        except Exception:
            if attempt == max_attempts:
                logger.exception(
                    "failed to flush batch after %d attempts; dropping %d readings and %d alerts",
                    max_attempts,
                    len(batch),
                    len(pending_alerts),
                )
                _stats["consumer_errors"] = _stats.get("consumer_errors", 0) + 1
                batch.clear()
                pending_alerts.clear()
            else:
                await asyncio.sleep(0.05 * attempt)


async def consume_forever() -> None:
    """The background consumer loop. Evaluates rules in memory and flushes
    readings + alerts in atomic micro-batches. On cancellation (shutdown),
    drains any remaining queued items and flushes the final batch before exiting.
    """
    batch: list[QueueItem] = []
    pending_alerts: list[AlertItem] = []
    last_flush = time.monotonic()
    last_prune = time.monotonic()

    try:
        while True:
            try:
                timeout = max(0.0, WRITE_FLUSH_INTERVAL_SECONDS - (time.monotonic() - last_flush))
                try:
                    first_item = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    first_item = None

                wall_now = datetime.now(timezone.utc)
                if first_item is not None:
                    _process_one(first_item, batch, pending_alerts, wall_now)
                    # Greedily drain additional ready items up to WRITE_BATCH_SIZE
                    # without scheduling extra asyncio.wait_for timers.
                    while len(batch) < WRITE_BATCH_SIZE:
                        try:
                            nxt = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        _process_one(nxt, batch, pending_alerts, wall_now)

                now = time.monotonic()
                if batch and (
                    len(batch) >= WRITE_BATCH_SIZE
                    or now - last_flush >= WRITE_FLUSH_INTERVAL_SECONDS
                ):
                    await _flush_with_retry(batch, pending_alerts)
                    last_flush = time.monotonic()
                elif not batch:
                    last_flush = now

                if now - last_prune >= PRUNE_INTERVAL_SECONDS:
                    engine.prune_stale_windows(wall_now)
                    await store.prune_old_readings(READINGS_RETENTION_SECONDS)
                    last_prune = now

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("unexpected error in consumer iteration")
                _stats["consumer_errors"] = _stats.get("consumer_errors", 0) + 1
                await asyncio.sleep(0.05)
    except asyncio.CancelledError:
        # Graceful shutdown drain: process all remaining items in `queue` and
        # flush the final batch before letting CancelledError finish.
        wall_now = datetime.now(timezone.utc)
        while not queue.empty():
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            _process_one(item, batch, pending_alerts, wall_now)
            if len(batch) >= WRITE_BATCH_SIZE:
                await _flush_with_retry(batch, pending_alerts)
        if batch or pending_alerts:
            await _flush_with_retry(batch, pending_alerts)
        raise
