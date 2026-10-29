"""App wiring. On startup: open the DB, load active rules into the
in-memory engine, replay recent readings so window rules aren't empty
right after a restart, then launch the background consumer task.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import ingestion
from app.db import close_db, init_db
from app.routers import alerts, readings, rules

logger = logging.getLogger("campus_sensor_alerts.main")
_consumer_task: asyncio.Task | None = None


def _log_task_death(task: asyncio.Task) -> None:
    """A background task that exits without being cancelled is always a
    bug worth seeing immediately, not discovering later from a stuck
    queue. See ingestion.consume_forever's own try/except for why this is
    a last resort rather than the main error handling."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("background consumer task died unexpectedly", exc_info=exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _consumer_task
    ingestion.accepting = True
    await init_db()
    await ingestion.load_rules_into_engine()
    await ingestion.rehydrate_windows()
    _consumer_task = asyncio.create_task(ingestion.consume_forever())
    _consumer_task.add_done_callback(_log_task_death)

    yield

    ingestion.accepting = False
    _consumer_task.cancel()  # consumer drains the queue on cancel
    try:
        await _consumer_task
    except asyncio.CancelledError:
        pass
    await close_db()


app = FastAPI(title="Campus Sensor Monitoring and Alerts", lifespan=lifespan)

app.include_router(readings.router)
app.include_router(rules.router)
app.include_router(alerts.router)


@app.get("/health")
async def health():
    return {"status": "ok", **ingestion.stats()}
