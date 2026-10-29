"""SQLite persistence layer, using aiosqlite so DB calls don't block the
event loop. SQLite (not Postgres/MySQL) is chosen deliberately: this is a
single-process service for a campus-scale deployment, and SQLite with WAL
mode comfortably handles our write pattern (batched inserts from one
writer, occasional reads from the API). See ARCHITECTURE.md for the
trade-off if this needed to scale to multiple processes.
"""
from __future__ import annotations

import aiosqlite
from datetime import datetime, timezone

from app.config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT NOT NULL,
    type TEXT NOT NULL,
    value REAL NOT NULL,
    timestamp TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_sensor_type_ts
    ON readings (sensor_id, type, timestamp);

CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    sensor_id TEXT,
    metric TEXT NOT NULL,
    kind TEXT NOT NULL,
    operator TEXT NOT NULL,
    threshold REAL NOT NULL,
    window_seconds INTEGER,
    aggregation TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id INTEGER NOT NULL,
    sensor_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    trigger_value REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    created_at TEXT NOT NULL,
    acknowledged_at TEXT,
    resolved_at TEXT,
    FOREIGN KEY (rule_id) REFERENCES rules(id)
);
CREATE INDEX IF NOT EXISTS idx_alerts_rule_sensor_created
    ON alerts (rule_id, sensor_id, created_at);
"""

_conn: aiosqlite.Connection | None = None


async def init_db() -> aiosqlite.Connection:
    global _conn
    _conn = await aiosqlite.connect(DB_PATH)
    # WAL mode lets readers (GET /alerts, GET /rules) run concurrently with
    # the background writer without blocking each other on file locks.
    await _conn.execute("PRAGMA journal_mode=WAL;")
    await _conn.execute("PRAGMA synchronous=NORMAL;")
    await _conn.executescript(SCHEMA)
    await _conn.commit()
    return _conn


async def close_db() -> None:
    if _conn is not None:
        await _conn.close()


def get_conn() -> aiosqlite.Connection:
    assert _conn is not None, "DB not initialized — call init_db() first"
    return _conn


def iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)
