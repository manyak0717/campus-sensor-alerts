"""Typed database access. Every query the API or the background consumer
needs lives here, so SQL stays out of the route handlers.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from app.db import get_conn, iso, parse_iso
from app.config import ALERT_COOLDOWN_SECONDS


# ----------------------------------------------------------------- readings

async def insert_readings_batch(rows: Iterable[tuple[str, str, float, datetime]]) -> None:
    """Bulk insert. One round trip for the whole batch instead of one per
    reading — this is most of why batching matters for throughput."""
    conn = get_conn()
    await conn.executemany(
        "INSERT INTO readings (sensor_id, type, value, timestamp) VALUES (?, ?, ?, ?)",
        [(sid, typ, val, iso(ts)) for sid, typ, val, ts in rows],
    )
    await conn.commit()


async def recent_readings_for_rehydration(lookback_seconds: int):
    """Everything newer than `lookback_seconds` ago, oldest first, so the
    rule engine can replay them in order and rebuild its rolling windows
    after a restart."""
    conn = get_conn()
    cutoff = iso(datetime.now(timezone.utc) - timedelta(seconds=lookback_seconds))
    cursor = await conn.execute(
        "SELECT sensor_id, type, value, timestamp FROM readings "
        "WHERE timestamp >= ? ORDER BY timestamp ASC",
        (cutoff,),
    )
    rows = await cursor.fetchall()
    await cursor.close()
    return [(r[0], r[1], r[2], parse_iso(r[3])) for r in rows]


# --------------------------------------------------------------------- rules

async def insert_rule(rule: dict) -> int:
    conn = get_conn()
    cursor = await conn.execute(
        "INSERT INTO rules (name, sensor_id, metric, kind, operator, threshold, "
        "window_seconds, aggregation, enabled, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            rule["name"], rule.get("sensor_id"), rule["metric"], rule["kind"],
            rule["operator"], rule["threshold"], rule.get("window_seconds"),
            rule.get("aggregation"), int(rule.get("enabled", True)),
            iso(datetime.now(timezone.utc)),
        ),
    )
    await conn.commit()
    return cursor.lastrowid


async def list_rules() -> list[dict]:
    conn = get_conn()
    cursor = await conn.execute(
        "SELECT id, name, sensor_id, metric, kind, operator, threshold, "
        "window_seconds, aggregation, enabled, created_at FROM rules ORDER BY id"
    )
    rows = await cursor.fetchall()
    await cursor.close()
    return [_row_to_rule(r) for r in rows]


async def get_rule(rule_id: int) -> Optional[dict]:
    conn = get_conn()
    cursor = await conn.execute(
        "SELECT id, name, sensor_id, metric, kind, operator, threshold, "
        "window_seconds, aggregation, enabled, created_at FROM rules WHERE id = ?",
        (rule_id,),
    )
    row = await cursor.fetchone()
    await cursor.close()
    return _row_to_rule(row) if row else None


async def update_rule(rule_id: int, fields: dict) -> Optional[dict]:
    if not fields:
        return await get_rule(rule_id)
    conn = get_conn()
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    await conn.execute(
        f"UPDATE rules SET {set_clause} WHERE id = ?",
        (*fields.values(), rule_id),
    )
    await conn.commit()
    return await get_rule(rule_id)


async def set_rule_enabled(rule_id: int, enabled: bool) -> Optional[dict]:
    return await update_rule(rule_id, {"enabled": int(enabled)})


def _row_to_rule(row) -> dict:
    return {
        "id": row[0], "name": row[1], "sensor_id": row[2], "metric": row[3],
        "kind": row[4], "operator": row[5], "threshold": row[6],
        "window_seconds": row[7], "aggregation": row[8],
        "enabled": bool(row[9]), "created_at": row[10],
    }


# -------------------------------------------------------------------- alerts

async def create_alert_if_not_cooling_down(rule_id: int, sensor_id: str, metric: str,
                                             trigger_value: float, now: datetime) -> Optional[int]:
    """Create a new open alert for (rule, sensor) unless one already fired
    within ALERT_COOLDOWN_SECONDS, or an earlier one is still open. Returns
    the new alert id, or None if suppressed by cooldown.

    This is a simple guard against alert storms: a single faulty sensor
    stuck above threshold would otherwise generate one alert per reading.
    """
    conn = get_conn()
    cutoff = iso(now - timedelta(seconds=ALERT_COOLDOWN_SECONDS))
    cursor = await conn.execute(
        "SELECT id FROM alerts WHERE rule_id = ? AND sensor_id = ? "
        "AND (status = 'open' OR created_at >= ?) LIMIT 1",
        (rule_id, sensor_id, cutoff),
    )
    existing = await cursor.fetchone()
    await cursor.close()
    if existing:
        return None

    cursor = await conn.execute(
        "INSERT INTO alerts (rule_id, sensor_id, metric, trigger_value, status, created_at) "
        "VALUES (?, ?, ?, ?, 'open', ?)",
        (rule_id, sensor_id, metric, trigger_value, iso(now)),
    )
    await conn.commit()
    return cursor.lastrowid


async def list_alerts(status: Optional[str] = None, sensor_id: Optional[str] = None,
                       rule_id: Optional[int] = None) -> list[dict]:
    conn = get_conn()
    query = (
        "SELECT a.id, a.rule_id, r.name, a.sensor_id, a.metric, a.trigger_value, "
        "a.status, a.created_at, a.acknowledged_at, a.resolved_at "
        "FROM alerts a JOIN rules r ON r.id = a.rule_id WHERE 1=1"
    )
    params: list = []
    if status:
        query += " AND a.status = ?"
        params.append(status)
    if sensor_id:
        query += " AND a.sensor_id = ?"
        params.append(sensor_id)
    if rule_id:
        query += " AND a.rule_id = ?"
        params.append(rule_id)
    query += " ORDER BY a.created_at DESC"
    cursor = await conn.execute(query, params)
    rows = await cursor.fetchall()
    await cursor.close()
    return [
        {
            "id": r[0], "rule_id": r[1], "rule_name": r[2], "sensor_id": r[3],
            "metric": r[4], "trigger_value": r[5], "status": r[6],
            "created_at": r[7], "acknowledged_at": r[8], "resolved_at": r[9],
        }
        for r in rows
    ]


async def set_alert_status(alert_id: int, status: str, timestamp_field: str) -> Optional[dict]:
    conn = get_conn()
    await conn.execute(
        f"UPDATE alerts SET status = ?, {timestamp_field} = ? WHERE id = ?",
        (status, iso(datetime.now(timezone.utc)), alert_id),
    )
    await conn.commit()
    alerts = await list_alerts()
    for a in alerts:
        if a["id"] == alert_id:
            return a
    return None
