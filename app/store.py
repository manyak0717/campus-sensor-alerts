"""Typed database access: every query lives here, SQL stays out of the route handlers."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from app.db import get_conn, iso, parse_iso, write_lock


class InvalidAlertTransitionError(ValueError):
    """e.g. acknowledging an already resolved alert."""


# ----------------------------------------------------------------- readings

async def flush_ingestion_batch(
    reading_rows: Iterable[tuple[str, str, float, datetime]],
    alert_rows: Iterable[tuple[int, str, str, float, datetime]],
) -> tuple[int, int]:
    """Persist readings and the alerts they triggered in one transaction."""
    readings_list = [(sid, typ, val, iso(ts)) for sid, typ, val, ts in reading_rows]
    alerts_list = [(rid, sid, metric, val, iso(ts)) for rid, sid, metric, val, ts in alert_rows]
    if not readings_list and not alerts_list:
        return 0, 0

    conn = get_conn()
    async with write_lock:
        try:
            if readings_list:
                await conn.executemany(
                    "INSERT INTO readings (sensor_id, type, value, timestamp) VALUES (?, ?, ?, ?)",
                    readings_list,
                )
            if alerts_list:
                await conn.executemany(
                    "INSERT INTO alerts (rule_id, sensor_id, metric, trigger_value, status, created_at) "
                    "VALUES (?, ?, ?, ?, 'open', ?)",
                    alerts_list,
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    return len(readings_list), len(alerts_list)


async def recent_readings_for_rehydration(lookback_seconds: int):
    conn = get_conn()
    cutoff = iso(datetime.now(timezone.utc) - timedelta(seconds=lookback_seconds))
    cursor = await conn.execute(
        "SELECT sensor_id, type, value, timestamp FROM readings "
        "WHERE timestamp >= ? ORDER BY timestamp ASC, id ASC",
        (cutoff,),
    )
    rows = await cursor.fetchall()
    await cursor.close()
    return [(r[0], r[1], r[2], parse_iso(r[3])) for r in rows]


async def recent_readings_for_rule(rule: dict):
    if rule.get("kind") != "window" or not rule.get("window_seconds"):
        return []
    conn = get_conn()
    cutoff = iso(datetime.now(timezone.utc) - timedelta(seconds=rule["window_seconds"]))
    params: list = [rule["metric"], cutoff]
    sensor_clause = ""
    if rule.get("sensor_id") is not None:
        sensor_clause = " AND sensor_id = ?"
        params.append(rule["sensor_id"])
    cursor = await conn.execute(
        "SELECT sensor_id, type, value, timestamp FROM readings "
        f"WHERE type = ? AND timestamp >= ?{sensor_clause} "
        "ORDER BY timestamp ASC, id ASC",
        params,
    )
    rows = await cursor.fetchall()
    await cursor.close()
    return [(r[0], r[1], r[2], parse_iso(r[3])) for r in rows]


async def prune_old_readings(retention_seconds: int) -> int:
    conn = get_conn()
    cutoff = iso(datetime.now(timezone.utc) - timedelta(seconds=retention_seconds))
    async with write_lock:
        cursor = await conn.execute("DELETE FROM readings WHERE timestamp < ?", (cutoff,))
        deleted = cursor.rowcount
        await cursor.close()
        await conn.commit()
    return deleted


# --------------------------------------------------------------------- rules

async def insert_rule(rule: dict) -> int:
    conn = get_conn()
    async with write_lock:
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
        rule_id = cursor.lastrowid
        await cursor.close()
        await conn.commit()
    return rule_id


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
    async with write_lock:
        cursor = await conn.execute(
            f"UPDATE rules SET {set_clause} WHERE id = ?",
            (*fields.values(), rule_id),
        )
        await cursor.close()
        await conn.commit()
    return await get_rule(rule_id)


async def delete_rule(rule_id: int) -> bool:
    conn = get_conn()
    async with write_lock:
        cursor = await conn.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
        deleted = cursor.rowcount > 0
        await cursor.close()
        await conn.commit()
    return deleted


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

async def active_or_recent_alerts(lookback_seconds: int) -> list[tuple[int, str, str, datetime]]:
    """Open/acknowledged or recent alerts, to seed cooldown state on startup."""
    conn = get_conn()
    cutoff = iso(datetime.now(timezone.utc) - timedelta(seconds=lookback_seconds))
    cursor = await conn.execute(
        "SELECT rule_id, sensor_id, status, created_at FROM alerts "
        "WHERE status IN ('open', 'acknowledged') OR created_at >= ? "
        "ORDER BY created_at ASC, id ASC",
        (cutoff,),
    )
    rows = await cursor.fetchall()
    await cursor.close()
    return [(r[0], r[1], r[2], parse_iso(r[3])) for r in rows]


def _row_to_alert(r) -> dict:
    return {
        "id": r[0], "rule_id": r[1], "rule_name": r[2], "sensor_id": r[3],
        "metric": r[4], "trigger_value": r[5], "status": r[6],
        "created_at": r[7], "acknowledged_at": r[8], "resolved_at": r[9],
    }


async def get_alert(alert_id: int) -> Optional[dict]:
    conn = get_conn()
    cursor = await conn.execute(
        "SELECT a.id, a.rule_id, r.name, a.sensor_id, a.metric, a.trigger_value, "
        "a.status, a.created_at, a.acknowledged_at, a.resolved_at "
        "FROM alerts a JOIN rules r ON r.id = a.rule_id WHERE a.id = ?",
        (alert_id,),
    )
    row = await cursor.fetchone()
    await cursor.close()
    return _row_to_alert(row) if row else None


async def list_alerts(
    status: Optional[str] = None,
    sensor_id: Optional[str] = None,
    rule_id: Optional[int] = None,
    limit: int = 500,
    offset: int = 0,
) -> list[dict]:
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
    query += " ORDER BY a.created_at DESC, a.id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    cursor = await conn.execute(query, params)
    rows = await cursor.fetchall()
    await cursor.close()
    return [_row_to_alert(r) for r in rows]


async def set_alert_status(alert_id: int, status: str, timestamp_field: str) -> Optional[dict]:
    if timestamp_field not in ("acknowledged_at", "resolved_at"):
        raise ValueError(f"invalid timestamp_field {timestamp_field!r}")

    existing = await get_alert(alert_id)
    if not existing:
        return None

    current_status = existing["status"]
    if status == "acknowledged" and current_status == "resolved":
        raise InvalidAlertTransitionError("Cannot acknowledge an alert that is already resolved")
    if current_status == status:
        return existing

    conn = get_conn()
    async with write_lock:
        cursor = await conn.execute(
            f"UPDATE alerts SET status = ?, {timestamp_field} = ? WHERE id = ?",
            (status, iso(datetime.now(timezone.utc)), alert_id),
        )
        await cursor.close()
        await conn.commit()
    return await get_alert(alert_id)
