"""Organizer-facing alert management: view, acknowledge, resolve."""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from app import ingestion, store
from app.models import AlertOut

router = APIRouter(prefix="/alerts", tags=["alerts"])


def _out(a: dict) -> AlertOut:
    return AlertOut(
        id=a["id"], ruleId=a["rule_id"], ruleName=a["rule_name"],
        sensorId=a["sensor_id"], metric=a["metric"], triggerValue=a["trigger_value"],
        status=a["status"], createdAt=a["created_at"],
        acknowledgedAt=a["acknowledged_at"], resolvedAt=a["resolved_at"],
    )


@router.get("", response_model=list[AlertOut])
async def get_alerts(
    status: Optional[str] = Query(default=None, pattern="^(open|acknowledged|resolved)$"),
    sensorId: Optional[str] = None,
    ruleId: Optional[int] = None,
    limit: int = Query(default=500, ge=1, le=10_000),
    offset: int = Query(default=0, ge=0),
):
    alerts = await store.list_alerts(
        status=status, sensor_id=sensorId, rule_id=ruleId, limit=limit, offset=offset
    )
    return [_out(a) for a in alerts]


@router.post("/{alert_id}/acknowledge", response_model=AlertOut)
async def acknowledge_alert(alert_id: int):
    try:
        alert = await store.set_alert_status(alert_id, "acknowledged", "acknowledged_at")
    except store.InvalidAlertTransitionError as e:
        raise HTTPException(409, str(e))
    if not alert:
        raise HTTPException(404, "Alert not found")
    ingestion.engine.update_alert_status(alert["rule_id"], alert["sensor_id"], "acknowledged")
    return _out(alert)


@router.post("/{alert_id}/resolve", response_model=AlertOut)
async def resolve_alert(alert_id: int):
    try:
        alert = await store.set_alert_status(alert_id, "resolved", "resolved_at")
    except store.InvalidAlertTransitionError as e:
        raise HTTPException(409, str(e))
    if not alert:
        raise HTTPException(404, "Alert not found")
    ingestion.engine.update_alert_status(alert["rule_id"], alert["sensor_id"], "resolved")
    return _out(alert)
