"""Organizer-facing alert management: view, acknowledge, resolve."""
from fastapi import APIRouter, HTTPException, Query

from app import store
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
    status: str | None = Query(default=None, pattern="^(open|acknowledged|resolved)$"),
    sensorId: str | None = None,
    ruleId: int | None = None,
):
    alerts = await store.list_alerts(status=status, sensor_id=sensorId, rule_id=ruleId)
    return [_out(a) for a in alerts]


@router.post("/{alert_id}/acknowledge", response_model=AlertOut)
async def acknowledge_alert(alert_id: int):
    alert = await store.set_alert_status(alert_id, "acknowledged", "acknowledged_at")
    if not alert:
        raise HTTPException(404, "Alert not found")
    return _out(alert)


@router.post("/{alert_id}/resolve", response_model=AlertOut)
async def resolve_alert(alert_id: int):
    alert = await store.set_alert_status(alert_id, "resolved", "resolved_at")
    if not alert:
        raise HTTPException(404, "Alert not found")
    return _out(alert)
