"""Organizer-facing rule management: create, edit, enable, disable."""
from fastapi import APIRouter, HTTPException

from app import ingestion, store
from app.models import RuleCreate, RuleOut, RuleUpdate

router = APIRouter(prefix="/rules", tags=["rules"])


def _out(rule: dict) -> RuleOut:
    return RuleOut(
        id=rule["id"], name=rule["name"], sensorId=rule["sensor_id"],
        metric=rule["metric"], kind=rule["kind"], operator=rule["operator"],
        threshold=rule["threshold"], windowSeconds=rule["window_seconds"],
        aggregation=rule["aggregation"], enabled=rule["enabled"],
        createdAt=rule["created_at"],
    )


@router.post("", response_model=RuleOut, status_code=201)
async def create_rule(body: RuleCreate):
    row = {
        "name": body.name, "sensor_id": body.sensorId, "metric": body.metric,
        "kind": body.kind.value, "operator": body.operator.value,
        "threshold": body.threshold, "window_seconds": body.windowSeconds,
        "aggregation": body.aggregation.value if body.aggregation else None,
        "enabled": body.enabled,
    }
    rule_id = await store.insert_rule(row)
    rule = await store.get_rule(rule_id)
    ingestion.engine.load_rule(rule)
    return _out(rule)


@router.get("", response_model=list[RuleOut])
async def get_rules():
    return [_out(r) for r in await store.list_rules()]


@router.get("/{rule_id}", response_model=RuleOut)
async def get_rule(rule_id: int):
    rule = await store.get_rule(rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found")
    return _out(rule)


@router.put("/{rule_id}", response_model=RuleOut)
async def edit_rule(rule_id: int, body: RuleUpdate):
    existing = await store.get_rule(rule_id)
    if not existing:
        raise HTTPException(404, "Rule not found")

    fields = {}
    if body.name is not None:
        fields["name"] = body.name
    if body.sensorId is not None:
        fields["sensor_id"] = body.sensorId
    if body.metric is not None:
        fields["metric"] = body.metric
    if body.operator is not None:
        fields["operator"] = body.operator.value
    if body.threshold is not None:
        fields["threshold"] = body.threshold
    if body.windowSeconds is not None:
        fields["window_seconds"] = body.windowSeconds
    if body.aggregation is not None:
        fields["aggregation"] = body.aggregation.value

    rule = await store.update_rule(rule_id, fields)
    # Window shape may have changed (windowSeconds/aggregation) — drop the
    # cached in-memory windows for this rule so stale state can't linger.
    # They are rebuilt lazily from new readings (no startup-style replay
    # here; documented as a known simplification in ARCHITECTURE.md).
    ingestion.engine.load_rule(rule)
    ingestion.engine.reset_windows_for_rule(rule_id)
    return _out(rule)


@router.post("/{rule_id}/enable", response_model=RuleOut)
async def enable_rule(rule_id: int):
    rule = await store.set_rule_enabled(rule_id, True)
    if not rule:
        raise HTTPException(404, "Rule not found")
    ingestion.engine.load_rule(rule)
    return _out(rule)


@router.post("/{rule_id}/disable", response_model=RuleOut)
async def disable_rule(rule_id: int):
    rule = await store.set_rule_enabled(rule_id, False)
    if not rule:
        raise HTTPException(404, "Rule not found")
    ingestion.engine.load_rule(rule)
    return _out(rule)
