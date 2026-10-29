"""Pydantic request/response models. Kept separate from the DB layer so the
wire format (what clients send/receive) can evolve independently of how
rows are stored.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import AliasChoices, BaseModel, Field, field_validator

from app.config import MAX_WINDOW_SECONDS


# ---------------------------------------------------------------- readings

class Reading(BaseModel):
    sensorId: str = Field(min_length=1)
    type: str = Field(min_length=1)  # e.g. "temperature", "air_quality", "occupancy"
    value: float = Field(allow_inf_nan=False)
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def _ensure_tz_aware(cls, v: datetime) -> datetime:
        # Readings without a timezone are assumed UTC, matching the
        # simulator and the example reading in the brief ("...Z").
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


class ReadingBatch(BaseModel):
    readings: list[Reading] = Field(min_length=1, max_length=20_000)


class IngestAck(BaseModel):
    accepted: int
    queueDepth: int


# -------------------------------------------------------------------- rules

class RuleKind(str, Enum):
    single = "single"
    window = "window"


class Operator(str, Enum):
    gt = ">"
    gte = ">="
    lt = "<"
    lte = "<="


class Aggregation(str, Enum):
    avg = "avg"
    min = "min"
    max = "max"


class RuleCreate(BaseModel):
    name: str = Field(min_length=1)
    sensorId: Optional[str] = None  # None = applies to all sensors of `metric`
    metric: str = Field(min_length=1, validation_alias=AliasChoices("metric", "sensorType"))  # matches Reading.type
    kind: RuleKind
    operator: Operator
    threshold: float = Field(allow_inf_nan=False)
    windowSeconds: Optional[int] = Field(default=None, gt=0, le=MAX_WINDOW_SECONDS)  # required if kind == window
    aggregation: Optional[Aggregation] = None  # required if kind == window
    enabled: bool = True

    @field_validator("windowSeconds")
    @classmethod
    def _window_required_for_window_rules(cls, v, info):
        kind = info.data.get("kind")
        if kind == RuleKind.window and not v:
            raise ValueError("windowSeconds is required for a window rule")
        return v

    @field_validator("aggregation")
    @classmethod
    def _aggregation_required_for_window_rules(cls, v, info):
        kind = info.data.get("kind")
        if kind == RuleKind.window and not v:
            raise ValueError("aggregation is required for a window rule")
        return v


class RuleUpdate(BaseModel):
    name: Optional[str] = None
    sensorId: Optional[str] = None
    metric: Optional[str] = Field(default=None, validation_alias=AliasChoices("metric", "sensorType"))
    operator: Optional[Operator] = None
    threshold: Optional[float] = Field(default=None, allow_inf_nan=False)
    windowSeconds: Optional[int] = Field(default=None, gt=0, le=MAX_WINDOW_SECONDS)
    aggregation: Optional[Aggregation] = None


class RuleOut(BaseModel):
    id: int
    name: str
    sensorId: Optional[str]
    metric: str
    kind: RuleKind
    operator: Operator
    threshold: float
    windowSeconds: Optional[int]
    aggregation: Optional[Aggregation]
    enabled: bool
    createdAt: datetime


# ------------------------------------------------------------------- alerts

class AlertStatus(str, Enum):
    open = "open"
    acknowledged = "acknowledged"
    resolved = "resolved"


class AlertOut(BaseModel):
    id: int
    ruleId: int
    ruleName: str
    sensorId: str
    metric: str
    triggerValue: float
    status: AlertStatus
    createdAt: datetime
    acknowledgedAt: Optional[datetime]
    resolvedAt: Optional[datetime]
