"""In-memory rolling-window state, alert cooldown state, and rule evaluation."""
from __future__ import annotations

import bisect
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import math
from typing import Optional

from app.config import ALERT_COOLDOWN_SECONDS


@dataclass
class SlidingWindow:
    """O(1)-amortized rolling avg/min/max/count over the last `window_seconds`."""

    window_seconds: int
    _history: deque = field(default_factory=deque)     # (seq, ts, value), oldest first
    _sum: float = 0.0
    _min_deque: deque = field(default_factory=deque)   # (seq, ts, value), increasing
    _max_deque: deque = field(default_factory=deque)   # (seq, ts, value), decreasing
    _seq: int = 0
    _max_ts: Optional[datetime] = None

    def add(self, ts: datetime, value: float) -> None:
        if self.window_seconds <= 0 or not math.isfinite(value):
            return

        if self._max_ts is None or ts > self._max_ts:
            self._max_ts = ts

        cutoff = self._max_ts - timedelta(seconds=self.window_seconds)
        # If this reading is already older than the active window, expire stale
        # entries and ignore the expired late arrival.
        if ts < cutoff:
            self._expire(self._max_ts)
            return

        self._seq += 1
        seq = self._seq

        # Fast path (99.99% of readings): non-decreasing timestamp order
        if not self._history or ts >= self._history[-1][1]:
            self._history.append((seq, ts, value))
            self._sum += value

            while self._min_deque and self._min_deque[-1][2] >= value:
                self._min_deque.pop()
            self._min_deque.append((seq, ts, value))

            while self._max_deque and self._max_deque[-1][2] <= value:
                self._max_deque.pop()
            self._max_deque.append((seq, ts, value))

            self._expire(self._max_ts)
        else:
            # Out-of-order arrival within the active window: insert in timestamp
            # order and rebuild deques and exact sum to prevent corruption.
            items = list(self._history)
            keys = [item[1] for item in items]
            idx = bisect.bisect_right(keys, ts)
            items.insert(idx, (seq, ts, value))
            self._rebuild(items, cutoff)

    def _rebuild(self, items: list[tuple[int, datetime, float]], cutoff: datetime) -> None:
        self._history.clear()
        self._min_deque.clear()
        self._max_deque.clear()
        self._sum = 0.0
        for seq, ts, val in items:
            if ts < cutoff:
                continue
            self._history.append((seq, ts, val))
            self._sum += val
            while self._min_deque and self._min_deque[-1][2] >= val:
                self._min_deque.pop()
            self._min_deque.append((seq, ts, val))
            while self._max_deque and self._max_deque[-1][2] <= val:
                self._max_deque.pop()
            self._max_deque.append((seq, ts, val))

    def _expire(self, reference_now: datetime) -> None:
        cutoff = reference_now - timedelta(seconds=self.window_seconds)
        while self._history and self._history[0][1] < cutoff:
            old_seq, _old_ts, old_val = self._history.popleft()
            self._sum -= old_val
            if self._min_deque and self._min_deque[0][0] == old_seq:
                self._min_deque.popleft()
            if self._max_deque and self._max_deque[0][0] == old_seq:
                self._max_deque.popleft()
        if not self._history:
            self._sum = 0.0
            self._min_deque.clear()
            self._max_deque.clear()

    @property
    def count(self) -> int:
        return len(self._history)

    def avg(self) -> Optional[float]:
        return self._sum / len(self._history) if self._history else None

    def min(self) -> Optional[float]:
        return self._min_deque[0][2] if self._min_deque else None

    def max(self) -> Optional[float]:
        return self._max_deque[0][2] if self._max_deque else None

    def aggregate(self, agg: str) -> Optional[float]:
        if agg == "avg":
            return self.avg()
        if agg == "min":
            return self.min()
        if agg == "max":
            return self.max()
        raise ValueError(f"unknown aggregation {agg!r}")


@dataclass
class AlertCooldownState:
    status: Optional[str] = None          # 'open', 'acknowledged', 'resolved', or None
    last_created_at: Optional[datetime] = None


class RuleEngine:
    """Holds every active rule, rolling-window state, and in-memory cooldown state."""

    def __init__(self) -> None:
        self.rules: dict[int, dict] = {}
        self._windows: dict[tuple[int, str], SlidingWindow] = {}
        self._cooldowns: dict[tuple[int, str], AlertCooldownState] = {}

    def load_rule(self, rule: dict) -> None:
        self.rules[rule["id"]] = rule

    def drop_rule(self, rule_id: int) -> None:
        self.rules.pop(rule_id, None)
        self.reset_windows_for_rule(rule_id)
        for key in [k for k in self._cooldowns if k[0] == rule_id]:
            del self._cooldowns[key]

    def reset_windows_for_rule(self, rule_id: int) -> None:
        for key in [k for k in self._windows if k[0] == rule_id]:
            del self._windows[key]

    def _window_for(self, rule: dict, sensor_id: str) -> SlidingWindow:
        key = (rule["id"], sensor_id)
        win = self._windows.get(key)
        if win is None:
            win = SlidingWindow(window_seconds=rule["window_seconds"])
            self._windows[key] = win
        return win

    def seed_window(self, rule_id: int, sensor_id: str, ts: datetime, value: float) -> None:
        rule = self.rules.get(rule_id)
        if rule is None or not rule.get("enabled") or rule.get("kind") != "window":
            return
        self._window_for(rule, sensor_id).add(ts, value)

    def seed_alert_state(self, rule_id: int, sensor_id: str, status: str, created_at: datetime) -> None:
        key = (rule_id, sensor_id)
        existing = self._cooldowns.get(key)
        if existing is None:
            self._cooldowns[key] = AlertCooldownState(status=status, last_created_at=created_at)
        else:
            if existing.last_created_at is None or created_at >= existing.last_created_at:
                existing.last_created_at = created_at
            # Keep active status if any alert for this pair is open/acknowledged
            if status in ("open", "acknowledged"):
                existing.status = status
            elif existing.status not in ("open", "acknowledged"):
                existing.status = status

    def update_alert_status(self, rule_id: int, sensor_id: str, status: str) -> None:
        key = (rule_id, sensor_id)
        state = self._cooldowns.get(key)
        if state is None:
            self._cooldowns[key] = AlertCooldownState(status=status)
        else:
            state.status = status

    def should_create_alert(self, rule_id: int, sensor_id: str, now: datetime) -> bool:
        """In-memory O(1) check replacing per-reading SQLite SELECT queries."""
        key = (rule_id, sensor_id)
        state = self._cooldowns.get(key)
        if state is not None:
            if state.status == "acknowledged":
                return False  # stays quiet until resolved
            if state.last_created_at is not None:
                cutoff = now - timedelta(seconds=ALERT_COOLDOWN_SECONDS)
                if state.last_created_at >= cutoff:
                    return False

        self._cooldowns[key] = AlertCooldownState(status="open", last_created_at=now)
        return True

    def prune_stale_windows(self, now: datetime) -> int:
        """Evict expired windows so one-off sensor ids can't leak memory."""
        removed = 0
        for key, win in list(self._windows.items()):
            ref = max(now, win._max_ts) if win._max_ts else now
            win._expire(ref)
            if win.count == 0:
                del self._windows[key]
                removed += 1

        return removed

    def matching_rules(self, sensor_id: str, metric: str):
        for rule in self.rules.values():
            if not rule["enabled"]:
                continue
            if rule["metric"] != metric:
                continue
            if rule["sensor_id"] is not None and rule["sensor_id"] != sensor_id:
                continue
            yield rule

    def evaluate(self, sensor_id: str, metric: str, value: float, ts: datetime):
        if not math.isfinite(value):
            return []
        fired = []
        for rule in self.matching_rules(sensor_id, metric):
            if rule["kind"] == "single":
                observed = value
            else:
                win = self._window_for(rule, sensor_id)
                win.add(ts, value)
                observed = win.aggregate(rule["aggregation"])
                if observed is None:
                    continue
            if _compare(observed, rule["operator"], rule["threshold"]):
                fired.append((rule, observed))
        return fired


def _compare(value: float, op: str, threshold: float) -> bool:
    if op == ">":
        return value > threshold
    if op == ">=":
        return value >= threshold
    if op == "<":
        return value < threshold
    if op == "<=":
        return value <= threshold
    raise ValueError(f"unknown operator {op!r}")
