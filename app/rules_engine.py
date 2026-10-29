"""In-memory rolling-window state + rule evaluation.

Design: each ACTIVE window-rule gets its own SlidingWindow instance per
sensor it has seen readings from, keyed by (rule_id, sensor_id). That keeps
each window's running aggregate exact for that rule's own windowSeconds.

A rule with sensorId=None ("applies to every sensor of this metric") is
evaluated independently per sensor: each sensor gets its own window rather
than every sensor sharing one blended average. That is almost always the
useful interpretation — you want to know THIS room got too hot, not that
the campus-wide average got too hot.

Known simplification (documented, not hidden): editing an existing rule's
windowSeconds while it has live windows does not retroactively resize
them; the simplest safe behavior here is that rule edits replace the rule
(new id's worth of semantics isn't required by the brief, but see
store.update_rule / ARCHITECTURE.md for how this is handled).
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class SlidingWindow:
    """O(1)-amortized rolling avg/min/max over the last `window_seconds`.

    Classic sliding-window-minimum/maximum technique: a monotonic deque
    gives O(1) amortized min/max without rescanning the window on every
    reading. A running sum gives O(1) average the same way.
    """

    window_seconds: int
    _history: deque = field(default_factory=deque)     # (ts, value), oldest first
    _sum: float = 0.0
    _min_deque: deque = field(default_factory=deque)   # (ts, value), increasing
    _max_deque: deque = field(default_factory=deque)   # (ts, value), decreasing

    def add(self, ts: datetime, value: float) -> None:
        self._history.append((ts, value))
        self._sum += value

        while self._min_deque and self._min_deque[-1][1] >= value:
            self._min_deque.pop()
        self._min_deque.append((ts, value))

        while self._max_deque and self._max_deque[-1][1] <= value:
            self._max_deque.pop()
        self._max_deque.append((ts, value))

        self._expire(ts)

    def _expire(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self.window_seconds)
        while self._history and self._history[0][0] < cutoff:
            old_ts, old_val = self._history.popleft()
            self._sum -= old_val
            if self._min_deque and self._min_deque[0][0] == old_ts and self._min_deque[0][1] == old_val:
                self._min_deque.popleft()
            if self._max_deque and self._max_deque[0][0] == old_ts and self._max_deque[0][1] == old_val:
                self._max_deque.popleft()

    @property
    def count(self) -> int:
        return len(self._history)

    def avg(self):
        return self._sum / len(self._history) if self._history else None

    def min(self):
        return self._min_deque[0][1] if self._min_deque else None

    def max(self):
        return self._max_deque[0][1] if self._max_deque else None

    def aggregate(self, agg: str):
        if agg == "avg":
            return self.avg()
        if agg == "min":
            return self.min()
        if agg == "max":
            return self.max()
        raise ValueError(f"unknown aggregation {agg!r}")


class RuleEngine:
    """Holds every active rule plus the rolling-window state window-rules need.

    This is the single source of truth for "what should I check right now"
    — the database is the durable record, this is the fast in-memory copy
    the hot path (evaluate()) actually runs against.
    """

    def __init__(self) -> None:
        self.rules: dict[int, dict] = {}                           # rule_id -> rule row
        self._windows: dict[tuple[int, str], SlidingWindow] = {}   # (rule_id, sensor_id)

    def load_rule(self, rule: dict) -> None:
        self.rules[rule["id"]] = rule

    def drop_rule(self, rule_id: int) -> None:
        self.rules.pop(rule_id, None)
        for key in [k for k in self._windows if k[0] == rule_id]:
            del self._windows[key]

    def reset_windows_for_rule(self, rule_id: int) -> None:
        """Called after a rule's windowSeconds/aggregation changes, so stale
        window state doesn't silently keep using the old window length."""
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
        """Replay a historical reading into a window rule's state. Used only
        during startup rehydration (see ingestion.rehydrate_windows)."""
        rule = self.rules.get(rule_id)
        if rule is None:
            return
        self._window_for(rule, sensor_id).add(ts, value)

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
        """Feed one reading through every matching rule.

        Returns a list of (rule, observed_value) for rules that fired on
        THIS reading. Window-rule state is updated here regardless of
        whether the rule fires, since every reading needs to count toward
        the rolling average even on the readings that don't trigger it.
        """
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
