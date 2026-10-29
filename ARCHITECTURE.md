# Architecture

## The core idea: decouple "accept" from "process"

The brief's hard requirement is surviving a burst of 10,000 readings/sec
without falling over. The thing that kills a naive version of this system
is doing everything synchronously inside the HTTP handler: validate →
evaluate every rule → write to SQLite → return. Under a burst, that chain
is as slow as its slowest link (disk I/O), and every request pays for it,
so the whole server backs up and starts timing out or dropping connections.

So the handler does the minimum needed to safely say "got it" and nothing
else:

```
POST /readings  ->  validate shape  ->  push onto an in-memory queue  ->  return 202
```

A single background task (`consume_forever` in `app/ingestion.py`) owns
everything downstream: pulling items off the queue, running them through
the rule engine, and batching writes to SQLite. This means:

- **The HTTP accept path is O(1) and non-blocking.** A reading can be
  accepted in microseconds regardless of how backed up processing is.
- **Rule evaluation still happens immediately**, just not on the request
  thread — it happens the moment the background task pulls the item off
  the queue, which under normal load is near-instant. Alerts don't wait
  for a DB write to fire.
- **The DB write is what gets batched**, not rule evaluation, because disk
  I/O is the actually expensive part. Writes flush every 500 items or every
  0.5 seconds, whichever comes first (`WRITE_BATCH_SIZE`,
  `WRITE_FLUSH_INTERVAL_SECONDS` in `app/config.py`) — so a burst gets
  amortized into a handful of big `executemany` calls instead of 10,000
  individual round trips.
- **Backpressure is explicit.** The queue has a bounded size
  (`INGEST_QUEUE_MAXSIZE`). If it's actually full — meaning the consumer
  genuinely can't keep up, not just momentarily busy — new readings get a
  `503` instead of being silently dropped or queued without limit until the
  process runs out of memory. A sensor's retry logic (or the simulator)
  can treat that as "back off and retry," which is a much better failure
  mode than an unbounded queue that eventually OOMs the server.

This is the main throughput/reliability trade-off in the whole design:
**reading acceptance and alert correctness are near-real-time; durability
to disk lags by up to ~0.5 seconds under load.** If the process crashes in
that half-second window, a handful of the most recent readings could be
lost before they hit disk. Given the use case (monitoring, not financial
transactions), that's the right trade — losing a fraction of a second of
telemetry during a crash is acceptable; making every single reading wait
on a disk write is not, at 10,000/sec.

## Rule engine

Two rule kinds, both evaluated per-reading as it's dequeued:

- **single** — compare one reading directly against a threshold
  (`value > 30`, `value == "ERROR"`, etc). O(1).
- **window** — compare an aggregate (min/max/avg/count) over the last N
  seconds against a threshold. This is the part that has to be cheap, or
  it defeats the point of batching the writes.

### Why the window math is O(1) amortized, not O(n)

The naive way to compute "average occupancy over the last 5 minutes" is to
keep every reading in the window and recompute the aggregate from scratch
each time. That's O(n) per reading, and n grows with how busy the sensor
is — exactly the wrong property under burst load.

Instead, `SlidingWindow` in `app/rules_engine.py` keeps three running
structures per (rule, sensor) pair:

- a **running sum and count** for `avg` — update by addition on arrival,
  subtraction on expiry. O(1).
- two **monotonic deques** for `min` and `max` — when a new value arrives,
  pop anything from the back of the deque that it invalidates (anything
  it's more extreme than) before pushing itself on; the front of the deque
  is always the current window's min/max. Each value is pushed once and
  popped at most once over its lifetime, so this is O(1) amortized per
  reading, not O(window size).

Expiry (`_expire()`) runs on each access and trims anything older than the
window from the front of each structure. This is what I hand-verified
during testing: I fed in three readings (20, 22, 35) and confirmed the
engine's `avg` rule fired with `triggerValue` exactly equal to
`(20+22+35)/3 = 25.666...`, not an approximation — and separately verified
that values expire out of the window (and stop affecting the aggregate)
once they age past it.

### Restart recovery

In-memory windows start empty on process restart. Naively, that means a
window rule could silently under-count (or fail to fire) for up to a full
window's duration right after every restart, which is a real correctness
gap, not just a cosmetic one. `rehydrate_windows()` in `app/ingestion.py`
fixes this: on startup, before the server starts accepting new traffic in
earnest, it replays the last `MAX_WINDOW_SECONDS` of readings from SQLite
back through `seed_window()`, so rolling windows come back populated with
real history instead of empty. I specifically tested this by restarting
the server mid-window with an asymmetric set of readings (chosen so that
"rehydration worked" and "rehydration happened to look right by accident"
would give different, distinguishable average values) — the rehydrated
average matched the pre-restart calculation exactly.

### Alert cooldown

A rule that's genuinely broken (sensor stuck above threshold) would
otherwise fire on every single reading forever, flooding the alerts table
and whoever's on the other end of it. `RuleEngine.should_create_alert` in
`app/rules_engine.py` enforces a cooldown window (`ALERT_COOLDOWN_SECONDS`) per
(rule, sensor) pair, in memory (seeded from SQLite on startup), so the hot
path does no per-reading queries — the first breach creates an alert, subsequent
breaches within the cooldown are suppressed, and the cooldown resets once
it elapses. Verified by sending a rule-breaking reading twice in quick
succession and confirming only one alert exists, then waiting out the
cooldown and confirming a third breach creates a new one. An acknowledged
alert keeps suppressing new ones until it is resolved.

## Load test results

Ran with `load_test.py` against a locally running instance, two scenarios,
both sending exactly 10,000 readings:

| Scenario | Shape | Accept latency | Accept throughput | Drain latency (fully on disk) | Total | Errors |
|---|---|---|---|---|---|---|
| A | 1 request, all 10,000 readings in one batch | 0.084s | ~119,800 readings/s | 0.237s | 0.320s | 0 |
| B | 200 concurrent requests, 50 readings each | 0.833s | ~12,000 readings/s | 0.059s | 0.892s | 0 |

(Raw numbers in `load_test_results.json`, produced by the script, not
hand-typed.)

Both scenarios finish well inside the 1-second burst window with zero
`503`s and zero dropped readings. The two scenarios are deliberately
different shapes of the same 10,000-reading burst:

- **Scenario A** is the best case — one connection, one validation pass,
  one bulk enqueue. It's closer to "a gateway that's already aggregated
  readings from many sensors" than to the physical reality of independent
  sensors.
- **Scenario B** is closer to the real scenario the brief describes: many
  independent sensors each making their own request around the same
  moment. It's slower on raw accept throughput (more HTTP overhead, more
  connections competing for the event loop) but still comfortably finishes
  the whole burst in under a second with no errors — and notice its drain
  latency is actually *shorter* than scenario A's: smaller batches arriving
  over a slightly longer window give the background consumer more chances
  to flush along the way, instead of one single enormous batch landing all
  at once at the end.

The gap between "accept" and "drain" in both rows is the queue doing its
job: the system says "got it" fast and finishes the actual disk writes a
little after, exactly as designed above.

## What went wrong while building this (and the actual bug)

I'll leave this in because it's a more honest and more useful account of
the system than pretending it worked on the first try.

After wiring up the full pipeline, a basic test — post one reading, check
`/health` — showed readings being *accepted* but never *persisted*:
`queueDepth` stuck at 1, `persisted` stuck at 0, no alerts, forever. The
consumer task looked alive (no exception in the logs), but nothing was
moving.

My first hypothesis was that the background task had silently died:
`asyncio.Task`s swallow exceptions by default unless something awaits
their result, so a bug deep in the consumer loop could kill it permanently
without a single log line. I added a `try/except` around the loop body and
a `add_done_callback` in `main.py` specifically to surface that case if it
ever happened. Restarted — same symptom, but now provably *no* exception
was being thrown. That ruled out "the task died," but the defensive
logging was worth keeping regardless, since a silently-dead background
task is exactly the kind of bug that's invisible until it's a 3am page.

Second attempt, which was a dead end: I tried reproducing it with an
in-process `httpx.ASGITransport` test instead of a real running server, to
iterate faster. That approach doesn't invoke FastAPI's lifespan startup
the same way a real server does, which led me to a red herring (the
consumer task object looked like it was `None`) that was actually just a
gap in how I'd wired the test, not the real bug. Abandoned it and went
back to testing against the actual running server, which is the only
environment that matters here anyway.

With the real server, I added `print(..., flush=True)` at the top of the
consumer loop and on every iteration. That's what actually found it: the
loop was spinning through *thousands of iterations per second* with
`item=None` every time, well before any reading should have timed out.
The consumer wasn't stuck — it was running flat-out and still never seeing
new items.

The actual bug, in the line that computes how long to wait for the next
queue item:

```python
timeout = max(0.0, WRITE_FLUSH_INTERVAL_SECONDS - (time.monotonic() - last_flush))
item = await asyncio.wait_for(queue.get(), timeout=timeout)
```

`last_flush` was only ever updated inside the branch that actually wrote a
non-empty batch to disk. On an **idle** queue — the normal state whenever
nothing has arrived recently, which is most of the time — that branch
never runs, so `last_flush` goes stale almost immediately. Once
`time.monotonic() - last_flush` exceeds `WRITE_FLUSH_INTERVAL_SECONDS`,
`timeout` clamps to `0.0` and stays there permanently. From that point on,
every loop iteration calls `asyncio.wait_for(queue.get(), timeout=0)`,
which is effectively "check once, immediately give up if nothing's there
right now" — a tight busy-spin rather than an actual wait. In practice
that spin wins the race against the HTTP handler coroutine almost every
time: a reading that gets `put_nowait()`'d onto the queue a moment later
very rarely gets seen by a `get()` call that isn't actually blocking long
enough to be woken by it. The server *looked* idle and well-behaved (low
CPU most of the time, no errors) right up until the exact moment a request
came in, and then the race was already lost.

The fix is one line — restart the flush clock whenever the wait genuinely
timed out with nothing to flush, not only when something was flushed:

```python
elif not batch:
    last_flush = now
```

That keeps `timeout` oscillating correctly between `0` and
`WRITE_FLUSH_INTERVAL_SECONDS` instead of collapsing to a permanent `0`,
which restores `queue.get()`'s normal blocking behavior and lets it
actually be woken up the instant something is enqueued.

Confirmed the fix two ways: measured CPU usage on an idle server (flat and
low instead of pinned), and re-ran the full test suite — single-reading
rules, window rules with hand-verified math, cooldown suppression,
disable-rule suppression, restart recovery — against the live server, all
passing with real printed output rather than assumed. This is also why
`load_test.py` reports concrete numbers pulled from `/health` rather than
just "it worked": a system that silently stops processing under exactly
the conditions this project is supposed to prove it survives is the worst
possible failure mode to ship undetected, and I wanted the same rigor
applied to the burst test that caught this bug in the first place.

## Known limitations / what I'd change for real scale

- **SQLite is a single-writer bottleneck.** WAL mode lets reads happen
  concurrently with writes, but there's still one writer. This is fine
  well past the 10,000/sec burst target (see load test above) because
  writes are batched, but a sustained, much higher steady-state load would
  eventually want Postgres, or a proper time-series store (TimescaleDB,
  InfluxDB) built for this exact shape of data.
- **Single process, single queue.** This runs on one machine with one
  in-memory queue and one in-memory rule engine. Horizontal scaling (more
  than one instance) would need the queue and rule state to move to
  something shared — Redis or Kafka for the queue, and either a shared
  rule-state store or sensor-based sharding so each instance only owns a
  subset of sensors.
- **At-least-once, not exactly-once, around a crash.** The up-to-0.5s
  durability lag means a crash at the wrong instant can lose the most
  recent unwritten readings. For this use case that's an acceptable
  trade for throughput; it would not be for something like billing.
