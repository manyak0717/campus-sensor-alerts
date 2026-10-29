# Campus Sensor Monitoring and Alerts

A backend that ingests sensor readings from across campus (temperature, air
quality, occupancy, whatever), lets organizers define alert rules on them,
and raises alerts when those rules trip — all while staying correct under a
sudden burst of thousands of readings arriving at once.

Built with FastAPI + SQLite (async, WAL mode). No external services needed
to run it.

## Why this exists

Campus sensors don't report at a nice steady trickle. A fire drill, a big
event, a network hiccup that dumps a backlog all at once — any of these can
turn "a few readings a second" into "ten thousand readings in the next
second." The brief asked for a system that keeps working through that, so
the design centers on one idea: **accepting a reading and processing a
reading are two different jobs, and they should never block each other.**

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

The server listens on `http://127.0.0.1:8000`. SQLite file (`sensor_alerts.db`)
is created automatically on first run.

## Try it

```bash
# create a rule: alert if temperature > 30 on any sensor
curl -X POST localhost:8000/rules -H "Content-Type: application/json" -d '{
  "name": "high-temp",
  "sensorType": "temperature",
  "kind": "single",
  "operator": "gt",
  "threshold": 30
}'

# send a reading that breaks the rule
curl -X POST localhost:8000/readings -H "Content-Type: application/json" -d '{
  "sensorId": "library-floor-2-temp-1",
  "type": "temperature",
  "value": 34.5,
  "timestamp": "2026-01-01T12:00:00Z"
}'

# see the alert
curl localhost:8000/alerts
```

Or run the simulator to generate a steady stream of realistic readings with
occasional spikes:

```bash
python simulator.py --sensors 20 --rate 5 --duration 60 --spike library-floor-2
```

## API

| Endpoint | Purpose |
|---|---|
| `POST /readings` | Ingest one reading |
| `POST /readings/batch` | Ingest up to 20,000 readings in one call |
| `POST /rules` | Create an alert rule |
| `GET /rules`, `GET /rules/{id}` | List / fetch rules |
| `PUT /rules/{id}` | Edit a rule |
| `POST /rules/{id}/enable`, `/disable` | Toggle a rule without deleting it |
| `GET /alerts` | List alerts (filter by `status`, `sensorId`, `ruleId`) |
| `POST /alerts/{id}/acknowledge`, `/resolve` | Alert lifecycle |
| `GET /health` | Liveness + ingestion stats (`accepted`, `persisted`, `queueDepth`, `alerts_created`) |

Two rule kinds:
- **single** — fires on one reading, e.g. `temperature > 30`
- **window** — fires on an aggregate over a trailing time window, e.g.
  `avg(occupancy) over 5 minutes > 40`

## Load testing

```bash
python load_test.py
```

Fires 10,000 readings at the running server in two different shapes (one
giant batch, and 200 concurrent smaller batches) and reports real measured
throughput and latency. See `ARCHITECTURE.md` for the actual numbers and
what they mean.

## What's in `ARCHITECTURE.md`

The design decisions, the throughput/reliability trade-offs, the load test
results, and — because it's more useful than pretending it went
smoothly — a real bug I hit and fixed while building this: a busy-loop that
made the ingestion pipeline silently stop processing anything.
