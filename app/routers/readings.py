"""Ingestion endpoints. Intentionally thin: validate, enqueue, return. All
the real work (rule evaluation, persistence) happens in the background
consumer — see app/ingestion.py.
"""
from fastapi import APIRouter, HTTPException

from app import ingestion
from app.models import IngestAck, Reading, ReadingBatch

router = APIRouter(tags=["readings"])


def _to_queue_item(r: Reading):
    return (r.sensorId, r.type, r.value, r.timestamp)


@router.post("/readings", response_model=IngestAck, status_code=202)
async def ingest_single(reading: Reading):
    if not ingestion.queue_has_room(1):
        raise HTTPException(status_code=503, detail="Ingest queue is full, retry shortly")
    ingestion.enqueue([_to_queue_item(reading)])
    return IngestAck(accepted=1, queueDepth=ingestion.queue.qsize())


@router.post("/readings/batch", response_model=IngestAck, status_code=202)
async def ingest_batch(batch: ReadingBatch):
    n = len(batch.readings)
    if not ingestion.queue_has_room(n):
        raise HTTPException(status_code=503, detail="Ingest queue is full, retry shortly")
    ingestion.enqueue([_to_queue_item(r) for r in batch.readings])
    return IngestAck(accepted=n, queueDepth=ingestion.queue.qsize())
