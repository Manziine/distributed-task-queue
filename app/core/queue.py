import asyncio
import hashlib
import json
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

import redis.asyncio as redis

from app.core.config import settings


class JobPriority(str, Enum):
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD = "DEAD"  # max retries exhausted → dead-letter queue


# Priority scores (lower = higher priority in Redis sorted set)
PRIORITY_SCORES = {
    JobPriority.HIGH: 1,
    JobPriority.NORMAL: 2,
    JobPriority.LOW: 3,
}

QUEUE_KEY = "dtq:queue"
DLQ_KEY = "dtq:dead_letter"
PROCESSING_KEY = "dtq:processing"


_redis_client: Optional[redis.Redis] = None


async def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20,
        )
    return _redis_client


def _job_id() -> str:
    return f"job_{uuid.uuid4().hex[:12]}"


def _payload_hash(job_type: str, payload: dict) -> str:
    """SHA-256 hash of job type + payload for idempotency."""
    raw = json.dumps({"type": job_type, "payload": payload}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


async def enqueue_job(
    job_type: str,
    payload: dict,
    priority: JobPriority = JobPriority.NORMAL,
    max_retries: int = 3,
    delay_seconds: int = 0,
    idempotency_key: Optional[str] = None,
) -> dict:
    """
    Enqueue a job onto the priority queue.

    Uses a Redis Sorted Set where:
      score = priority_level * 1e12 + unix_timestamp
    This ensures HIGH priority jobs always come before NORMAL, 
    and within the same priority, FIFO ordering is preserved.
    """
    r = await get_redis()

    # Idempotency check
    idem_key = idempotency_key or _payload_hash(job_type, payload)
    existing_id = await r.get(f"dtq:idem:{idem_key}")
    if existing_id:
        return {"job_id": existing_id, "status": "DUPLICATE", "idempotency_key": idem_key}

    job_id = _job_id()
    now = datetime.utcnow()
    run_at = now + timedelta(seconds=delay_seconds)

    job_data = {
        "job_id": job_id,
        "job_type": job_type,
        "payload": json.dumps(payload),
        "priority": priority.value,
        "status": JobStatus.QUEUED.value,
        "max_retries": max_retries,
        "attempt": 0,
        "created_at": now.isoformat(),
        "run_at": run_at.isoformat(),
        "idempotency_key": idem_key,
    }

    # Store job data
    await r.hset(f"dtq:job:{job_id}", mapping=job_data)
    await r.expire(f"dtq:job:{job_id}", 86400 * 7)  # 7 days TTL

    # Add to priority queue
    # Score: priority * 10^12 + unix_timestamp (ensures priority ordering + FIFO within priority)
    score = PRIORITY_SCORES[priority] * 1_000_000_000_000 + run_at.timestamp()
    await r.zadd(QUEUE_KEY, {job_id: score})

    # Store idempotency key (24h)
    await r.setex(f"dtq:idem:{idem_key}", 86400, job_id)

    return {
        "job_id": job_id,
        "status": "QUEUED",
        "priority": priority.value,
        "position": await r.zrank(QUEUE_KEY, job_id),
        "run_at": run_at.isoformat(),
    }


async def dequeue_job(timeout: int = 5) -> Optional[dict]:
    """
    Atomically pop the highest-priority job from the queue.
    Uses BZPOPMIN to block until a job is available (no polling).
    """
    r = await get_redis()

    # Only pop jobs whose run_at <= now
    now_score = 3 * 1_000_000_000_000 + datetime.utcnow().timestamp()
    
    # Get jobs ready to run (score <= now across all priorities that are due)
    ready = await r.zrangebyscore(QUEUE_KEY, 0, now_score, start=0, num=1)
    if not ready:
        return None

    job_id = ready[0]
    
    # Atomically move from queue → processing
    pipe = r.pipeline()
    pipe.zrem(QUEUE_KEY, job_id)
    pipe.hset(f"dtq:job:{job_id}", mapping={
        "status": JobStatus.PROCESSING.value,
        "started_at": datetime.utcnow().isoformat(),
    })
    pipe.zadd(PROCESSING_KEY, {job_id: datetime.utcnow().timestamp()})
    await pipe.execute()

    raw = await r.hgetall(f"dtq:job:{job_id}")
    if raw:
        raw["payload"] = json.loads(raw.get("payload", "{}"))
    return raw


async def complete_job(job_id: str, result: Any) -> None:
    """Mark a job as successfully completed."""
    r = await get_redis()
    await r.hset(f"dtq:job:{job_id}", mapping={
        "status": JobStatus.COMPLETED.value,
        "result": json.dumps(result),
        "completed_at": datetime.utcnow().isoformat(),
    })
    await r.zrem(PROCESSING_KEY, job_id)


async def fail_job(job_id: str, error: str, job_data: dict) -> None:
    """
    Handle a failed job: retry with exponential backoff or move to DLQ.
    """
    r = await get_redis()
    attempt = int(job_data.get("attempt", 0)) + 1
    max_retries = int(job_data.get("max_retries", 3))

    if attempt >= max_retries:
        # Max retries exhausted → dead-letter queue
        await r.hset(f"dtq:job:{job_id}", mapping={
            "status": JobStatus.DEAD.value,
            "last_error": error,
            "attempt": attempt,
            "died_at": datetime.utcnow().isoformat(),
        })
        await r.zrem(PROCESSING_KEY, job_id)
        await r.zadd(DLQ_KEY, {job_id: datetime.utcnow().timestamp()})
    else:
        # Exponential backoff: 2^attempt seconds
        backoff_seconds = 2 ** attempt
        run_at = datetime.utcnow() + timedelta(seconds=backoff_seconds)
        priority = job_data.get("priority", JobPriority.NORMAL.value)
        priority_score = PRIORITY_SCORES.get(JobPriority(priority), 2)
        score = priority_score * 1_000_000_000_000 + run_at.timestamp()

        pipe = r.pipeline()
        pipe.hset(f"dtq:job:{job_id}", mapping={
            "status": JobStatus.RETRYING.value,
            "attempt": attempt,
            "last_error": error,
            "next_retry_at": run_at.isoformat(),
        })
        pipe.zrem(PROCESSING_KEY, job_id)
        pipe.zadd(QUEUE_KEY, {job_id: score})
        await pipe.execute()


async def get_queue_stats() -> dict:
    """Return real-time queue statistics."""
    r = await get_redis()
    queued = await r.zcard(QUEUE_KEY)
    processing = await r.zcard(PROCESSING_KEY)
    dlq = await r.zcard(DLQ_KEY)

    # Count by priority (scores in ranges)
    high = await r.zcount(QUEUE_KEY, 1_000_000_000_000, 1_999_999_999_999)
    normal = await r.zcount(QUEUE_KEY, 2_000_000_000_000, 2_999_999_999_999)
    low = await r.zcount(QUEUE_KEY, 3_000_000_000_000, 3_999_999_999_999)

    return {
        "queued": {"total": queued, "HIGH": high, "NORMAL": normal, "LOW": low},
        "processing": processing,
        "dead_letter": dlq,
    }


async def get_job_status(job_id: str) -> Optional[dict]:
    """Get current status of a job."""
    r = await get_redis()
    raw = await r.hgetall(f"dtq:job:{job_id}")
    if not raw:
        return None
    if "payload" in raw:
        raw["payload"] = json.loads(raw["payload"])
    if "result" in raw:
        raw["result"] = json.loads(raw["result"])
    return raw
