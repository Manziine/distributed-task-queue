from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Any
from enum import Enum

from app.core.queue import (
    enqueue_job, get_job_status, get_queue_stats,
    JobPriority, JobStatus
)

router = APIRouter()


class EnqueueRequest(BaseModel):
    job_type: str = Field(..., description="Registered job type (e.g. send_email, http_webhook)")
    payload: dict = Field(..., description="Job-specific input data")
    priority: JobPriority = Field(JobPriority.NORMAL, description="Queue priority")
    max_retries: int = Field(3, ge=0, le=10, description="Max retry attempts before dead-letter")
    delay_seconds: int = Field(0, ge=0, le=86400, description="Delay before job becomes eligible")
    idempotency_key: Optional[str] = Field(None, description="Prevent duplicate job submission")

    class Config:
        json_schema_extra = {
            "example": {
                "job_type": "send_email",
                "payload": {"to": "user@example.com", "subject": "Hello", "body": "Welcome!"},
                "priority": "HIGH",
                "max_retries": 3,
                "delay_seconds": 0,
            }
        }


@router.post("/jobs", status_code=202)
async def enqueue(req: EnqueueRequest):
    """
    Submit a job to the distributed queue.
    
    Returns immediately with job_id — processing happens asynchronously.
    Use GET /api/jobs/{job_id} to poll status.
    """
    VALID_TYPES = {"send_email", "http_webhook", "generate_report", "resize_image"}
    if req.job_type not in VALID_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown job_type '{req.job_type}'. Valid types: {sorted(VALID_TYPES)}"
        )

    result = await enqueue_job(
        job_type=req.job_type,
        payload=req.payload,
        priority=req.priority,
        max_retries=req.max_retries,
        delay_seconds=req.delay_seconds,
        idempotency_key=req.idempotency_key,
    )
    return result


@router.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Get the current status and result of a job."""
    job = await get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return job


@router.get("/dashboard")
async def dashboard():
    """Real-time queue statistics dashboard."""
    stats = await get_queue_stats()
    return {
        "queue": stats,
        "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
    }
