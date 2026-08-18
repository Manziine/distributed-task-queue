import asyncio
import logging
import signal
import traceback
from datetime import datetime

from app.core.queue import dequeue_job, complete_job, fail_job, get_queue_stats
from app.worker.handlers import HANDLER_REGISTRY

logger = logging.getLogger(__name__)


class Worker:
    """
    A single worker process.
    - Polls the priority queue for jobs
    - Dispatches to the correct handler function
    - Reports success/failure back to the queue engine
    """

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.running = False
        self.current_job_id: str | None = None
        self.jobs_processed = 0
        self.jobs_failed = 0
        self.started_at = datetime.utcnow()

    async def start(self):
        """Main worker loop — runs until stopped."""
        self.running = True
        logger.info(f"[{self.worker_id}] Worker started")

        while self.running:
            try:
                job = await dequeue_job(timeout=5)

                if job is None:
                    await asyncio.sleep(0.5)  # Brief sleep when queue is empty
                    continue

                await self._process_job(job)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.worker_id}] Unexpected worker error: {e}")
                await asyncio.sleep(1)

        logger.info(f"[{self.worker_id}] Worker stopped. Processed: {self.jobs_processed}, Failed: {self.jobs_failed}")

    async def _process_job(self, job: dict):
        """Execute a single job."""
        job_id = job["job_id"]
        job_type = job["job_type"]
        payload = job.get("payload", {})

        self.current_job_id = job_id
        logger.info(f"[{self.worker_id}] Processing job {job_id} (type={job_type}, attempt={job.get('attempt', 0)})")

        start_time = datetime.utcnow()

        try:
            # Dispatch to registered handler
            handler = HANDLER_REGISTRY.get(job_type)
            if not handler:
                raise ValueError(f"No handler registered for job type: '{job_type}'")

            result = await handler(payload)

            await complete_job(job_id, result)
            self.jobs_processed += 1

            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            logger.info(f"[{self.worker_id}] ✅ Job {job_id} completed in {duration_ms}ms")

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.warning(f"[{self.worker_id}] ❌ Job {job_id} failed: {error_msg}")
            await fail_job(job_id, error_msg, job)
            self.jobs_failed += 1

        finally:
            self.current_job_id = None

    def stop(self):
        self.running = False

    def status(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "status": "busy" if self.current_job_id else "idle",
            "current_job": self.current_job_id,
            "jobs_processed": self.jobs_processed,
            "jobs_failed": self.jobs_failed,
            "uptime_seconds": int((datetime.utcnow() - self.started_at).total_seconds()),
        }


class WorkerPool:
    """
    Manages a pool of Worker instances running concurrently.
    Workers scale up/down based on configuration.
    """

    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self.workers: list[Worker] = []
        self.tasks: list[asyncio.Task] = []

    async def start(self):
        logger.info(f"Starting worker pool with {self.num_workers} workers")
        self.workers = [Worker(f"worker-{i+1}") for i in range(self.num_workers)]
        self.tasks = [asyncio.create_task(w.start()) for w in self.workers]

        # Graceful shutdown on SIGTERM/SIGINT
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop)

        await asyncio.gather(*self.tasks, return_exceptions=True)

    def stop(self):
        logger.info("Shutting down worker pool...")
        for worker in self.workers:
            worker.stop()
        for task in self.tasks:
            task.cancel()

    def pool_status(self) -> dict:
        stats = [w.status() for w in self.workers]
        busy = sum(1 for w in stats if w["status"] == "busy")
        return {
            "total_workers": self.num_workers,
            "busy": busy,
            "idle": self.num_workers - busy,
            "workers": stats,
        }
