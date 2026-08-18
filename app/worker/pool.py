"""Worker Pool Entry Point — run with: python -m app.worker.pool"""
import asyncio
import os
from app.worker.worker import WorkerPool


async def main():
    num_workers = int(os.environ.get("NUM_WORKERS", 4))
    pool = WorkerPool(num_workers=num_workers)
    await pool.start()


if __name__ == "__main__":
    asyncio.run(main())
