# Distributed Task Queue

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

**A production-grade distributed job processing system** — the infrastructure problem that every company at scale has solved and every senior engineer must understand.

</div>

---

## 🎯 Why This Project Matters

Companies like Stripe, Shopify, Airbnb, and GitHub all run millions of background jobs every day — sending emails, processing payments, generating reports, resizing images. This is the infrastructure that makes it possible.

This project demonstrates:
- **Distributed systems thinking**: Workers scale independently from the API
- **Reliability engineering**: Retry logic, dead-letter queues, idempotency
- **Redis mastery**: Sorted sets for priority queues, pub/sub for worker communication
- **Production operations**: Job monitoring dashboard, metrics, alerting hooks

## 🏗️ Architecture

```
API Server (FastAPI)
       │
       │ LPUSH job → Redis Queue
       ▼
┌─────────────────────────────────────┐
│           Redis                     │
│  ┌──────────────┐  ┌─────────────┐  │
│  │ Priority Queue│  │ Scheduled   │  │
│  │ (Sorted Set) │  │ Jobs (ZSET) │  │
│  └──────┬───────┘  └──────┬──────┘  │
│         │                 │         │
└─────────┼─────────────────┼─────────┘
          │                 │
   ┌──────▼──────────────────▼──────┐
   │         Worker Pool            │
   │  ┌────────┐  ┌────────┐        │
   │  │Worker 1│  │Worker 2│  ...   │
   │  └───┬────┘  └───┬────┘        │
   └──────┼───────────┼─────────────┘
          │           │
    ┌─────▼───────────▼──────┐
    │       PostgreSQL        │
    │  (job status, history,  │
    │   results, audit log)   │
    └────────────────────────┘
```

## ✅ Features

| Feature | Details |
|---|---|
| ⚡ Priority queues | 3 levels: HIGH / NORMAL / LOW (Redis sorted sets) |
| 🔄 Automatic retry | Exponential backoff: 1s → 2s → 4s → 8s → 16s |
| ☠️ Dead-letter queue | Failed jobs after max retries moved to DLQ for inspection |
| ⏰ Scheduled jobs | Cron-style scheduling via Redis sorted sets |
| 🔑 Idempotency | Duplicate job detection via SHA-256 payload hash |
| 📊 Dashboard API | Real-time queue stats, worker health, job history |
| 🔌 Extensible | Register new job types as simple Python functions |
| 🐳 Docker | Full multi-container local environment |
| 📡 Webhooks | POST callback to a URL when job completes or fails |

## 🚀 Quick Start

```bash
git clone https://github.com/Manziine/distributed-task-queue.git
cd distributed-task-queue

cp .env.example .env
docker compose up --build

# The API is now running at http://localhost:8000
```

### Enqueue a Job

```bash
# Enqueue a high-priority email job
curl -X POST http://localhost:8000/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome!",
      "body": "Thanks for signing up."
    },
    "priority": "HIGH",
    "max_retries": 3
  }'

# Response:
{
  "job_id": "job_a3f9b21c",
  "status": "QUEUED",
  "position": 1,
  "estimated_start": "in ~2 seconds"
}
```

### Check Job Status

```bash
curl http://localhost:8000/api/jobs/job_a3f9b21c

# Response:
{
  "job_id": "job_a3f9b21c",
  "status": "COMPLETED",
  "result": {"sent": true, "message_id": "msg_xyz"},
  "attempts": 1,
  "duration_ms": 234,
  "completed_at": "2025-08-18T03:12:01Z"
}
```

### Queue Dashboard

```bash
curl http://localhost:8000/api/dashboard

# Response:
{
  "queued": {"HIGH": 0, "NORMAL": 12, "LOW": 5},
  "processing": 3,
  "completed_today": 1847,
  "failed_today": 3,
  "dead_letter": 2,
  "workers": [
    {"id": "worker-1", "status": "busy", "current_job": "job_a3f9b21c"},
    {"id": "worker-2", "status": "idle"}
  ]
}
```

## 📁 Project Structure

```
distributed-task-queue/
├── app/
│   ├── api/
│   │   ├── jobs.py         # POST /api/jobs, GET /api/jobs/:id
│   │   └── dashboard.py    # GET /api/dashboard — queue stats
│   ├── core/
│   │   ├── config.py       # Settings
│   │   ├── database.py     # Async PostgreSQL
│   │   └── redis.py        # Redis connection & queue operations
│   ├── models/
│   │   └── job.py          # Job SQLAlchemy model + JobStatus enum
│   ├── worker/
│   │   ├── pool.py         # Worker pool manager
│   │   ├── worker.py       # Individual worker (blocking loop)
│   │   ├── scheduler.py    # Cron job scheduler
│   │   └── handlers/
│   │       ├── email.py    # Job handler: send_email
│   │       ├── report.py   # Job handler: generate_report
│   │       └── webhook.py  # Job handler: http_webhook
│   └── main.py
├── .github/workflows/ci.yml
├── docker-compose.yml
├── Dockerfile.api
├── Dockerfile.worker
├── requirements.txt
├── .env.example
└── README.md
```

## 💡 Key Design Decisions

### Why Redis Sorted Sets for Priority Queues?
- `ZADD` with score = priority level + timestamp ensures strict ordering
- `BZPOPMIN` atomically pops and blocks — no polling, no race conditions
- Single Redis command handles priority, ordering, and atomic dequeue

### Why separate Dockerfiles for API and Worker?
- API and workers scale independently
- In production: 1 API instance, N worker instances based on queue depth
- Workers can be added without redeploying the API

### Exponential Backoff Strategy
```
Attempt 1 → wait 2^1 = 2s  → retry
Attempt 2 → wait 2^2 = 4s  → retry
Attempt 3 → wait 2^3 = 8s  → retry
Attempt 4 → wait 2^4 = 16s → retry
Attempt 5 → dead-letter queue
```
Prevents thundering herd when a downstream service recovers.

### Idempotency
Each job is hashed (SHA-256 of type + payload). Submitting the same job twice within 24h returns the existing job ID. Prevents double-processing in retry storms.

## 🛠️ Built By

**Arnaud Ineza Manzi** — Backend & Infrastructure Engineer
📧 ainezamanzi@gmail.com | 🔗 [LinkedIn](https://linkedin.com/in/arnaud-ineza-manzi-471221272) | 🐙 [GitHub](https://github.com/Manziine)
