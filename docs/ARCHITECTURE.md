# Architecture

## Overview

The Distributed Task Queue separates concerns into three layers:

1. **API Layer** — FastAPI server that accepts job submissions and returns status
2. **Queue Layer** — Redis sorted sets for priority ordering and scheduling
3. **Worker Layer** — Pool of async Python workers that consume and execute jobs

## Data Flow

`
Client → POST /api/jobs → API → Redis ZADD → Worker BZPOPMIN → Handler → Redis HSET (status)
Client → GET /api/jobs/:id → API → Redis HGETALL → Response
`

## Priority Queue Implementation

Jobs are stored in a Redis Sorted Set (ZSET) with score:
`
score = priority_level * 1_000_000_000_000 + unix_timestamp
`

This ensures:
- HIGH priority jobs (score 1e12 range) always sort before NORMAL (2e12 range)
- Within same priority, earlier jobs sort first (FIFO)
- BZPOPMIN atomically dequeues the lowest-score (highest-priority) job

## Retry Strategy

Failed jobs use exponential backoff before re-enqueue:
`
attempt 1: wait 2^1 = 2s
attempt 2: wait 2^2 = 4s
attempt 3: wait 2^3 = 8s
attempt n: wait 2^n seconds
max_retries: move to Dead Letter Queue
`

## Idempotency

Each job has a SHA-256 hash of {type}+{payload}. Submitting the same job within 24h returns the existing job ID, preventing duplicate processing.
