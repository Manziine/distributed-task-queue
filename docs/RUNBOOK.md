# Operations Runbook

## Starting the Stack

`ash
docker compose up -d
`

## Scaling Workers

`ash
# Scale to 8 worker instances
docker compose up -d --scale worker=8
`

## Checking Queue Depth

`ash
curl http://localhost:8000/api/dashboard
`

## Inspecting a Failed Job

`ash
curl http://localhost:8000/api/jobs/job_abc123
# Look for status: FAILED or DEAD and last_error field
`

## Draining the Dead Letter Queue

`ash
# List dead jobs
redis-cli ZRANGE dtq:dead_letter 0 -1 WITHSCORES

# Inspect a specific dead job
redis-cli HGETALL dtq:job:job_abc123

# Requeue a dead job (reset status to QUEUED)
redis-cli HSET dtq:job:job_abc123 status QUEUED attempt 0
redis-cli ZADD dtq:queue 2000000000000 job_abc123
redis-cli ZREM dtq:dead_letter job_abc123
`

## Monitoring

- Queue stats: GET /api/dashboard
- Prometheus metrics (if enabled): GET /metrics
- Redis: edis-cli INFO stats
