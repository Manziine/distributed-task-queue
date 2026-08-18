---
name: Bug Report
about: Report a bug or unexpected behavior
labels: bug
---

## Describe the Bug
A clear description of the bug.

## Steps to Reproduce
1. Enqueue job with POST /api/jobs payload: ...
2. Wait for worker to process
3. Check status with GET /api/jobs/:id
4. See error

## Expected Behavior
Job should complete successfully with result {...}

## Environment
- OS:
- Python version:
- Redis version:
- Docker version:
