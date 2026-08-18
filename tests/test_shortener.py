import pytest
from app.core.queue import _job_id, _payload_hash, JobPriority, PRIORITY_SCORES

def test_priority_scores_ordered():
    assert PRIORITY_SCORES[JobPriority.HIGH] < PRIORITY_SCORES[JobPriority.NORMAL]
    assert PRIORITY_SCORES[JobPriority.NORMAL] < PRIORITY_SCORES[JobPriority.LOW]

def test_job_id_unique():
    ids = {_job_id() for _ in range(100)}
    assert len(ids) == 100  # All unique

def test_job_priority_enum():
    assert JobPriority('HIGH') == JobPriority.HIGH
    assert JobPriority('NORMAL') == JobPriority.NORMAL
    assert JobPriority('LOW') == JobPriority.LOW
