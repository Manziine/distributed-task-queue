import pytest

@pytest.mark.asyncio
async def test_job_id_format():
    from app.core.queue import _job_id
    jid = _job_id()
    assert jid.startswith('job_')
    assert len(jid) == 16

@pytest.mark.asyncio
async def test_payload_hash_deterministic():
    from app.core.queue import _payload_hash
    h1 = _payload_hash('send_email', {'to': 'a@b.com'})
    h2 = _payload_hash('send_email', {'to': 'a@b.com'})
    assert h1 == h2

@pytest.mark.asyncio
async def test_payload_hash_different():
    from app.core.queue import _payload_hash
    h1 = _payload_hash('send_email', {'to': 'a@b.com'})
    h2 = _payload_hash('send_email', {'to': 'c@d.com'})
    assert h1 != h2
