import pytest

@pytest.mark.asyncio
async def test_send_email_handler_success():
    from app.worker.handlers import handle_send_email
    result = await handle_send_email({'to': 'test@example.com', 'subject': 'Test', 'body': 'Hello'})
    assert result['sent'] == True
    assert result['to'] == 'test@example.com'

@pytest.mark.asyncio
async def test_send_email_handler_missing_to():
    from app.worker.handlers import handle_send_email
    with pytest.raises(ValueError, match='Missing required field'):
        await handle_send_email({'subject': 'No recipient'})

@pytest.mark.asyncio
async def test_generate_report_handler():
    from app.worker.handlers import handle_generate_report
    result = await handle_generate_report({'type': 'csv', 'filters': {}})
    assert 'report_id' in result
    assert result['type'] == 'csv'
    assert result['rows'] > 0

@pytest.mark.asyncio
async def test_resize_image_handler():
    from app.worker.handlers import handle_resize_image
    result = await handle_resize_image({'source_url': 'https://example.com/img.jpg'})
    assert 'variants' in result
    assert len(result['variants']) > 0

@pytest.mark.asyncio
async def test_resize_image_missing_url():
    from app.worker.handlers import handle_resize_image
    with pytest.raises(ValueError):
        await handle_resize_image({})
