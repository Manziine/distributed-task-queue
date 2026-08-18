"""
Job Handler Registry
====================
Register job handlers here. Each handler is an async function that:
  - Accepts a payload dict
  - Returns a result dict (serializable)
  - Raises an exception on failure (will trigger retry)
"""
import asyncio
import logging
import aiohttp

logger = logging.getLogger(__name__)


# ─── Handler: send_email ───────────────────────────────────────────────────────
async def handle_send_email(payload: dict) -> dict:
    """
    Simulate sending an email.
    In production: integrate with SendGrid, SES, or Postmark.
    """
    to = payload.get("to")
    subject = payload.get("subject", "(no subject)")
    body = payload.get("body", "")

    if not to:
        raise ValueError("Missing required field: 'to'")

    # Simulate email sending latency
    await asyncio.sleep(0.1)

    logger.info(f"Email sent to {to}: {subject}")
    return {"sent": True, "to": to, "subject": subject, "provider": "simulated"}


# ─── Handler: http_webhook ─────────────────────────────────────────────────────
async def handle_http_webhook(payload: dict) -> dict:
    """
    POST a JSON payload to a webhook URL.
    Used for async notifications to external services.
    """
    url = payload.get("url")
    data = payload.get("data", {})
    headers = payload.get("headers", {})

    if not url:
        raise ValueError("Missing required field: 'url'")

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=data, headers=headers) as response:
            if response.status >= 400:
                raise RuntimeError(f"Webhook failed: HTTP {response.status} from {url}")
            return {"delivered": True, "status_code": response.status, "url": url}


# ─── Handler: generate_report ─────────────────────────────────────────────────
async def handle_generate_report(payload: dict) -> dict:
    """
    Simulate generating a data report (CSV/PDF export).
    In production: run DB queries, build file, upload to S3.
    """
    report_type = payload.get("type", "csv")
    filters = payload.get("filters", {})

    # Simulate heavy computation
    await asyncio.sleep(0.5)

    report_id = f"report_{id(payload):x}"
    logger.info(f"Generated {report_type} report: {report_id}")
    return {
        "report_id": report_id,
        "type": report_type,
        "rows": 1847,
        "download_url": f"https://storage.example.com/reports/{report_id}.{report_type}",
        "expires_at": "2025-08-25T00:00:00Z",
    }


# ─── Handler: resize_image ────────────────────────────────────────────────────
async def handle_resize_image(payload: dict) -> dict:
    """
    Simulate image resizing to multiple dimensions.
    In production: use Pillow or Sharp, upload variants to S3/CDN.
    """
    source_url = payload.get("source_url")
    sizes = payload.get("sizes", [[800, 600], [400, 300], [150, 150]])

    if not source_url:
        raise ValueError("Missing required field: 'source_url'")

    await asyncio.sleep(0.3)  # Simulate processing

    variants = [
        {"width": w, "height": h, "url": f"https://cdn.example.com/img/{w}x{h}.jpg"}
        for w, h in sizes
    ]
    return {"original": source_url, "variants": variants, "format": "jpeg", "quality": 85}


# ─── Handler Registry ─────────────────────────────────────────────────────────
HANDLER_REGISTRY: dict = {
    "send_email": handle_send_email,
    "http_webhook": handle_http_webhook,
    "generate_report": handle_generate_report,
    "resize_image": handle_resize_image,
}
