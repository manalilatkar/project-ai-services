"""
One-shot vision pipeline: image files → extract VLM endpoint directly.
"""

import time
from pathlib import Path

from clients.extract_client import ExtractClient
from common.misc_utils import get_logger
from db.manager import db_manager
from settings import settings

logger = get_logger("pipeline.oneshot_path")


async def run_oneshot_path(
    job_id: str,
    staged_path: Path,
    extract_client: ExtractClient | None = None,
) -> dict:
    """Full one-shot orchestration: submit image → poll extract → return raw result.

    Status is already 'extracting' (set by the caller); this function only
    records the extract job id, polls until completion, and stores elapsed time.

    Args:
        job_id: Job identifier.
        staged_path: Path to the staged image file on disk.
        extract_client: Pre-configured :class:`ExtractClient` instance.
            Defaults to a new instance using ``settings.invoice.extract_url``.

    Returns:
        Raw extraction result dict from the extract service.
    """
    if extract_client is None:
        extract_client = ExtractClient(base_url=settings.invoice.extract_url)

    logger.info(
        f"[{job_id}] One-Shot path: forwarding {staged_path.name!r} to extract VLM"
    )

    t0 = time.monotonic()
    extract_job_id = await extract_client.submit_extraction(
        job_id=job_id,
        file_path=staged_path,
    )
    db_manager.update_job(job_id=job_id, extract_job_id=extract_job_id)

    raw = await extract_client.poll_job(extract_job_id)
    elapsed = round(time.monotonic() - t0, 2)

    logger.info(f"[{job_id}] One-Shot extraction completed in {elapsed}s")
    db_manager.update_job(job_id=job_id, job_metadata={"oneshot_latency_s": elapsed})

    return raw
