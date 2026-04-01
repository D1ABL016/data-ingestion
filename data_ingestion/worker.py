from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID

from data_ingestion.database import async_session_factory
from data_ingestion.models.job import Job
from data_ingestion.services.mapping_processor import process_mappings_file
from data_ingestion.services.store_processor import process_stores_file
from data_ingestion.services.upload_cleanup import delete_upload_file_if_job_terminal
from data_ingestion.services.user_processor import process_users_file

logger = logging.getLogger(__name__)


async def process_upload_job(job_id: UUID, filepath: str, file_type: str) -> None:
    try:
        async with async_session_factory() as session:
            if file_type == "stores":
                await process_stores_file(session, job_id, filepath)
            elif file_type == "users":
                await process_users_file(session, job_id, filepath)
            elif file_type == "mappings":
                await process_mappings_file(session, job_id, filepath)
    except Exception:
        logger.exception("Job %s failed", job_id)
        try:
            async with async_session_factory() as session:
                job = await session.get(Job, job_id)
                if job and job.status not in ("DONE", "FAILED"):
                    job.status = "FAILED"
                    job.completed_at = datetime.now(timezone.utc)
                    await session.commit()
        except Exception:
            logger.exception("Could not mark job %s FAILED", job_id)
    finally:
        await delete_upload_file_if_job_terminal(job_id)
