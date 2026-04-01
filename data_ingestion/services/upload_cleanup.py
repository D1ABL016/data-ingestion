"""Remove uploaded CSV files only for terminal jobs (DONE / FAILED)."""

from __future__ import annotations

import logging
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_ingestion.database import async_session_factory
from data_ingestion.models.job import Job

logger = logging.getLogger(__name__)

_TERMINAL = ("DONE", "FAILED")


async def delete_upload_file_for_job(session: AsyncSession, job: Job) -> None:
    """Unlink ``job.file_path`` if set; clear column. Caller commits."""
    if not job.file_path:
        return
    if job.status not in _TERMINAL:
        return
    path = Path(job.file_path)
    try:
        path.unlink(missing_ok=True)
        logger.info("Deleted upload file for job %s: %s", job.id, path)
    except OSError as e:
        logger.warning("Could not delete upload file %s: %s", path, e)
    job.file_path = None


async def delete_upload_file_if_job_terminal(job_id: UUID) -> None:
    async with async_session_factory() as session:
        job = await session.get(Job, job_id)
        if not job:
            return
        if job.status not in _TERMINAL:
            return
        await delete_upload_file_for_job(session, job)
        await session.commit()


async def sweep_terminal_job_upload_files() -> None:
    """Scheduler: delete on-disk files for any DONE/FAILED job that still has ``file_path``."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(Job).where(Job.status.in_(_TERMINAL), Job.file_path.isnot(None))
        )
        jobs = result.scalars().all()
        for job in jobs:
            await delete_upload_file_for_job(session, job)
        if jobs:
            await session.commit()
