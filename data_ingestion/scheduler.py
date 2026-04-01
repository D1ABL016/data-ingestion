from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import delete

from data_ingestion.database import async_session_factory
from data_ingestion.models.job import Job
from data_ingestion.services.upload_cleanup import sweep_terminal_job_upload_files

logger = logging.getLogger(__name__)


async def cleanup_orphaned_uploads_and_jobs() -> None:
    await sweep_terminal_job_upload_files()

    cutoff_jobs = datetime.now(timezone.utc) - timedelta(hours=24)
    async with async_session_factory() as session:
        await session.execute(
            delete(Job).where(
                Job.status.in_(("DONE", "FAILED")),
                Job.completed_at.isnot(None),
                Job.completed_at < cutoff_jobs,
            )
        )
        await session.commit()


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    scheduler.add_job(
        cleanup_orphaned_uploads_and_jobs,
        "interval",
        hours=1,
        id="cleanup_uploads_jobs",
        replace_existing=True,
        max_instances=1,
    )
    return scheduler
