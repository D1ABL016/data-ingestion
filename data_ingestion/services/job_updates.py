from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from data_ingestion.models.job import Job


async def load_job(session: AsyncSession, job_id: UUID) -> Job | None:
    return await session.get(Job, job_id)


async def append_job_progress(
    session: AsyncSession,
    job: Job,
    *,
    ingested_delta: int = 0,
    failed_delta: int = 0,
    processed_delta: int | None = None,
    new_errors: list[dict[str, Any]] | None = None,
    status: str | None = None,
    total_rows: int | None = None,
) -> None:
    if total_rows is not None:
        job.total_rows = total_rows
    job.ingested += ingested_delta
    job.failed += failed_delta
    if processed_delta is not None:
        job.processed_rows += processed_delta
    elif ingested_delta or failed_delta:
        job.processed_rows += ingested_delta + failed_delta
    if new_errors:
        errs = list(job.errors or [])
        errs.extend(new_errors)
        job.errors = errs
        flag_modified(job, "errors")
    if status:
        job.status = status
    await session.commit()


async def finalize_job(session: AsyncSession, job: Job, *, failed_final: bool = False) -> None:
    job.status = "FAILED" if failed_final else "DONE"
    job.completed_at = datetime.now(timezone.utc)
    await session.commit()
