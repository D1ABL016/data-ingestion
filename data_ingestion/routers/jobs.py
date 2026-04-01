from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from data_ingestion.database import get_session
from data_ingestion.models.job import Job
from data_ingestion.schemas.job_response import JobErrorItem, JobProgress, JobStatusResponse

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get(
    "/{job_id}",
    response_model=JobStatusResponse,
    status_code=status.HTTP_200_OK,
)
async def get_job_status(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> JobStatusResponse:
    job = await session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    errors_raw = job.errors or []
    errors = [JobErrorItem.model_validate(e) for e in errors_raw]

    return JobStatusResponse(
        job_id=job.id,
        status=job.status,  # type: ignore[arg-type]
        file_type=job.file_type,  # type: ignore[arg-type]
        progress=JobProgress(total=job.total_rows, processed=job.processed_rows),
        ingested=job.ingested,
        failed=job.failed,
        errors=errors,
        started_at=job.started_at,
        completed_at=job.completed_at,
    )
