from pathlib import Path
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from data_ingestion.config import settings
from data_ingestion.database import get_session
from data_ingestion.models.job import Job
from data_ingestion.schemas.job_response import UploadAcceptedResponse
from data_ingestion.services.prerequisites import stores_and_users_exist
from data_ingestion.utils.csv_headers import validate_headers_bytes
from data_ingestion.worker import process_upload_job

router = APIRouter(prefix="/upload", tags=["upload"])

MAX_BYTES = settings.max_upload_mb * 1024 * 1024


def _ensure_csv(filename: str | None) -> None:
    if not filename or not filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are accepted",
        )


def _resolved_upload_path(job_id: UUID) -> Path:
    Path(settings.upload_dir).mkdir(parents=True, exist_ok=True)
    return (Path(settings.upload_dir) / f"{job_id}.csv").resolve()


@router.post("/stores", status_code=status.HTTP_202_ACCEPTED, response_model=UploadAcceptedResponse)
async def upload_stores(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
    file: UploadFile = File(...),
) -> UploadAcceptedResponse:
    _ensure_csv(file.filename)
    content = await file.read()
    if len(content) > MAX_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File too large")
    try:
        validate_headers_bytes(content, "stores")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)) from e

    job_id = uuid4()
    path = _resolved_upload_path(job_id)
    job = Job(id=job_id, file_type="stores", status="PENDING", errors=[], file_path=str(path))
    session.add(job)
    await session.commit()

    path.write_bytes(content)

    background_tasks.add_task(process_upload_job, job_id, str(path), "stores")

    return UploadAcceptedResponse(
        job_id=job_id,
        poll_url=f"/api/v1/jobs/{job_id}",
    )


@router.post("/users", status_code=status.HTTP_202_ACCEPTED, response_model=UploadAcceptedResponse)
async def upload_users(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
    file: UploadFile = File(...),
) -> UploadAcceptedResponse:
    _ensure_csv(file.filename)
    content = await file.read()
    if len(content) > MAX_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File too large")
    try:
        validate_headers_bytes(content, "users")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)) from e

    job_id = uuid4()
    path = _resolved_upload_path(job_id)
    job = Job(id=job_id, file_type="users", status="PENDING", errors=[], file_path=str(path))
    session.add(job)
    await session.commit()

    path.write_bytes(content)

    background_tasks.add_task(process_upload_job, job_id, str(path), "users")

    return UploadAcceptedResponse(
        job_id=job_id,
        poll_url=f"/api/v1/jobs/{job_id}",
    )


@router.post("/mappings", status_code=status.HTTP_202_ACCEPTED, response_model=UploadAcceptedResponse)
async def upload_mappings(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
    file: UploadFile = File(...),
) -> UploadAcceptedResponse:
    _ensure_csv(file.filename)
    if not await stores_and_users_exist(session):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Upload stores and users first",
        )

    content = await file.read()
    if len(content) > MAX_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File too large")
    try:
        validate_headers_bytes(content, "mappings")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)) from e

    job_id = uuid4()
    path = _resolved_upload_path(job_id)
    job = Job(id=job_id, file_type="mappings", status="PENDING", errors=[], file_path=str(path))
    session.add(job)
    await session.commit()

    path.write_bytes(content)

    background_tasks.add_task(process_upload_job, job_id, str(path), "mappings")

    return UploadAcceptedResponse(
        job_id=job_id,
        poll_url=f"/api/v1/jobs/{job_id}",
    )
