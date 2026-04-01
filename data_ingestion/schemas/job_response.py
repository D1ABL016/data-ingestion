from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

JobStatus = Literal["PENDING", "PROCESSING", "DONE", "FAILED"]
FileType = Literal["stores", "users", "mappings"]


class JobErrorItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row: int
    column: str
    value: str = ""
    reason: str


class JobProgress(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    processed: int


class JobStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: UUID
    status: JobStatus
    file_type: FileType
    progress: JobProgress
    ingested: int
    failed: int
    errors: list[JobErrorItem]
    started_at: datetime | None = None
    completed_at: datetime | None = None


class UploadAcceptedResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: UUID
    status: Literal["PENDING"] = "PENDING"
    message: str = Field(default="File accepted. Processing started in background.")
    poll_url: str


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = "ok"
    db: str = "connected"
