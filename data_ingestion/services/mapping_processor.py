from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from data_ingestion.models.pjp import PermanentJourneyPlan
from data_ingestion.models.store import Store
from data_ingestion.models.user import User
from data_ingestion.schemas.mapping_schema import MappingRowIn
from data_ingestion.services.job_updates import append_job_progress, finalize_job, load_job
from data_ingestion.services.validation_errors import pydantic_errors_to_records
from data_ingestion.utils.csv_headers import count_data_rows_after_header
from data_ingestion.utils.csv_headers import read_csv_with_strict_headers
from data_ingestion.utils.csv_headers import validate_headers_only

CHUNK_SIZE = 1000


def _row_dict(series: pd.Series) -> dict[str, Any]:
    return {str(k).strip().lower(): v for k, v in series.items()}


def _line_no(header_idx: int, data_index: int) -> int:
    return header_idx + 2 + data_index


async def _user_id_map(session: AsyncSession, user_keys: set[str]) -> dict[str, int]:
    if not user_keys:
        return {}
    stmt = select(User.id, User.username).where(func.lower(User.username).in_(user_keys))
    res = await session.execute(stmt)
    out: dict[str, int] = {}
    for uid, uname in res.fetchall():
        out[str(uname).strip().lower()] = int(uid)
    return out


async def _store_pk_map(session: AsyncSession, store_ids: set[str]) -> dict[str, int]:
    if not store_ids:
        return {}
    keys = {s.strip() for s in store_ids if s and str(s).strip()}
    stmt = select(Store.id, Store.store_id).where(Store.store_id.in_(keys))
    res = await session.execute(stmt)
    return {str(sid).strip(): int(pk) for pk, sid in res.fetchall()}


async def process_mappings_file(session: AsyncSession, job_id: UUID, filepath: str) -> None:
    job = await load_job(session, job_id)
    if not job:
        return

    header_idx = validate_headers_only(filepath, "mappings")
    total = count_data_rows_after_header(filepath, header_idx)

    job.total_rows = total
    job.status = "PROCESSING"
    await session.commit()

    data_index = 0
    chunks = read_csv_with_strict_headers(filepath, "mappings", chunksize=CHUNK_SIZE)

    try:
        for chunk in chunks:
            chunk_rows = int(chunk.shape[0])
            chunk_errors: list[dict[str, Any]] = []
            pending: list[tuple[int, MappingRowIn]] = []

            for _, series in chunk.iterrows():
                line_no = _line_no(header_idx, data_index)
                data_index += 1
                raw = _row_dict(series)
                try:
                    row_in = MappingRowIn.model_validate(raw)
                except ValidationError as e:
                    chunk_errors.extend(pydantic_errors_to_records(e, line_no))
                    continue
                pending.append((line_no, row_in))

            user_keys = {r.username.strip().lower() for _, r in pending}
            store_keys = {r.store_id.strip() for _, r in pending}
            uid_map = await _user_id_map(session, user_keys)
            sid_map = await _store_pk_map(session, store_keys)

            to_insert: list[tuple[int, str, dict[str, Any]]] = []
            for line_no, r in pending:
                u_l = r.username.strip().lower()
                uid = uid_map.get(u_l)
                if uid is None:
                    chunk_errors.append(
                        {
                            "row": line_no,
                            "column": "username",
                            "value": r.username,
                            "reason": "User not found in database",
                        }
                    )
                    continue
                sk = r.store_id.strip()
                sid = sid_map.get(sk)
                if sid is None:
                    chunk_errors.append(
                        {
                            "row": line_no,
                            "column": "store_id",
                            "value": r.store_id,
                            "reason": "Store not found in database",
                        }
                    )
                    continue
                to_insert.append(
                    (
                        line_no,
                        r.username,
                        {
                            "user_id": uid,
                            "store_id": sid,
                            "date": r.date,
                            "is_active": bool(r.is_active),
                        },
                    )
                )

            ingested = 0
            if to_insert:
                maps = [t[2] for t in to_insert]

                def _bulk(sync_session, mps: list[dict[str, Any]] = maps) -> None:
                    sync_session.bulk_insert_mappings(PermanentJourneyPlan, mps)

                try:
                    await session.run_sync(_bulk)
                    ingested = len(maps)
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    for line_no, uname, one_map in to_insert:

                        def _one(sync_session, om: dict[str, Any] = one_map) -> None:
                            sync_session.bulk_insert_mappings(PermanentJourneyPlan, [om])

                        try:
                            await session.run_sync(_one)
                            await session.commit()
                            ingested += 1
                        except IntegrityError:
                            await session.rollback()
                            chunk_errors.append(
                                {
                                    "row": line_no,
                                    "column": "username",
                                    "value": uname,
                                    "reason": "Duplicate mapping (user_id, store_id, date) or constraint violation",
                                }
                            )

            job = await load_job(session, job_id)
            if not job:
                return
            await append_job_progress(
                session,
                job,
                ingested_delta=ingested,
                failed_delta=chunk_rows - ingested,
                processed_delta=chunk_rows,
                new_errors=chunk_errors or None,
            )

        job = await load_job(session, job_id)
        if job:
            await finalize_job(session, job, failed_final=False)
    except Exception:
        job = await load_job(session, job_id)
        if job:
            job.status = "FAILED"
            job.completed_at = datetime.now(timezone.utc)
            await session.commit()
        raise
