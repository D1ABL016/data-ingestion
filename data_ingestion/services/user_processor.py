from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import func, text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from data_ingestion.models.user import User
from data_ingestion.schemas.user_schema import UserRowIn
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


async def _usernames_in_db_lower(session: AsyncSession, names: set[str]) -> set[str]:
    if not names:
        return set()
    stmt = select(func.lower(User.username)).where(func.lower(User.username).in_(names))
    res = await session.execute(stmt)
    return set(res.scalars().all())


async def collect_file_usernames(filepath: str, header_idx: int) -> set[str]:
    cols = list(pd.read_csv(filepath, skiprows=header_idx, nrows=0, header=0).columns)
    uname_col = next((c for c in cols if str(c).strip().lower() == "username"), None)
    if uname_col is None:
        return set()
    dtypes = {uname_col: str}
    names: set[str] = set()
    for chunk in pd.read_csv(
        filepath,
        skiprows=header_idx,
        header=0,
        usecols=[uname_col],
        chunksize=5000,
        dtype=dtypes,
    ):
        for v in chunk[str(uname_col)].tolist():
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip().lower()
            if s:
                names.add(s)
    return names


async def process_users_file(session: AsyncSession, job_id: UUID, filepath: str) -> None:
    job = await load_job(session, job_id)
    if not job:
        return

    header_idx = validate_headers_only(filepath, "users")
    total = count_data_rows_after_header(filepath, header_idx)
    file_usernames = await collect_file_usernames(filepath, header_idx)

    job.total_rows = total
    job.status = "PROCESSING"
    await session.commit()

    seen_usernames: set[str] = set()
    data_index = 0
    supervisor_pairs: list[tuple[str, str]] = []

    chunks = read_csv_with_strict_headers(filepath, "users", chunksize=CHUNK_SIZE)

    try:
        for chunk in chunks:
            chunk_rows = int(chunk.shape[0])
            chunk_errors: list[dict[str, Any]] = []
            candidates: list[tuple[int, UserRowIn]] = []

            sup_needed: set[str] = set()
            for _, series in chunk.iterrows():
                line_no = _line_no(header_idx, data_index)
                data_index += 1
                raw = _row_dict(series)
                try:
                    row_in = UserRowIn.model_validate(raw)
                except ValidationError as e:
                    recs = pydantic_errors_to_records(e, line_no)
                    for rec in recs:
                        if "cannot be their own" in rec.get("reason", "").lower():
                            rec["column"] = "supervisor_username"
                    chunk_errors.extend(recs)
                    continue

                ukey = row_in.username.strip().lower()
                # check if username is already seen in the current chunk
                if ukey in seen_usernames:
                    chunk_errors.append(
                        {
                            "row": line_no,
                            "column": "username",
                            "value": row_in.username,
                            "reason": "Duplicate username in file",
                        }
                    )
                    continue
                seen_usernames.add(ukey)

                if row_in.supervisor_username:
                    sup_needed.add(row_in.supervisor_username.strip().lower())

                candidates.append((line_no, row_in))

            db_sup_exists = await _usernames_in_db_lower(session, sup_needed)

            validated: list[tuple[int, UserRowIn]] = []
            for line_no, row_in in candidates:
                if row_in.supervisor_username:
                    skey = row_in.supervisor_username.strip().lower()
                    if skey not in file_usernames and skey not in db_sup_exists:
                        chunk_errors.append(
                            {
                                "row": line_no,
                                "column": "supervisor_username",
                                "value": row_in.supervisor_username,
                                "reason": "Supervisor username not found in file or database",
                            }
                        )
                        continue
                validated.append((line_no, row_in))

            mappings: list[dict[str, Any]] = []
            for _, r in validated:
                mappings.append(
                    {
                        "username": r.username.strip(),
                        "email": r.email.strip(),
                        "first_name": r.first_name,
                        "last_name": r.last_name,
                        "user_type": int(r.user_type),
                        "phone_number": r.phone_number,
                        "supervisor_id": None,
                        "is_active": bool(r.is_active),
                    }
                )

            ingested = 0
            chunk_pairs: list[tuple[str, str]] = []
            if mappings:

                def _bulk(sync_session, maps: list[dict[str, Any]] = mappings) -> None:
                    sync_session.bulk_insert_mappings(User, maps)

                try:
                    await session.run_sync(_bulk)
                    ingested = len(mappings)
                    await session.commit()
                    for _, r in validated:
                        db_sup_exists.add(r.username.strip().lower())
                        if r.supervisor_username:
                            chunk_pairs.append(
                                (
                                    r.username.strip().lower(),
                                    r.supervisor_username.strip().lower(),
                                )
                            )
                except IntegrityError:
                    await session.rollback()
                    for line_no, r in validated:
                        one = [
                            {
                                "username": r.username.strip(),
                                "email": r.email.strip(),
                                "first_name": r.first_name,
                                "last_name": r.last_name,
                                "user_type": int(r.user_type),
                                "phone_number": r.phone_number,
                                "supervisor_id": None,
                                "is_active": bool(r.is_active),
                            }
                        ]

                        def _one(sync_session, m: list[dict[str, Any]] = one) -> None:
                            sync_session.bulk_insert_mappings(User, m)

                        try:
                            await session.run_sync(_one)
                            await session.commit()
                            ingested += 1
                            db_sup_exists.add(r.username.strip().lower())
                            if r.supervisor_username:
                                chunk_pairs.append(
                                    (
                                        r.username.strip().lower(),
                                        r.supervisor_username.strip().lower(),
                                    )
                                )
                        except IntegrityError:
                            await session.rollback()
                            chunk_errors.append(
                                {
                                    "row": line_no,
                                    "column": "username",
                                    "value": r.username,
                                    "reason": "Duplicate username in database or constraint violation",
                                }
                            )

            supervisor_pairs.extend(chunk_pairs)

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

        if supervisor_pairs:
            upd = text(
                """
                UPDATE users AS u
                SET supervisor_id = s.id
                FROM users AS s
                WHERE lower(u.username) = :uname AND lower(s.username) = :sname
                """
            )
            for u_l, s_l in supervisor_pairs:
                await session.execute(upd, {"uname": u_l, "sname": s_l})
            await session.commit()

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
