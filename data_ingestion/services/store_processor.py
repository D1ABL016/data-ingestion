from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import pandas as pd
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from data_ingestion.models.lookup import City, Country, Region, State, StoreBrand, StoreType
from data_ingestion.models.store import Store
from data_ingestion.schemas.store_schema import StoreRowIn
from data_ingestion.services.job_updates import append_job_progress, finalize_job, load_job
from data_ingestion.services.validation_errors import pydantic_errors_to_records
from data_ingestion.utils.csv_headers import count_data_rows_after_header, get_header_row_index
from data_ingestion.utils.csv_headers import read_csv_with_strict_headers
from data_ingestion.utils.lookup_cache import ensure_lookup_ids
from data_ingestion.utils.normalize import normalize_optional_lookup

CHUNK_SIZE = 1000

_lookup_models = (StoreBrand, StoreType, City, State, Country, Region)


def _row_dict(series: pd.Series) -> dict[str, Any]:
    return {str(k).strip().lower(): v for k, v in series.items()}


def _line_no(header_idx: int, data_index: int) -> int:
    return header_idx + 2 + data_index


def _lookup_id(cache: dict[str, int], normalized: str | None) -> int | None:
    if not normalized:
        return None
    return cache.get(normalized)


def _store_mapping(r: StoreRowIn, caches: dict[str, dict[str, int]]) -> dict[str, Any]:
    b = normalize_optional_lookup(r.store_brand)
    t = normalize_optional_lookup(r.store_type)
    c = normalize_optional_lookup(r.city)
    st = normalize_optional_lookup(r.state)
    co = normalize_optional_lookup(r.country)
    rg = normalize_optional_lookup(r.region)
    return {
        "store_id": r.store_id.strip(),
        "store_external_id": (r.store_external_id or "")[:255],
        "name": r.name,
        "title": r.title,
        "store_brand_id": _lookup_id(caches["store_brands"], b),
        "store_type_id": _lookup_id(caches["store_types"], t),
        "city_id": _lookup_id(caches["cities"], c),
        "state_id": _lookup_id(caches["states"], st),
        "country_id": _lookup_id(caches["countries"], co),
        "region_id": _lookup_id(caches["regions"], rg),
        "latitude": float(r.latitude if r.latitude is not None else 0.0),
        "longitude": float(r.longitude if r.longitude is not None else 0.0),
        "is_active": bool(r.is_active),
    }


async def process_stores_file(session: AsyncSession, job_id: UUID, filepath: str) -> None:
    job = await load_job(session, job_id)
    if not job:
        return

    header_idx = get_header_row_index(filepath)
    total = count_data_rows_after_header(filepath, header_idx)
    job.total_rows = total
    job.status = "PROCESSING"
    await session.commit()

    lookup_cache: dict[str, dict[str, int]] = {m.__tablename__: {} for m in _lookup_models}
    seen_store_ids: set[str] = set()
    data_index = 0

    chunks = read_csv_with_strict_headers(filepath, "stores", chunksize=CHUNK_SIZE)

    try:
        for chunk in chunks:
            chunk_rows = int(chunk.shape[0])
            chunk_errors: list[dict[str, Any]] = []
            validated: list[tuple[int, StoreRowIn]] = []

            for _, series in chunk.iterrows():
                line_no = _line_no(header_idx, data_index)
                data_index += 1
                raw = _row_dict(series)
                try:
                    row_in = StoreRowIn.model_validate(raw)
                except ValidationError as e:
                    chunk_errors.extend(pydantic_errors_to_records(e, line_no))
                    continue

                sid_key = row_in.store_id.strip().lower()
                if sid_key in seen_store_ids:
                    chunk_errors.append(
                        {
                            "row": line_no,
                            "column": "store_id",
                            "value": row_in.store_id,
                            "reason": "Duplicate store_id in file",
                        }
                    )
                    continue
                seen_store_ids.add(sid_key)
                validated.append((line_no, row_in))

            brand_names: set[str] = set()
            type_names: set[str] = set()
            city_names: set[str] = set()
            state_names: set[str] = set()
            country_names: set[str] = set()
            region_names: set[str] = set()
            for _, r in validated:
                if x := normalize_optional_lookup(r.store_brand):
                    brand_names.add(x)
                if x := normalize_optional_lookup(r.store_type):
                    type_names.add(x)
                if x := normalize_optional_lookup(r.city):
                    city_names.add(x)
                if x := normalize_optional_lookup(r.state):
                    state_names.add(x)
                if x := normalize_optional_lookup(r.country):
                    country_names.add(x)
                if x := normalize_optional_lookup(r.region):
                    region_names.add(x)

            await ensure_lookup_ids(session, StoreBrand, brand_names, lookup_cache["store_brands"])
            await ensure_lookup_ids(session, StoreType, type_names, lookup_cache["store_types"])
            await ensure_lookup_ids(session, City, city_names, lookup_cache["cities"])
            await ensure_lookup_ids(session, State, state_names, lookup_cache["states"])
            await ensure_lookup_ids(session, Country, country_names, lookup_cache["countries"])
            await ensure_lookup_ids(session, Region, region_names, lookup_cache["regions"])

            mappings = [_store_mapping(r, lookup_cache) for _, r in validated]

            ingested = 0
            if mappings:

                def _bulk(sync_session, maps: list[dict[str, Any]] = mappings) -> None:
                    sync_session.bulk_insert_mappings(Store, maps)

                try:
                    await session.run_sync(_bulk)
                    ingested = len(mappings)
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    for line_no, r in validated:
                        one = [_store_mapping(r, lookup_cache)]

                        def _one(sync_session, m: list[dict[str, Any]] = one) -> None:
                            sync_session.bulk_insert_mappings(Store, m)

                        try:
                            await session.run_sync(_one)
                            await session.commit()
                            ingested += 1
                        except IntegrityError:
                            await session.rollback()
                            chunk_errors.append(
                                {
                                    "row": line_no,
                                    "column": "store_id",
                                    "value": r.store_id,
                                    "reason": "Duplicate store_id in database or constraint violation",
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
