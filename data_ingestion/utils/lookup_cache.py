from typing import Any, Type

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession


async def get_or_create_lookup(
    session: AsyncSession,
    model: Type[Any],
    name: str,
    cache: dict[str, int],
) -> int:
    normalized = name.strip().lower()
    if normalized in cache:
        return cache[normalized]
    stmt = pg_insert(model).values(name=normalized)
    stmt = stmt.on_conflict_do_nothing(index_elements=["name"])
    await session.execute(stmt)
    result = await session.execute(select(model).where(model.name == normalized))
    row = result.scalar_one()
    cache[normalized] = row.id
    return row.id


async def ensure_lookup_ids(
    session: AsyncSession,
    model: Type[Any],
    names: set[str],
    cache: dict[str, int],
) -> None:
    missing = [n for n in names if n and n not in cache]
    if not missing:
        return
    for n in missing:
        stmt = pg_insert(model).values(name=n).on_conflict_do_nothing(index_elements=["name"])
        await session.execute(stmt)
    result = await session.execute(select(model).where(model.name.in_(missing)))
    for row in result.scalars():
        cache[row.name] = row.id
