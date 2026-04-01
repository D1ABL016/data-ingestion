from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_ingestion.models.store import Store
from data_ingestion.models.user import User


async def stores_and_users_exist(session: AsyncSession) -> bool:
    st = await session.execute(select(Store.id).limit(1))
    us = await session.execute(select(User.id).limit(1))
    return st.scalar_one_or_none() is not None and us.scalar_one_or_none() is not None
