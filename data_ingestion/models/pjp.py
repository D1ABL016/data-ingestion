from datetime import date

from sqlalchemy import Boolean, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from data_ingestion.database import Base


class PermanentJourneyPlan(Base):
    __tablename__ = "permanent_journey_plans"
    __table_args__ = (
        UniqueConstraint("user_id", "store_id", "date", name="uq_pjp_user_store_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    store_id: Mapped[int] = mapped_column(ForeignKey("stores.id"), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
