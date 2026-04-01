from sqlalchemy import Boolean, Float, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from data_ingestion.database import Base


class Store(Base):
    __tablename__ = "stores"
    __table_args__ = (Index("ix_stores_store_external_id", "store_external_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    store_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    store_external_id: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    store_brand_id: Mapped[int | None] = mapped_column(ForeignKey("store_brands.id"), nullable=True)
    store_type_id: Mapped[int | None] = mapped_column(ForeignKey("store_types.id"), nullable=True)
    city_id: Mapped[int | None] = mapped_column(ForeignKey("cities.id"), nullable=True)
    state_id: Mapped[int | None] = mapped_column(ForeignKey("states.id"), nullable=True)
    country_id: Mapped[int | None] = mapped_column(ForeignKey("countries.id"), nullable=True)
    region_id: Mapped[int | None] = mapped_column(ForeignKey("regions.id"), nullable=True)
    latitude: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    longitude: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
