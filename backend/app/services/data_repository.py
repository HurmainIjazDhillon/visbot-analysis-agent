from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings


class DataQueryError(RuntimeError):
    pass


class DataRepository:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._engine = create_engine(self._normalize_database_url(self._settings.database_url), future=True)

    def execute_query(self, query: str) -> list[dict]:
        try:
            with self._engine.connect() as connection:
                result = connection.execute(text(query))
                return [self._normalize_row(dict(row._mapping)) for row in result]
        except SQLAlchemyError as exc:
            raise DataQueryError(f"Database query failed: {exc.__class__.__name__}: {exc}") from exc

    def _normalize_database_url(self, database_url: str) -> str:
        if database_url.startswith("postgresql://"):
            return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
        return database_url

    def _normalize_row(self, row: dict) -> dict:
        normalized: dict = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                normalized[key] = value.strftime("%d %b %Y, %I:%M:%S %p")
            elif isinstance(value, date):
                normalized[key] = value.strftime("%d %b %Y")
            else:
                normalized[key] = value
        return normalized


data_repository = DataRepository()
