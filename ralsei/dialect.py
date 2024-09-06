from __future__ import annotations
from typing import Optional

from ralsei.sql_adapter import ToSql
from ralsei.types import Sql


class BaseDialectInfo:
    name: str = "base"
    autoincrement_key: ToSql = Sql("SERIAL PRIMARY KEY")
    supports_column_if_not_exists: bool = True
    supports_rowcount: bool = True


class PostgresDialectInfo(BaseDialectInfo):
    name = "postgres"


class SqliteDialectInfo(BaseDialectInfo):
    name = "sqlite"
    autoincrement_key = Sql("INTEGER PRIMARY KEY AUTOINCREMENT")
    supports_column_if_not_exists = False
    supports_rowcount = False


class DialectNotFoundError(KeyError):
    pass


class DialectRegistry:
    def __init__(self, by_name: Optional[dict[str, BaseDialectInfo]]) -> None:
        self._by_name = by_name or {}

    def register(self, dialect_name: str, dialect_info: BaseDialectInfo):
        self._by_name[dialect_name] = dialect_info

    def get_info(self, dialect_name: str) -> BaseDialectInfo:
        dialect_info = self._by_name.get(dialect_name, None)
        if not dialect_info:
            raise DialectNotFoundError(dialect_name)

        return dialect_info

    def copy(self) -> DialectRegistry:
        return DialectRegistry(self._by_name.copy())


DEFAULT_DIALECT_REGISTRY = DialectRegistry(
    {
        "postgresql": PostgresDialectInfo(),
        "sqlite": SqliteDialectInfo(),
    }
)

__all__ = [
    "BaseDialectInfo",
    "PostgresDialectInfo",
    "SqliteDialectInfo",
    "DialectNotFoundError",
    "DialectRegistry",
    "DEFAULT_DIALECT_REGISTRY",
]
