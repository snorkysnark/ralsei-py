from __future__ import annotations
from abc import ABC, abstractproperty
from typing import Optional
import sqlalchemy

from ralsei.sql_adapter import ToSql
from ralsei.types import Sql


class DialectRegistry:
    def __init__(self) -> None:
        self._by_dialect = {}
        self._by_driver = {}

    @staticmethod
    def create_default() -> DialectRegistry:
        registry = DialectRegistry()
        registry.register_dialect("postgresql", PostgresDialect)
        registry.register_dialect("sqlite", SqliteDialect)
        return registry

    def register_dialect(
        self,
        dialect_name: str,
        dialect_class: type[Dialect],
        driver: Optional[str] = None,
    ):
        if driver:
            self._by_driver[f"{dialect_name}+{driver}"] = dialect_class
        else:
            self._by_dialect[dialect_name] = dialect_class

    def from_sqlalchemy(self, sqlalchemy_dialect: sqlalchemy.Dialect) -> Dialect:
        dialect_type = self._by_driver.get(
            f"{sqlalchemy_dialect.name}+{sqlalchemy_dialect.driver}", None
        ) or self._by_dialect.get(sqlalchemy_dialect.name, None)

        if dialect_type:
            return dialect_type(sqlalchemy_dialect)
        else:
            raise KeyError(
                f"Dialect not found: {sqlalchemy_dialect.name}+{sqlalchemy_dialect.driver}"
            )


class Dialect(ABC):
    def __init__(self, sqlalchemy: sqlalchemy.Dialect) -> None:
        self.sqlalchemy = sqlalchemy

    @abstractproperty
    def autoincrement_key(self) -> ToSql:
        ...

    @property
    def supports_column_if_not_exists(self) -> bool:
        return True

    @property
    def name(self):
        return self.sqlalchemy.name


class PostgresDialect(Dialect):
    @property
    def autoincrement_key(self) -> ToSql:
        return Sql("SERIAL PRIMARY KEY")


class SqliteDialect(Dialect):
    @property
    def autoincrement_key(self) -> ToSql:
        return Sql("INTEGER PRIMARY KEY AUTOINCREMENT")

    @property
    def supports_column_if_not_exists(self) -> bool:
        return False


default_registry = DialectRegistry.create_default()

__all__ = [
    "Dialect",
    "DialectRegistry",
    "default_registry",
    "PostgresDialect",
    "SqliteDialect",
]
