from __future__ import annotations
import sqlalchemy
from dataclasses import dataclass

from .types import Sql


@dataclass()
class DialectInfo:
    sqlalchemy: sqlalchemy.Dialect
    serial_primary_key: Sql

    @staticmethod
    def from_sqlalchemy(dialect: sqlalchemy.Dialect) -> DialectInfo:
        return DialectInfo(
            dialect,
            Sql(
                "INTEGER PRIMARY KEY AUTOINCREMENT"
                if dialect.name == "sqlite"
                else "SERIAL PRIMARY KEY"
            ),
        )

    @property
    def name(self):
        return self.sqlalchemy.name
