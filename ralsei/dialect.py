import sqlalchemy
from dataclasses import dataclass


@dataclass
class DialectMetadata:
    supports_column_if_not_exists: bool = True
    supports_rowcount: bool = True


@dataclass
class DialectInfo:
    sqlalchemy: sqlalchemy.Dialect
    meta: DialectMetadata

    @property
    def name(self) -> str:
        return self.sqlalchemy.name
