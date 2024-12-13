import sqlalchemy
from dataclasses import dataclass

from ralsei.types import ToSql


@dataclass
class DialectMetadata:
    autoincrement_primary_key: ToSql
    supports_column_if_not_exists: bool = True
    supports_rowcount: bool = True


@dataclass
class DialectInfo:
    sqlalchemy: sqlalchemy.Dialect
    meta: DialectMetadata
