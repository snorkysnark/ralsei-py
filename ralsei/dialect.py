from typing import Callable

from ralsei.sql_adapter import ToSql
from ralsei.types import Sql


class BaseDialectInfo:
    autoincrement_key: ToSql = Sql("SERIAL PRIMARY KEY")
    supports_column_if_not_exists: bool = True
    supports_rowcount: bool = True


type DialectLike = BaseDialectInfo | type[BaseDialectInfo]

dialect_registry: dict[str, DialectLike] = {}


def register_dialect[T: DialectLike](driver: str) -> Callable[[T], T]:
    def wrapper(dialect: T):
        dialect_registry[driver] = dialect
        return dialect

    return wrapper


@register_dialect("postgresql")
class PostgresDialectInfo(BaseDialectInfo):
    pass


@register_dialect("sqlite")
class SqliteDialectInfo(BaseDialectInfo):
    name = "sqlite"
    autoincrement_key = Sql("INTEGER PRIMARY KEY AUTOINCREMENT")
    supports_column_if_not_exists = False
    supports_rowcount = False


__all__ = [
    "BaseDialectInfo",
    "PostgresDialectInfo",
    "SqliteDialectInfo",
    "register_dialect",
]
