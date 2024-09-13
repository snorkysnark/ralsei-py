from typing import Callable

from ralsei.types import Sql, ToSql
from ralsei.console import console


class BaseDialectInfo:
    autoincrement_key: ToSql = Sql("SERIAL PRIMARY KEY")
    supports_column_if_not_exists: bool = True
    supports_rowcount: bool = True


type DialectInfo = BaseDialectInfo | type[BaseDialectInfo]


_dialect_map: dict[str, DialectInfo] = {}


def register_dialect[D: DialectInfo](driver: str) -> Callable[[D], D]:
    def decorator(dialect: D):
        _dialect_map[driver] = dialect
        return dialect

    return decorator


def get_dialect(driver: str) -> DialectInfo:
    if dialect := _dialect_map.get(driver, None):
        return dialect
    else:
        console.log("Unknown sql driver:", driver)
        return BaseDialectInfo


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
    "DialectInfo",
    "register_dialect",
    "get_dialect",
]
