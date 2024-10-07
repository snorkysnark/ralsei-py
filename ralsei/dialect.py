from typing import Callable

from ralsei.types import Sql, ToSql
from ralsei.console import console


class BaseDialectInfo:
    """SQL dialect settings"""

    autoincrement_key: ToSql = Sql("SERIAL PRIMARY KEY")
    supports_column_if_not_exists: bool = True
    supports_rowcount: bool = True


type DialectInfo = BaseDialectInfo | type[BaseDialectInfo]
"""You can use both a class instance or a class as dialect"""


_dialect_map: dict[str, DialectInfo] = {}


def register_dialect[D: DialectInfo](driver: str) -> Callable[[D], D]:
    """Decorator for registering a custom dialect

    Example:
        .. code-block:: python

            @register_dialect("duckdb")
            class DuckdbDialectInfo(BaseDialectInfo):
                pass
    """

    def decorator(dialect: D):
        _dialect_map[driver] = dialect
        return dialect

    return decorator


def get_dialect(driver: str) -> DialectInfo:
    """Get DialectInfo for a given sqlalchemy dialect name"""

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
