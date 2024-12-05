import inspect
from typing import TYPE_CHECKING, Any, Callable
from ralsei.types import ToSql

if TYPE_CHECKING:
    from .environment import SqlEnvironment


class SqlAdapter:
    """Transform values into their SQL representation"""

    def __init__(
        self, mapping: dict[type, Callable[["SqlEnvironment", Any], str]] | None = None
    ) -> None:
        self._mapping = mapping or {}

    def register_type[
        T
    ](self, type_: type[T], to_sql: Callable[["SqlEnvironment", T], str]):
        """Register SQL renderer function for a type"""

        self._mapping[type_] = to_sql

    def to_sql(self, env: "SqlEnvironment", value: Any) -> str:
        """Get SQL representation of a value"""

        for parent_class in inspect.getmro(type(value)):
            if parent_class in self._mapping:
                return self._mapping[parent_class](env, value)

        raise KeyError("Unsupported type", type(value))

    def copy(self) -> "SqlAdapter":
        return SqlAdapter({**self._mapping})


default_adapter = SqlAdapter()
default_adapter.register_type(
    str, lambda _, value: "'{}'".format(value.replace("'", "''"))
)
default_adapter.register_type(int, lambda _, value: str(value))
default_adapter.register_type(float, lambda _, value: str(value))
default_adapter.register_type(type(None), lambda _, value: "NULL")
default_adapter.register_type(ToSql, lambda env, value: value.to_sql(env))


__all__ = ["SqlAdapter", "default_adapter"]
