import inspect
from typing import Any, Callable


class SqlAdapter:
    """Transform values into their SQL representation"""

    def __init__(self) -> None:
        self._mapping: dict[type, Callable[[Any], str]] = {}

    def register_type[T](self, type_: type[T], to_sql: Callable[[T], str]):
        """Register SQL renderer function for a type"""

        self._mapping[type_] = to_sql

    def to_sql(self, value: Any) -> str:
        """Get SQL representation of a value"""

        for parent_class in inspect.getmro(type(value)):
            if parent_class in self._mapping:
                return self._mapping[parent_class](value)

        raise KeyError("Unsupported type", type(value))

    def format(self, source: str, /, *args, **kwargs) -> str:
        """Similar to :py:meth:`str.format`, but applies :py:meth:`~SqlAdapter.to_sql` to each parameter"""

        return source.format(
            *map(self.to_sql, args),
            **{key: self.to_sql(value) for key, value in kwargs.items()},
        )


__all__ = ["SqlAdapter"]
