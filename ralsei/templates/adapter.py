from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, TypeVar
import inspect

T = TypeVar("T")


class SqlAdapter:
    def __init__(
        self, dialect: str, mapping: Optional[dict[type, Callable[[Any], str]]] = None
    ) -> None:
        self._mapping = mapping or {
            str: lambda value: "'{}'".format(value.replace("'", "''")),
            int: str,
            float: str,
            ToSql: lambda value: value.to_sql(self),
        }
        self._dialect = dialect

    @property
    def dialect(self) -> str:
        return self._dialect

    def register_type(self, type_: type[T], to_sql: Callable[[T], str]):
        self._mapping[type_] = to_sql

    def to_sql(self, value: Any) -> str:
        for parent_class in inspect.getmro(type(value)):
            if parent_class in self._mapping:
                return self._mapping[parent_class](value)

        raise KeyError("Unsupported type", type(value))

    def format(self, source: str, /, *args, **kwargs) -> str:
        return source.format(
            *map(self.to_sql, args),
            **{key: self.to_sql(value) for key, value in kwargs.items()},
        )


class ToSql(ABC):
    @abstractmethod
    def to_sql(self, adapter: SqlAdapter) -> str:
        ...
