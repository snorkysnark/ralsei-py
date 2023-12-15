from abc import ABC, abstractmethod
from typing import Any, Callable, TypeVar
import inspect

T = TypeVar("T")


class SqlAdapter:
    def __init__(self) -> None:
        self._type_adapters = {}

    def register_type(self, type_: type[T], to_sql: Callable[[T], str]):
        self._type_adapters[type_] = to_sql

    def to_sql(self, value: Any) -> str:
        for parent_class in inspect.getmro(type(value)):
            if parent_class in self._type_adapters:
                return self._type_adapters[parent_class](value)

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


def create_default_adapter(dialect: str) -> SqlAdapter:
    adapter = SqlAdapter()
    adapter.register_type(str, lambda value: "'{}'".format(value.replace("'", "''")))
    adapter.register_type(int, str)
    adapter.register_type(float, str)
    adapter.register_type(ToSql, lambda value: value.to_sql(adapter))

    return adapter
