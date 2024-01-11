from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, TypeVar
import inspect

if TYPE_CHECKING:
    from ralsei.jinja import SqlEnvironment

T = TypeVar("T")


class SqlAdapter:
    def __init__(self) -> None:
        self._mapping = {}

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
    def to_sql(self, env: "SqlEnvironment") -> str:
        ...


def create_adapter_for_env(env: "SqlEnvironment"):
    adapter = SqlAdapter()
    adapter.register_type(str, lambda value: "'{}'".format(value.replace("'", "''")))
    adapter.register_type(int, str)
    adapter.register_type(float, str)
    adapter.register_type(ToSql, lambda value: value.to_sql(env))

    return adapter


__all__ = ["SqlAdapter", "ToSql", "create_adapter_for_env"]
