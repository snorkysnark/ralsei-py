from __future__ import annotations
from typing import Any, TypeVar, Generic, cast

from .primitives import Sql, Identifier
from ..adapter import ToSql
from ..environment import SqlEnvironment


TYPE = TypeVar("TYPE", str, Sql)


class Column(Generic[TYPE]):
    def __init__(self, name: str, type: TYPE) -> None:
        self.name = name
        self.type = type

    @property
    def identifier(self) -> Identifier:
        return Identifier(self.name)

    @property
    def definition(self) -> ColumnDefinition:
        if not isinstance(self.type, Sql):
            raise RuntimeError(
                "You must pre-render the column before requesting its definition"
            )

        return ColumnDefinition(cast(Column[Sql], self))

    def render(self, env: SqlEnvironment, /, **params: Any) -> Column[Sql]:
        return (
            cast(Column[Sql], self)
            if isinstance(self.type, Sql)
            else Column(self.name, Sql(env.render(self.type, **params)))
        )


class ColumnDefinition(ToSql):
    def __init__(self, column: Column[Sql]) -> None:
        self.column = column

    def to_sql(self, env: SqlEnvironment) -> str:
        return env.adapter.format("{} {}", self.column.identifier, self.column.type)


__all__ = ["Column", "ColumnDefinition"]
