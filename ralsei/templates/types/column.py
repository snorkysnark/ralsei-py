from __future__ import annotations
from typing import Any

from .renderable import RendersToSelf
from .primitives import Sql, Identifier
from ..adapter import SqlAdapter, ToSql
from ..environment import SqlEnvironment


class Column:
    def __init__(self, name: str, type: str) -> None:
        self.name = name
        self.type_template = type

    def render(self, env: SqlEnvironment, **params: Any) -> ColumnRendered:
        return ColumnRendered(self.name, env.render(self.type_template, **params))


class ColumnRendered(RendersToSelf):
    def __init__(self, name: str, type: str) -> None:
        self.name = name
        self.type = Sql(type)

    @property
    def identifier(self) -> Identifier:
        return Identifier(self.name)

    @property
    def definition(self) -> ColumnDefinition:
        return ColumnDefinition(self)


class ColumnDefinition(ToSql):
    def __init__(self, column: ColumnRendered) -> None:
        self.column = column

    def to_sql(self, adapter: SqlAdapter) -> str:
        return adapter.format("{} {}", self.column.identifier, self.column.type)


__all__ = ["Column", "ColumnRendered", "ColumnDefinition"]
