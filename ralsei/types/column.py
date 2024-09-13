from __future__ import annotations
from typing import TYPE_CHECKING, Any
from abc import ABC, abstractmethod

from .to_sql import ToSql
from .primitives import Sql, Identifier

if TYPE_CHECKING:
    from ralsei.jinja import ISqlEnvironment


class ColumnBase(ABC):
    def __init__(self, name: str) -> None:
        self.name = name

    @property
    def identifier(self) -> Identifier:
        return Identifier(self.name)

    @abstractmethod
    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ColumnRendered: ...


class Column(ColumnBase):
    def __init__(self, name: str, type: str) -> None:
        super().__init__(name)
        self._template = type

    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ColumnRendered:
        return ColumnRendered(self.name, env.render(self._template, **params))


class ColumnRendered(ColumnBase):
    def __init__(self, name: str, type: str) -> None:
        super().__init__(name)
        self.type = Sql(type)

    @property
    def definition(self):
        return ColumnDefinition(self)

    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ColumnRendered:
        return self


class ColumnDefinition(ToSql):
    def __init__(self, column: ColumnRendered) -> None:
        self.column = column

    def to_sql(self, env: "ISqlEnvironment") -> str:
        return env.adapter.format("{} {}", self.column.identifier, self.column.type)


__all__ = ["ColumnBase", "Column", "ColumnRendered", "ColumnDefinition"]
