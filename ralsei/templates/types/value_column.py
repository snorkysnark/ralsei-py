from __future__ import annotations
from typing import TYPE_CHECKING, Any
from abc import abstractmethod

from .primitives import Identifier, Placeholder
from .column import ColumnBase, Column, ColumnRendered
from ..adapter import ToSql

if TYPE_CHECKING:
    from ..environment import SqlEnvironment


FROM_NAME = object()


def infer_value(name: str, value: Any = FROM_NAME) -> Any:
    return Placeholder(name) if value == FROM_NAME else value


class ValueColumnBase(ColumnBase):
    def __init__(self, name: str, value: Any = FROM_NAME) -> None:
        super().__init__(name)
        self.value = infer_value(name, value)

    @property
    def set_statement(self) -> ValueColumnSetStatement:
        return ValueColumnSetStatement(self)

    @abstractmethod
    def render(self, env: "SqlEnvironment", /, **params: Any) -> ValueColumnRendered:
        ...


class ValueColumn(Column, ValueColumnBase):
    def __init__(self, name: str, type: str, value: Any = FROM_NAME) -> None:
        Column.__init__(self, name, type)
        ValueColumnBase.__init__(self, name, value)

    def render(self, env: "SqlEnvironment", /, **params: Any) -> ValueColumnRendered:
        return ValueColumnRendered(
            self.name, env.render(self._template, **params), self.value
        )


class ValueColumnRendered(ColumnRendered, ValueColumnBase):
    def __init__(self, name: str, type: str, value: Any = FROM_NAME) -> None:
        ColumnRendered.__init__(self, name, type)
        ValueColumnBase.__init__(self, name, value)

    def render(self, env: "SqlEnvironment", /, **params: Any) -> ValueColumnRendered:
        return self


class ValueColumnSetStatement(ToSql):
    def __init__(self, value_column: ValueColumnBase) -> None:
        self.value_column = value_column

    def to_sql(self, env: "SqlEnvironment") -> str:
        return env.adapter.format(
            "{} = {}", self.value_column.identifier, self.value_column.value
        )


class IdColumn(ToSql):
    def __init__(self, name: str, value: Any = FROM_NAME) -> None:
        self.name = name
        self.value = infer_value(name, value)

    @property
    def identifier(self) -> Identifier:
        return Identifier(self.name)

    def to_sql(self, env: "SqlEnvironment") -> str:
        return env.adapter.format("{} = {}", self.identifier, self.value)


__all__ = [
    "ValueColumnBase",
    "ValueColumn",
    "ValueColumnRendered",
    "ValueColumnSetStatement",
    "IdColumn",
]
