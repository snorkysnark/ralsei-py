from __future__ import annotations
from typing import Any

from .primitives import Identifier, Placeholder
from .column import Column, ColumnRendered
from ..adapter import ToSql, SqlAdapter
from ..environment import SqlEnvironment


FROM_NAME = object()


def infer_value(name: str, value: Any = FROM_NAME) -> Any:
    return Placeholder(name) if value == FROM_NAME else value


class ValueColumn(Column):
    def __init__(self, name: str, type: str, value: Any = FROM_NAME) -> None:
        super().__init__(name, type)
        self.value = infer_value(name, value)

    def render(self, env: SqlEnvironment, /, **params: Any) -> ValueColumnRendered:
        return ValueColumnRendered(
            self.name, env.render(self.type_template, **params), self.value
        )


class ValueColumnRendered(ColumnRendered):
    def __init__(self, name: str, type: str, value: Any = FROM_NAME) -> None:
        super().__init__(name, type)
        self.value = infer_value(name, value)

    @property
    def set_statement(self) -> ValueColumnSetStatement:
        return ValueColumnSetStatement(self)


class ValueColumnSetStatement(ToSql):
    def __init__(self, value_column: ValueColumnRendered) -> None:
        self.value_column = value_column

    def to_sql(self, adapter: SqlAdapter) -> str:
        return adapter.format(
            "{} = {}", self.value_column.identifier, self.value_column.value
        )


class IdColumn(ToSql):
    def __init__(self, name: str, value: Any = FROM_NAME) -> None:
        self.name = name
        self.value = infer_value(name, value)

    @property
    def identifier(self) -> Identifier:
        return Identifier(self.name)

    def to_sql(self, adapter: SqlAdapter) -> str:
        return adapter.format("{} = {}", self.identifier, self.value)


__all__ = ["ValueColumn", "ValueColumnRendered", "ValueColumnSetStatement", "IdColumn"]
