from __future__ import annotations
from typing import Any, TypeVar

from .primitives import Identifier, Sql, Placeholder
from .column import Column
from ..adapter import ToSql
from ..environment import SqlEnvironment


FROM_NAME = object()


def infer_value(name: str, value: Any = FROM_NAME) -> Any:
    return Placeholder(name) if value == FROM_NAME else value


TYPE = TypeVar("TYPE", str, Sql)


class ValueColumn(Column[TYPE]):
    def __init__(self, name: str, type: TYPE, value: Any = FROM_NAME) -> None:
        super().__init__(name, type)
        self.value = infer_value(name, value)

    @property
    def set_statement(self) -> ValueColumnSetStatement:
        return ValueColumnSetStatement(self)


class ValueColumnSetStatement(ToSql):
    def __init__(self, value_column: ValueColumn) -> None:
        self.value_column = value_column

    def to_sql(self, env: SqlEnvironment) -> str:
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

    def to_sql(self, env: SqlEnvironment) -> str:
        return env.adapter.format("{} = {}", self.identifier, self.value)


__all__ = ["ValueColumn", "ValueColumnSetStatement", "IdColumn"]
