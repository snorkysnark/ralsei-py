from __future__ import annotations
from typing import TYPE_CHECKING, Any
from abc import abstractmethod

from .to_sql import ToSql
from .primitives import Identifier, Placeholder
from .column import ColumnBase, Column, ColumnRendered

if TYPE_CHECKING:
    from ralsei.jinja import ISqlEnvironment


FROM_NAME = object()


def infer_value(name: str, value: Any = FROM_NAME) -> Any:
    return Placeholder(name) if value == FROM_NAME else value


class ValueColumnBase(ColumnBase):
    """Column and its associated value, base class for :py:class:`~ValueColumn` and :py:class:`~ValueColumnRendered`

    Args:
        name: column name
        value: value that will be applied to the column

            By default, will be set to a :py:class:`ralsei.types.Placeholder` with the same name as the column
    """

    value: Any

    def __init__(self, name: str, value: Any = FROM_NAME) -> None:
        super().__init__(name)
        self.value = infer_value(name, value)

    @property
    def set_statement(self) -> ValueColumnSetStatement:
        """As set statement (name = value)"""

        return ValueColumnSetStatement(self)

    @abstractmethod
    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ValueColumnRendered:
        """Turn into the rendered version"""


class ValueColumn(Column, ValueColumnBase):
    """Column template class with an associated value

    Args:
        name: column name
        type: jinja template of the column type,
            like ``INT REFERENCES {{other}}(id)``
        value: value that will be applied to the column

            By default, will be set to a :py:class:`ralsei.types.Placeholder` with the same name as the column
    """

    def __init__(self, name: str, type: str, value: Any = FROM_NAME) -> None:
        Column.__init__(self, name, type)
        ValueColumnBase.__init__(self, name, value)

    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ValueColumnRendered:
        """Render the type template"""

        return ValueColumnRendered(
            self.name, env.render(self._template, **params), self.value
        )


class ValueColumnRendered(ColumnRendered, ValueColumnBase):
    """Rendered column class with an associated value

    Args:
        name: column name
        type: column type as raw sql string,
            like ``INT REFERENCES "other"(id)``
        value: value that will be applied to the column

            By default, will be set to a :py:class:`ralsei.types.Placeholder` with the same name as the column
    """

    def __init__(self, name: str, type: str, value: Any = FROM_NAME) -> None:
        ColumnRendered.__init__(self, name, type)
        ValueColumnBase.__init__(self, name, value)

    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ValueColumnRendered:
        return self


class ValueColumnSetStatement(ToSql):
    """Renders to ``name = value``, for use in UPDATE statements"""

    value_column: ValueColumnBase

    def __init__(self, value_column: ValueColumnBase) -> None:
        self.value_column = value_column

    def to_sql(self, env: "ISqlEnvironment") -> str:
        return env.adapter.format(
            "{} = {}", self.value_column.identifier, self.value_column.value
        )


class IdColumn(ToSql):
    """Column name and its associated value, used inside a WHERE clause for locating a row

    Renders to ``name = value``

    Args:
        name: column name
        value: value that uniquely identifies a row

            By default, will be set to a :py:class:`ralsei.types.Placeholder` with the same name as the column
    """

    name: str
    value: Any

    def __init__(self, name: str, value: Any = FROM_NAME) -> None:
        self.name = name
        self.value = infer_value(name, value)

    @property
    def identifier(self) -> Identifier:
        """:py:attr:`~IdColumn.name` wrapped in a SQL identifier"""

        return Identifier(self.name)

    def to_sql(self, env: "ISqlEnvironment") -> str:
        return env.adapter.format("{} = {}", self.identifier, self.value)


__all__ = [
    "ValueColumnBase",
    "ValueColumn",
    "ValueColumnRendered",
    "ValueColumnSetStatement",
    "IdColumn",
]
