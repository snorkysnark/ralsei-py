from __future__ import annotations
from typing import TYPE_CHECKING, Any
from abc import ABC, abstractmethod

from .to_sql import ToSql
from .primitives import Sql, Identifier

if TYPE_CHECKING:
    from ralsei.jinja import ISqlEnvironment


class ColumnBase(ABC):
    """Base class for :py:class:`~Column` and :py:class:`~ColumnRendered`"""

    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    @property
    def identifier(self) -> Identifier:
        """:py:attr:`~ColumnBase.name` wrapped in a SQL identifier"""

        return Identifier(self.name)

    @abstractmethod
    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ColumnRendered:
        """Turn into the rendered version"""


class Column(ColumnBase):
    """Column template class

    Args:
        name: column name
        type: jinja template of the column type,
            like ``INT REFERENCES {{other}}(id)``
    """

    def __init__(self, name: str, type: str) -> None:
        super().__init__(name)
        self._template = type

    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ColumnRendered:
        """Render the type template"""

        return ColumnRendered(self.name, env.render(self._template, **params))


class ColumnRendered(ColumnBase):
    """Rendered column class

    Args:
        name: column name
        type: column type as raw sql string,
            like ``INT REFERENCES "other"(id)``
    """

    type: Sql

    def __init__(self, name: str, type: str) -> None:
        super().__init__(name)
        self.type = Sql(type)

    @property
    def definition(self) -> ColumnDefinition:
        """As column definition (name + type)"""

        return ColumnDefinition(self)

    def render(self, env: "ISqlEnvironment", /, **params: Any) -> ColumnRendered:
        return self


class ColumnDefinition(ToSql):
    """Renders to ``table_name TYPE``, for use in table definitions"""

    column: ColumnRendered

    def __init__(self, column: ColumnRendered) -> None:
        self.column = column

    def to_sql(self, env: "ISqlEnvironment") -> str:
        return env.adapter.format("{} {}", self.column.identifier, self.column.type)


__all__ = ["ColumnBase", "Column", "ColumnRendered", "ColumnDefinition"]
