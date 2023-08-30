from __future__ import annotations
from typing import Any
from psycopg.sql import SQL, Composed, Identifier, Placeholder

from .column import Column, ColumnRendered
from ralsei.renderer import RalseiRenderer

_FROM_NAME = object()


class ValueColumn(Column):
    def __init__(self, name: str, type: str, value: Any = _FROM_NAME):
        """
        Defines a column that should be filled with some value

        Args:
            name: column name
            type: Everything that comes after column name:
                data type, `REFERENCES`, `UNIQUE`, `DEFAULT`, etc.

                Can be a jinja template
            value: value that will be baked into the `INSERT` statement.
                Can be a SQL placeholder or a constant

                By default, equals to a SQL placeholder
                    with the same name as the column
        """
        super().__init__(name, type)
        self.value = Placeholder(name) if value == _FROM_NAME else value

    def render(
        self, renderer: RalseiRenderer, params: dict = {}
    ) -> ValueColumnRendered:
        """
        Put through the jinja renderer

        Args:
            renderer: jinja sql renderer
            params: template parameters

        Returns:
            column with the `type` field rendered
        """
        return ValueColumnRendered(super().render(renderer, params), self.value)


class ValueColumnRendered(ColumnRendered):
    def __init__(self, column: ColumnRendered, value: Any):
        """
        Defines a column that should be filled with some value

        Args:
            column: column name and type
            value: value that will be baked into the `INSERT` statement.
                Can be a SQL placeholder or a constant
        """
        super().__init__(column.name, column.type)
        self.value = value

    @property
    def definition(self) -> Composed:
        """
        Returns:
            column definition (name + type)
        """
        return super().__sql__()

    @property
    def set(self) -> Composed:
        """
        `SET` statement contents

        Returns:
            name = value
        """
        return SQL("{} = {}").format(self.ident, self.value)


class IdColumn:
    def __init__(self, name: str, value: Any = _FROM_NAME):
        """
        Defines a column value that uniquely identifies a particular row

        Args:
            name: column name
            value: value that will be baked into a `WHERE` statement.
                Can be a SQL placeholder or a constant

                By default, equals to a SQL placeholder
                with the same name as the column
        """
        self.name = name
        self.value = Placeholder(name) if value == _FROM_NAME else value

    def __sql__(self) -> Composed:
        """
        sql representation

        Returns:
            name = value
        """
        return SQL("{} = {}").format(Identifier(self.name), self.value)
