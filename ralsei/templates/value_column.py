from __future__ import annotations
from typing import Any
from psycopg.sql import SQL, Composed, Identifier, Placeholder

from .column import Column, ColumnRendered
from ralsei.renderer import RalseiRenderer

_FROM_NAME = object()


class ValueColumn(Column):
    def __init__(self, name: str, type: str, value: Any = _FROM_NAME):
        super().__init__(name, type)
        self.value = Placeholder(name) if value == _FROM_NAME else value

    def render(
        self, renderer: RalseiRenderer, params: dict = {}
    ) -> ValueColumnRendered:
        return ValueColumnRendered(super().render(renderer, params), self.value)


class ValueColumnRendered(ColumnRendered):
    def __init__(self, column: ColumnRendered, value: Any):
        super().__init__(column.name, column.type)
        self.value = value

    @property
    def definition(self) -> Composed:
        return super().__sql__()

    @property
    def set(self) -> Composed:
        return SQL("{} = {}").format(self.ident, self.value)


class IdColumn:
    def __init__(self, name: str, value: Any = _FROM_NAME):
        self.name = name
        self.value = Placeholder(name) if value == _FROM_NAME else value

    def __sql__(self) -> Composed:
        return SQL("{} = {}").format(Identifier(self.name), self.value)
