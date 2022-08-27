from dataclasses import dataclass
from typing import Any

from psycopg.sql import SQL, Identifier, Placeholder

from .renderer import Column, ColumnRendered, RalseiRenderer

_FROM_NAME = object()


class ValueColumn:
    def __init__(self, name: str, type: str, value: Any = _FROM_NAME):
        self.column = Column(name, type)

        if value == _FROM_NAME:
            self.value = Placeholder(name)
        else:
            self.value = value

    def render(self, renderer: RalseiRenderer, params: dict = {}):
        return ValueColumnRendered(self.column.render(renderer, params), self.value)


@dataclass
class ValueColumnRendered:
    column: ColumnRendered
    value: Any

    @property
    def definition(self):
        return self.column.definition

    @property
    def ident(self):
        return self.column.ident

    @property
    def set(self):
        return SQL("{} = {}").format(self.ident, self.value)

    @property
    def add(self):
        return self.column.add

    @property
    def drop_if_exists(self):
        return self.column.drop_if_exists


class IdColumn:
    def __init__(self, name: str, value: Any = _FROM_NAME):
        self.name = name

        if value == _FROM_NAME:
            self.value = Placeholder(name)
        else:
            self.value = value

    def __sql__(self):
        return SQL("{} = {}").format(Identifier(self.name), self.value)
