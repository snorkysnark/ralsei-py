from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from psycopg.sql import SQL, Identifier, Composed

from ralsei.renderer import RalseiRenderer


@dataclass
class Table:
    name: str
    schema: Optional[str] = None

    def __sql__(self):
        if self.schema:
            return SQL("{}.{}").format(Identifier(self.schema), Identifier(self.name))
        else:
            return Identifier(self.name)


@dataclass
class Column:
    name: str
    type: str

    def render(self, renderer: RalseiRenderer, params: dict = {}):
        return ColumnRendered(self.name, renderer.render(self.type, params))


@dataclass
class ColumnRendered:
    name: str
    type: Composed

    def __sql__(self):
        return SQL("{} {}").format(Identifier(self.name), self.type)
