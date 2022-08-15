from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from psycopg.sql import SQL, Identifier, Composed
from jinja_psycopg import JinjaPsycopg


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

    @property
    def add(self):
        return SQL("ADD COLUMN {} {}").format(Identifier(self.name), self.type)

    @property
    def drop(self):
        return SQL("DROP COLUMN {}").format(Identifier(self.name))


class RalseiRenderer(JinjaPsycopg):
    def _prepare_environment(self):
        super()._prepare_environment()

        self._env.globals["Column"] = Column


DEFAULT_RENDERER = RalseiRenderer()
