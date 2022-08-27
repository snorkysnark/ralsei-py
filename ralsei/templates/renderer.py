from __future__ import annotations
from dataclasses import dataclass
from psycopg.sql import SQL, Identifier, Composed
from jinja_psycopg import JinjaPsycopg


@dataclass
class Column:
    name: str
    type: str
    add_if_not_exists: bool = False

    def render(self, renderer: RalseiRenderer, params: dict = {}):
        return ColumnRendered(
            name=self.name,
            type=renderer.render(self.type, params),
            add_if_not_exists=self.add_if_not_exists,
        )


@dataclass
class ColumnRendered:
    name: str
    type: Composed
    add_if_not_exists: bool = False

    def __sql__(self):
        return SQL("{} {}").format(Identifier(self.name), self.type)

    @property
    def definition(self):
        return SQL("{} {}").format(Identifier(self.name), self.type)

    @property
    def ident(self):
        return Identifier(self.name)

    @property
    def add(self):
        return SQL("ADD COLUMN {if_not_exists}{name} {type}").format(
            if_not_exists=SQL("IF NOT EXISTS ") if self.add_if_not_exists else SQL(""),
            name=Identifier(self.name),
            type=self.type,
        )

    @property
    def drop_if_exists(self):
        return SQL("DROP COLUMN IF EXISTS {}").format(Identifier(self.name))


class RalseiRenderer(JinjaPsycopg):
    def _prepare_environment(self):
        super()._prepare_environment()

        self._env.globals["Column"] = Column


DEFAULT_RENDERER = RalseiRenderer()
