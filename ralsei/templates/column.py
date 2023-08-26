from __future__ import annotations
from typing import TYPE_CHECKING
from dataclasses import dataclass
from psycopg.sql import SQL, Identifier, Composed

# Avoid circular import
if TYPE_CHECKING:
    from ralsei.renderer import RalseiRenderer


@dataclass
class Column:
    name: str
    type: str

    def render(self, renderer: RalseiRenderer, params: dict = {}) -> ColumnRendered:
        return ColumnRendered(
            name=self.name,
            type=renderer.render(self.type, params),
        )


@dataclass
class ColumnRendered:
    name: str
    type: Composed

    def __sql__(self) -> Composed:
        return SQL("{} {}").format(Identifier(self.name), self.type)

    @property
    def ident(self) -> Identifier:
        return Identifier(self.name)

    def add(self, if_not_exists: bool) -> Composed:
        return SQL("ADD COLUMN {if_not_exists}{name} {type}").format(
            if_not_exists=SQL("IF NOT EXISTS ") if if_not_exists else SQL(""),
            name=Identifier(self.name),
            type=self.type,
        )

    def drop(self, if_exists: bool) -> Composed:
        return SQL("DROP COLUMN {if_exists}{name}").format(
            if_exists=SQL("IF EXISTS ") if if_exists else SQL(""),
            name=Identifier(self.name),
        )
