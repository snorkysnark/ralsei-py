from __future__ import annotations
from typing import TYPE_CHECKING
from dataclasses import dataclass
from psycopg.sql import SQL, Identifier, Composed

# Avoid circular import
if TYPE_CHECKING:
    from ralsei.renderer import RalseiRenderer


@dataclass
class Column:
    """
    Column definition in `ADD COLUMN` or `CREATE TABLE` statement

    Attributes:
        name: column name
        type: Everything that comes after column name:
            data type, `REFERENCES`, `UNIQUE`, `DEFAULT`, etc.

            Can be a jinja template
    """

    name: str
    type: str

    def render(self, renderer: RalseiRenderer, params: dict = {}) -> ColumnRendered:
        """
        Put through the jinja renderer

        Args:
            renderer: jinja sql renderer
            params: template parameters

        Returns:
            column with the `type` field rendered
        """
        return ColumnRendered(
            name=self.name,
            type=renderer.render(self.type, params),
        )


@dataclass
class ColumnRendered:
    """
    Column definition in `ADD COLUMN` or `CREATE TABLE` statement

    Attributes:
        name: column name
        type: Everything that comes after column name:
            data type, `REFERENCES`, `UNIQUE`, `DEFAULT`, etc.

            Rendered using psycopg
    """

    name: str
    type: Composed

    def __sql__(self) -> Composed:
        """
        sql representation

        Returns:
            name + type
        """
        return SQL("{} {}").format(Identifier(self.name), self.type)

    @property
    def ident(self) -> Identifier:
        """
        Column name as psycopg identifier

        Returns:
            wrapped column name
        """
        return Identifier(self.name)

    def add(self, if_not_exists: bool) -> Composed:
        """
        Generate `ADD COLUMN statement`

        Args:
            if_not_exists: whether to use `IF NOT EXISTS`
        Returns:
            `ADD COLUMN`
        """
        return SQL("ADD COLUMN {if_not_exists}{name} {type}").format(
            if_not_exists=SQL("IF NOT EXISTS ") if if_not_exists else SQL(""),
            name=Identifier(self.name),
            type=self.type,
        )

    def drop(self, if_exists: bool) -> Composed:
        """
        Generate `DROP COLUMN statement`

        Args:
            if_exists: whether to use `IF EXISTS`
        Returns:
            `DROP COLUMN`
        """
        return SQL("DROP COLUMN {if_exists}{name}").format(
            if_exists=SQL("IF EXISTS ") if if_exists else SQL(""),
            name=Identifier(self.name),
        )
