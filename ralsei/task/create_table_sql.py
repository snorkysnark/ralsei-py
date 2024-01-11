from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable

from .base import TaskDef, TaskImpl, SqlLike
from ralsei.types import Table
from ralsei.jinja import SqlalchemyEnvironment
from ralsei.context import ConnectionContext
from ralsei import db_actions


@dataclass
class CreateTableSql(TaskDef):
    """Runs a ``CREATE TABLE`` sql script

    .. admonition:: Example

        **unnest.sql**

        .. code-block:: sql

            CREATE TABLE {{table}}(
                id SERIAL PRIMARY KEY,
                name TEXT
            );
            {%-split-%}
            INSERT INTO {{table}}(name)
            SELECT json_array_elements_text(json->'names')
            FROM {{sources}};

        **pipeline.py**

        .. code-block:: python

            "unnest": CreateTableSql(
                sql=Path("./unnest.sql").read_text(),
                table=table_names,
                params={"table": table_sources},
            )
    """

    sql: str | list[str]
    """sql template string"""
    table: Table
    """Table being created"""
    params: dict = field(default_factory=dict)
    """parameters passed to the jinja template"""
    view: bool = False
    """whether this is a ``VIEW`` instead of a ``TABLE``"""

    class Impl(TaskImpl):
        def __init__(self, this: CreateTableSql, env: SqlalchemyEnvironment) -> None:
            template_params = {"table": this.table, "view": this.view, **this.params}
            self._sql = (
                env.render_split(this.sql, **template_params)
                if isinstance(this.sql, str)
                else [env.render(sql, **template_params) for sql in this.sql]
            )

            self._drop_sql = env.render(
                "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
                table=this.table,
                view=this.view,
            )

            self._table = this.table

        @property
        def output(self) -> Any:
            return self._table

        def exists(self, ctx: ConnectionContext) -> bool:
            return db_actions.table_exists(ctx, self._table)

        def run(self, ctx: ConnectionContext) -> None:
            ctx.connection.executescript(self._sql)

        def delete(self, ctx: ConnectionContext) -> None:
            ctx.connection.execute(self._drop_sql)

        def sql_scripts(self) -> Iterable[tuple[str, SqlLike]]:
            yield "Main", self._sql
            yield "Drop table", self._drop_sql


__all__ = ["CreateTableSql"]
