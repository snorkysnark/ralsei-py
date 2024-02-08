from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable

from .base import TaskDef, TaskImpl, ExistsStatus
from ralsei.types import Table
from ralsei.jinja import SqlEnvironment
from ralsei.connection import SqlConnection
from ralsei import db_actions


@dataclass
class CreateTableSql(TaskDef):
    """Runs a ``CREATE TABLE`` sql script

    Variables passed to the template: :py:attr:`~table`, :py:attr:`~view`, `**`:py:attr:`~params`

    Example:

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
    """Sql template strings

    Individual statements must be either separated by ``{%split%}`` tag or pre-split into a list
    """
    table: Table
    """Table being created"""
    params: dict = field(default_factory=dict)
    """parameters passed to the jinja template"""
    view: bool = False
    """whether this is a ``VIEW`` instead of a ``TABLE``"""

    class Impl(TaskImpl):
        def __init__(self, this: CreateTableSql, env: SqlEnvironment) -> None:
            template_params = {"table": this.table, "view": this.view, **this.params}
            self._sql = (
                env.render_sql_split(this.sql, **template_params)
                if isinstance(this.sql, str)
                else [env.render_sql(sql, **template_params) for sql in this.sql]
            )

            self._drop_sql = env.render_sql(
                "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
                table=this.table,
                view=this.view,
            )

            self._table = this.table

        @property
        def output(self) -> Any:
            return self._table

        def exists(self, conn: SqlConnection) -> ExistsStatus:
            return ExistsStatus(db_actions.table_exists(conn, self._table))

        def run(self, conn: SqlConnection) -> None:
            conn.sqlalchemy.executescript(self._sql)

        def delete(self, conn: SqlConnection) -> None:
            conn.sqlalchemy.execute(self._drop_sql)

        def sql_scripts(self) -> Iterable[tuple[str, object | list[object]]]:
            yield "Main", self._sql
            yield "Drop table", self._drop_sql


__all__ = ["CreateTableSql"]
