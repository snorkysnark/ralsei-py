from __future__ import annotations
from dataclasses import dataclass

from .common import (
    Context,
    Table,
    TaskImpl,
    TaskDef,
    checks,
)


@dataclass
class CreateTableSql(TaskDef):
    sql: str
    table: Table
    params: dict = {}
    view: bool = False

    class Impl(TaskImpl):
        def __init__(self, this: CreateTableSql, ctx: Context) -> None:
            self.__sql = ctx.jinja.render_script(
                this.sql, table=this.table, view=this.view, **this.params
            )
            self.__drop_sql = ctx.jinja.render(
                "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
                table=this.table,
                view=this.view,
            )

            self.__table = this.table
            self.__is_view = this.view

        def exists(self, ctx: Context) -> bool:
            return checks.table_exists(ctx, self.__table, self.__is_view)

        def run(self, ctx: Context) -> None:
            ctx.connection.executescript(self.__sql)

        def delete(self, ctx: Context) -> None:
            ctx.connection.execute(self.__drop_sql)
