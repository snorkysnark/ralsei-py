from __future__ import annotations
from dataclasses import dataclass, field

from .common import (
    Context,
    Table,
    TaskImpl,
    TaskDef,
    actions,
)


@dataclass
class CreateTableSql(TaskDef):
    sql: str
    table: Table
    params: dict = field(default_factory=dict)
    view: bool = False

    class Impl(TaskImpl):
        def __init__(self, this: CreateTableSql, ctx: Context) -> None:
            self.__sql = ctx.jinja.render_split(
                this.sql, table=this.table, view=this.view, **this.params
            )
            self.__drop_sql = ctx.jinja.render(
                "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
                table=this.table,
                view=this.view,
            )

            self.__table = this.table

        def exists(self, ctx: Context) -> bool:
            return actions.table_exists(ctx, self.__table)

        def run(self, ctx: Context) -> None:
            ctx.connection.executescript(self.__sql)

        def delete(self, ctx: Context) -> None:
            ctx.connection.execute(self.__drop_sql)
