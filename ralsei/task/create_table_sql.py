from __future__ import annotations
from dataclasses import dataclass

from .common import (
    TaskImpl,
    TaskDef,
    Table,
    checks,
    merge_params,
    renderer,
)


@dataclass
class CreateTableSql(TaskDef):
    sql: str
    table: Table
    params: dict = {}
    view: bool = False

    class Impl(TaskImpl):
        def __init__(self, this: CreateTableSql, ctx: RalseiContext) -> None:
            jinja_args = merge_params(
                {"table": this.table, "view": this.view}, this.params
            )

            self.scripts["Create"] = self.__sql = renderer.render(this.sql, jinja_args)
            self.scripts["Drop"] = self.__drop_sql = renderer.render(
                "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
                jinja_args,
            )

            self.__table = this.table
            self.__is_view = this.view

        def exists(self, ctx: RalseiContext) -> bool:
            return checks.table_exists(ctx, self.__table, self.__is_view)

        def run(self, ctx: RalseiContext) -> None:
            with ctx.pg.cursor() as curs:
                curs.execute(self.__sql)

        def delete(self, ctx: RalseiContext) -> None:
            with ctx.pg.cursor() as curs:
                curs.execute(self.__drop_sql)
