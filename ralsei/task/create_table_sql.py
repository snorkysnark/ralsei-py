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
    sql: str | list[str]
    table: Table
    params: dict = field(default_factory=dict)
    view: bool = False

    class Impl(TaskImpl):
        def __init__(self, this: CreateTableSql, ctx: Context) -> None:
            template_params = {"table": this.table, "view": this.view, **this.params}
            self._sql = (
                ctx.jinja.render_split(this.sql, **template_params)
                if isinstance(this.sql, str)
                else [ctx.jinja.render(sql, **template_params) for sql in this.sql]
            )

            self._drop_sql = ctx.jinja.render(
                "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
                table=this.table,
                view=this.view,
            )

            self._table = this.table

        def exists(self, ctx: Context) -> bool:
            return actions.table_exists(ctx, self._table)

        def run(self, ctx: Context) -> None:
            ctx.connection.executescript(self._sql)

        def delete(self, ctx: Context) -> None:
            ctx.connection.execute(self._drop_sql)
