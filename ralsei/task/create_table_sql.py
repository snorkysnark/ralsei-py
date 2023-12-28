from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable

from .common import (
    SqlalchemyEnvironment,
    ConnectionContext,
    Table,
    TaskImpl,
    TaskDef,
    SqlLike,
    actions,
)


@dataclass
class CreateTableSql(TaskDef):
    sql: str | list[str]
    table: Table
    params: dict = field(default_factory=dict)
    view: bool = False

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
            return actions.table_exists(ctx, self._table)

        def run(self, ctx: ConnectionContext) -> None:
            ctx.connection.executescript(self._sql)

        def delete(self, ctx: ConnectionContext) -> None:
            ctx.connection.execute(self._drop_sql)

        def sql_scripts(self) -> Iterable[tuple[str, SqlLike]]:
            yield "Main", self._sql
            yield "Drop table", self._drop_sql
