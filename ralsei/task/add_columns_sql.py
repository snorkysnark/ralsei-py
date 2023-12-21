from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, cast

from .common import (
    TaskDef,
    TaskImpl,
    Table,
    Renderable,
    ColumnRendered,
    Context,
    actions,
)


@dataclass
class AddColumnsSql(TaskDef):
    sql: str | list[str]
    table: Table
    columns: Optional[list[Renderable[ColumnRendered]]] = None
    params: dict = field(default_factory=dict)

    class Impl(TaskImpl):
        def __init__(self, this: AddColumnsSql, ctx: Context) -> None:
            columns = this.columns

            if isinstance(this.sql, str):
                template_module = ctx.jinja.from_string(this.sql).make_module(
                    {"table": this.table, **this.params}
                )
                if not columns:
                    columns = cast(
                        Optional[list[Renderable[ColumnRendered]]],
                        getattr(template_module, "columns", None),
                    )

                self._sql = template_module.render_split()
            else:
                self._sql = [
                    ctx.jinja.render(sql, table=this.table, **this.params)
                    for sql in this.sql
                ]

            if columns is None:
                raise ValueError("Columns not specified")

            rendered_columns = [
                col.render(ctx.jinja.inner, table=this.table, **this.params)
                for col in columns
            ]
            self._column_names = [col.name for col in rendered_columns]

            self._add_columns = actions.add_columns(
                ctx.jinja, this.table, rendered_columns
            )
            self._drop_columns = actions.drop_columns(
                ctx.jinja, this.table, rendered_columns
            )

            self._table = this.table

        def exists(self, ctx: Context) -> bool:
            return actions.columns_exist(ctx, self._table, self._column_names)

        def run(self, ctx: Context) -> None:
            self._add_columns(ctx)
            ctx.connection.executescript(self._sql)

        def delete(self, ctx: Context) -> None:
            self._drop_columns(ctx)
