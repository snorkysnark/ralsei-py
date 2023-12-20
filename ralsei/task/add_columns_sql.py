from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional

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
    sql: str
    table: Table
    columns: Optional[list[Renderable[ColumnRendered]]] = None
    params: dict = field(default_factory=dict)

    class Impl(TaskImpl):
        def __init__(self, this: AddColumnsSql, ctx: Context) -> None:
            # jinja_args = merge_params({"table": this.table}, this.params)

            template_module = ctx.jinja.from_string(this.sql).make_module(
                {"table": this.table, **this.params}
            )

            columns: Optional[
                list[Renderable[ColumnRendered]]
            ] = this.columns or getattr(template_module, "columns", None)
            if columns is None:
                raise ValueError("Columns not specified")

            rendered_columns = [
                col.render(ctx.jinja.inner, table=this.table, **this.params)
                for col in columns
            ]
            self.__column_names = [col.name for col in rendered_columns]

            self.__add_columns = actions.add_columns(
                ctx.jinja, this.table, rendered_columns
            )
            self.__sql = template_module.render_split()
            self.__drop_columns = actions.drop_columns(
                ctx.jinja, this.table, rendered_columns
            )

            self.__table = this.table

        def exists(self, ctx: Context) -> bool:
            return actions.columns_exist(ctx, self.__table, self.__column_names)

        def run(self, ctx: Context) -> None:
            self.__add_columns(ctx)
            ctx.connection.executescript(self.__sql)

        def delete(self, ctx: Context) -> None:
            self.__drop_columns(ctx)
