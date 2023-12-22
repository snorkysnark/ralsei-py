from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Sequence, cast
from sqlalchemy import TextClause

from .common import (
    TaskDef,
    TaskImpl,
    Table,
    ColumnBase,
    Context,
    actions,
    expect_optional,
)


@dataclass
class AddColumnsSql(TaskDef):
    sql: str | list[str]
    table: Table
    columns: Optional[Sequence[ColumnBase]] = None
    params: dict = field(default_factory=dict)

    class Impl(TaskImpl):
        def __init__(self, this: AddColumnsSql, ctx: Context) -> None:
            def render_script() -> (
                tuple[list[TextClause], Optional[Sequence[ColumnBase]]]
            ):
                if isinstance(this.sql, str):
                    template_module = ctx.jinja.from_string(this.sql).make_module(
                        {"table": this.table, **this.params}
                    )
                    columns = cast(
                        Optional[Sequence[ColumnBase]],
                        getattr(template_module, "columns", None),
                    )

                    return template_module.render_split(), columns
                else:
                    return [
                        ctx.jinja.render(sql, table=this.table, **this.params)
                        for sql in this.sql
                    ], None

            self._sql, template_columns = render_script()
            columns = expect_optional(
                this.columns or template_columns, "Columns not specified"
            )

            rendered_columns = [
                col.render(ctx.jinja.text, table=this.table, **this.params)
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
