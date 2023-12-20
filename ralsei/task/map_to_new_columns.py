from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from returns.maybe import Maybe

from .common import (
    TaskDef,
    TaskImpl,
    Context,
    OneToOne,
    Renderable,
    ValueColumnRendered,
    IdColumn,
    Table,
    ValueColumnRendered,
    Identifier,
    actions,
    expect_optional,
)


@dataclass
class MapToNewColumns(TaskDef):
    select: str
    table: Table
    columns: list[Renderable[ValueColumnRendered]]
    fn: OneToOne
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict = {}

    class Impl(TaskImpl):
        def __init__(self, this: MapToNewColumns, ctx: Context) -> None:
            self.__table = this.table
            self.__fn = this.fn

            columns_rendered = [
                column.render(ctx.jinja.inner, table=this.table, **this.params)
                for column in this.columns
            ]
            self.__column_names = [column.name for column in columns_rendered]

            if this.is_done_column:
                columns_rendered.append(
                    ValueColumnRendered(this.is_done_column, "BOOL DEFAULT FALSE", True)
                )
            self.__commit_each = bool(this.is_done_column)

            id_fields = expect_optional(
                this.id_fields
                or (
                    Maybe.from_optional(getattr(this.fn, "id_fields", None))
                    .map(lambda names: [IdColumn(name) for name in names])
                    .value_or(None)
                ),
                "Must provide id_fields if using is_done_column",
            )
            self.__select = ctx.jinja.render(
                this.select,
                table=this.table,
                is_done=(
                    Maybe.from_optional(this.is_done_column)
                    .map(Identifier)
                    .value_or(None)
                ),
                **this.params,
            )
            self.__add_columns = actions.add_columns(
                ctx.jinja,
                this.table,
                columns_rendered,
                if_not_exists=self.__commit_each,
            )
            self.__update = ctx.jinja.render(
                """\
                UPDATE {{table}} SET
                {{columns | sqljoin(',\\n', attribute='set_statement')}}
                WHERE
                {{id_fields | sqljoin(' AND ')}};""",
                table=this.table,
                columns=columns_rendered,
                id_fields=id_fields,
            )
            self.__drop_columns = actions.drop_columns(
                ctx.jinja,
                this.table,
                columns_rendered,
            )

        def exists(self, ctx: Context) -> bool:
            return actions.columns_exist(ctx, self.__table, self.__column_names)

        def run(self, ctx: Context) -> None:
            self.__add_columns(ctx)

            for input_row in map(dict, ctx.connection.execute(self.__select)):
                ctx.connection.execute(self.__update, self.__fn(**input_row))

                if self.__commit_each:
                    ctx.connection.commit()

        def delete(self, ctx: Context) -> None:
            self.__drop_columns(ctx)
