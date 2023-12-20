from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
from returns.maybe import Maybe

from .common import (
    TaskDef,
    TaskImpl,
    Context,
    OneToMany,
    Table,
    Renderable,
    ColumnRendered,
    IdColumn,
    ValueColumnRendered,
    Identifier,
    actions,
    expect_optional,
)


@dataclass
class TableIdFields:
    table: Table
    id_fields: list[IdColumn]
    is_done_column: ColumnRendered


@dataclass
class MapToNewTable(TaskDef):
    table: Table
    columns: list[str | Renderable[ValueColumnRendered]]
    fn: OneToMany
    select: Optional[str] = None
    source_table: Optional[Table] = None
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict = field(default_factory=dict)

    class Impl(TaskImpl):
        def __init__(self, this: MapToNewTable, ctx: Context) -> None:
            self.__table = this.table
            self.__fn = this.fn

            source_id_fields = (
                TableIdFields(
                    expect_optional(
                        this.source_table,
                        "Must provide id_fields if using is_done_column",
                    ),
                    expect_optional(
                        this.id_fields
                        or (
                            Maybe.from_optional(getattr(this.fn, "id_fields", None))
                            .map(lambda names: [IdColumn(name) for name in names])
                            .value_or(None)
                        ),
                        "Cannot create is_done_column when source_table is None",
                    ),
                    ColumnRendered(this.is_done_column, "BOOL DEFAULT FALSE"),
                )
                if this.is_done_column
                else None
            )

            template_params = {
                "table": this.table,
                "source": this.source_table,
                "is_done": (
                    Maybe.from_optional(this.is_done_column)
                    .map(Identifier)
                    .value_or(None)
                ),
                **this.params,
            }

            self.__select = (
                Maybe.from_optional(this.select)
                .map(lambda sql: ctx.jinja.render(sql, **template_params))
                .value_or(None)
            )

            definitions, insert_columns = [], []
            for column in this.columns:
                if isinstance(column, str):
                    rendered = ctx.jinja.render(column, **template_params)
                    definitions.append(rendered)
                else:
                    rendered = column.render(ctx.jinja.inner, **template_params)
                    insert_columns.append(rendered)
                    definitions.append(rendered.definition)

            self.__create_table = ctx.jinja.render(
                """\
                CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                    {{ definition | sqljoin(',\\n    ') }}
                );""",
                table=this.table,
                definition=definitions,
                if_not_exists=this.is_done_column is not None,
            )
            self.__insert = ctx.jinja.render(
                """\
                INSERT INTO {{ table }}(
                    {{ columns | sqljoin(',\\n    ', attribute='identifier') }}
                )
                VALUES (
                    {{ columns | sqljoin(',\\n    ', attribute='value') }}
                );""",
                table=this.table,
                columns=insert_columns,
            )
            self.__drop_table = ctx.jinja.render(
                "DROP TABLE IF EXISTS {{table}};", table=this.table
            )

            if source_id_fields:
                self.__add_marker = actions.add_columns(
                    ctx.jinja,
                    source_id_fields.table,
                    [source_id_fields.is_done_column],
                    if_not_exists=True,
                )
                self.__set_marker = ctx.jinja.render(
                    """\
                    UPDATE {{source}}
                    SET {{is_done}} = TRUE
                    WHERE {{id_fields | sqljoin(' AND ')}};""",
                    source=source_id_fields.table,
                    is_done=source_id_fields.is_done_column.identifier,
                    id_fields=source_id_fields.id_fields,
                )
                self.__drop_marker = actions.drop_columns(
                    ctx.jinja,
                    source_id_fields.table,
                    [source_id_fields.is_done_column],
                    if_exists=True,
                )

        def exists(self, ctx: Context) -> bool:
            return actions.table_exists(ctx, self.__table)

        def run(self, ctx: Context) -> None:
            ctx.connection.execute(self.__create_table)
            if self.__add_marker:
                self.__add_marker(ctx)

            def iter_input_rows():
                if self.__select is not None:
                    for input_row in map(dict, ctx.connection.execute(self.__select)):
                        yield input_row

                        if self.__set_marker is not None:
                            ctx.connection.execute(self.__set_marker, input_row)
                            ctx.connection.commit()
                else:
                    yield {}

            for input_row in iter_input_rows():
                for output_row in self.__fn(**input_row):
                    ctx.connection.execute(self.__insert, output_row)

        def delete(self, ctx: Context) -> None:
            if self.__drop_marker:
                self.__drop_marker(ctx)
            ctx.connection.execute(self.__drop_table)


__all__ = ["MapToNewTable"]
