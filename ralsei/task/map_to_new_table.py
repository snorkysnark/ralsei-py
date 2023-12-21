from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Optional
from returns.maybe import Maybe
from sqlalchemy import TextClause

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
    Sql,
    ToSql,
    actions,
    expect_optional,
)


@dataclass
class SourceIdFields:
    table: Table
    id_fields: list[IdColumn]
    is_done_column: ColumnRendered


@dataclass
class SourceIdScripts:
    add_marker: Callable[[Context], None]
    set_marker: TextClause
    drop_marker: Callable[[Context], None]


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
            self._table = this.table
            self._fn = this.fn

            source_id_fields = (
                SourceIdFields(
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

            self._select = (
                Maybe.from_optional(this.select)
                .map(lambda sql: ctx.jinja.render(sql, **template_params))
                .value_or(None)
            )

            definitions: list[ToSql] = []
            insert_columns: list[ValueColumnRendered] = []
            for column in this.columns:
                if isinstance(column, str):
                    rendered = Sql(ctx.jinja.inner.render(column, **template_params))
                    definitions.append(rendered)
                else:
                    rendered = column.render(ctx.jinja.inner, **template_params)
                    insert_columns.append(rendered)
                    definitions.append(rendered.definition)

            self._create_table = ctx.jinja.render(
                """\
                CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                    {{ definition | join(',\\n    ') }}
                );""",
                table=this.table,
                definition=definitions,
                if_not_exists=this.is_done_column is not None,
            )
            self._insert = ctx.jinja.render(
                """\
                INSERT INTO {{ table }}(
                    {{ columns | join(',\\n    ', attribute='identifier') }}
                )
                VALUES (
                    {{ columns | join(',\\n    ', attribute='value') }}
                );""",
                table=this.table,
                columns=insert_columns,
            )
            self._drop_table = ctx.jinja.render(
                "DROP TABLE IF EXISTS {{table}};", table=this.table
            )

            self._source_id_scripts = (
                SourceIdScripts(
                    actions.add_columns(
                        ctx.jinja,
                        source_id_fields.table,
                        [source_id_fields.is_done_column],
                        if_not_exists=True,
                    ),
                    ctx.jinja.render(
                        """\
                    UPDATE {{source}}
                    SET {{is_done}} = TRUE
                    WHERE {{id_fields | join(' AND ')}};""",
                        source=source_id_fields.table,
                        is_done=source_id_fields.is_done_column.identifier,
                        id_fields=source_id_fields.id_fields,
                    ),
                    actions.drop_columns(
                        ctx.jinja,
                        source_id_fields.table,
                        [source_id_fields.is_done_column],
                        if_exists=True,
                    ),
                )
                if source_id_fields
                else None
            )

        def exists(self, ctx: Context) -> bool:
            return actions.table_exists(ctx, self._table)

        def run(self, ctx: Context) -> None:
            ctx.connection.execute(self._create_table)
            if self._source_id_scripts:
                self._source_id_scripts.add_marker(ctx)

            def iter_input_rows():
                if self._select is not None:
                    for input_row in map(
                        lambda row: row._asdict(), ctx.connection.execute(self._select)
                    ):
                        yield input_row

                        if self._source_id_scripts:
                            ctx.connection.execute(
                                self._source_id_scripts.set_marker, input_row
                            )
                            ctx.connection.commit()
                else:
                    yield {}

            for input_row in iter_input_rows():
                for output_row in self._fn(**input_row):
                    ctx.connection.execute(self._insert, output_row)

        def delete(self, ctx: Context) -> None:
            if self._source_id_scripts:
                self._source_id_scripts.drop_marker(ctx)
            ctx.connection.execute(self._drop_table)


__all__ = ["MapToNewTable"]
