from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence
from returns.maybe import Maybe
from sqlalchemy import TextClause

from .common import (
    TaskDef,
    TaskImpl,
    SqlLike,
    SqlalchemyEnvironment,
    ConnectionContext,
    OneToMany,
    Table,
    TableSource,
    IdColumn,
    ValueColumnBase,
    ColumnRendered,
    ValueColumnRendered,
    Identifier,
    Sql,
    ToSql,
    actions,
    expect_optional,
    track,
)


@dataclass
class MarkerScripts:
    add_marker: actions.add_columns
    set_marker: TextClause
    drop_marker: actions.drop_columns


@dataclass
class MapToNewTable(TaskDef):
    table: Table
    columns: Sequence[str | ValueColumnBase]
    fn: OneToMany
    select: Optional[str] = None
    source_table: Optional[TableSource] = None
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict = field(default_factory=dict)
    yield_per: Optional[int] = None

    class Impl(TaskImpl):
        def __init__(self, this: MapToNewTable, env: SqlalchemyEnvironment) -> None:
            self._table = this.table
            self._fn = this.fn
            self._yield_per = this.yield_per

            source_table = env.resolve(this.source_table)

            template_params = {
                "table": this.table,
                "source": source_table,
                "is_done": (
                    Maybe.from_optional(this.is_done_column)
                    .map(Identifier)
                    .value_or(None)
                ),
                **this.params,
            }

            self._select = (
                Maybe.from_optional(this.select)
                .map(lambda sql: env.render(sql, **template_params))
                .value_or(None)
            )

            definitions: list[ToSql] = []
            insert_columns: list[ValueColumnRendered] = []
            for column in this.columns:
                if isinstance(column, str):
                    rendered = Sql(env.text.render(column, **template_params))
                    definitions.append(rendered)
                else:
                    rendered = column.render(env.text, **template_params)
                    insert_columns.append(rendered)
                    definitions.append(rendered.definition)

            self._create_table = env.render(
                """\
                CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                    {{ definition | join(',\\n    ') }}
                );""",
                table=this.table,
                definition=definitions,
                if_not_exists=this.is_done_column is not None,
            )
            self._insert = env.render(
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
            self._drop_table = env.render(
                "DROP TABLE IF EXISTS {{table}};", table=this.table
            )

            def create_marker_scripts():
                if this.is_done_column:
                    if not source_table:
                        raise ValueError(
                            "Cannot create is_done_column when source_table is None"
                        )

                    id_fields = expect_optional(
                        this.id_fields
                        or (
                            Maybe.from_optional(getattr(this.fn, "id_fields", None))
                            .map(lambda names: [IdColumn(name) for name in names])
                            .value_or(None)
                        ),
                        ValueError("Must provide id_fields if using is_done_column"),
                    )
                    is_done_column = ColumnRendered(
                        this.is_done_column, "BOOL DEFAULT FALSE"
                    )

                    return MarkerScripts(
                        actions.add_columns(
                            env,
                            source_table,
                            [is_done_column],
                            if_not_exists=True,
                        ),
                        env.render(
                            """\
                            UPDATE {{source}}
                            SET {{is_done}} = TRUE
                            WHERE {{id_fields | join(' AND ')}};""",
                            source=source_table,
                            is_done=is_done_column.identifier,
                            id_fields=id_fields,
                        ),
                        actions.drop_columns(
                            env,
                            source_table,
                            [is_done_column],
                            if_exists=True,
                        ),
                    )

            self._marker_scripts = create_marker_scripts()

        @property
        def output(self) -> Any:
            return self._table

        def exists(self, ctx: ConnectionContext) -> bool:
            return actions.table_exists(ctx, self._table)

        def run(self, ctx: ConnectionContext) -> None:
            ctx.connection.execute(self._create_table)
            if self._marker_scripts:
                self._marker_scripts.add_marker(ctx)

            def iter_input_rows(select: TextClause):
                with ctx.connection.execute_with_length_hint(
                    select, yield_per=self._yield_per
                ) as result:
                    for input_row in map(
                        lambda row: row._asdict(),
                        track(result, description="Task progress..."),
                    ):
                        yield input_row

                        if self._marker_scripts:
                            ctx.connection.execute(
                                self._marker_scripts.set_marker, input_row
                            )
                            ctx.connection.commit()

            for input_row in (
                Maybe.from_optional(self._select).map(iter_input_rows).value_or([{}])
            ):
                for output_row in self._fn(**input_row):
                    ctx.connection.execute(self._insert, output_row)

        def delete(self, ctx: ConnectionContext) -> None:
            if self._marker_scripts:
                self._marker_scripts.drop_marker(ctx)
            ctx.connection.execute(self._drop_table)

        def sql_scripts(self) -> Iterable[tuple[str, SqlLike]]:
            if self._marker_scripts:
                yield "Add marker", self._marker_scripts.add_marker.statements
            yield "Create table", self._create_table
            yield "Insert", self._insert
            if self._marker_scripts:
                yield "Set marker", self._marker_scripts.set_marker
            yield "Drop table", self._drop_table
            if self._marker_scripts:
                yield "Drop marker", self._marker_scripts.drop_marker.statements


__all__ = ["MapToNewTable"]
