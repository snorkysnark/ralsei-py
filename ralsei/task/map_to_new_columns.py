from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence
from returns.maybe import Maybe

from ralsei.console import track

from .base import TaskImpl, TaskDef, SqlLike
from ralsei.graph import OutputOf
from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    ValueColumnRendered,
    Identifier,
)
from ralsei.wrappers import OneToOne
from ralsei.jinja import SqlalchemyEnvironment
from ralsei.utils import expect_optional
from ralsei import db_actions
from ralsei.context import ConnectionContext


@dataclass
class MapToNewColumns(TaskDef):
    select: str
    table: Table | OutputOf
    columns: Sequence[ValueColumnBase]
    fn: OneToOne
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict = field(default_factory=dict)
    yield_per: Optional[int] = None

    class Impl(TaskImpl):
        def __init__(self, this: MapToNewColumns, env: SqlalchemyEnvironment) -> None:
            self._table = env.resolve(this.table)
            self._fn = this.fn
            self._yield_per = this.yield_per

            columns_rendered = [
                column.render(env.text, table=self._table, **this.params)
                for column in this.columns
            ]
            self._column_names = [column.name for column in columns_rendered]

            if this.is_done_column:
                columns_rendered.append(
                    ValueColumnRendered(this.is_done_column, "BOOL DEFAULT FALSE", True)
                )
            self._commit_each = bool(this.is_done_column)

            id_fields = expect_optional(
                this.id_fields
                or (
                    Maybe.from_optional(getattr(this.fn, "id_fields", None))
                    .map(lambda names: [IdColumn(name) for name in names])
                    .value_or(None)
                ),
                ValueError("Must provide id_fields if using is_done_column"),
            )
            self._select = env.render(
                this.select,
                table=self._table,
                is_done=(
                    Maybe.from_optional(this.is_done_column)
                    .map(Identifier)
                    .value_or(None)
                ),
                **this.params,
            )
            self._add_columns = db_actions.AddColumns(
                env,
                self._table,
                columns_rendered,
                if_not_exists=self._commit_each,
            )
            self._update = env.render(
                """\
                UPDATE {{table}} SET
                {{columns | join(',\\n', attribute='set_statement')}}
                WHERE
                {{id_fields | join(' AND ')}};""",
                table=self._table,
                columns=columns_rendered,
                id_fields=id_fields,
            )
            self._drop_columns = db_actions.DropColumns(
                env,
                self._table,
                columns_rendered,
            )

        @property
        def output(self) -> Any:
            return self._table

        def exists(self, ctx: ConnectionContext) -> bool:
            return db_actions.columns_exist(ctx, self._table, self._column_names)

        def run(self, ctx: ConnectionContext) -> None:
            self._add_columns(ctx)

            with ctx.connection.execute_with_length_hint(
                self._select, yield_per=self._yield_per
            ) as result:
                for input_row in map(
                    lambda row: row._asdict(),
                    track(result, description="Task progress..."),
                ):
                    ctx.connection.execute(self._update, self._fn(**input_row))

                    if self._commit_each:
                        ctx.connection.commit()

        def delete(self, ctx: ConnectionContext) -> None:
            self._drop_columns(ctx)

        def sql_scripts(self) -> Iterable[tuple[str, SqlLike]]:
            yield "Add columns", self._add_columns.statements
            yield "Select", self._select
            yield "Update", self._update
            yield "Drop columns", self._drop_columns.statements


__all__ = ["MapToNewColumns"]
