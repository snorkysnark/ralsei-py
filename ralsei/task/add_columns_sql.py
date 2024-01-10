from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence, cast
from sqlalchemy import TextClause

from .common import (
    TaskDef,
    TaskImpl,
    SqlLike,
    TableSource,
    ColumnBase,
    SqlalchemyEnvironment,
    ConnectionContext,
    actions,
    expect_optional,
)


@dataclass
class AddColumnsSql(TaskDef):
    sql: str | list[str]
    table: TableSource
    columns: Optional[Sequence[ColumnBase]] = None
    params: dict = field(default_factory=dict)

    class Impl(TaskImpl):
        def __init__(self, this: AddColumnsSql, env: SqlalchemyEnvironment) -> None:
            self._table = env.resolve(this.table)

            def render_script() -> (
                tuple[list[TextClause], Optional[Sequence[ColumnBase]]]
            ):
                if isinstance(this.sql, str):
                    template_module = env.from_string(this.sql).make_module(
                        {"table": self._table, **this.params}
                    )
                    columns = cast(
                        Optional[Sequence[ColumnBase]],
                        getattr(template_module, "columns", None),
                    )

                    return template_module.render_split(), columns
                else:
                    return [
                        env.render(sql, table=self._table, **this.params)
                        for sql in this.sql
                    ], None

            self._sql, template_columns = render_script()
            columns = expect_optional(
                this.columns or template_columns, ValueError("Columns not specified")
            )

            rendered_columns = [
                col.render(env.text, table=self._table, **this.params)
                for col in columns
            ]
            self._column_names = [col.name for col in rendered_columns]

            self._add_columns = actions.add_columns(env, self._table, rendered_columns)
            self._drop_columns = actions.drop_columns(
                env, self._table, rendered_columns
            )

        @property
        def output(self) -> Any:
            return self._table

        def exists(self, ctx: ConnectionContext) -> bool:
            return actions.columns_exist(ctx, self._table, self._column_names)

        def run(self, ctx: ConnectionContext) -> None:
            self._add_columns(ctx)
            ctx.connection.executescript(self._sql)

        def delete(self, ctx: ConnectionContext) -> None:
            self._drop_columns(ctx)

        def sql_scripts(self) -> Iterable[tuple[str, SqlLike]]:
            yield "Add columns", self._add_columns.statements
            yield "Main", self._sql
            yield "Drop columns", self._drop_columns.statements
