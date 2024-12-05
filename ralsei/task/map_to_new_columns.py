from dataclasses import field
from typing import Any, Sequence, Optional

from ralsei.graph import Resolves
from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    Identifier,
    ValueColumnRendered,
)
from ralsei.wrappers import OneToOne, get_popped_fields
from ralsei.jinja import SqlEnvironment
from ralsei.connection import ConnectionEnvironment
from ralsei.console import track

from .base import TaskDef, Task
from .colum_output import ColumnOutput, ColumnOutputResumable
from .rowcontext import RowContext


class MapToNewColumns(TaskDef):
    select: str
    table: Resolves[Table]
    columns: Sequence[ValueColumnBase]
    fn: OneToOne
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict[str, Any] = field(default_factory=dict)

    class Impl(Task[ColumnOutput]):
        def __init__(self, this: "MapToNewColumns", env: SqlEnvironment) -> None:
            table = env.resolve(this.table)

            popped_fields = get_popped_fields(this.fn)
            self.__fn = this.fn
            self.__popped_fields: set[str] = (
                set(popped_fields) if popped_fields else set()
            )

            params = {**this.params, "table": table}
            if this.is_done_column:
                params["is_done"] = Identifier(this.is_done_column)

            columns = [column.render(env, **params) for column in this.columns]
            if this.is_done_column:
                columns.append(
                    ValueColumnRendered(this.is_done_column, "BOOL DEFAULT FALSE", True)
                )

            self.__resumable = bool(this.is_done_column)
            self.__select = env.render_sql(this.select, **params)
            self.output = (
                ColumnOutputResumable(env, table, columns, select=self.__select)
                if self.__resumable
                else ColumnOutput(env, table, columns)
            )

            id_fields = this.id_fields or (
                [IdColumn(name) for name in popped_fields] if popped_fields else None
            )
            if not id_fields:
                raise ValueError(
                    "id_fields not found, must be explicitly provided or inferred from function"
                )

            self.__update = env.render_sql(
                """\
                UPDATE {{table}} SET
                {{columns | join(',\\n', attribute='set_statement')}}
                WHERE
                {{id_fields | join(' AND ')}};""",
                table=table,
                columns=columns,
                id_fields=id_fields,
            )

        def run(self, conn: ConnectionEnvironment):
            self.output.add_columns(conn)

            for input_row in map(
                lambda row: row._asdict(),
                track(
                    conn.execute_with_length_hint(self.__select),
                    description="Task progress...",
                ),
            ):
                with RowContext.from_input_row(input_row, self.__popped_fields):
                    conn.execute(self.__update, self.__fn(**input_row))
                    if self.__resumable:
                        conn.sqlalchemy.commit()

            conn.sqlalchemy.commit()
