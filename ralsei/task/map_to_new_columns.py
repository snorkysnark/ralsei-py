from typing import Any, Optional, Sequence

from ralsei.console import track
from ralsei.graph import Resolves
from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    ValueColumnRendered,
    Identifier,
)
from ralsei.wrappers import OneToOne, get_popped_fields
from ralsei.connection import ConnectionEnvironment
from ralsei import db_actions

from .base import TaskDef
from .context import RowContext
from .add_columns import AddColumnsTask


class MapToNewColumns(TaskDef):
    select: str
    table: Resolves[Table]
    columns: Sequence[ValueColumnBase]
    fn: OneToOne
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    yield_per: Optional[int] = None

    class Impl(AddColumnsTask):
        def prepare(self, this: "MapToNewColumns"):
            table = self.resolve(this.table)

            popped_fields = get_popped_fields(this.fn)
            self.__fn = this.fn
            self.__popped_fields: set[str] = (
                set(popped_fields) if popped_fields else set()
            )

            columns_raw = [*this.columns]
            if this.is_done_column:
                columns_raw.append(
                    ValueColumnRendered(this.is_done_column, "BOOL DEFAULT FALSE", True)
                )
            self._prepare_columns(
                table, columns_raw, if_not_exists=bool(this.is_done_column)
            )
            self.__commit_each = bool(this.is_done_column)

            locals: dict[str, Any] = {"table": table}
            if this.is_done_column:
                locals["is_done"] = Identifier(this.is_done_column)

            self.__select = self.env.render_sql(this.select, **locals)

            id_fields = this.id_fields or (
                [IdColumn(name) for name in popped_fields] if popped_fields else None
            )
            self.__update = self.env.render_sql(
                """\
                UPDATE {{table}} SET
                {{columns | join(',\\n', attribute='set_statement')}}
                WHERE
                {{id_fields | join(' AND ')}};""",
                table=self._table,
                columns=self._columns,
                id_fields=id_fields,
            )

            self._scripts["Add columns"] = self._add_columns
            self._scripts["Select"] = self.__select
            self._scripts["Update"] = self.__update
            self._scripts["Drop columns"] = self._drop_columns

        def _run(self, conn: ConnectionEnvironment):
            self._add_columns(conn)

            for input_row in map(
                lambda row: row._asdict(),
                track(
                    conn.execute_with_length_hint(self.__select),
                    description="Task progress...",
                ),
            ):
                with RowContext.from_input_row(input_row, self.__popped_fields):
                    conn.sqlalchemy.execute(self.__update, self.__fn(**input_row))

                    if self.__commit_each:
                        conn.sqlalchemy.commit()

        def _exists(self, conn: ConnectionEnvironment) -> bool:
            if not db_actions.columns_exist(
                conn, self._table, (col.name for col in self._columns)
            ):
                return False
            else:
                # non-resumable or resumable with no more inputs
                return (
                    not self.__commit_each
                    or conn.sqlalchemy.execute(self.__select).first() is None
                )


__all__ = ["MapToNewColumns"]
