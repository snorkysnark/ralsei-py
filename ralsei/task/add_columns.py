from typing import Sequence

import sqlalchemy
from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment
from ralsei.injector import DIContext
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, ColumnRendered

from .base import Settled, Task, ImplTask


class AddColumnsBase[T: Task](ImplTask[T]):
    def __init__(
        self,
        task: Settled[T],
        env: SqlEnvironment,
        table: Table,
        columns: Sequence[ColumnRendered],
        *,
        if_not_exists: bool = False,
    ) -> None:
        super().__init__(task)

        self._table = table
        self._columns = columns

        self._add_columns = db_actions.AddColumns(
            env, table, columns, if_not_exists=if_not_exists
        )
        self._drop_columns = db_actions.DropColumns(env, table, columns, if_exists=True)

    def delete(self, context: DIContext):
        conn = context.get(ConnectionEnvironment)
        self._drop_columns(conn)
        conn.commit()

    def skip(self, context: DIContext) -> bool:
        conn = context.get(sqlalchemy.Connection)
        return db_actions.columns_exist(
            conn, self._table, (col.name for col in self._columns)
        )
