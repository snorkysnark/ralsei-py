from typing import Sequence

import sqlalchemy
from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment
from ralsei.injector import inject, service
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, ColumnRendered

from .base import Settled, Task, ImplTask


class ImplAddColumns[T: Task](ImplTask[T]):
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

    @inject
    def delete(self, conn: ConnectionEnvironment = service()):
        self._drop_columns(conn)
        conn.commit()

    @inject
    def skip(self, conn: sqlalchemy.Connection = service()) -> bool:
        return db_actions.columns_exist(
            conn, self._table, (col.name for col in self._columns)
        )
