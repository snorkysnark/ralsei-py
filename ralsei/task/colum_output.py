from collections.abc import Sequence
from typing import Any
from sqlalchemy import TextClause

from ralsei.connection import ConnectionEnvironment
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, ColumnRendered
from ralsei import db_actions

from .base import TaskOutput


class ColumnOutput(TaskOutput):
    def __init__(
        self,
        env: SqlEnvironment,
        table: Table,
        columns: Sequence[ColumnRendered],
        *,
        if_not_exists: bool = False,
    ) -> None:
        self.table = table
        self.columns = columns

        self.add_columns = db_actions.AddColumns(
            env, self.table, self.columns, if_not_exists=if_not_exists
        )
        self._drop_columns = db_actions.DropColumns(
            env, self.table, self.columns, if_exists=True
        )

    def exists(self, conn: ConnectionEnvironment) -> bool:
        return db_actions.columns_exist(
            conn.sqlalchemy, self.table, (col.name for col in self.columns)
        )

    def delete(self, conn: ConnectionEnvironment):
        self._drop_columns(conn)
        conn.sqlalchemy.commit()

    def as_import(self) -> Any:
        return self.table


class ColumnOutputResumable(ColumnOutput):
    def __init__(
        self,
        env: SqlEnvironment,
        table: Table,
        columns: Sequence[ColumnRendered],
        *,
        select: TextClause,
    ) -> None:
        super().__init__(env, table, columns, if_not_exists=True)
        self._select = select

    def exists(self, conn: ConnectionEnvironment) -> bool:
        if not super().exists(conn):
            return False
        else:
            return conn.execute(self._select).first() is None
