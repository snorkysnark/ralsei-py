from typing import Any, Sequence

from ralsei.connection import ConnectionEnvironment
from ralsei.graph import Resolves
from ralsei.types import Table, ColumnBase, ColumnRendered
from ralsei import db_actions

from .base import TaskImpl


class AddColumnsTask(TaskImpl):
    _table: Table
    _columns: list[ColumnRendered]

    def _prepare_columns(
        self,
        table: Resolves[Table],
        columns: Sequence[ColumnBase],
        *,
        if_not_exists: bool = False,
    ):
        self._table = self.resolve(table)

        self._columns = [col.render(self.env, table=self._table) for col in columns]
        # self._column_names = [col.name for col in rendered_columns]
        self.scripts["Add Columns"] = self._add_columns = db_actions.AddColumns(
            self.env, self._table, self._columns, if_not_exists=if_not_exists
        )
        self.scripts["Drop Columns"] = self._drop_columns = db_actions.DropColumns(
            self.env, self._table, self._columns, if_exists=True
        )

    @property
    def output(self) -> Any:
        return self._table

    def _exists(self, conn: ConnectionEnvironment) -> bool:
        return db_actions.columns_exist(
            conn, self._table, (col.name for col in self._columns)
        )

    def _delete(self, conn: ConnectionEnvironment):
        self._drop_columns(conn)
