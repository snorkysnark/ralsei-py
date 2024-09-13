from typing import Any
from sqlalchemy.sql.elements import TextClause

from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment
from ralsei.types import Table

from .base import TaskImpl


class CreateTableTask(TaskImpl):
    _table: Table
    _drop_sql: TextClause

    def _prepare_table(self, table: Table, view: bool = False):
        self._table = table
        self._drop_sql = self.env.render_sql(
            "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
            table=table,
            view=view,
        )

    @property
    def output(self) -> Any:
        return self._table

    def _exists(self, conn: ConnectionEnvironment) -> bool:
        return db_actions.table_exists(conn, self._table)

    def _delete(self, conn: ConnectionEnvironment):
        conn.sqlalchemy.execute(self._drop_sql)
