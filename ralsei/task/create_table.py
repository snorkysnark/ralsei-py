from typing import Any

from ralsei.db_actions import table_exists
from ralsei.types import Table
from ralsei.jinja import SqlEnvironment
from ralsei.connection import SqlConnection


class CreateTableMixin:
    def __init__(self, table: Table, env: SqlEnvironment) -> None:
        self._table = table
        self.__drop = env.render_sql("DROP TABLE IF EXISTS {{table}}", table=table)

    def delete(self, conn: SqlConnection):
        conn.sqlalchemy.execute(self.__drop)

    @property
    def output(self) -> Any:
        return self._table

    def exists(self, conn: SqlConnection) -> bool:
        return table_exists(conn, self._table)


__all__ = ["CreateTableMixin"]
