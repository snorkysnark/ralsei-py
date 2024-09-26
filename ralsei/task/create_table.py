from typing import Any
from sqlalchemy.sql.elements import TextClause

from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment
from ralsei.types import Table

from .base import TaskImpl


class CreateTableTask(TaskImpl):
    """Base class for a task that performs table creation

    All you have to do is call :py:meth:`~_prepare_table` from within :py:meth:`ralsei.task.TaskImpl.prepare`.

    :py:attr:`~output`, :py:meth:`~_exists` and :py:meth:`~_delete` are implemented for you,
    leaving only the :py:meth:`ralsei.task.TaskImpl._run` part

    Example:
        .. code-block:: python

            import pandas as pd

            class UploadCsv(TaskDef):
                table: Table
                path: Path

                class Impl(CreateTableTask):
                    def prepare(self, this: "UploadCsv"):
                        self._prepare_table(this.table)
                        self.__path = this.path

                    def _run(self, conn: ConnectionEnvironment):
                        with self.__path.open() as file:
                            pd.read_csv(file).to_sql(
                                self._table.name,
                                conn.sqlalchemy,
                                schema=self._table.schema
                            )

    """

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


__all__ = ["CreateTableTask"]
