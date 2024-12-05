from typing import Any
import sqlalchemy

from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, ColumnRendered
from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment

from .base import TaskOutput


class TableOutput(TaskOutput):
    def __init__(self, env: SqlEnvironment, table: Table, view: bool = False) -> None:
        self._table = table
        self._drop_sql = env.render_sql(
            "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
            table=table,
            view=view,
        )

    def create_marker(self, conn: ConnectionEnvironment):
        pass

    def exists(self, conn: ConnectionEnvironment) -> bool:
        return db_actions.table_exists(conn.sqlalchemy, self._table)

    def delete(self, conn: ConnectionEnvironment):
        conn.execute(self._drop_sql)
        conn.sqlalchemy.commit()

    def as_import(self) -> Any:
        return self._table


class TableOutputResumable(TableOutput):
    def __init__(
        self,
        env: SqlEnvironment,
        table: Table,
        *,
        select: sqlalchemy.TextClause,
        source_table: Table,
        marker_column: ColumnRendered
    ) -> None:
        super().__init__(env, table)

        self._select = select
        self._add_marker = db_actions.AddColumns(
            env, source_table, [marker_column], if_not_exists=True
        )
        self._drop_marker = db_actions.DropColumns(
            env, source_table, [marker_column], if_exists=True
        )

    def create_marker(self, conn: ConnectionEnvironment):
        self._add_marker(conn)

    def exists(self, conn: ConnectionEnvironment) -> bool:
        if not super().exists(conn):
            return False
        else:
            # Check that task has no more inputs
            return conn.execute(self._select).first() is None

    def delete(self, conn: ConnectionEnvironment):
        self._drop_marker(conn)
        super().delete(conn)
