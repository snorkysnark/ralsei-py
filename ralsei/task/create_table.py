import sqlalchemy
from ralsei import db_actions
from ralsei.injector import DIContext
from ralsei.types import Table
from ralsei.jinja import SqlEnvironment

from .base import Task, Impl


class ImplCreateTable[T: Task](Impl[T]):
    def __init__(self, env: SqlEnvironment, table: Table, view: bool = False) -> None:
        self.table = table
        self._drop_sql = env.render_sql(
            "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
            table=table,
            view=view,
        )

    def delete(self, decl: T, context: DIContext):
        conn = context.get(sqlalchemy.Connection)
        conn.execute(self._drop_sql)
        conn.commit()

    def skip(self, decl: T, context: DIContext) -> bool:
        return db_actions.table_exists(context.get(sqlalchemy.Connection), self.table)
