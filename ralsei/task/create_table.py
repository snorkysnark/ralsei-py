import sqlalchemy
from ralsei import db_actions
from ralsei.injector import DIContext
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table

from .base import ImplTask, Settled, Task


class CreateTableBase[T: Task](ImplTask[T]):
    def __init__(
        self, task: Settled[T], env: SqlEnvironment, table: Table, view: bool = False
    ) -> None:
        super().__init__(task)

        self._table = table
        self._drop_sql = env.render_sql(
            "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
            table=table,
            view=view,
        )

    def delete(self, context: DIContext):
        conn = context.get(sqlalchemy.Connection)
        conn.execute(self._drop_sql)
        conn.commit()

    def skip(self, context: DIContext) -> bool:
        return db_actions.table_exists(context.get(sqlalchemy.Connection), self._table)
